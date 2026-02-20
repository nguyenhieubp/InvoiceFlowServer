import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { POChargeHistory } from './entities/po-charge-history.entity';
import { AuditPo } from './entities/audit-po.entity';
import { FastApiClientService } from '../../services/fast-api-client.service';

function safeNumber(val: any): number {
  const num = Number(val);
  return isNaN(num) ? 0 : num;
}

@Injectable()
export class FastIntegrationService {
  private readonly logger = new Logger(FastIntegrationService.name);

  constructor(
    private readonly fastApiClient: FastApiClientService,
    @InjectRepository(POChargeHistory)
    private readonly historyRepo: Repository<POChargeHistory>,
    @InjectRepository(AuditPo)
    private readonly auditRepo: Repository<AuditPo>,
  ) {}

  /**
   * Đẩy phí đơn hàng lên Fast API (POCharges)
   * Có logic merge với lịch sử (Lần 1, Lần 2...)
   * Logic mới: Chỉ lưu vào History NẾU Fast API trả về thành công (status = 1)
   */
  async syncPOCharges(payload: {
    master: {
      dh_so: string;
      dh_ngay: string;
      dh_dvcs: string;
    };
    detail: Array<{
      dong: number;
      ma_cp: string;
      cp01_nt: number;
      cp02_nt: number;
      cp03_nt: number;
      cp04_nt: number;
      cp05_nt: number;
      cp06_nt: number;
    }>;
  }): Promise<any> {
    const { master, detail } = payload;
    this.logger.log(
      `[FastIntegration] Syncing POCharges for order ${master.dh_so} (Merge Logic)...`,
    );

    let result: any;
    let errorMessage: string | null = null;
    let status: string = 'SUCCESS';
    let mergedPayloadForAudit: any = null;

    try {
      // 1. Fetch Existing History
      const existingHistory = await this.historyRepo.find({
        where: { dh_so: master.dh_so },
      });
      this.logger.log(
        `[FastIntegration] Found ${existingHistory.length} existing records for ${master.dh_so}`,
      );
      if (existingHistory.length > 0) {
        this.logger.log(
          `[FastIntegration] Existing record [0]: ${JSON.stringify(existingHistory[0])}`,
        );
      }

      // 2. In-Memory Merge
      // 2. In-Memory Merge
      // Use string key to avoid type mismatch (number vs string)
      const historyMap = new Map<string, POChargeHistory>();
      for (const h of existingHistory) {
        historyMap.set(String(h.dong), h);
      }

      this.logger.log(
        `[FastIntegration] History Map Keys: ${Array.from(historyMap.keys()).join(', ')}`,
      );

      // Count total audits to determine the "Round" (ignoring status)
      const auditCount = await this.auditRepo.count({
        where: {
          dh_so: master.dh_so,
          action: 'SYNC_PO_CHARGES',
        },
      });
      this.logger.log(
        `[FastIntegration] Order ${master.dh_so} has ${auditCount} total syncs (ignoring status). Round: ${auditCount + 1}`,
      );

      for (const item of detail) {
        const itemKey = String(item.dong);
        let entry = historyMap.get(itemKey);

        if (!entry) {
          this.logger.log(
            `[FastIntegration] New entry created for line ${itemKey}`,
          );
          entry = this.historyRepo.create({
            dh_so: master.dh_so,
            dong: item.dong,
            ma_cp: item.ma_cp,
            cp01_nt: 0,
            cp02_nt: 0,
            cp03_nt: 0,
            cp04_nt: 0,
            cp05_nt: 0,
            cp06_nt: 0,
          });
          historyMap.set(itemKey, entry);
        } else {
          this.logger.log(
            `[FastIntegration] Found existing entry for line ${itemKey}: ${JSON.stringify(entry)}`,
          );
        }

        // Logic 1: CP01 (Base Cost) - Preserve existing if present, else take new
        if (
          safeNumber(entry.cp01_nt) === 0 &&
          item.cp01_nt !== undefined &&
          item.cp01_nt !== null
        ) {
          entry.cp01_nt = safeNumber(item.cp01_nt);
        }

        // Logic 2: Stack into next available slot (CP02 -> CP06)
        // Find the first column from cp02_nt to cp06_nt that is 0 (empty)
        const inputFee = safeNumber(item.cp02_nt);

        if (inputFee !== 0) {
          this.logger.log(
            `[DEBUG] Processing Line ${itemKey}. InputFee (CP02): ${inputFee}`,
          );
          let mapped = false;
          for (let i = 2; i <= 6; i++) {
            const key = `cp0${i}_nt` as keyof POChargeHistory;
            const rawValue = (entry as any)[key];
            const currentValue = safeNumber(rawValue);

            this.logger.log(
              `[DEBUG] Checking ${key}: Raw='${rawValue}', Safe=${currentValue}`,
            );

            if (currentValue === 0) {
              (entry as any)[key] = inputFee;
              this.logger.log(
                `[FastIntegration] Line ${item.dong}: Found empty slot ${key}, mapping fee ${inputFee}`,
              );
              mapped = true;
              break; // Stop after filling one slot
            }
          }

          if (!mapped) {
            this.logger.warn(
              `[FastIntegration] Order ${master.dh_so} Line ${item.dong}: All fee columns (cp02-cp06) are full. Dropping fee ${inputFee}.`,
            );
          }
        } else {
          this.logger.log(`[DEBUG] Line ${itemKey}: InputFee is 0, skipping.`);
        }

        entry.ma_cp = item.ma_cp;
      }

      const finalMergedList = Array.from(historyMap.values()).sort(
        (a, b) => a.dong - b.dong,
      );

      const mergedPayload = {
        master,
        detail: finalMergedList.map((h) => ({
          dong: h.dong,
          ma_cp: h.ma_cp,
          cp01_nt: safeNumber(h.cp01_nt),
          cp02_nt: safeNumber(h.cp02_nt),
          cp03_nt: safeNumber(h.cp03_nt),
          cp04_nt: safeNumber(h.cp04_nt),
          cp05_nt: safeNumber(h.cp05_nt),
          cp06_nt: safeNumber(h.cp06_nt),
        })),
      };
      mergedPayloadForAudit = mergedPayload;

      // 3. Persist History FIRST (to ensure stacking works for next retry even if sync fails)
      this.logger.log(
        `[FastIntegration] Persisting fee history to DB before syncing...`,
      );
      await this.historyRepo.save(finalMergedList);

      // 4. Submit to Fast API
      this.logger.log(
        `[FastIntegration] Submitting merged payload: ${JSON.stringify(mergedPayload.detail)}`,
      );
      result = await this.fastApiClient.submitPOCharges(mergedPayload);

      // 5. Validate Response
      let isSuccess = false;
      if (Array.isArray(result) && result.length > 0) {
        if (result[0].status === 1) isSuccess = true;
      } else if (result && result.status === 1) {
        isSuccess = true;
      }

      if (!isSuccess) {
        status = 'ERROR';
        errorMessage = Array.isArray(result)
          ? result[0]?.message
          : result?.message || 'Sync POCharges failed (unknown status)';
        // LOG WARNING but do not revert DB (as per "Stacking" requirement)
        this.logger.warn(
          `[FastIntegration] Sync failed but DB updated: ${errorMessage}`,
        );
        throw new BadRequestException(errorMessage);
      }

      this.logger.log(
        `[FastIntegration] POCharges for order ${master.dh_so} synced successfully`,
      );

      return result;
    } catch (error: any) {
      status = 'ERROR';
      errorMessage = error?.message || String(error);
      this.logger.error(
        `[FastIntegration] Failed to sync POCharges for ${master.dh_so}: ${errorMessage}`,
      );
      throw error;
    } finally {
      // Save Audit Log
      try {
        let logDate: Date | null = null;
        try {
          if (master.dh_ngay) {
            const parsedDate = new Date(master.dh_ngay);
            if (!isNaN(parsedDate.getTime())) {
              logDate = parsedDate;
            } else {
              this.logger.warn(
                `[FastIntegration] Invalid date format for dh_ngay: ${master.dh_ngay}, defaulting to null`,
              );
            }
          }
        } catch (dateError: any) {
          this.logger.warn(
            `[FastIntegration] Error parsing date: ${dateError.message}`,
          );
        }

        // Always create a new audit log
        await this.auditRepo.save({
          dh_so: master.dh_so,
          dh_ngay: logDate,
          action: 'SYNC_PO_CHARGES',
          payload: mergedPayloadForAudit || payload,
          response: result,
          status: status,
          error: errorMessage,
        });
      } catch (auditError: any) {
        this.logger.error(
          `[FastIntegration] CRITICAL: Failed to save audit log for ${master.dh_so}: ${auditError.message}`,
          auditError.stack,
        );
      }
    }
  }

  async getAuditLogs(
    search?: string,
    dateFrom?: string,
    dateTo?: string,
  ): Promise<AuditPo[]> {
    const query = this.auditRepo
      .createQueryBuilder('audit')
      .orderBy('audit.created_at', 'DESC')
      .take(50); // Limit to latest 50 for performance

    if (search) {
      query.andWhere('audit.dh_so ILIKE :search', { search: `%${search}%` });
    }

    if (dateFrom) {
      query.andWhere('audit.dh_ngay >= :dateFrom', {
        dateFrom: new Date(dateFrom),
      });
    }

    if (dateTo) {
      // End of day for dateTo (though dh_ngay is usually just a date)
      const endOfDay = new Date(dateTo);
      endOfDay.setHours(23, 59, 59, 999);
      query.andWhere('audit.dh_ngay <= :dateTo', { dateTo: endOfDay });
    }

    return query.getMany();
  }

  async retrySync(id: number): Promise<any> {
    const audit = await this.auditRepo.findOne({ where: { id } });
    if (!audit) {
      throw new BadRequestException(`Audit log #${id} not found`);
    }

    if (!audit.payload) {
      throw new BadRequestException(`Audit log #${id} has no payload to retry`);
    }

    this.logger.log(
      `[FastIntegration] Retrying sync for PO ${audit.dh_so} from audit #${id}`,
    );
    return this.syncPOCharges(audit.payload);
  }
}
