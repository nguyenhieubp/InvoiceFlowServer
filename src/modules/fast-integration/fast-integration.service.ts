import { Injectable, Logger, BadRequestException, NotFoundException } from '@nestjs/common';
import { format } from 'date-fns';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between } from 'typeorm';
import { POChargeHistory } from './entities/po-charge-history.entity';
import { AuditPo } from './entities/audit-po.entity';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { SHOPEE_FEE_CONFIG, TIKTOK_FEE_CONFIG } from './constants/fee-config.constant';
import { OrderFeeService } from '../order-fee/order-fee.service';

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
    private readonly orderFeeService: OrderFeeService,
  ) { }

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
      ngay_phi1?: string | null;
      ngay_phi2?: string | null;
      ngay_phi3?: string | null;
      ngay_phi4?: string | null;
      ngay_phi5?: string | null;
      ngay_phi6?: string | null;
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
      `[FastIntegration] Syncing POCharges for order ${master.dh_so}...`,
    );

    let result: any;
    let errorMessage: string | null = null;
    let status: string = 'SUCCESS';
    let mergedPayloadForAudit: any = null;

    try {
      // 1. Determine the round for this order (0-indexed)
      //    Counts ALL syncs (success + failed) so retries always advance to next column
      const auditCount = await this.auditRepo.count({
        where: { dh_so: master.dh_so, action: 'SYNC_PO_CHARGES' },
      });
      const slots = ['cp01_nt', 'cp02_nt', 'cp03_nt', 'cp04_nt', 'cp05_nt', 'cp06_nt'] as const;
      const targetSlot = slots[Math.min(auditCount, 5)]; // cap at cp06
      this.logger.log(`[FastIntegration] Order ${master.dh_so}: round ${auditCount + 1}, target slot: ${targetSlot}`);


      // 2. Load existing history
      const existingHistory = await this.historyRepo.find({
        where: { dh_so: master.dh_so },
      });
      const historyMap = new Map<string, POChargeHistory>();
      for (const h of existingHistory) {
        historyMap.set(String(h.dong), h);
      }

      // 3. Apply new values into targetSlot for all incoming rows
      for (const item of detail) {
        const key = String(item.dong);
        let entry = historyMap.get(key);

        if (!entry) {
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
            ngay_phi1: null,
            ngay_phi2: null,
            ngay_phi3: null,
            ngay_phi4: null,
            ngay_phi5: null,
            ngay_phi6: null,
          });
          historyMap.set(key, entry);
        }

        entry.ma_cp = item.ma_cp;
        (entry as any)[targetSlot] = safeNumber(item.cp01_nt);

        // ngay_phiN ứng với dong=N: lưu ngày đẩy cho dòng này
        const dongIndex = item.dong; // dong 1→ngay_phi1, dong 2→ngay_phi2...
        const ngaySlot = `ngay_phi${dongIndex}` as keyof POChargeHistory;
        const ngayFromMaster = (master as any)[`ngay_phi${dongIndex}`];
        (entry as any)[ngaySlot] = ngayFromMaster ? new Date(ngayFromMaster) : new Date();
      }

      // 4. Build merged payload from history
      const finalList = Array.from(historyMap.values()).sort((a, b) => a.dong - b.dong);

      // Build ngay_phi1–6: ngay_phiN lấy từ row có dong=N trong history
      const historyByDong = new Map(finalList.map(h => [h.dong, h]));
      const mergedMaster = {
        ...master,
        ngay_phi1: historyByDong.get(1)?.ngay_phi1 ? format(new Date(historyByDong.get(1)!.ngay_phi1!), "yyyy-MM-dd'T'HH:mm:ss") : null,
        ngay_phi2: historyByDong.get(2)?.ngay_phi2 ? format(new Date(historyByDong.get(2)!.ngay_phi2!), "yyyy-MM-dd'T'HH:mm:ss") : null,
        ngay_phi3: historyByDong.get(3)?.ngay_phi3 ? format(new Date(historyByDong.get(3)!.ngay_phi3!), "yyyy-MM-dd'T'HH:mm:ss") : null,
        ngay_phi4: historyByDong.get(4)?.ngay_phi4 ? format(new Date(historyByDong.get(4)!.ngay_phi4!), "yyyy-MM-dd'T'HH:mm:ss") : null,
        ngay_phi5: historyByDong.get(5)?.ngay_phi5 ? format(new Date(historyByDong.get(5)!.ngay_phi5!), "yyyy-MM-dd'T'HH:mm:ss") : null,
        ngay_phi6: historyByDong.get(6)?.ngay_phi6 ? format(new Date(historyByDong.get(6)!.ngay_phi6!), "yyyy-MM-dd'T'HH:mm:ss") : null,
      };

      const mergedPayload = {
        master: mergedMaster,
        detail: finalList.map((h) => ({
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

      // 5. Persist history before submitting
      await this.historyRepo.save(finalList);

      // 6. Submit to Fast API
      this.logger.log(
        `[FastIntegration] Submitting payload: ${JSON.stringify(mergedPayload.detail)}`,
      );
      result = await this.fastApiClient.submitPOCharges(mergedPayload);

      // Validate Response
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
        this.logger.warn(`[FastIntegration] Sync failed: ${errorMessage}`);
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

  async batchSyncPOCharges(startDate: string, endDate: string, platform?: string): Promise<any> {
    this.logger.log(`[FastIntegration] Starting batch sync PO Charges for ${startDate} to ${endDate} (Platform: ${platform || 'ALL'})`);

    try {
      const startStr = startDate;
      const endStr = endDate;

      let fees: any[] = [];

      if (!platform || platform.toLowerCase() === 'shopee') {
        const result = await this.orderFeeService.findShopeeFees(1, 100000, undefined, undefined, startStr, endStr);
        fees = fees.concat(result.data.map(item => ({ ...item, platform: 'shopee' })));
      }

      if (!platform || platform.toLowerCase() === 'tiktok') {
        const result = await this.orderFeeService.findTikTokFees(1, 100000, undefined, undefined, startStr, endStr);
        fees = fees.concat(result.data.map(item => ({ ...item, platform: 'tiktok' })));
      }

      this.logger.log(`[FastIntegration] Found ${fees.length} fees to sync`);

      const results: any[] = [];
      let successCount = 0;
      let failCount = 0;

      for (const item of fees) {
        if (!item.erpOrderCode) {
          this.logger.warn(`[FastIntegration] Order missing erpOrderCode, skipping.`);
          failCount++;
          continue;
        }

        const orderDate = item.invoiceDate
          ? new Date(item.invoiceDate).toISOString()
          : (item.orderCreatedAt ? new Date(item.orderCreatedAt).toISOString() : new Date().toISOString());

        const config = (item.platform || '').toLowerCase() === 'tiktok' ? TIKTOK_FEE_CONFIG : SHOPEE_FEE_CONFIG;

        const dateStr = item.invoiceDate
          ? format(new Date(item.invoiceDate), "yyyy-MM-dd'T'HH:mm:ss")
          : (item.orderCreatedAt ? format(new Date(item.orderCreatedAt), "yyyy-MM-dd'T'HH:mm:ss") : format(new Date(), "yyyy-MM-dd'T'HH:mm:ss"));


        const master = {
          dh_so: item.erpOrderCode,
          dh_ngay: orderDate,
          dh_dvcs: "TTM",
          ngay_phi1: dateStr,
          ngay_phi2: dateStr,
          ngay_phi3: dateStr,
          ngay_phi4: dateStr,
          ngay_phi5: dateStr,
          ngay_phi6: dateStr,
        };

        const details: any[] = [];
        config.forEach((rule) => {
          const value = Number(item[rule.field]);
          if (value && value !== 0 && !isNaN(value)) {
            const code = rule.defaultCode;

            const detail = {
              dong: rule.row,
              ma_cp: code,
              cp01_nt: 0,
              cp02_nt: 0,
              cp03_nt: 0,
              cp04_nt: 0,
              cp05_nt: 0,
              cp06_nt: 0,
            };

            if (rule.targetCol === "cp02_nt") {
              detail.cp02_nt = value;
            } else {
              detail.cp01_nt = value;
            }
            details.push(detail);
          }
        });

        if (details.length === 0) {
          continue;
        }

        try {
          await this.syncPOCharges({ master, detail: details });
          successCount++;
        } catch (error: any) {
          failCount++;
          results.push({ order: item.erpOrderCode, error: error.message });
        }
      }

      return { success: true, message: `Thành công ${successCount}, Thất bại ${failCount}`, total: fees.length, errors: results };
    } catch (error: any) {
      throw new BadRequestException(`Batch sync failed: ${error.message}`);
    }
  }

  async getAuditLogs(
    search?: string,
    dateFrom?: string,
    dateTo?: string,
    status?: string,
  ): Promise<AuditPo[]> {
    const query = this.auditRepo
      .createQueryBuilder('audit')
      .orderBy('audit.created_at', 'DESC')
      .take(500);

    if (search) {
      query.andWhere('audit.dh_so ILIKE :search', { search: `%${search}%` });
    }

    if (status) {
      query.andWhere('audit.status = :status', { status: status.toUpperCase() });
    }

    if (dateFrom) {
      const start = new Date(dateFrom);
      start.setHours(0, 0, 0, 0);
      query.andWhere('audit.dh_ngay >= :dateFrom', { dateFrom: start });
    }

    if (dateTo) {
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

  /**
   * Xoá một audit log theo ID
   */
  async deleteAuditLog(id: number): Promise<{ success: boolean; message: string }> {
    const audit = await this.auditRepo.findOne({ where: { id } });
    if (!audit) {
      throw new NotFoundException(`Audit log #${id} không tồn tại`);
    }
    const dhSo = audit.dh_so;

    // Cascade: delete po_charge_history for this order
    if (dhSo) {
      await this.historyRepo.delete({ dh_so: dhSo });
      this.logger.log(`[FastIntegration] Deleted po_charge_history for ${dhSo}`);
    }

    await this.auditRepo.delete(id);
    this.logger.log(`[FastIntegration] Deleted audit log #${id} (${dhSo})`);
    return { success: true, message: `Đã xoá log #${id} và lịch sử phí của ${dhSo}` };
  }

  /**
   * Xoá nhiều audit log theo khoảng ngày (created_at)
   */
  async deleteAuditLogsByDateRange(
    startDate: string,
    endDate: string,
    status?: string,
  ): Promise<{ success: boolean; message: string; deleted: number }> {
    if (!startDate || !endDate) {
      throw new BadRequestException('startDate và endDate là bắt buộc');
    }

    const start = new Date(startDate);
    start.setHours(0, 0, 0, 0);
    const end = new Date(endDate);
    end.setHours(23, 59, 59, 999);

    const query = this.auditRepo
      .createQueryBuilder('audit')
      .where('audit.dh_ngay >= :start', { start })
      .andWhere('audit.dh_ngay <= :end', { end });

    if (status) {
      query.andWhere('audit.status = :status', { status: status.toUpperCase() });
    }

    const logsToDelete = await query.select(['audit.id', 'audit.dh_so']).getMany();
    const ids = logsToDelete.map(l => l.id);

    if (ids.length === 0) {
      return { success: true, message: 'Không có log nào để xoá', deleted: 0 };
    }

    // Cascade: delete po_charge_history for all affected orders
    const uniqueDhSo = [...new Set(logsToDelete.map(l => l.dh_so).filter((s): s is string => !!s))];
    if (uniqueDhSo.length > 0) {
      for (const dhSo of uniqueDhSo) {
        await this.historyRepo.delete({ dh_so: dhSo });
      }
      this.logger.log(`[FastIntegration] Deleted po_charge_history for ${uniqueDhSo.length} orders`);
    }

    const result = await this.auditRepo.delete(ids);
    const deleted = result.affected || 0;
    this.logger.log(`[FastIntegration] Deleted ${deleted} audit logs (${startDate} -> ${endDate})`);
    return { success: true, message: `Đã xoá ${deleted} log và lịch sử phí tương ứng`, deleted };
  }
}
