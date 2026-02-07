import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { POChargeHistory } from './entities/po-charge-history.entity';
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

        try {
            // 1. Fetch Existing History
            const existingHistory = await this.historyRepo.find({
                where: { dh_so: master.dh_so },
            });

            // 2. In-Memory Merge
            // Map existing history by 'dong'
            const historyMap = new Map<number, POChargeHistory>();
            for (const h of existingHistory) {
                historyMap.set(h.dong, h);
            }

            // Merge incoming payload into map (create new or update)
            // Note: We use POChargeHistory objects (or clear copies) to track state
            for (const item of detail) {
                let entry = historyMap.get(item.dong);
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
                    });
                    historyMap.set(item.dong, entry);
                }

                // Accumulate values from payload (instead of overwrite)
                // This handles cases where multiple payload items map to same 'dong' but different fields
                // or if we just want to ADD to existing history (though current logic seems to be "merge" as in "update latest state")

                // User requirement "Nó ko lấy được giá trị" implies value might be lost.
                // If the DB has 0, and payload has 1221, it becomes 1221.
                // If DB has 1221, and payload has 0. 
                //    Number(0) !== 0 is False. So it keeps 1221. This is correct for "Update only provided fields".

                // BUT, what if the user wants to OVERWRITE with the new payload? 
                // If payload says 1221, and we have 0, we set 1221.
                // The logical issue might be if `item.dong` is duplicated in `detail` array?
                // In the user's payload, `dong` is unique (1,2,3,4,5).

                // Let's ensure we are using the incoming value if it exists.
                if (item.cp01_nt !== undefined && item.cp01_nt !== null) entry.cp01_nt = safeNumber(item.cp01_nt);
                if (item.cp02_nt !== undefined && item.cp02_nt !== null) entry.cp02_nt = safeNumber(item.cp02_nt);
                if (item.cp03_nt !== undefined && item.cp03_nt !== null) entry.cp03_nt = safeNumber(item.cp03_nt);
                if (item.cp04_nt !== undefined && item.cp04_nt !== null) entry.cp04_nt = safeNumber(item.cp04_nt);
                if (item.cp05_nt !== undefined && item.cp05_nt !== null) entry.cp05_nt = safeNumber(item.cp05_nt);
                if (item.cp06_nt !== undefined && item.cp06_nt !== null) entry.cp06_nt = safeNumber(item.cp06_nt);

                // Update metadata
                entry.ma_cp = item.ma_cp;
            }

            // Prepare final merged list for API
            // Sort by 'dong' ASC
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

            // 3. Submit to Fast API
            this.logger.log(
                `[FastIntegration] Submitting merged payload: ${JSON.stringify(mergedPayload.detail)}`,
            );
            const result = await this.fastApiClient.submitPOCharges(mergedPayload);

            // 4. Validate Response
            let isSuccess = false;
            if (Array.isArray(result) && result.length > 0) {
                if (result[0].status === 1) isSuccess = true;
            } else if (result && result.status === 1) {
                isSuccess = true;
            }

            if (!isSuccess) {
                const errorMsg = Array.isArray(result)
                    ? result[0]?.message
                    : result?.message || 'Sync POCharges failed (unknown status)';
                throw new BadRequestException(errorMsg);
            }

            // 5. If Success -> Persist History
            this.logger.log(
                `[FastIntegration] Fast API success. Persisting history to DB...`,
            );
            await this.historyRepo.save(finalMergedList);

            this.logger.log(
                `[FastIntegration] POCharges for order ${master.dh_so} synced and saved successfully`,
            );

            return result;
        } catch (error: any) {
            this.logger.error(
                `[FastIntegration] Failed to sync POCharges for ${master.dh_so}: ${error?.message || error}`,
            );
            throw error;
        }
    }
}
