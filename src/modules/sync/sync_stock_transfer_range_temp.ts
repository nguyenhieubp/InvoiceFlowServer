import { Logger } from '@nestjs/common';

export class SyncStockTransferRangeTemp {
  private logger = new Logger(SyncStockTransferRangeTemp.name);

  // Mock method to satisfy 'this.syncStockTransfer' call found in the code
  // In the real SyncService, this method exists.
  async syncStockTransfer(
    dateStr: string,
    brandName: string,
  ): Promise<{
    recordsCount: number;
    savedCount: number;
    updatedCount: number;
    errors?: string[];
  }> {
    return {
      recordsCount: 0,
      savedCount: 0,
      updatedCount: 0,
      errors: [],
    };
  }

  /**
   * Đồng bộ dữ liệu xuất kho theo khoảng thời gian
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  async syncStockTransferRange(
    startDate: string,
    endDate: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    recordsCount: number;
    savedCount: number;
    updatedCount: number;
    errors?: string[];
  }> {
    try {
      this.logger.log(
        `[Stock Transfer Range] Bắt đầu đồng bộ từ ${startDate} đến ${endDate}${
          brand ? ` cho brand ${brand}` : ' cho tất cả brands'
        }`,
      );

      const brands = brand ? [brand] : ['f3', 'labhair', 'yaman', 'menard'];
      let totalRecordsCount = 0;
      let totalSavedCount = 0;
      let totalUpdatedCount = 0;
      const allErrors: string[] = [];

      // Parse dates từ DDMMMYYYY sang Date object
      const parseDate = (dateStr: string): Date => {
        const day = parseInt(dateStr.substring(0, 2));
        const monthStr = dateStr.substring(2, 5).toUpperCase();
        const year = parseInt(dateStr.substring(5, 9));
        const monthMap: Record<string, number> = {
          JAN: 0,
          FEB: 1,
          MAR: 2,
          APR: 3,
          MAY: 4,
          JUN: 5,
          JUL: 6,
          AUG: 7,
          SEP: 8,
          OCT: 9,
          NOV: 10,
          DEC: 11,
        };
        const month = monthMap[monthStr] || 0;
        return new Date(year, month, day);
      };

      const fromDate = parseDate(startDate);
      const toDate = parseDate(endDate);

      if (fromDate > toDate) {
        throw new Error('startDate phải nhỏ hơn hoặc bằng endDate');
      }

      // Generate danh sách các ngày cần sync
      const datesToSync: string[] = [];
      const currentDate = new Date(fromDate);
      while (currentDate <= toDate) {
        const day = String(currentDate.getDate()).padStart(2, '0');
        const monthNames = [
          'JAN',
          'FEB',
          'MAR',
          'APR',
          'MAY',
          'JUN',
          'JUL',
          'AUG',
          'SEP',
          'OCT',
          'NOV',
          'DEC',
        ];
        const month = monthNames[currentDate.getMonth()];
        const year = currentDate.getFullYear();
        const dateStr = `${day}${month}${year}`;
        datesToSync.push(dateStr);
        currentDate.setDate(currentDate.getDate() + 1);
      }

      this.logger.log(
        `[Stock Transfer Range] Sẽ đồng bộ ${datesToSync.length} ngày từ ${startDate} đến ${endDate}`,
      );

      // Loop qua từng brand
      for (const brandName of brands) {
        for (const dateStr of datesToSync) {
          try {
            const result = await this.syncStockTransfer(dateStr, brandName);
            totalRecordsCount += result.recordsCount;
            totalSavedCount += result.savedCount;
            totalUpdatedCount += result.updatedCount;
            if (result.errors && result.errors.length > 0) {
              allErrors.push(...result.errors);
            }
          } catch (error: any) {
            const errorMsg = `[${brandName}] Lỗi khi đồng bộ ngày ${dateStr}: ${
              error?.message || error
            }`;
            this.logger.error(errorMsg);
            allErrors.push(errorMsg);
          }
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ thành công ${totalRecordsCount} records xuất kho từ ${startDate} đến ${endDate}. Đã lưu ${totalSavedCount} records mới`,
        recordsCount: totalRecordsCount,
        savedCount: totalSavedCount,
        updatedCount: totalUpdatedCount,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      const errorMsg = `Lỗi khi đồng bộ stock transfer range: ${
        error?.message || error
      }`;
      this.logger.error(errorMsg);
      return {
        success: false,
        message: errorMsg,
        recordsCount: 0,
        savedCount: 0,
        updatedCount: 0,
        errors: [errorMsg],
      };
    }
  }
}
