import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { SyncService } from '../services/sync.service';

@Injectable()
export class SyncTask {
  private readonly logger = new Logger(SyncTask.name);

  constructor(private readonly syncService: SyncService) {}

  /**
   * Helper function: Format ngày hôm qua thành format DDMMMYYYY
   * Ví dụ: 21DEC2025
   */
  private formatYesterdayDate(): string {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const day = yesterday.getDate().toString().padStart(2, '0');
    const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    const month = months[yesterday.getMonth()];
    const year = yesterday.getFullYear();
    return `${day}${month}${year}`;
  }

  /**
   * Helper function: Sync FaceID cho ngày T-1
   */
  private async syncFaceIdForYesterday(cronName: string): Promise<void> {
    const date = this.formatYesterdayDate();
    this.logger.log(`[${cronName}] Đang đồng bộ FaceID cho ngày ${date} (T-1)`);
    
    const result = await this.syncService.syncFaceIdByDate(date);
    
    if (result.success) {
      this.logger.log(
        `[${cronName}] Hoàn thành đồng bộ FaceID: ${result.message}. ` +
        `Đã lưu ${result.savedCount} records mới, cập nhật ${result.updatedCount} records.`
      );
    } else {
      this.logger.error(`[${cronName}] Lỗi khi đồng bộ FaceID: ${result.message}`);
      if (result.errors && result.errors.length > 0) {
        this.logger.error(`[${cronName}] Chi tiết lỗi: ${result.errors.join('; ')}`);
      }
    }
  }

  // Chạy mỗi ngày lúc 1:00 AM - Đồng bộ dữ liệu xuất kho
  @Cron('0 1 * * *', {
    name: 'daily-stock-transfer-sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyStockTransferSync() {
    this.logger.log('Bắt đầu đồng bộ dữ liệu xuất kho tự động (scheduled task)...');
    try {
      const date = this.formatYesterdayDate();

      // Đồng bộ dữ liệu xuất kho cho từng brand
      const brands = ['f3', 'labhair', 'yaman', 'menard'];
      for (const brand of brands) {
        try {
          this.logger.log(`[Scheduled Stock Transfer] Đang đồng bộ xuất kho brand ${brand} cho ngày ${date}`);
          const result = await this.syncService.syncStockTransfer(date, brand);
          if (result.success) {
            this.logger.log(`[Scheduled Stock Transfer] Hoàn thành đồng bộ xuất kho brand ${brand}: ${result.message}`);
          } else {
            this.logger.error(`[Scheduled Stock Transfer] Lỗi khi đồng bộ xuất kho brand ${brand}: ${result.message}`);
          }
        } catch (error) {
          this.logger.error(`[Scheduled Stock Transfer] Lỗi khi đồng bộ xuất kho ${brand} cho ngày ${date}: ${error.message}`);
        }
      }

      this.logger.log('Hoàn thành đồng bộ dữ liệu xuất kho tự động');
    } catch (error) {
      this.logger.error(`Lỗi khi đồng bộ dữ liệu xuất kho tự động: ${error.message}`);
    }
  }

  // Chạy mỗi ngày lúc 3:00 AM - Đồng bộ dữ liệu bán hàng
  @Cron('0 3 * * *', {
    name: 'daily-sales-sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailySalesSync() {
    this.logger.log('Bắt đầu đồng bộ dữ liệu bán hàng tự động (scheduled task)...');
    try {
      const date = this.formatYesterdayDate();
      
      // Đồng bộ từng brand tuần tự
      const brands = ['f3', 'labhair', 'yaman', 'menard'];
      for (const brand of brands) {
        try {
          this.logger.log(`[Scheduled] Đang đồng bộ brand ${brand} cho ngày ${date}`);
          await this.syncService.syncBrand(brand, date);
          this.logger.log(`[Scheduled] Hoàn thành đồng bộ brand ${brand} cho ngày ${date}`);
        } catch (error) {
          this.logger.error(`[Scheduled] Lỗi khi đồng bộ ${brand} cho ngày ${date}: ${error.message}`);
        }
      }

      this.logger.log('Hoàn thành đồng bộ dữ liệu bán hàng tự động');
    } catch (error) {
      this.logger.error(`Lỗi khi đồng bộ dữ liệu bán hàng tự động: ${error.message}`);
    }
  }

  // Chạy mỗi ngày lúc 2:00 AM - Đồng bộ FaceID (ngày T-1)
  @Cron('0 2 * * *', {
    name: 'daily-faceid-sync-2am',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyFaceIdSync2AM() {
    this.logger.log('Bắt đầu đồng bộ FaceID tự động lúc 2h sáng (scheduled task)...');
    try {
      await this.syncFaceIdForYesterday('Scheduled FaceID Sync 2AM');
      this.logger.log('Hoàn thành đồng bộ FaceID tự động lúc 2h sáng');
    } catch (error) {
      this.logger.error(`Lỗi khi đồng bộ FaceID tự động lúc 2h sáng: ${error.message}`);
    }
  }

  // Chạy mỗi ngày lúc 12:00 PM - Đồng bộ FaceID (ngày T-1)
  @Cron('0 12 * * *', {
    name: 'daily-faceid-sync-12pm',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyFaceIdSync12PM() {
    this.logger.log('Bắt đầu đồng bộ FaceID tự động lúc 12h trưa (scheduled task)...');
    try {
      await this.syncFaceIdForYesterday('Scheduled FaceID Sync 12PM');
      this.logger.log('Hoàn thành đồng bộ FaceID tự động lúc 12h trưa');
    } catch (error) {
      this.logger.error(`Lỗi khi đồng bộ FaceID tự động lúc 12h trưa: ${error.message}`);
    }
  }
}

