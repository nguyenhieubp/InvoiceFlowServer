import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { SyncService } from '../services/sync.service';

@Injectable()
export class SyncTask {
  private readonly logger = new Logger(SyncTask.name);

  constructor(private readonly syncService: SyncService) {}

  // Chạy mỗi ngày lúc 2:00 AM
  @Cron('0 2 * * *', {
    name: 'daily-sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailySync() {
    this.logger.log('Bắt đầu đồng bộ dữ liệu tự động (scheduled task)...');
    try {
      // Format ngày hôm qua (DDMMMYYYY)
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const day = yesterday.getDate().toString().padStart(2, '0');
      const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
      const month = months[yesterday.getMonth()];
      const year = yesterday.getFullYear();
      const date = `${day}${month}${year}`;
      
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
      this.logger.log('Hoàn thành đồng bộ dữ liệu tự động');
    } catch (error) {
      this.logger.error(`Lỗi khi đồng bộ tự động: ${error.message}`);
    }
  }

  // Có thể thêm các cron job khác nếu cần
  // Ví dụ: đồng bộ mỗi 6 giờ một lần
  // @Cron('0 */6 * * *')
  // async handlePeriodicSync() {
  //   this.logger.log('Đồng bộ định kỳ...');
  //   await this.syncService.syncAllBrands();
  // }
}

