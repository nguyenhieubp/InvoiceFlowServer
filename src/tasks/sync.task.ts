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
      await this.syncService.syncAllBrands();
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

