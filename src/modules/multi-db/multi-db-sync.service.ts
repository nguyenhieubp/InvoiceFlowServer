import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { MultiDbService } from './multi-db.service';

@Injectable()
export class MultiDbSyncService {
  private readonly logger = new Logger(MultiDbSyncService.name);

  constructor(private readonly multiDbService: MultiDbService) {}

  /**
   * Sync order fees every day at 1 AM
   */
  @Cron('0 1 * * *', {
    name: 'sync-order-fees',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleOrderFeeSync() {
    try {
      const result = await this.multiDbService.syncAllOrderFees();

      this.logger.log(
        `Order fee sync completed: ${result.synced} records synced, ${result.failed} failed`,
      );
    } catch (error) {
      this.logger.error('Order fee sync failed', error);
    }
  }

  /**
   * Manual trigger for testing
   */
  async triggerManualSync() {
    return this.handleOrderFeeSync();
  }
}
