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
      // Calculate Date T-1 (Yesterday)
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);

      const startAt = new Date(yesterday);
      startAt.setHours(0, 0, 0, 0);

      const endAt = new Date(yesterday);
      endAt.setHours(23, 59, 59, 999);

      // Convert to ISO string for DB query
      const startAtStr = startAt.toISOString();
      const endAtStr = endAt.toISOString();

      const result = await this.multiDbService.syncAllOrderFees(
        startAtStr,
        endAtStr,
      );

      this.logger.log(
        `Order fee sync completed (T-1): ${result.synced} records synced, ${result.failed} failed`,
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

  /**
   * Sync order fees by date range
   */
  async rangeSyncOrderFees(startAt: string, endAt: string) {
    try {
      const result = await this.multiDbService.syncAllOrderFees(startAt, endAt);

      this.logger.log(
        `Order fee sync completed: ${result.synced} records synced, ${result.failed} failed`,
      );
      return result;
    } catch (error) {
      this.logger.error('Order fee sync failed', error);
      throw error;
    }
  }
}
