import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { MultiDbService } from './multi-db.service';
import { MultiDbSyncService } from './multi-db-sync.service';

@Controller('multi-db')
export class MultiDbController {
  constructor(
    private readonly multiDbService: MultiDbService,
    private readonly multiDbSyncService: MultiDbSyncService,
  ) {}

  /**
   * GET /multi-db/info
   * Lấy thông tin về các databases
   */
  @Get('info')
  async getDatabasesInfo() {
    return this.multiDbService.getDatabasesInfo();
  }

  /**
   * GET /multi-db/check-connections
   * Kiểm tra kết nối đến tất cả databases
   */
  @Get('check-connections')
  async checkConnections() {
    return this.multiDbService.checkConnections();
  }

  @Get('order-fees/:erpCode')
  async getOrderFees(@Param('erpCode') erpCode: string) {
    return this.multiDbService.getOrderFees(erpCode);
  }

  /**
   * POST /multi-db/sync-order-fees
   * Manually trigger order fee sync (for testing)
   */
  @Get('sync-order-fees')
  async syncOrderFees() {
    return this.multiDbSyncService.triggerManualSync();
  }

  /**
   * POST /multi-db/sync-order-fees/:erpCode
   * Manually sync a specific order (for testing)
   * Query: ?brand=menard or ?brand=yaman (optional)
   */
  @Post('sync-order-fees/:erpCode')
  async syncOrderFeeByCode(
    @Param('erpCode') erpCode: string,
    @Query('brand') brand?: string,
  ) {
    return this.multiDbService.syncOrderFeeByCode(erpCode, brand);
  }

  @Post('range-sync-order-fees')
  async rangeSyncOrderFees(
    @Body() dateRange: { startAt: string; endAt: string },
  ) {
    return this.multiDbSyncService.rangeSyncOrderFees(
      dateRange.startAt,
      dateRange.endAt,
    );
  }
}
