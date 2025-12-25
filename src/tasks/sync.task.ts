import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { SyncService } from '../services/sync.service';
import { SalesService } from '../modules/sales/sales.service';
import { FastApiInvoiceFlowService } from '../services/fast-api-invoice-flow.service';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Sale } from '../entities/sale.entity';

@Injectable()
export class SyncTask {
  private readonly logger = new Logger(SyncTask.name);

  constructor(
    private readonly syncService: SyncService,
    @Inject(forwardRef(() => SalesService))
    private readonly salesService: SalesService,
    private readonly fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
  ) {}

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

  /**
   * Helper function: Đồng bộ dữ liệu bán hàng cho ngày T-1
   */
  private async syncSalesForYesterday(cronName: string): Promise<void> {
    const date = this.formatYesterdayDate();
    this.logger.log(`[${cronName}] Bắt đầu đồng bộ dữ liệu bán hàng cho ngày ${date} (T-1)...`);
    
    try {
      // Đồng bộ từng brand tuần tự
      const brands = ['f3', 'labhair', 'yaman', 'menard'];
      for (const brand of brands) {
        try {
          this.logger.log(`[${cronName}] Đang đồng bộ brand ${brand} cho ngày ${date}`);
          await this.syncService.syncBrand(brand, date);
          this.logger.log(`[${cronName}] Hoàn thành đồng bộ brand ${brand} cho ngày ${date}`);
        } catch (error) {
          this.logger.error(`[${cronName}] Lỗi khi đồng bộ ${brand} cho ngày ${date}: ${error.message}`);
        }
      }

      this.logger.log(`[${cronName}] Hoàn thành đồng bộ dữ liệu bán hàng tự động`);
    } catch (error) {
      this.logger.error(`[${cronName}] Lỗi khi đồng bộ dữ liệu bán hàng tự động: ${error.message}`);
    }
  }


  // Chạy mỗi ngày lúc 3:00 AM - Đồng bộ dữ liệu bán hàng (ngày T-1)
  @Cron('0 3 * * *', {
    name: 'daily-sales-sync-3am',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailySalesSync3AM() {
    await this.syncSalesForYesterday('Sales Sync 3AM');
  }
}

