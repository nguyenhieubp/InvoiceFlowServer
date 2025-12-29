import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { SyncService } from '../modules/sync/sync.service';
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


  // Chạy mỗi ngày lúc 2:30 AM - Đồng bộ báo cáo nộp quỹ cuối ca (ngày T-1)
  @Cron('30 2 * * *', {
    name: 'daily-shift-end-cash-sync-2-30am',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyShiftEndCashSync230AM() {
    this.logger.log('Bắt đầu đồng bộ báo cáo nộp quỹ cuối ca tự động (scheduled task)...');
    try {
      const date = this.formatYesterdayDate();

      // Đồng bộ báo cáo nộp quỹ cuối ca cho tất cả brands
      const brands = ['f3', 'labhair', 'yaman', 'menard'];
      for (const brand of brands) {
        try {
          this.logger.log(`[Scheduled ShiftEndCash] Đang đồng bộ báo cáo nộp quỹ cuối ca brand ${brand} cho ngày ${date}`);
          const result = await this.syncService.syncShiftEndCash(date, brand);
          if (result.success) {
            this.logger.log(
              `[Scheduled ShiftEndCash] Hoàn thành đồng bộ brand ${brand}: ${result.recordsCount} records, ${result.savedCount} saved, ${result.updatedCount} updated`,
            );
          } else {
            this.logger.error(`[Scheduled ShiftEndCash] Lỗi khi đồng bộ brand ${brand}: ${result.message}`);
            if (result.errors && result.errors.length > 0) {
              result.errors.forEach((error) => {
                this.logger.error(`[Scheduled ShiftEndCash] ${error}`);
              });
            }
          }
        } catch (error: any) {
          this.logger.error(`[Scheduled ShiftEndCash] Lỗi khi đồng bộ ${brand} cho ngày ${date}: ${error?.message || error}`);
        }
      }

      this.logger.log('Hoàn thành đồng bộ báo cáo nộp quỹ cuối ca tự động');
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ báo cáo nộp quỹ cuối ca tự động: ${error?.message || error}`);
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

  /**
   * Helper function: Format ngày đầu tháng hiện tại thành format DDMMMYYYY
   * Ví dụ: 01DEC2025
   */
  private formatFirstDayOfCurrentMonth(): string {
    const now = new Date();
    const day = '01';
    const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    const month = months[now.getMonth()];
    const year = now.getFullYear();
    return `${day}${month}${year}`;
  }

  /**
   * Helper function: Format ngày cuối tháng hiện tại thành format DDMMMYYYY
   * Ví dụ: 31DEC2025
   */
  private formatLastDayOfCurrentMonth(): string {
    const now = new Date();
    const lastDay = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    const day = lastDay.toString().padStart(2, '0');
    const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    const month = months[now.getMonth()];
    const year = now.getFullYear();
    return `${day}${month}${year}`;
  }

  // Chạy mỗi ngày lúc 4:00 AM - Đồng bộ promotion cho tháng hiện tại
  @Cron('0 4 * * *', {
    name: 'daily-promotion-sync-4am',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyPromotionSync4AM() {
    this.logger.log('Bắt đầu đồng bộ promotion tự động cho tháng hiện tại (scheduled task)...');
    try {
      const dateFrom = this.formatFirstDayOfCurrentMonth();
      const dateTo = this.formatLastDayOfCurrentMonth();

      this.logger.log(`[Scheduled Promotion] Đang đồng bộ promotion cho tháng hiện tại: ${dateFrom} - ${dateTo}`);

      // Đồng bộ promotion cho tất cả brands
      const brands = ['f3', 'labhair', 'yaman', 'menard'];
      for (const brand of brands) {
        try {
          this.logger.log(`[Scheduled Promotion] Đang đồng bộ promotion brand ${brand} cho tháng ${dateFrom} - ${dateTo}`);
          const result = await this.syncService.syncPromotion(dateFrom, dateTo, brand);
          if (result.success) {
            this.logger.log(
              `[Scheduled Promotion] Hoàn thành đồng bộ promotion brand ${brand}: ${result.recordsCount} records, ${result.savedCount} saved, ${result.updatedCount} updated`,
            );
          } else {
            this.logger.error(`[Scheduled Promotion] Lỗi khi đồng bộ promotion brand ${brand}: ${result.message}`);
          }
        } catch (error: any) {
          this.logger.error(`[Scheduled Promotion] Lỗi khi đồng bộ promotion ${brand} cho tháng ${dateFrom} - ${dateTo}: ${error?.message || error}`);
        }
      }

      this.logger.log('Hoàn thành đồng bộ promotion tự động cho tháng hiện tại');
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ promotion tự động: ${error?.message || error}`);
    }
  }
}

