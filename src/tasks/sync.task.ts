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

  // Chạy mỗi ngày lúc 2:00 AM - Tạo hóa đơn nhập xuất kho cho các đơn hàng ngày T-1
  // @Cron('0 2 * * *', {
  //   name: 'daily-warehouse-invoice-2am',
  //   timeZone: 'Asia/Ho_Chi_Minh',
  // })
  async handleDailyWarehouseInvoice2AM() {
    this.logger.log('Bắt đầu tạo hóa đơn nhập xuất kho tự động lúc 2h sáng (scheduled task)...');
    try {
      // Lấy ngày T-1
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      yesterday.setHours(0, 0, 0, 0);
      const tomorrow = new Date(yesterday);
      tomorrow.setDate(tomorrow.getDate() + 1);

      // Lấy các đơn hàng trong ngày T-1 chưa xử lý
      const unprocessedSales = await this.saleRepository
        .createQueryBuilder('sale')
        .where('sale.isProcessed = :isProcessed', { isProcessed: false })
        .andWhere('sale.docDate >= :yesterday', { yesterday })
        .andWhere('sale.docDate < :tomorrow', { tomorrow })
        .orderBy('sale.docDate', 'DESC')
        .addOrderBy('sale.docCode', 'ASC')
        .getMany();

      if (unprocessedSales.length === 0) {
        this.logger.log('Không có đơn hàng nào trong ngày T-1 cần tạo hóa đơn nhập xuất kho');
        return;
      }

      // Group by docCode
      const orderMap = new Map<string, Sale[]>();
      for (const sale of unprocessedSales) {
        const docCode = sale.docCode;
        if (!orderMap.has(docCode)) {
          orderMap.set(docCode, []);
        }
        orderMap.get(docCode)!.push(sale);
      }

      const docCodes = Array.from(orderMap.keys());
      this.logger.log(`Tìm thấy ${docCodes.length} đơn hàng trong ngày T-1 cần tạo hóa đơn nhập xuất kho`);

      let successCount = 0;
      let failureCount = 0;

      // Xử lý từng đơn hàng
      for (const docCode of docCodes) {
        try {
          this.logger.log(`[Warehouse Invoice] Đang tạo hóa đơn nhập xuất kho cho đơn hàng: ${docCode}`);
          
          // Build invoice data
          const invoiceData = await this.salesService.buildInvoiceDataForWarehouse(docCode);
          
          // Lấy order data để lấy customer info
          const orderData = await this.salesService.findByOrderCode(docCode);
          
          // Tạo warehouseRelease (xuất kho) với ioType: O
          await this.fastApiInvoiceFlowService.createWarehouseRelease({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          });

          // Tạo warehouseReceipt (nhập kho) với ioType: I
          await this.fastApiInvoiceFlowService.createWarehouseReceipt({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          });

          successCount++;
          this.logger.log(`[Warehouse Invoice] ✓ Tạo hóa đơn nhập xuất kho thành công cho đơn hàng: ${docCode}`);
        } catch (error: any) {
          failureCount++;
          this.logger.error(
            `[Warehouse Invoice] ✗ Lỗi khi tạo hóa đơn nhập xuất kho cho đơn hàng ${docCode}: ${error?.message || error}`,
          );
        }
      }

      this.logger.log(
        `[Warehouse Invoice] Hoàn thành tạo hóa đơn nhập xuất kho: ${successCount} thành công, ${failureCount} thất bại`,
      );
    } catch (error: any) {
      this.logger.error(`Lỗi khi tạo hóa đơn nhập xuất kho tự động: ${error?.message || error}`);
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

