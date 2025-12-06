import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Sale } from '../entities/sale.entity';
import { SalesService } from '../modules/sales/sales.service';

@Injectable()
export class AutoInvoiceTask {
  private readonly logger = new Logger(AutoInvoiceTask.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    private salesService: SalesService,
  ) {}

  /**
   * Tự động tạo hóa đơn cho các đơn hàng chưa xử lý
   * Chạy mỗi 10 phút
   */
  @Cron('*/10 * * * *', {
    name: 'auto-create-invoice',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleAutoCreateInvoice() {
    this.logger.log('Bắt đầu tự động tạo hóa đơn cho các đơn hàng chưa xử lý...');

    try {
      // Lấy tất cả các đơn hàng chưa xử lý (group by docCode)
      const unprocessedSales = await this.saleRepository
        .createQueryBuilder('sale')
        .where('sale.isProcessed = :isProcessed', { isProcessed: false })
        .orderBy('sale.docDate', 'DESC')
        .addOrderBy('sale.docCode', 'ASC')
        .getMany();

      if (unprocessedSales.length === 0) {
        this.logger.log('Không có đơn hàng nào cần xử lý');
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
      this.logger.log(`Tìm thấy ${docCodes.length} đơn hàng chưa xử lý`);

      let successCount = 0;
      let failureCount = 0;

      // Xử lý từng đơn hàng
      for (const docCode of docCodes) {
        try {
          this.logger.log(`Đang tạo hóa đơn cho đơn hàng: ${docCode}`);
          await this.salesService.createInvoiceViaFastApi(docCode);
          successCount++;
          this.logger.log(`✓ Tạo hóa đơn thành công cho đơn hàng: ${docCode}`);
        } catch (error: any) {
          failureCount++;
          this.logger.error(
            `✗ Lỗi khi tạo hóa đơn cho đơn hàng ${docCode}: ${error?.message || error}`,
          );
        }
      }

      this.logger.log(
        `Hoàn thành tự động tạo hóa đơn: ${successCount} thành công, ${failureCount} thất bại`,
      );
    } catch (error: any) {
      this.logger.error(`Lỗi khi tự động tạo hóa đơn: ${error?.message || error}`);
    }
  }

  /**
   * Tự động tạo hóa đơn cho các đơn hàng trong ngày
   * Chạy mỗi giờ
   */
  @Cron('0 * * * *', {
    name: 'auto-create-invoice-hourly',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleAutoCreateInvoiceHourly() {
    this.logger.log('Bắt đầu tự động tạo hóa đơn cho các đơn hàng trong ngày...');

    try {
      // Lấy các đơn hàng trong ngày hôm nay chưa xử lý
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const tomorrow = new Date(today);
      tomorrow.setDate(tomorrow.getDate() + 1);

      const unprocessedSales = await this.saleRepository
        .createQueryBuilder('sale')
        .where('sale.isProcessed = :isProcessed', { isProcessed: false })
        .andWhere('sale.docDate >= :today', { today })
        .andWhere('sale.docDate < :tomorrow', { tomorrow })
        .orderBy('sale.docDate', 'DESC')
        .addOrderBy('sale.docCode', 'ASC')
        .getMany();

      if (unprocessedSales.length === 0) {
        this.logger.log('Không có đơn hàng nào trong ngày cần xử lý');
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
      this.logger.log(`Tìm thấy ${docCodes.length} đơn hàng trong ngày chưa xử lý`);

      let successCount = 0;
      let failureCount = 0;

      // Xử lý từng đơn hàng (giới hạn 50 đơn mỗi lần để tránh quá tải)
      const limitedDocCodes = docCodes.slice(0, 50);
      for (const docCode of limitedDocCodes) {
        try {
          this.logger.log(`Đang tạo hóa đơn cho đơn hàng: ${docCode}`);
          await this.salesService.createInvoiceViaFastApi(docCode);
          successCount++;
          this.logger.log(`✓ Tạo hóa đơn thành công cho đơn hàng: ${docCode}`);
        } catch (error: any) {
          failureCount++;
          this.logger.error(
            `✗ Lỗi khi tạo hóa đơn cho đơn hàng ${docCode}: ${error?.message || error}`,
          );
        }
      }

      this.logger.log(
        `Hoàn thành tự động tạo hóa đơn trong ngày: ${successCount} thành công, ${failureCount} thất bại`,
      );
    } catch (error: any) {
      this.logger.error(`Lỗi khi tự động tạo hóa đơn trong ngày: ${error?.message || error}`);
    }
  }
}

