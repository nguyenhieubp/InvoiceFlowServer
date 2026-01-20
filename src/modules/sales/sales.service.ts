import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';

import { Sale } from '../../entities/sale.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';

import { SalesQueryService } from './sales-query.service';
import { SalesInvoiceService } from './sales-invoice.service';
import { SalesWarehouseService } from './sales-warehouse.service';
import { SalesSyncService } from './sales-sync.service';

@Injectable()
export class SalesService {
  private readonly logger = new Logger(SalesService.name);
  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    private salesQueryService: SalesQueryService,
    private salesSyncService: SalesSyncService,
    private salesInvoiceService: SalesInvoiceService,
    private salesWarehouseService: SalesWarehouseService,
  ) {}

  /**
   * Lấy stock transfer theo id
   */
  async getStockTransferById(id: string): Promise<StockTransfer | null> {
    return this.salesQueryService.getStockTransferById(id);
  }

  /**
   * Xử lý warehouse receipt/release/transfer từ stock transfer theo docCode
   */
  async processWarehouseFromStockTransferByDocCode(
    docCode: string,
  ): Promise<any> {
    return this.salesWarehouseService.processWarehouseFromStockTransferByDocCode(
      docCode,
    );
  }

  /**
   * Retry batch các warehouse processed failed theo date range
   */
  async retryWarehouseFailedByDateRange(
    dateFrom: string,
    dateTo: string,
  ): Promise<{
    success: boolean;
    message: string;
    totalProcessed: number;
    successCount: number;
    failedCount: number;
    errors: string[];
  }> {
    return this.salesWarehouseService.retryWarehouseFailedByDateRange(
      dateFrom,
      dateTo,
    );
  }

  /**
   * Xử lý warehouse receipt/release/transfer từ stock transfer
   */
  async processWarehouseFromStockTransfer(
    stockTransfer: StockTransfer,
  ): Promise<any> {
    return this.salesWarehouseService.processWarehouseFromStockTransfer(
      stockTransfer,
    );
  }

  async findAllOrders(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
    date?: string; // Format: DDMMMYYYY (ví dụ: 04DEC2025)
    dateFrom?: string; // Format: YYYY-MM-DD hoặc ISO string
    dateTo?: string; // Format: YYYY-MM-DD hoặc ISO string
    search?: string; // Search query để tìm theo docCode, customer name, code, mobile
    statusAsys?: boolean; // Filter theo statusAsys (true/false)
    export?: boolean; // Nếu true, trả về sales items riêng lẻ (không group, không paginate) để export Excel
    typeSale?: string; // Type sale: "WHOLESALE" or "RETAIL"
  }) {
    return this.salesQueryService.findAllOrders(options);
  }

  async getStatusAsys(
    statusAsys?: string,
    page?: number,
    limit?: number,
    brand?: string,
    dateFrom?: string,
    dateTo?: string,
    search?: string,
  ) {
    return this.salesQueryService.getStatusAsys(
      statusAsys,
      page,
      limit,
      brand,
      dateFrom,
      dateTo,
      search,
    );
  }

  /**
   * Đồng bộ lại đơn lỗi - check lại với Loyalty API
   * Nếu tìm thấy trong Loyalty, cập nhật itemCode (mã vật tư) và statusAsys = true
   * Xử lý theo batch từ database để tránh load quá nhiều vào memory
   */
  async syncErrorOrders(): Promise<{
    total: number;
    success: number;
    failed: number;
    updated: Array<{
      id: string;
      docCode: string;
      itemCode: string;
      oldItemCode: string;
      newItemCode: string;
    }>;
  }> {
    return this.salesSyncService.syncErrorOrders();
  }

  /**
   * Đồng bộ lại một đơn hàng cụ thể - check lại với Loyalty API
   * Nếu tìm thấy trong Loyalty, cập nhật itemCode (mã vật tư) và statusAsys = true
   */
  async syncErrorOrderByDocCode(docCode: string): Promise<{
    success: boolean;
    message: string;
    updated: number;
    failed: number;
    details: Array<{
      id: string;
      itemCode: string;
      oldItemCode: string;
      newItemCode: string;
    }>;
  }> {
    return this.salesSyncService.syncErrorOrderByDocCode(docCode);
  }

  /**
   * Đồng bộ dữ liệu từ Zappy API và lưu vào database
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 04DEC2025)
   * @param brand - Brand name (f3, labhair, yaman, menard). Nếu không có thì dùng default
   * @returns Kết quả đồng bộ
   */
  async syncFromZappy(
    date: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    ordersCount: number;
    salesCount: number;
    customersCount: number;
    errors?: string[];
  }> {
    return this.salesSyncService.syncFromZappy(date, brand);
  }

  /**
   * Đồng bộ sale từ khoảng thời gian cho tất cả các nhãn
   * @param startDate - Ngày bắt đầu theo format DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc theo format DDMMMYYYY (ví dụ: 01DEC2025)
   * @returns Kết quả đồng bộ tổng hợp
   */
  async syncSalesByDateRange(
    startDate: string,
    endDate: string,
  ): Promise<{
    success: boolean;
    message: string;
    totalOrdersCount: number;
    totalSalesCount: number;
    totalCustomersCount: number;
    brandResults: Array<{
      brand: string;
      ordersCount: number;
      salesCount: number;
      customersCount: number;
      errors?: string[];
    }>;
    errors?: string[];
    invoiceProcessing?: {
      success: boolean;
      totalProcessed: number;
      successCount: number;
      failedCount: number;
      errors: string[];
      details: Array<any>;
    };
  }> {
    this.logger.log(
      `[Two-Phase Sync] Bắt đầu đồng bộ Sales từ ${startDate} đến ${endDate}`,
    );

    // Phase 1: Sync from Zappy
    this.logger.log(`[Two-Phase Sync] Phase 1: Syncing from Zappy...`);
    const syncResult = await this.salesSyncService.syncSalesByDateRange(
      startDate,
      endDate,
    );

    // Phase 2: Process Invoices
    this.logger.log(
      `[Two-Phase Sync] Phase 2: Processing Fast API Invoices...`,
    );
    const invoiceResult =
      await this.salesInvoiceService.processInvoicesByDateRange(
        startDate,
        endDate,
      );

    return {
      ...syncResult,
      message: `${syncResult.message}. Phase 2: ${invoiceResult.message}`,
      invoiceProcessing: invoiceResult,
    };
  }

  async findOne(id: string) {
    const sale = await this.saleRepository.findOne({
      where: { id },
      relations: ['customer'],
    });

    if (!sale) {
      throw new NotFoundException(`Sale with ID ${id} not found`);
    }

    return sale;
  }

  // Delegated Methods

  async findByOrderCode(docCode: string) {
    return this.salesInvoiceService.findByOrderCode(docCode);
  }

  async markProcessedOrdersFromInvoices() {
    return this.salesInvoiceService.markProcessedOrdersFromInvoices();
  }

  async createInvoiceViaFastApi(docCode: string, forceRetry: boolean = false) {
    return this.salesInvoiceService.createInvoiceViaFastApi(
      docCode,
      forceRetry,
    );
  }

  async processSingleOrder(docCode: string, forceRetry: boolean = false) {
    return this.salesInvoiceService.processSingleOrder(docCode, forceRetry);
  }

  async createStockTransfer(createDto: any) {
    return this.salesInvoiceService.createStockTransfer(createDto);
  }

  async getIncorrectStockTransfers(
    page: number,
    limit: number,
    search?: string,
  ) {
    return this.salesQueryService.getIncorrectStockTransfers(
      page,
      limit,
      search,
    );
  }
}
