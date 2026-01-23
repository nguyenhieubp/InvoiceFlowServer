import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Sale } from '../../../entities/sale.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { SalesQueryService } from './sales-query.service';
import { SalesInvoiceService } from '../invoice/sales-invoice.service';
import { SalesWarehouseService } from './sales-warehouse.service';
import { SalesSyncService } from './sales-sync.service';
import { LoyaltyService } from 'src/services/loyalty.service';

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
    private loyaltyService: LoyaltyService,
  ) {}

  async findAllOrders(options: any) {
    return this.salesQueryService.findAllOrders(options);
  }

  async findAllAggregatedOrders(options: any) {
    return this.salesQueryService.findAllAggregatedOrders(options);
  }

  // Warehouse Method Delegation

  async getStockTransferById(id: string) {
    return this.salesWarehouseService.getStockTransferById(id);
  }

  async processWarehouseFromStockTransfer(stockTransfer: StockTransfer) {
    return this.salesWarehouseService.processWarehouseFromStockTransfer(
      stockTransfer,
    );
  }

  async processWarehouseFromStockTransferByDocCode(docCode: string) {
    return this.salesWarehouseService.processWarehouseFromStockTransferByDocCode(
      docCode,
    );
  }

  async retryWarehouseFailedByDateRange(dateFrom: string, dateTo: string) {
    return this.salesWarehouseService.retryWarehouseFailedByDateRange(
      dateFrom,
      dateTo,
    );
  }

  // ... (rest of methods)

  async getStatusAsys(
    statusAsys?: string,
    page: number = 1,
    limit: number = 10,
    brand?: string,
    dateFrom?: string,
    dateTo?: string,
    search?: string,
  ) {
    try {
      this.logger.log('Scanning for error orders (Missing Material/DVCS)...');

      // 1. Get candidates (isProcessed = false)
      const query = this.saleRepository.createQueryBuilder('sale');
      query.where('sale.isProcessed = :isProcessed', { isProcessed: false });
      query.andWhere("(sale.itemCode IS NOT NULL AND sale.itemCode != '')");
      query.leftJoinAndSelect('sale.customer', 'customer');

      if (brand) query.andWhere('sale.brand = :brand', { brand });

      if (search && search.trim() !== '') {
        const searchPattern = `%${search.trim().toLowerCase()}%`;
        query.andWhere(
          "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search)",
          { search: searchPattern },
        );
      }

      // Date logic (Simplified from SalesFilterService)
      if (dateFrom) {
        query.andWhere('sale.docDate >= :dateFrom', {
          dateFrom: `${dateFrom} 00:00:00`,
        });
      }
      if (dateTo) {
        query.andWhere('sale.docDate <= :dateTo', {
          dateTo: `${dateTo} 23:59:59`,
        });
      }

      query.orderBy('sale.docDate', 'DESC');

      // Scan Limit
      const SCAN_LIMIT = 200; // Scan latest 200 items to check for errors
      query.take(SCAN_LIMIT);

      const candidates = await query.getMany();
      const errors: any[] = [];
      const productCache = new Map<string, any>();

      // 1. Batch Fetch Departments (Optimization)
      const uniqueBranchCodes = Array.from(
        new Set(
          candidates
            .map((s) => s.branchCode)
            .filter((b) => !!b && b.trim() !== ''),
        ),
      );
      const departmentMap =
        await this.loyaltyService.fetchLoyaltyDepartments(uniqueBranchCodes);

      // Sequential Check (safer for rate limits than Promise.all(200))
      // Could optimize with batches of 10 if needed
      for (const sale of candidates) {
        let isError = false;

        // Check Product (Missing Material?)
        let product = productCache.get(sale.itemCode);
        if (product === undefined) {
          product = await this.loyaltyService.checkProduct(sale.itemCode);
          productCache.set(sale.itemCode, product);
        }

        if (!product) {
          isError = true;
        } else {
          // Check DVCS (Missing Branch Mapping?)
          if (sale.branchCode) {
            // Check in batch map first
            const department = departmentMap.get(sale.branchCode);
            let maDvcs = department?.ma_dvcs;

            // Fallback: If not in batch map (rare), try single fetch
            if (!maDvcs) {
              maDvcs = await this.loyaltyService.fetchMaDvcs(sale.branchCode);
            }

            if (!maDvcs) {
              isError = true;
            }
          }
        }

        if (isError) {
          errors.push({
            ...sale,
            statusAsys: false, // Mark as error for frontend
            materialCode: product?.materialCode || null, // Enrich with found materialCode
          });
        }
      }

      // Pagination in memory
      const total = errors.length;
      const startIndex = (page - 1) * limit;
      const endIndex = startIndex + limit;
      const paginatedItems = errors.slice(startIndex, endIndex);

      return {
        data: paginatedItems,
        total: total,
        page,
        limit,
        totalPages: Math.ceil(total / limit),
      };
    } catch (error) {
      this.logger.error('Error scanning for error orders', error);
      throw error;
    }
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
    // this.logger.log(
    //   `[Two-Phase Sync] Phase 2: Processing Fast API Invoices...`,
    // );
    // const invoiceResult =
    //   await this.salesInvoiceService.processInvoicesByDateRange(
    //     startDate,
    //     endDate,
    //   );

    return {
      ...syncResult,
      // message: `${syncResult.message}. Phase 2: ${invoiceResult.message}`,
      message: `${syncResult.message}. Phase 2 (Auto Invoice) DISABLED per user request.`,
      // invoiceProcessing: invoiceResult,
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

  async createInvoiceViaFastApi(
    docCode: string,
    forceRetry: boolean = false,
    options?: { onlySalesOrder?: boolean },
  ) {
    return this.salesInvoiceService.createInvoiceViaFastApi(
      docCode,
      forceRetry,
      options,
    );
  }

  async processSingleOrder(docCode: string, forceRetry: boolean = false) {
    return this.salesInvoiceService.processSingleOrder(docCode, forceRetry);
  }

  async createStockTransfer(createDto: any) {
    return this.salesInvoiceService.createStockTransfer(createDto);
  }
}
