import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { Sale } from '../../../entities/sale.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { FastApiInvoice } from '../../../entities/fast-api-invoice.entity';
import { Invoice } from '../../../entities/invoice.entity';
import { DailyCashio } from '../../../entities/daily-cashio.entity';
import { InvoiceFlowOrchestratorService } from '../flows/invoice-flow-orchestrator.service';
import { SaleReturnHandlerService } from '../flows/sale-return-handler.service';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import * as _ from 'lodash';
import { FastApiInvoiceFlowService } from '../../../services/fast-api-invoice-flow.service';
import { SalesPayloadService } from './sales-payload.service';
import * as SalesUtils from '../../../utils/sales.utils';
import * as ConvertUtils from '../../../utils/convert.utils';
import { SalesQueryService } from '../services/sales-query.service';

@Injectable()
export class SalesInvoiceService {
  private readonly logger = new Logger(SalesInvoiceService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    private salesQueryService: SalesQueryService,
    private invoiceFlowOrchestratorService: InvoiceFlowOrchestratorService,
    private saleReturnHandlerService: SaleReturnHandlerService,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private salesPayloadService: SalesPayloadService,
  ) {}

  /**
   * Tạo hóa đơn qua Fast API từ đơn hàng
   */
  async createInvoiceViaFastApi(
    docCode: string,
    forceRetry: boolean = false,
    options?: { onlySalesOrder?: boolean },
  ): Promise<any> {
    try {
      // ============================================
      // 1. CHECK INVOICE ĐÃ TẠO
      // ============================================
      // if (!forceRetry && !options?.onlySalesOrder) {
      //   const existingInvoice = await this.fastApiInvoiceRepository.findOne({
      //     where: { docCode },
      //   });

      //   if (existingInvoice && existingInvoice.status === 1) {
      //     return {
      //       success: true,
      //       message: `Đơn hàng ${docCode} đã được tạo hóa đơn thành công trước đó`,
      //       result: existingInvoice.fastApiResponse
      //         ? JSON.parse(existingInvoice.fastApiResponse)
      //         : null,
      //       alreadyExists: true,
      //     };
      //   }
      // }

      // ============================================
      // 2. LẤY DỮ LIỆU ĐƠN HÀNG
      // ============================================
      const orderData = await this.findByOrderCode(docCode);
      const docCodesForStockTransfer =
        StockTransferUtils.getDocCodesForStockTransfer([docCode]);
      const stockTransfers = await this.stockTransferRepository.find({
        where: { soCode: In(docCodesForStockTransfer) },
      });

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(
          `Order ${docCode} not found or has no sales`,
        );
      }

      const hasX = /_X$/.test(docCode);

      if (_.isEmpty(stockTransfers)) {
        if (hasX) {
          // [AUTO-FLOW] For _X orders:
          // 1. Process Action 0 (Original Order)
          // 2. Process Action 1 (Cancellation Order)
          this.logger.log(
            `[AutoFlow] Detected _X order ${docCode}. Executing Action 0 -> Action 1 sequence.`,
          );

          // Step 1: Action 0
          try {
            await this.saleReturnHandlerService.handleSaleOrderWithUnderscoreX(
              orderData,
              docCode,
              0,
            );
          } catch (e) {
            this.logger.warn(
              `[AutoFlow] Action 0 failed or already exists for ${docCode}: ${e.message}. Continuing to Action 1.`,
            );
            // Continue even if Action 0 fails (maybe it already exists)
          }

          // Step 2: Action 1 (The actual _X order processing)
          return await this.saleReturnHandlerService.handleSaleOrderWithUnderscoreX(
            orderData,
            docCode,
            1,
          );
        } else {
          return await this.saleReturnHandlerService.handleSaleOrderWithUnderscoreX(
            orderData,
            docCode ?? '',
            0,
          );
        }
      }

      // Có stock transfer (hoặc trường hợp còn lại) -> xử lý bình thường
      return await this.processSingleOrder(docCode, forceRetry, options);
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi tạo hóa đơn cho ${docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  async processSingleOrder(
    docCode: string,
    forceRetry: boolean = false,
    options?: { onlySalesOrder?: boolean },
  ): Promise<any> {
    try {
      // Kiểm tra xem đơn hàng đã có trong bảng kê hóa đơn chưa (đã tạo thành công)
      // Nếu forceRetry = true, bỏ qua check này để cho phép retry
      if (!forceRetry && !options?.onlySalesOrder) {
        const existingInvoice = await this.fastApiInvoiceRepository.findOne({
          where: { docCode },
        });

        if (existingInvoice && existingInvoice.status === 1) {
          return {
            success: true,
            message: `Đơn hàng ${docCode} đã được tạo hóa đơn thành công trước đó`,
            result: existingInvoice.fastApiResponse
              ? JSON.parse(existingInvoice.fastApiResponse)
              : null,
            alreadyExists: true,
          };
        }
      }

      // Lấy thông tin đơn hàng
      const orderData = await this.findByOrderCode(docCode);

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(
          `Order ${docCode} not found or has no sales`,
        );
      }

      // [NEW] Handle onlySalesOrder option
      if (options?.onlySalesOrder) {
        // [FIX] Build invoice data using Payload Service to ensure 'detail' is populated correctly
        const invoiceData =
          await this.salesPayloadService.buildFastApiInvoiceData(orderData, {
            onlySalesOrder: true,
          });

        await this.fastApiInvoiceFlowService.createOrUpdateCustomer({
          ma_kh: SalesUtils.normalizeMaKh(orderData.customer?.code),
          ten_kh: orderData.customer?.name || '',
          dia_chi: orderData.customer?.address || undefined,
          so_cccd: orderData.customer?.idnumber || undefined,
          ngay_sinh: orderData.customer?.birthday
            ? ConvertUtils.formatDateYYYYMMDD(orderData.customer.birthday)
            : undefined,
          gioi_tinh: orderData.customer?.sexual || undefined,
          brand: orderData.brand || orderData.customer?.brand,
        });

        const result = await this.fastApiInvoiceFlowService.createSalesOrder({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });
        // We might want to persist partial status or return immediately
        return {
          success: true, // Assuming result check handles status
          message: 'Tạo Sales Order thành công (Sales Order Only mode)',
          result: result,
        };
      }

      // Delegate to orchestrator
      return await this.invoiceFlowOrchestratorService.orchestrateInvoiceCreation(
        docCode,
        orderData,
        forceRetry,
      );
    } catch (error: any) {
      this.logger.error(
        `Unexpected error processing order ${docCode}: ${error?.message || error}`,
      );
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: '',
        maKh: '',
        tenKh: '',
        ngayCt: new Date(),
        status: 0,
        message: `Lỗi hệ thống: ${error?.message || error}`,
        guid: null,
      });
      return {
        success: false,
        message: `Lỗi hệ thống: ${error?.message || error}`,
        result: null,
      };
    }
  }

  /**
   * Helper: Find and enrich order data
   */
  public async findByOrderCode(docCode: string) {
    // 1. [REFACTORED] Reuse Frontend Logic (SalesQueryService)
    // This fetches Sales, performs robust 1-1 Stick Transfer matching, and enriches data.
    const formattedSales =
      await this.salesQueryService.findByOrderCode(docCode);

    if (!formattedSales || formattedSales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // 2. Fetch Cashio (Required for Payment Payload)
    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code = :docCode', { docCode })
      .orWhere('cashio.master_code = :docCode', { docCode })
      .getMany();

    const ecoinCashio = cashioRecords.find((c) => c.fop_syscode === 'ECOIN');
    const voucherCashio = cashioRecords.find(
      (c) => c.fop_syscode === 'VOUCHER',
    );
    const selectedCashio =
      ecoinCashio || voucherCashio || cashioRecords[0] || null;

    // 3. Fetch Stock Transfers (Raw) for Root Payload
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { createdAt: 'ASC' },
    });

    // 4. Construct Order Data
    const firstSale = formattedSales[0];

    return {
      docCode,
      docDate: firstSale.docDate || new Date(),
      docSourceType: firstSale.docSourceType || null,
      ordertype: firstSale.ordertype || null,
      ordertypeName: firstSale.ordertypeName || null,
      branchCode: firstSale.branchCode || null,
      customer: firstSale.customer || null,
      brand: firstSale.brand || firstSale.customer?.brand || null,
      sourceCompany: firstSale.brand || firstSale.customer?.brand || null,
      sales: formattedSales,
      cashio: selectedCashio,
      stockTransfers,
    };
  }

  /**
   * Persist FastApiInvoice record
   */
  async saveFastApiInvoice(data: {
    docCode: string;
    maDvcs?: string;
    maKh?: string;
    tenKh?: string;
    ngayCt?: Date;
    status: number;
    message?: string;
    guid?: string | null;
    fastApiResponse?: string;
  }): Promise<FastApiInvoice> {
    return this.salesQueryService.saveFastApiInvoice(data);
  }

  /**
   * Update isProcessed status for sales
   */
  async markOrderAsProcessed(docCode: string): Promise<void> {
    return this.salesQueryService.markOrderAsProcessed(docCode);
  }

  /**
   * Retroactive fix: Mark processed orders based on existing invoices
   */
  async markProcessedOrdersFromInvoices(): Promise<{
    updated: number;
    message: string;
  }> {
    return this.salesQueryService.markProcessedOrdersFromInvoices();
  }

  /**
   * Tạo stock transfer từ STOCK_TRANSFER data
   * NOTE: This method is kept here as it's warehouse-related and may be moved to SalesWarehouseService later
   */
  async createStockTransfer(createDto: any): Promise<any> {
    // Implementation kept from original - this is a separate concern
    // TODO: Consider moving to SalesWarehouseService in future refactoring
    throw new Error('Not implemented - to be moved to SalesWarehouseService');
  }

  /**
   * Xử lý tạo hóa đơn cho danh sách đơn hàng trong khoảng thời gian (Phase 2)
   */
  async processInvoicesByDateRange(
    startDate: string,
    endDate: string,
  ): Promise<{
    success: boolean;
    message: string;
    totalProcessed: number;
    successCount: number;
    failedCount: number;
    errors: string[];
    details: Array<{
      date: string;
      processed: number;
      success: number;
      failed: number;
    }>;
  }> {
    const parseDate = (dateStr: string): Date => {
      const day = parseInt(dateStr.substring(0, 2));
      const monthStr = dateStr.substring(2, 5).toUpperCase();
      const year = parseInt(dateStr.substring(5, 9));
      const monthMap: Record<string, number> = {
        JAN: 0,
        FEB: 1,
        MAR: 2,
        APR: 3,
        MAY: 4,
        JUN: 5,
        JUL: 6,
        AUG: 7,
        SEP: 8,
        OCT: 9,
        NOV: 10,
        DEC: 11,
      };
      return new Date(year, monthMap[monthStr] || 0, day);
    };

    const formatDate = (date: Date): string => {
      const day = date.getDate().toString().padStart(2, '0');
      const months = [
        'JAN',
        'FEB',
        'MAR',
        'APR',
        'MAY',
        'JUN',
        'JUL',
        'AUG',
        'SEP',
        'OCT',
        'NOV',
        'DEC',
      ];
      return `${day}${months[date.getMonth()]}${date.getFullYear()}`;
    };

    const start = parseDate(startDate);
    const end = parseDate(endDate);
    const errors: string[] = [];
    const details: Array<{
      date: string;
      processed: number;
      success: number;
      failed: number;
    }> = [];
    let totalProcessed = 0;
    let totalSuccess = 0;
    let totalFailed = 0;

    let currentDate = new Date(start);
    while (currentDate <= end) {
      const dateStr = formatDate(currentDate);
      this.logger.log(`[Phase 2] Bắt đầu xử lý hóa đơn cho ngày ${dateStr}...`);

      try {
        const startOfDay = new Date(currentDate);
        startOfDay.setHours(0, 0, 0, 0);
        const endOfDay = new Date(currentDate);
        endOfDay.setHours(23, 59, 59, 999);

        const sales = await this.saleRepository
          .createQueryBuilder('sale')
          .select('DISTINCT sale.docCode', 'docCode')
          .where('sale.docDate >= :startOfDay AND sale.docDate <= :endOfDay', {
            startOfDay,
            endOfDay,
          })
          .getRawMany();

        const docCodes = sales.map((s) => s.docCode);
        this.logger.log(
          `[Phase 2] Tìm thấy ${docCodes.length} đơn hàng cho ngày ${dateStr}`,
        );

        let daySuccess = 0;
        let dayFailed = 0;

        // OPTIMIZED: Parallelize with Concurrency Limit
        const CONCURRENCY_LIMIT = 5;
        const chunks: string[][] = [];
        for (let i = 0; i < docCodes.length; i += CONCURRENCY_LIMIT) {
          chunks.push(docCodes.slice(i, i + CONCURRENCY_LIMIT));
        }

        for (const chunk of chunks) {
          const chunkPromises = chunk.map(async (docCode) => {
            try {
              // Gọi hàm createInvoiceViaFastApi cho từng đơn (để handle cả logic _X)
              // ForceRetry = false để skip các đơn đã thành công rồi
              const result = await this.createInvoiceViaFastApi(docCode, false);
              return { docCode, result, error: null };
            } catch (err: any) {
              return { docCode, result: null, error: err };
            }
          });

          const chunkResults = await Promise.all(chunkPromises);

          for (const res of chunkResults) {
            totalProcessed++;
            if (res.error) {
              dayFailed++;
              totalFailed++;
              errors.push(`[${dateStr}] ${res.docCode}: ${res.error.message}`);
            } else if (res.result) {
              const result = res.result;
              if (result.success || result.alreadyExists) {
                daySuccess++;
                totalSuccess++;
              } else {
                dayFailed++;
                totalFailed++;
                errors.push(`[${dateStr}] ${res.docCode}: ${result.message}`);
              }
            }
          }
        }

        details.push({
          date: dateStr,
          processed: docCodes.length,
          success: daySuccess,
          failed: dayFailed,
        });
      } catch (error: any) {
        const msg = `Lỗi khi xử lý ngày ${dateStr}: ${error.message}`;
        this.logger.error(msg);
        errors.push(msg);
      }

      currentDate.setDate(currentDate.getDate() + 1);
    }

    return {
      success: totalFailed === 0,
      message: `Hoàn tất xử lý hóa đơn từ ${startDate} đến ${endDate}. Tổng: ${totalProcessed}, Thành công: ${totalSuccess}, Lỗi: ${totalFailed}`,
      totalProcessed,
      successCount: totalSuccess,
      failedCount: totalFailed,
      errors,
      details,
    };
  }
  async retryFailedInvoices(): Promise<{
    processed: number;
    success: number;
    failed: number;
    results: any[];
  }> {
    this.logger.log('[Retry] Starting batch retry for failed invoices...');

    // 1. Get all failed invoices
    const failedInvoices = await this.fastApiInvoiceRepository.find({
      where: { status: 0 },
      select: ['docCode', 'status', 'id', 'updatedAt'], // Select basic fields
      order: { updatedAt: 'DESC' }, // Process newest failures first
    });

    if (failedInvoices.length === 0) {
      return { processed: 0, success: 0, failed: 0, results: [] };
    }

    this.logger.log(
      `[Retry] Found ${failedInvoices.length} failed invoices. Processing...`,
    );

    const results: any[] = [];
    let successCount = 0;
    let failCount = 0;

    // 2. Process each invoice
    const BATCH_SIZE = 5; // Parallel concurrency
    // Process in chunks to avoid overwhelming the system
    for (let i = 0; i < failedInvoices.length; i += BATCH_SIZE) {
      const chunk = failedInvoices.slice(i, i + BATCH_SIZE);
      const chunkPromises = chunk.map(async (invoice) => {
        try {
          // Force retry = true
          const result = await this.processSingleOrder(invoice.docCode, true);
          return {
            docCode: invoice.docCode,
            success: result.success,
            message: result.message,
            error: result.success ? null : result.message || 'Unknown error',
          };
        } catch (error: any) {
          return {
            docCode: invoice.docCode,
            success: false,
            message: error?.message || 'Exception during retry',
            error: error?.message,
          };
        }
      });

      const chunkResults = await Promise.all(chunkPromises);

      chunkResults.forEach((r) => {
        if (r.success) successCount++;
        else failCount++;
        results.push(r);
      });

      // Small delay between chunks
      if (i + BATCH_SIZE < failedInvoices.length) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `[Retry] Completed. Success: ${successCount}, Failed: ${failCount}`,
    );

    return {
      processed: failedInvoices.length,
      success: successCount,
      failed: failCount,
      results,
    };
  }
} // End Class
