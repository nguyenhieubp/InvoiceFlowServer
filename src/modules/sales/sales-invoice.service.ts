import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { Sale } from '../../entities/sale.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { InvoiceDataEnrichmentService } from './invoice-data-enrichment.service';
import { InvoicePersistenceService } from './invoice-persistence.service';
import { InvoiceFlowOrchestratorService } from './invoice-flow-orchestrator.service';
import { SaleReturnHandlerService } from './sale-return-handler.service';
import * as StockTransferUtils from '../../utils/stock-transfer.utils';
import * as _ from 'lodash';

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
    private invoiceDataEnrichmentService: InvoiceDataEnrichmentService,
    private invoicePersistenceService: InvoicePersistenceService,
    private invoiceFlowOrchestratorService: InvoiceFlowOrchestratorService,
    private saleReturnHandlerService: SaleReturnHandlerService,
  ) {}

  /**
   * Tạo hóa đơn qua Fast API từ đơn hàng
   */
  async createInvoiceViaFastApi(
    docCode: string,
    forceRetry: boolean = false,
  ): Promise<any> {
    try {
      // ============================================
      // 1. CHECK INVOICE ĐÃ TẠO
      // ============================================
      if (!forceRetry) {
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
          return await this.saleReturnHandlerService.handleSaleOrderWithUnderscoreX(
            orderData,
            docCode ?? '',
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
      return await this.processSingleOrder(docCode, forceRetry);
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
  ): Promise<any> {
    try {
      // Kiểm tra xem đơn hàng đã có trong bảng kê hóa đơn chưa (đã tạo thành công)
      // Nếu forceRetry = true, bỏ qua check này để cho phép retry
      if (!forceRetry) {
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
      await this.invoicePersistenceService.saveFastApiInvoice({
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
   * Delegate to InvoiceDataEnrichmentService
   */
  async findByOrderCode(docCode: string) {
    return this.invoiceDataEnrichmentService.findByOrderCode(docCode);
  }

  /**
   * Delegate to InvoicePersistenceService
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
    return this.invoicePersistenceService.saveFastApiInvoice(data);
  }

  /**
   * Delegate to InvoicePersistenceService
   */
  async markOrderAsProcessed(docCode: string): Promise<void> {
    return this.invoicePersistenceService.markOrderAsProcessed(docCode);
  }

  /**
   * Đánh dấu lại các đơn hàng đã có invoice là đã xử lý
   * Method này dùng để xử lý các invoice đã được tạo trước đó
   */
  async markProcessedOrdersFromInvoices(): Promise<{
    updated: number;
    message: string;
  }> {
    return this.invoicePersistenceService.markProcessedOrdersFromInvoices();
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
}
