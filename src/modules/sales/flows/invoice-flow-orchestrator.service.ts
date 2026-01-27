import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { InvoiceValidationService } from '../../../services/invoice-validation.service';
import { LoyaltyService } from '../../../services/loyalty.service';
import { SpecialOrderHandlerService } from './special-order-handler.service';
import { NormalOrderHandlerService } from './normal-order-handler.service';
import { SaleReturnHandlerService } from './sale-return-handler.service';
import { InvoicePersistenceService } from '../invoice/invoice-persistence.service';
import * as SalesUtils from '../../../utils/sales.utils';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import {
  DOC_SOURCE_TYPES,
  ORDER_TYPES,
  STATUS,
  isServiceOrder,
} from '../constants/sales-invoice.constants';

@Injectable()
export class InvoiceFlowOrchestratorService {
  private readonly logger = new Logger(InvoiceFlowOrchestratorService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    private invoiceValidationService: InvoiceValidationService,
    private loyaltyService: LoyaltyService,
    private specialOrderHandlerService: SpecialOrderHandlerService,
    private normalOrderHandlerService: NormalOrderHandlerService,
    private saleReturnHandlerService: SaleReturnHandlerService,
    private invoicePersistenceService: InvoicePersistenceService,
  ) {}

  /**
   * Main orchestrator method - điều phối flow tạo hóa đơn
   */
  /**
   * Main orchestrator method - điều phối flow tạo hóa đơn
   */
  async orchestrateInvoiceCreation(
    docCode: string,
    orderData: any,
    forceRetry: boolean = false,
  ): Promise<any> {
    const maDvcs = orderData.branchCode || '';
    try {
      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(
          `Order ${docCode} not found or has no sales`,
        );
      }

      // ============================================
      // BƯỚC 1: Kiểm tra docSourceType trước (ưu tiên cao nhất)
      // ============================================
      const firstSale = orderData.sales?.[0];
      const docSourceTypeRaw =
        firstSale?.docSourceType ?? orderData.docSourceType ?? '';
      const docSourceType = docSourceTypeRaw
        ? String(docSourceTypeRaw).trim().toUpperCase()
        : '';

      // Xử lý SALE_RETURN
      if (docSourceType === DOC_SOURCE_TYPES.SALE_RETURN) {
        const validationResult =
          this.invoiceValidationService.validateOrderForInvoice({
            docCode,
            sales: orderData.sales,
          });
        if (!validationResult.success) {
          const errorMessage =
            validationResult.message ||
            `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          await this.recordFailure(docCode, orderData, errorMessage, maDvcs);
          return {
            success: false,
            message: errorMessage,
            result: null,
          };
        }

        const result = await this.saleReturnHandlerService.handleSaleReturnFlow(
          orderData,
          docCode,
        );

        // Save to database
        await this.invoicePersistenceService.saveFastApiInvoice({
          docCode,
          maDvcs: maDvcs,
          maKh: orderData.customer?.code || '',
          tenKh: orderData.customer?.name || '',
          ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
          status: result.status,
          message: result.message,
          guid: result.guid,
          fastApiResponse: JSON.stringify(
            result.fastApiResponse || result.result,
          ),
        });

        return {
          success: result.status === STATUS.SUCCESS,
          message: result.message,
          result: result.result,
        };
      }

      // ============================================
      // BƯỚC 2: Xác định loại đơn hàng (Single Pass Loop)
      // ============================================
      const orderType = this.determineOrderType(orderData.sales);

      // Nếu là đơn thường (NORMAL), validate xem có đủ điều kiện không
      if (orderType === ORDER_TYPES.NORMAL) {
        const validationResult =
          this.invoiceValidationService.validateOrderForInvoice({
            docCode,
            sales: orderData.sales,
          });

        if (!validationResult.success) {
          const errorMessage =
            validationResult.message ||
            `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          await this.recordFailure(docCode, orderData, errorMessage, maDvcs);

          return {
            success: false,
            message: errorMessage,
            result: null,
          };
        }
      }

      // ============================================
      // BƯỚC 3: Routing xử lý theo Order Type
      // ============================================
      // ============================================
      // BƯỚC 3: Routing xử lý theo Order Type
      // ============================================

      // Case 1: Service Order
      if (orderType === ORDER_TYPES.SERVICE) {
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.specialOrderHandlerService.executeServiceOrderFlow(
              orderData,
              docCode,
            ),
          true,
        );
      }

      // Case 2: Normal Order & Normal Exchange
      if (
        orderType === ORDER_TYPES.NORMAL ||
        orderType === ORDER_TYPES.NORMAL_EXCHANGE
      ) {
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.normalOrderHandlerService.handleNormalOrder(
              orderData,
              docCode,
            ),
          true,
        );
      }

      // Case 3: Split Card (Tách thẻ)
      if (orderType === ORDER_TYPES.CARD_SEPARATION) {
        // Previously SPLIT_CARD
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.specialOrderHandlerService.handleTachTheOrder(
              orderData,
              docCode,
            ),
          true,
        );
      }

      // Case 4: Standard Special Orders (Loyalty Exchange, Birthday, Investment, Bottle Exchange)
      // These all share the same handler logic with different descriptions passed inside handleStandardSpecialOrder if needed,
      // but strictly following previous logic: they call handleStandardSpecialOrder(orderData, docCode, orderType)
      return await this.executeWithPersistence(
        docCode,
        orderData,
        maDvcs,
        async () =>
          await this.specialOrderHandlerService.handleStandardSpecialOrder(
            orderData,
            docCode,
            orderType, // Pass the specific order type as description/type
          ),
        true,
      );
    } catch (error: any) {
      const errorMessage = `Lỗi hệ thống: ${error?.message || error}`;
      await this.recordFailure(docCode, orderData, errorMessage, maDvcs);
      return {
        success: false,
        message: errorMessage,
        result: null,
      };
    }
  }

  /**
   * Helper: Xác định loại đơn hàng dựa trên danh sách sales
   * Ưu tiên thứ tự check các loại đặc biệt
   */
  private determineOrderType(sales: any[]): string {
    // Single pass loop could be optimization, but for readability here we use find
    // Given the small number of items per order, multiple finds are negligible,
    // but a manual single pass is "Senior" level optimization.

    for (const s of sales) {
      if (SalesUtils.isDoiDiemOrder(s.ordertype, s.ordertypeName))
        return ORDER_TYPES.LOYALTY_EXCHANGE;
      if (SalesUtils.isDoiDvOrder(s.ordertype, s.ordertypeName))
        return ORDER_TYPES.NORMAL_EXCHANGE;
      if (SalesUtils.isTangSinhNhatOrder(s.ordertype, s.ordertypeName))
        return ORDER_TYPES.BIRTHDAY_GIFT;
      if (SalesUtils.isDauTuOrder(s.ordertype, s.ordertypeName))
        return ORDER_TYPES.INVESTMENT;
      if (SalesUtils.isTachTheOrder(s.ordertype, s.ordertypeName))
        return ORDER_TYPES.CARD_SEPARATION;
      if (SalesUtils.isDoiVoOrder(s.ordertype, s.ordertypeName))
        return ORDER_TYPES.BOTTLE_EXCHANGE;
      if (isServiceOrder(s.ordertypeName || s.ordertype))
        return ORDER_TYPES.SERVICE;
    }
    return ORDER_TYPES.NORMAL;
  }

  private async recordFailure(
    docCode: string,
    orderData: any,
    message: string,
    maDvcs: string,
  ) {
    this.logger.error(`Processing failed for ${docCode}: ${message}`);
    await this.invoicePersistenceService.saveFastApiInvoice({
      docCode,
      maDvcs: maDvcs || '',
      maKh: orderData.customer?.code || '',
      tenKh: orderData.customer?.name || '',
      ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
      status: 0,
      message: message,
      guid: null,
    });
  }

  /**
   * Helper: Execute handler and persist result
   */
  private async executeWithPersistence(
    docCode: string,
    orderData: any,
    maDvcs: string,
    handlerFn: () => Promise<{
      result: any;
      status: number;
      message: string;
      guid?: string;
      fastApiResponse?: any;
    }>,
    shouldMarkProcessed: boolean = true,
  ): Promise<any> {
    this.logger.log(`[Orchestrator] Executing handler for ${docCode}`);
    try {
      const { result, status, message, guid, fastApiResponse } =
        await handlerFn();

      // Save invoice status
      await this.invoicePersistenceService.saveFastApiInvoice({
        docCode,
        maDvcs: maDvcs || orderData.branchCode || '',
        maKh: orderData.customer?.code || '',
        tenKh: orderData.customer?.name || '',
        ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
        status: status,
        message: message,
        guid: guid,
        fastApiResponse: JSON.stringify(fastApiResponse || result),
      });

      if (status === STATUS.SUCCESS && shouldMarkProcessed) {
        await this.invoicePersistenceService.markOrderAsProcessed(docCode);
      }

      return {
        success: status === STATUS.SUCCESS,
        message: message,
        result: result,
      };
    } catch (error: any) {
      const errorMessage = `Lỗi hệ thống: ${error?.message || error}`;
      await this.recordFailure(docCode, orderData, errorMessage, maDvcs);
      return {
        success: false,
        message: errorMessage,
        result: null,
      };
    }
  }
}
