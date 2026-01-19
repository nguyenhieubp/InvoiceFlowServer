import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { InvoiceValidationService } from '../../services/invoice-validation.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { SpecialOrderHandlerService } from './special-order-handler.service';
import { NormalOrderHandlerService } from './normal-order-handler.service';
import { SaleReturnHandlerService } from './sale-return-handler.service';
import { InvoicePersistenceService } from './invoice-persistence.service';
import * as SalesUtils from '../../utils/sales.utils';
import * as StockTransferUtils from '../../utils/stock-transfer.utils';
import {
  DOC_SOURCE_TYPES,
  ORDER_TYPES,
  STATUS,
  isServiceOrder,
} from './sales-invoice.constants';

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
  async orchestrateInvoiceCreation(
    docCode: string,
    orderData: any,
    forceRetry: boolean = false,
  ): Promise<any> {
    try {
      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(
          `Order ${docCode} not found or has no sales`,
        );
      }

      // Fetch maDvcs from Loyalty API (only if not already provided)
      let maDvcs = orderData.branchCode || '';

      // Skip API call if branchCode already exists
      // Skip API call if branchCode already exists
      if (!maDvcs) {
        maDvcs = '';
      }

      // ============================================
      // BƯỚC 1: Kiểm tra docSourceType trước (ưu tiên cao nhất)
      // ============================================
      const firstSale =
        orderData.sales && orderData.sales.length > 0
          ? orderData.sales[0]
          : null;
      const docSourceTypeRaw =
        firstSale?.docSourceType ?? orderData.docSourceType ?? '';
      const docSourceType = docSourceTypeRaw
        ? String(docSourceTypeRaw).trim().toUpperCase()
        : '';

      // Xử lý SALE_RETURN
      // Nhưng vẫn phải validate chỉ cho phép "01.Thường" và "01. Thường"
      if (docSourceType === DOC_SOURCE_TYPES.SALE_RETURN) {
        // Validate chỉ cho phép "01.Thường" và "01. Thường"
        const validationResult =
          this.invoiceValidationService.validateOrderForInvoice({
            docCode,
            sales: orderData.sales,
          });
        if (!validationResult.success) {
          const errorMessage =
            validationResult.message ||
            `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          await this.invoicePersistenceService.saveFastApiInvoice({
            docCode,
            maDvcs: maDvcs,
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: STATUS.FAILED,
            message: errorMessage,
            guid: null,
            fastApiResponse: undefined,
          });
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
      // BƯỚC 2: Validate điều kiện tạo hóa đơn TRƯỚC khi xử lý các case đặc biệt
      // ============================================
      const sales = orderData.sales || [];
      const normalizeOrderType = (
        ordertypeName: string | null | undefined,
      ): string => {
        if (!ordertypeName) return '';
        return String(ordertypeName).trim().toLowerCase();
      };

      // Kiểm tra các loại đơn đặc biệt được phép xử lý
      const hasDoiDiemOrder = sales.some((s: any) =>
        SalesUtils.isDoiDiemOrder(s.ordertype, s.ordertypeName),
      );
      const hasDoiDvOrder = sales.some((s: any) =>
        SalesUtils.isDoiDvOrder(s.ordertype, s.ordertypeName),
      );
      const hasTangSinhNhatOrder = sales.some((s: any) =>
        SalesUtils.isTangSinhNhatOrder(s.ordertype, s.ordertypeName),
      );
      const hasDauTuOrder = sales.some((s: any) =>
        SalesUtils.isDauTuOrder(s.ordertype, s.ordertypeName),
      );
      const hasTachTheOrder = sales.some((s: any) =>
        SalesUtils.isTachTheOrder(s.ordertype, s.ordertypeName),
      );
      const hasDoiVoOrder = sales.some((s: any) =>
        SalesUtils.isDoiVoOrder(s.ordertype, s.ordertypeName),
      );
      const hasServiceOrder = sales.some((s: any) =>
        isServiceOrder(s.ordertypeName || s.ordertype),
      );

      // Nếu không phải các loại đơn đặc biệt được phép, validate chỉ cho phép "01.Thường"
      if (
        !hasDoiDiemOrder &&
        !hasDoiDvOrder &&
        !hasTangSinhNhatOrder &&
        !hasDauTuOrder &&
        !hasTachTheOrder &&
        !hasDoiVoOrder &&
        !hasServiceOrder
      ) {
        const validationResult =
          this.invoiceValidationService.validateOrderForInvoice({
            docCode,
            sales: orderData.sales,
          });

        if (!validationResult.success) {
          const errorMessage =
            validationResult.message ||
            `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
          await this.invoicePersistenceService.saveFastApiInvoice({
            docCode,
            maDvcs: maDvcs,
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: null,
          };
        }
      }

      // ============================================
      // BƯỚC 3: Xử lý các case đặc biệt (sau khi đã validate)
      // ============================================

      // 1. Dịch vụ
      if (hasServiceOrder) {
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.specialOrderHandlerService.executeServiceOrderFlow(
              orderData,
              docCode,
            ),
          true, // shouldMarkProcessed
        );
      }

      // 2. Đổi điểm
      if (hasDoiDiemOrder) {
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.specialOrderHandlerService.handleStandardSpecialOrder(
              orderData,
              docCode,
              ORDER_TYPES.LOYALTY_EXCHANGE,
            ),
          true,
        );
      }

      // 3. Đổi DV (có payment)
      if (hasDoiDvOrder) {
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

      // 4. Tặng sinh nhật
      if (hasTangSinhNhatOrder) {
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.specialOrderHandlerService.handleStandardSpecialOrder(
              orderData,
              docCode,
              ORDER_TYPES.BIRTHDAY_GIFT,
            ),
          true,
        );
      }

      // 5. Đầu tư
      if (hasDauTuOrder) {
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.specialOrderHandlerService.handleStandardSpecialOrder(
              orderData,
              docCode,
              ORDER_TYPES.INVESTMENT,
            ),
          true,
        );
      }

      // 6. Đổi vỏ
      if (hasDoiVoOrder) {
        return await this.executeWithPersistence(
          docCode,
          orderData,
          maDvcs,
          async () =>
            await this.specialOrderHandlerService.handleStandardSpecialOrder(
              orderData,
              docCode,
              ORDER_TYPES.BOTTLE_EXCHANGE,
            ),
          true,
        );
      }

      // 7. Tách thẻ (có fetch card)
      if (hasTachTheOrder) {
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

      // 8. Đơn thường (01. Thường / 07. Bán tài khoản)
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
      this.logger.error(`Lỗi khi xử lý ${docCode}: ${error?.message || error}`);
      // Log failure
      await this.invoicePersistenceService.saveFastApiInvoice({
        docCode,
        maDvcs: maDvcs || '',
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
}
