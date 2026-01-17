import {
  Injectable,
  Logger,
  NotFoundException,
  BadRequestException,
} from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { Sale } from '../../entities/sale.entity';
import { ProductItem } from '../../entities/product-item.entity';

import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { Invoice } from '../../entities/invoice.entity';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { N8nService } from '../../services/n8n.service';

import { InvoiceValidationService } from '../../services/invoice-validation.service';
import { CategoriesService } from '../categories/categories.service';

import * as _ from 'lodash';
import * as SalesUtils from '../../utils/sales.utils';
import * as StockTransferUtils from '../../utils/stock-transfer.utils';
import * as ConvertUtils from '../../utils/convert.utils';
import * as SalesCalculationUtils from '../../utils/sales-calculation.utils';
import { InvoiceLogicUtils } from '../../utils/invoice-logic.utils';
import * as SalesFormattingUtils from '../../utils/sales-formatting.utils';

import { SalesQueryService } from './sales-query.service';
import { SalesPayloadService } from './sales-payload.service';

import { PaymentService } from '../payment/payment.service';
import { forwardRef, Inject } from '@nestjs/common';

@Injectable()
export class SalesInvoiceService {
  private readonly logger = new Logger(SalesInvoiceService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,

    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    private httpService: HttpService,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private fastApiService: FastApiClientService,
    private loyaltyService: LoyaltyService,
    private n8nService: N8nService,
    private invoiceValidationService: InvoiceValidationService,
    private categoriesService: CategoriesService,

    private salesQueryService: SalesQueryService,
    private salesPayloadService: SalesPayloadService,
    @Inject(forwardRef(() => PaymentService))
    private paymentService: PaymentService,
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
          return await this.handleSaleOrderWithUnderscoreX(
            orderData,
            docCode ?? '',
            1,
          );
        } else {
          return await this.handleSaleOrderWithUnderscoreX(
            orderData,
            docCode ?? '',
            0,
          );
        }
      }

      // Có stock transfer (hoặc trường hợp còn lại) --> xử lý bình thường
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

      // Fetch maDvcs from Loyalty API
      let maDvcs = orderData.branchCode || '';
      try {
        const fetchedDvcs = await this.loyaltyService.fetchMaDvcs(
          orderData.branchCode,
        );
        if (fetchedDvcs) maDvcs = fetchedDvcs;
      } catch (e) {
        // Ignore error
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
      if (docSourceType === 'SALE_RETURN') {
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
          await this.saveFastApiInvoice({
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
        return await this.handleSaleReturnFlow(orderData, docCode);
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
      const hasServiceOrder = sales.some((s: any) => {
        const normalized = normalizeOrderType(s.ordertypeName || s.ordertype);
        return (
          normalized === '02. làm dịch vụ' || normalized === '02.làm dịch vụ'
        );
      });

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
          await this.saveFastApiInvoice({
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
        return await this.executeServiceOrderFlow(orderData, docCode);
      }

      // 2. Đổi điểm
      if (hasDoiDiemOrder) {
        return await this.handleStandardSpecialOrder(
          orderData,
          docCode,
          '03. Đổi điểm',
        );
      }

      // 3. Đổi DV (có payment)
      if (hasDoiDvOrder) {
        return await this.handleNormalOrder(orderData, docCode);
      }

      // 4. Tặng sinh nhật
      if (hasTangSinhNhatOrder) {
        return await this.handleStandardSpecialOrder(
          orderData,
          docCode,
          '05. Tặng sinh nhật',
        );
      }

      // 5. Đầu tư
      if (hasDauTuOrder) {
        return await this.handleStandardSpecialOrder(
          orderData,
          docCode,
          '06. Đầu tư',
        );
      }

      // 6. Đổi vỏ
      if (hasDoiVoOrder) {
        return await this.handleStandardSpecialOrder(
          orderData,
          docCode,
          'Đổi vỏ',
        );
      }

      // 7. Tách thẻ (có fetch card)
      if (hasTachTheOrder) {
        return await this.handleStandardSpecialOrder(
          orderData,
          docCode,
          '08. Tách thẻ',
          async () => {
            // Gọi API get_card để lấy issue_partner_code cho đơn "08. Tách thẻ"
            try {
              const cardResponse =
                await this.n8nService.fetchCardDataWithRetry(docCode);
              const cardData = this.n8nService.parseCardData(cardResponse);
              this.n8nService.mapIssuePartnerCodeToSales(
                orderData.sales || [],
                cardData,
              );
            } catch (e) {
              // Ignore error as per original logic
            }
          },
        );
      }

      // 8. Đơn thường (01. Thường / 07. Bán tài khoản)
      return await this.handleNormalOrder(orderData, docCode);
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
   * Helper xử lý các đơn hàng đặc biệt (Đổi điểm, Tặng sinh nhật, Đầu tư...)
   * Chỉ tạo Sales Order, không tạo Sales Invoice
   */
  /**
   * Wrapper execute common invoice action flow:
   * Try -> Process -> Log -> Save DB -> Mark Processed (optional) -> Return
   */
  private async executeInvoiceAction(
    docCode: string,
    orderData: any,
    actionName: string,
    processFn: () => Promise<{
      result: any;
      status: number;
      message: string;
      guid?: string;
      fastApiResponse?: any; // To store full JSON response
    }>,
    shouldMarkProcessed: boolean = true,
  ): Promise<any> {
    this.logger.log(`[SalesInvoice] Bắt đầu ${actionName}: ${docCode}`);
    try {
      const { result, status, message, guid, fastApiResponse } =
        await processFn();

      // Save invoice status
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: orderData.branchCode || '', // Fallback, usually overridden by existing record or specific logic if needed
        maKh: orderData.customer?.code || '',
        tenKh: orderData.customer?.name || '',
        ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
        status: status,
        message: message,
        guid: guid,
        fastApiResponse: JSON.stringify(fastApiResponse || result),
      });

      if (status === 1 && shouldMarkProcessed) {
        await this.markOrderAsProcessed(docCode);
      }

      return {
        success: status === 1,
        message: message,
        result: result,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi ${actionName} cho ${docCode}: ${error?.message || error}`,
      );
      // Log failure
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
   * Helper xử lý các đơn hàng đặc biệt (Đổi điểm, Tặng sinh nhật, Đầu tư...)
   * Chỉ tạo Sales Order, không tạo Sales Invoice
   */
  private async handleStandardSpecialOrder(
    orderData: any,
    docCode: string,
    description: string,
    beforeAction?: () => Promise<void>,
  ): Promise<any> {
    return this.executeInvoiceAction(
      docCode,
      orderData,
      `Xử lý đơn hàng ${description}`,
      async () => {
        if (beforeAction) {
          await beforeAction();
        }

        const invoiceData =
          await this.salesPayloadService.buildFastApiInvoiceData(orderData);

        // Call createSalesOrder
        const result = await this.fastApiInvoiceFlowService.createSalesOrder({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });

        const responseStatus =
          Array.isArray(result) && result.length > 0 && result[0].status === 1
            ? 1
            : 0;
        const apiMessage =
          Array.isArray(result) && result.length > 0 && result[0].message
            ? result[0].message
            : '';
        const responseMessage =
          responseStatus === 1
            ? `${description} thành công: ${apiMessage}`
            : `${description} thất bại: ${apiMessage}`;

        const responseGuid =
          Array.isArray(result) && result.length > 0 && result[0].guid
            ? Array.isArray(result[0].guid)
              ? result[0].guid[0]
              : result[0].guid
            : null;

        return {
          result,
          status: responseStatus,
          message: responseMessage,
          guid: responseGuid,
        };
      },
    );
  }

  /**
   * Helper xử lý đơn thường và đơn bán tài khoản
   */

  private async handleNormalOrder(
    orderData: any,
    docCode: string,
  ): Promise<any> {
    return this.executeInvoiceAction(
      docCode,
      orderData,
      'Xử lý đơn thường/DoiDV/TaiKhoan',
      async () => {
        // 1. Create/Update Customer
        if (orderData.customer?.code) {
          await this.fastApiInvoiceFlowService.createOrUpdateCustomer({
            ma_kh: SalesUtils.normalizeMaKh(orderData.customer.code),
            ten_kh: orderData.customer.name || '',
            dia_chi: orderData.customer.address || undefined,
            dien_thoai:
              orderData.customer.mobile ||
              orderData.customer.phone ||
              undefined,
            so_cccd: orderData.customer.idnumber || undefined,
            ngay_sinh: orderData.customer?.birthday
              ? ConvertUtils.formatDateYYYYMMDD(orderData.customer.birthday)
              : undefined,
            gioi_tinh: orderData.customer.sexual || undefined,
          });
        }

        const invoiceData =
          await this.salesPayloadService.buildFastApiInvoiceData(orderData);

        // 2. Create Sales Order
        const soResult = await this.fastApiInvoiceFlowService.createSalesOrder({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });

        const isSoSuccess =
          (Array.isArray(soResult) &&
            soResult.length > 0 &&
            soResult[0].status === 1) ||
          (soResult && soResult.status === 1);

        if (!isSoSuccess) {
          const message =
            Array.isArray(soResult) && soResult[0]?.message
              ? soResult[0].message
              : soResult?.message || 'Tạo Sales Order thất bại';
          return {
            status: 0,
            message: `Tạo Sales Order thất bại: ${message}`,
            result: { salesOrder: soResult },
            fastApiResponse: { salesOrder: soResult },
          };
        }

        // 3. Create Sales Invoice
        let siResult: any;
        try {
          siResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          });
        } catch (error: any) {
          const responseMessage =
            error?.response?.data?.message || error?.message || '';
          const isDuplicateError =
            typeof responseMessage === 'string' &&
            (responseMessage.toLowerCase().includes('đã tồn tại') ||
              responseMessage.toLowerCase().includes('pk_d81'));

          if (isDuplicateError) {
            return {
              status: 1,
              message: `Đơn hàng ${docCode} đã tồn tại trong Fast API`,
              result: error?.response?.data || {},
              fastApiResponse: error?.response?.data || {},
            };
          }
          // throw error;
          // Return failure with soResult preserved
          return {
            status: 0,
            message: `Lỗi tạo Sales Invoice: ${error?.message || error}`,
            result: {
              salesOrder: soResult,
              salesInvoiceError: error?.message || error,
            },
            fastApiResponse: {
              salesOrder: soResult,
              salesInvoiceError: error?.message || error,
            },
          };
        }

        const isSiSuccess =
          (Array.isArray(siResult) &&
            siResult.length > 0 &&
            siResult[0].status === 1) ||
          (siResult && siResult.status === 1);

        if (!isSiSuccess) {
          const message =
            Array.isArray(siResult) && siResult[0]?.message
              ? siResult[0].message
              : siResult?.message || 'Tạo Sales Invoice thất bại';
          return {
            status: 0,
            message: `Tạo Sales Invoice thất bại: ${message}`,
            result: { salesOrder: soResult, salesInvoice: siResult },
            fastApiResponse: { salesOrder: soResult, salesInvoice: siResult },
          };
        }

        // 4. Cashio Payment (Synced via PaymentMethod API)
        const cashioResult = {
          cashReceiptResults: [],
          creditAdviceResults: [],
        };

        try {
          const paymentDataList =
            await this.paymentService.findPaymentByDocCode(docCode);
          if (paymentDataList && paymentDataList.length > 0) {
            this.logger.log(
              `[Cashio] Found ${paymentDataList.length} payment records for order ${docCode}. Processing...`,
            );
            for (const paymentData of paymentDataList) {
              await this.fastApiInvoiceFlowService.processCashioPayment(
                paymentData,
              );
            }
            this.logger.log(
              `[Cashio] Successfully triggered payment sync for order ${docCode}`,
            );
          } else {
            this.logger.log(
              `[Cashio] No payment records found for order ${docCode}`,
            );
          }
        } catch (err) {
          this.logger.error(
            `[Cashio] Error processing payment sync for order ${docCode}: ${err?.message || err}`,
          );
        }

        // 5. Payment (Stock)
        let paymentResult: any = null;
        try {
          const docCodesForStockTransfer =
            StockTransferUtils.getDocCodesForStockTransfer([docCode]);
          const stockTransfers = await this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          });
          const stockCodes = Array.from(
            new Set(stockTransfers.map((st) => st.stockCode).filter(Boolean)),
          );

          if (stockCodes.length > 0) {
            paymentResult = await this.fastApiInvoiceFlowService.processPayment(
              docCode,
              orderData,
              invoiceData,
              stockCodes,
            );
          }
        } catch (e) {
          this.logger.warn(`[Payment] warning: ${e}`);
        }

        // 6. Build Result
        const responseStatus =
          siResult &&
          Array.isArray(siResult) &&
          siResult.length > 0 &&
          siResult[0].status === 1
            ? 1
            : 0;
        const responseMessage =
          responseStatus === 1
            ? 'Tạo hóa đơn thành công'
            : 'Tạo hóa đơn thất bại';
        const responseGuid =
          siResult && Array.isArray(siResult) && siResult.length > 0
            ? siResult[0].guid
            : null;

        return {
          result: {
            salesOrder: soResult,
            salesInvoice: siResult,
            cashio: cashioResult,
            payment: paymentResult,
          },
          status: responseStatus,
          message: responseMessage,
          guid: responseGuid,
          fastApiResponse: {
            salesOrder: soResult,
            salesInvoice: siResult,
            cashio: cashioResult,
            payment: paymentResult,
          },
        };
      },
    );
  }

  private async saveFastApiInvoice(data: {
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
    try {
      // Kiểm tra xem đã có chưa
      const existing = await this.fastApiInvoiceRepository.findOne({
        where: { docCode: data.docCode },
      });

      if (existing) {
        // Cập nhật record hiện có
        existing.status = data.status;
        existing.message = data.message || existing.message;
        existing.guid = data.guid || existing.guid;
        existing.fastApiResponse =
          data.fastApiResponse || existing.fastApiResponse;
        if (data.maDvcs) existing.maDvcs = data.maDvcs;
        if (data.maKh) existing.maKh = data.maKh;
        if (data.tenKh) existing.tenKh = data.tenKh;
        if (data.ngayCt) existing.ngayCt = data.ngayCt;

        const saved = await this.fastApiInvoiceRepository.save(existing);
        return Array.isArray(saved) ? saved[0] : saved;
      } else {
        // Tạo mới
        const fastApiInvoice = this.fastApiInvoiceRepository.create({
          docCode: data.docCode,
          maDvcs: data.maDvcs ?? null,
          maKh: data.maKh ?? null,
          tenKh: data.tenKh ?? null,
          ngayCt: data.ngayCt ?? new Date(),
          status: data.status,
          message: data.message ?? null,
          guid: data.guid ?? null,
          fastApiResponse: data.fastApiResponse ?? null,
        } as Partial<FastApiInvoice>);

        const saved = await this.fastApiInvoiceRepository.save(fastApiInvoice);
        return Array.isArray(saved) ? saved[0] : saved;
      }
    } catch (error: any) {
      this.logger.error(
        `Error saving FastApiInvoice for ${data.docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  private async markOrderAsProcessed(docCode: string): Promise<void> {
    // Tìm tất cả các sale có cùng docCode
    const sales = await this.saleRepository.find({
      where: { docCode },
    });

    // Cập nhật isProcessed = true cho tất cả các sale
    if (sales.length > 0) {
      await this.saleRepository.update({ docCode }, { isProcessed: true });
    }
  }

  /**
   * Đánh dấu lại các đơn hàng đã có invoice là đã xử lý
   * Method này dùng để xử lý các invoice đã được tạo trước đó
   */
  async markProcessedOrdersFromInvoices(): Promise<{
    updated: number;
    message: string;
  }> {
    // Tìm tất cả các invoice đã được in (isPrinted = true)
    const invoices = await this.invoiceRepository.find({
      where: { isPrinted: true },
    });

    let updatedCount = 0;
    const processedDocCodes = new Set<string>();

    // Duyệt qua các invoice và tìm docCode từ key
    // Key có thể là docCode hoặc có format INV_xxx_xxx
    for (const invoice of invoices) {
      let docCode: string | null = null;

      // Thử 1: Key chính là docCode (cho các invoice mới)
      const salesByKey = await this.saleRepository.find({
        where: { docCode: invoice.key },
        take: 1,
      });
      if (salesByKey.length > 0) {
        docCode = invoice.key;
      } else {
        // Thử 2: Tìm trong printResponse xem có docCode không
        try {
          if (invoice.printResponse) {
            const printResponse = JSON.parse(invoice.printResponse);

            // Tìm trong Message (là JSON string chứa array)
            if (printResponse.Message) {
              try {
                const messageData = JSON.parse(printResponse.Message);
                if (Array.isArray(messageData) && messageData.length > 0) {
                  const data = messageData[0];
                  if (data.key) {
                    // Extract docCode từ key (format: SO52.00005808_X -> SO52.00005808)
                    const keyParts = data.key.split('_');
                    if (keyParts.length > 0) {
                      const potentialDocCode = keyParts[0];
                      const salesByPotentialKey =
                        await this.saleRepository.find({
                          where: { docCode: potentialDocCode },
                          take: 1,
                        });
                      if (salesByPotentialKey.length > 0) {
                        docCode = potentialDocCode;
                      }
                    }
                  }
                }
              } catch (msgError) {
                // Message không phải JSON string, bỏ qua
              }
            }

            // Thử tìm trong Data nếu có
            if (
              !docCode &&
              printResponse.Data &&
              Array.isArray(printResponse.Data) &&
              printResponse.Data.length > 0
            ) {
              const data = printResponse.Data[0];
              if (data.key) {
                const keyParts = data.key.split('_');
                if (keyParts.length > 0) {
                  const potentialDocCode = keyParts[0];
                  const salesByPotentialKey = await this.saleRepository.find({
                    where: { docCode: potentialDocCode },
                    take: 1,
                  });
                  if (salesByPotentialKey.length > 0) {
                    docCode = potentialDocCode;
                  }
                }
              }
            }
          }
        } catch (error) {
          // Ignore parse errors
        }
      }

      // Nếu tìm thấy docCode, đánh dấu các sale là đã xử lý
      if (docCode && !processedDocCodes.has(docCode)) {
        const updateResult = await this.saleRepository.update(
          { docCode },
          { isProcessed: true },
        );
        if (updateResult.affected && updateResult.affected > 0) {
          updatedCount += updateResult.affected;
          processedDocCodes.add(docCode);
        }
      }
    }

    return {
      updated: updatedCount,
      message: `Đã đánh dấu ${processedDocCodes.size} đơn hàng là đã xử lý (${updatedCount} sale records)`,
    };
  }

  async findByOrderCode(docCode: string) {
    // Lấy tất cả sales có cùng docCode (cùng đơn hàng)
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    if (sales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // Join với daily_cashio để lấy cashio data
    // Join dựa trên: cashio.so_code = docCode HOẶC cashio.master_code = docCode
    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code = :docCode', { docCode })
      .orWhere('cashio.master_code = :docCode', { docCode })
      .getMany();

    // Ưu tiên ECOIN, sau đó VOUCHER, sau đó các loại khác
    const ecoinCashio = cashioRecords.find((c) => c.fop_syscode === 'ECOIN');
    const voucherCashio = cashioRecords.find(
      (c) => c.fop_syscode === 'VOUCHER',
    );
    const selectedCashio =
      ecoinCashio || voucherCashio || cashioRecords[0] || null;

    // Lấy tất cả itemCode unique từ sales
    const itemCodes = SalesUtils.extractUniqueItemCodes(sales);

    // Load tất cả products một lần
    const products =
      itemCodes.length > 0
        ? await this.productItemRepository.find({
            where: { maERP: In(itemCodes) },
          })
        : [];

    // Tạo map để lookup nhanh
    const productMap = SalesUtils.createProductMap(products);

    // Fetch card data và tạo card code map
    const [dataCard] = await this.n8nService.fetchCardData(docCode);
    const cardCodeMap = SalesUtils.createCardCodeMap(dataCard);

    // Enrich sales với product information từ database và card code
    const enrichedSales = sales.map((sale) => {
      const saleWithProduct = SalesUtils.enrichSaleWithProduct(
        sale,
        productMap,
      );
      return SalesUtils.enrichSaleWithCardCode(saleWithProduct, cardCodeMap);
    });

    // Fetch products từ Loyalty API cho các itemCode không có trong database hoặc không có dvt
    // BỎ QUA các sale có statusAsys = false (đơn lỗi) - không fetch từ Loyalty API
    const loyaltyProductMap = new Map<string, any>();
    // Filter itemCodes: chỉ fetch cho các sale không phải đơn lỗi
    const validItemCodes = SalesUtils.filterValidItemCodes(itemCodes, sales);

    // Fetch products từ Loyalty API sử dụng LoyaltyService
    if (validItemCodes.length > 0) {
      const fetchedProducts =
        await this.loyaltyService.fetchProducts(validItemCodes);
      fetchedProducts.forEach((product, itemCode) => {
        loyaltyProductMap.set(itemCode, product);
      });
    }

    // Enrich sales với product từ Loyalty API (thêm dvt từ unit)
    const enrichedSalesWithLoyalty = enrichedSales.map((sale) =>
      SalesUtils.enrichSaleWithLoyaltyProduct(sale, loyaltyProductMap),
    );

    // Fetch departments để lấy ma_dvcs
    const branchCodes = SalesUtils.extractUniqueBranchCodes(sales);

    const departmentMap = new Map<string, any>();
    // Fetch departments parallel để tối ưu performance
    if (branchCodes.length > 0) {
      const departmentPromises = branchCodes.map(async (branchCode) => {
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
            { headers: { accept: 'application/json' } },
          );
          const department = response?.data?.data?.items?.[0];
          return { branchCode, department };
        } catch (error) {
          this.logger.warn(
            `Failed to fetch department for branchCode ${branchCode}: ${error}`,
          );
          return { branchCode, department: null };
        }
      });

      const departmentResults = await Promise.all(departmentPromises);
      departmentResults.forEach(({ branchCode, department }) => {
        if (department) {
          departmentMap.set(branchCode, department);
        }
      });
    }

    // Fetch stock transfers để lấy ma_nx (ST* và RT* từ stock transfer)
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    // Sử dụng materialCode đã được lưu trong database (đã được đồng bộ từ Loyalty API khi sync)
    // Nếu chưa có materialCode trong database, mới fetch từ Loyalty API
    const stockTransferItemCodesWithoutMaterialCode = Array.from(
      new Set(
        stockTransfers
          .filter((st) => st.itemCode && !st.materialCode)
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    // Chỉ fetch materialCode cho các itemCode chưa có materialCode trong database
    const stockTransferLoyaltyMap = new Map<string, any>();
    if (stockTransferItemCodesWithoutMaterialCode.length > 0) {
      const fetchedStockTransferProducts =
        await this.loyaltyService.fetchProducts(
          stockTransferItemCodesWithoutMaterialCode,
        );
      fetchedStockTransferProducts.forEach((product, itemCode) => {
        stockTransferLoyaltyMap.set(itemCode, product);
      });
    }

    // Tạo map: soCode_materialCode -> stock transfer (phân biệt ST và RT)
    // Match theo: Mã ĐH (soCode) = Số hóa đơn (docCode) VÀ Mã SP (itemCode) -> materialCode = Mã hàng (ma_vt)
    // Ưu tiên dùng materialCode đã lưu trong database, nếu chưa có thì lấy từ Loyalty API
    // Lưu ý: Dùng array để lưu tất cả stock transfers cùng key (tránh ghi đè khi có nhiều records giống nhau)
    const stockTransferMapBySoCodeAndMaterialCode = new Map<
      string,
      { st?: StockTransfer[]; rt?: StockTransfer[] }
    >();
    stockTransfers.forEach((st) => {
      // Ưu tiên dùng materialCode đã lưu trong database
      // Nếu chưa có thì lấy từ Loyalty API (đã fetch ở trên)
      const materialCode =
        st.materialCode ||
        stockTransferLoyaltyMap.get(st.itemCode)?.materialCode;
      if (!materialCode) {
        // Bỏ qua nếu không có materialCode (không match được)
        return;
      }

      // Key: soCode_materialCode (Mã ĐH_Mã hàng từ Loyalty API)
      const soCode = st.soCode || st.docCode || docCode;
      const key = `${soCode}_${materialCode}`;

      if (!stockTransferMapBySoCodeAndMaterialCode.has(key)) {
        stockTransferMapBySoCodeAndMaterialCode.set(key, {});
      }
      const itemMap = stockTransferMapBySoCodeAndMaterialCode.get(key)!;
      // ST* - dùng array để lưu tất cả
      if (st.docCode.startsWith('ST')) {
        if (!itemMap.st) {
          itemMap.st = [];
        }
        itemMap.st.push(st);
      }
      // RT* - dùng array để lưu tất cả
      if (st.docCode.startsWith('RT')) {
        if (!itemMap.rt) {
          itemMap.rt = [];
        }
        itemMap.rt.push(st);
      }
    });

    // Enrich sales với department information và lấy maKho từ stock transfer
    const enrichedSalesWithDepartment = await Promise.all(
      enrichedSalesWithLoyalty.map(async (sale) => {
        const department = sale.branchCode
          ? departmentMap.get(sale.branchCode) || null
          : null;
        const maBp = department?.ma_bp || sale.branchCode || null;

        // Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
        const saleLoyaltyProduct = sale.itemCode
          ? loyaltyProductMap.get(sale.itemCode)
          : null;
        const saleMaterialCode = saleLoyaltyProduct?.materialCode;
        const finalMaKho =
          await this.salesQueryService.getMaKhoFromStockTransfer(
            sale,
            docCode,
            stockTransfers,
            saleMaterialCode,
          );

        // Lấy ma_nx từ stock transfer (phân biệt ST và RT)
        // Match stock transfer để lấy ma_nx
        const matchedStockTransfer = stockTransfers.find(
          (st) => st.soCode === docCode && st.itemCode === sale.itemCode,
        );
        const firstSt =
          matchedStockTransfer && matchedStockTransfer.docCode.startsWith('ST')
            ? matchedStockTransfer
            : null;
        const firstRt =
          matchedStockTransfer && matchedStockTransfer.docCode.startsWith('RT')
            ? matchedStockTransfer
            : null;

        return {
          ...sale,
          department: department,
          maKho: finalMaKho,
          // Thêm ma_nx từ stock transfer (lấy từ record đầu tiên)
          ma_nx_st: firstSt?.docCode || null, // ST* - mã nghiệp vụ từ stock transfer
          ma_nx_rt: firstRt?.docCode || null, // RT* - mã nghiệp vụ từ stock transfer
        };
      }),
    );

    // Tính tổng doanh thu của đơn hàng
    const totalRevenue = sales.reduce(
      (sum, sale) => sum + Number(sale.revenue),
      0,
    );
    const totalQty = sales.reduce((sum, sale) => sum + Number(sale.qty), 0);

    // Lấy thông tin chung từ sale đầu tiên
    const firstSale = sales[0];

    // Lấy thông tin khuyến mại từ Loyalty API cho các promCode trong đơn hàng
    // Fetch parallel để tối ưu performance
    const promotionsByCode: Record<string, any> = {};
    const uniquePromCodes = Array.from(
      new Set(
        sales
          .map((s) => s.promCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    if (uniquePromCodes.length > 0) {
      const promotionPromises = uniquePromCodes.map(async (promCode) => {
        try {
          // Gọi Loyalty API theo externalCode = promCode
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/promotions/item/external/${promCode}`,
            {
              headers: { accept: 'application/json' },
              timeout: 5000, // Timeout 5s để tránh chờ quá lâu
            },
          );

          const data = response?.data;
          return { promCode, data };
        } catch (error) {
          // Chỉ log error nếu không phải 404 (không tìm thấy promotion là bình thường)
          if (error?.response?.status !== 404) {
            this.logger.warn(
              `Lỗi khi lấy promotion cho promCode ${promCode}: ${error?.message || error}`,
            );
          }
          return { promCode, data: null };
        }
      });

      const promotionResults = await Promise.all(promotionPromises);
      promotionResults.forEach(({ promCode, data }) => {
        promotionsByCode[promCode] = {
          raw: data,
          main: data || null,
        };
      });
    }

    // Gắn promotion tương ứng vào từng dòng sale (chỉ để trả ra API, không lưu DB)
    // Và tính lại muaHangCkVip nếu chưa có hoặc cần override cho f3
    // Format sales giống findAllOrders để đảm bảo consistency với frontend
    // Format sales sau khi đã enrich promotion
    // Gắn promotion tương ứng vào từng dòng sale (chỉ để trả ra API, không lưu DB)
    // Và tính lại muaHangCkVip nếu chưa có hoặc cần override cho f3
    // Format sales giống findAllOrders để đảm bảo consistency với frontend
    // Format sales sau khi đã enrich promotion
    // [UPDATE] Tách line theo stock transfer nếu có
    const formattedSales: any[] = [];

    // 1. Prepare base sales data (Pre-format all sales lines)
    const preFormattedSales: any[] = [];
    for (const sale of enrichedSalesWithDepartment) {
      const loyaltyProduct = sale.itemCode
        ? loyaltyProductMap.get(sale.itemCode)
        : null;
      const department = sale.branchCode
        ? departmentMap.get(sale.branchCode)
        : null;
      const calculatedFields = SalesCalculationUtils.calculateSaleFields(
        sale,
        loyaltyProduct,
        department,
        sale.branchCode,
      );

      const orderForFormatting = {
        customer: firstSale.customer || null,
        cashioData: cashioRecords,
        cashioFopSyscode: selectedCashio?.fop_syscode || null,
        cashioTotalIn: selectedCashio?.total_in || null,
        brand: firstSale.customer?.brand || null,
        docDate: firstSale.docDate,
      };

      // Base format cho sale line gốc
      const formattedSale = await SalesFormattingUtils.formatSaleForFrontend(
        sale,
        loyaltyProduct,
        department,
        calculatedFields,
        orderForFormatting,
        this.categoriesService,
        this.loyaltyService,
        // stockTransfers,
      );

      // Thêm promotion info nếu có
      const promCode = sale.promCode;
      const promotion =
        promCode && promotionsByCode[promCode]
          ? promotionsByCode[promCode]
          : null;

      const baseSaleData = {
        ...formattedSale,
        promotion,
        promotionDisplayCode: SalesUtils.getPromotionDisplayCode(promCode),
      };

      preFormattedSales.push(baseSaleData);
    }

    // 2. Build final list using Stock Transfers as ROOT
    // Create pool of available stock transfers (filtered)
    const availableStockTransfers = stockTransfers.filter(
      (st) => st.doctype === 'SALE_STOCKOUT' || Number(st.qty || 0) < 0,
    );
    const usedSaleIds = new Set<string>();

    if (availableStockTransfers.length > 0) {
      // Loop available STs (ROOT)
      for (const st of availableStockTransfers) {
        // Find matched sale (case insensitive)
        const sale = preFormattedSales.find(
          (s) =>
            s.itemCode === st.itemCode ||
            s.itemCode?.toLowerCase().trim() ===
              st.itemCode?.toLowerCase().trim(),
        );

        let displayStockCode = st.stockCode || '';
        if (displayStockCode) {
          // Manual map as helper might be expensive in loop, or assume raw code.
          // But preserving original logic: try to map.
          const mapped =
            await this.categoriesService.mapWarehouseCode(displayStockCode);
          if (mapped) displayStockCode = mapped;
        }

        if (sale) {
          usedSaleIds.add(sale.id);
          const oldQty = Number(sale.qty || 1) || 1;
          const newQty = Math.abs(Number(st.qty || 0));
          const ratio = newQty / oldQty;

          formattedSales.push({
            ...sale,
            id: st.id || sale.id, // Use ST id for uniqueness
            qty: newQty, // Use ST qty
            // Recalculate financial fields
            revenue: Number(sale.revenue || 0) * ratio,
            tienHang: Number(sale.tienHang || 0) * ratio,
            linetotal: Number(sale.linetotal || 0) * ratio,
            discount: Number(sale.discount || 0) * ratio,
            chietKhauMuaHangGiamGia:
              Number(sale.chietKhauMuaHangGiamGia || 0) * ratio,
            other_discamt: Number(sale.other_discamt || 0) * ratio,

            maKho: displayStockCode,
            maLo: st.batchSerial || sale.maLo,
            soSerial: st.batchSerial || sale.soSerial,
            isStockTransferLine: true,
            stockTransferId: st.id,
            stockTransfer:
              StockTransferUtils.formatStockTransferForFrontend(st), // Singular ST
            stockTransfers: undefined, // Ensure no list
          });
        } else {
          // Fallback if no sale match
          formattedSales.push({
            // Minimal structure - try to copy what we can from ST
            id: st.id,
            docCode: st.docCode,
            itemCode: st.itemCode,
            itemName: st.itemName,
            qty: Math.abs(Number(st.qty || 0)),
            maKho: displayStockCode,
            maLo: st.batchSerial,
            soSerial: st.batchSerial,
            isStockTransferLine: true,
            stockTransferId: st.id,
            stockTransfer:
              StockTransferUtils.formatStockTransferForFrontend(st), // Singular ST
            stockTransfers: undefined,
            // Default values
            price: 0,
            revenue: 0,
          });
        }
      }

      // 3. Add remaining Sales lines (e.g. Services)
      preFormattedSales.forEach((s) => {
        if (!usedSaleIds.has(s.id)) formattedSales.push(s);
      });
    } else {
      // No STs, use original
      formattedSales.push(...preFormattedSales);
    }

    // Format customer object để match với frontend interface
    const formattedCustomer = firstSale.customer
      ? {
          ...firstSale.customer,
          // Map mobile -> phone nếu phone chưa có
          phone: firstSale.customer.phone || firstSale.customer.mobile || null,
          // Map address -> street nếu street chưa có
          street:
            firstSale.customer.street || firstSale.customer.address || null,
        }
      : null;

    return {
      docCode: firstSale.docCode,
      docDate: firstSale.docDate,
      branchCode: firstSale.branchCode,
      docSourceType:
        firstSale.docSourceType || (firstSale as any).docSourceType || null,
      customer: formattedCustomer,
      totalRevenue,
      totalQty,
      totalItems: sales.length,
      sales: formattedSales,
      promotions: promotionsByCode,
      // Cashio data từ join với daily_cashio
      cashioData: cashioRecords.length > 0 ? cashioRecords : null,
      cashioFopSyscode: selectedCashio?.fop_syscode || null,
      cashioFopDescription: selectedCashio?.fop_description || null,
      cashioCode: selectedCashio?.code || null,
      cashioMasterCode: selectedCashio?.master_code || null,
      cashioTotalIn: selectedCashio?.total_in || null,
      cashioTotalOut: selectedCashio?.total_out || null,
    };
  }

  /**
   * Xử lý flow tạo hóa đơn cho đơn hàng dịch vụ (02. Làm dịch vụ)
   * Flow:
   * 1. Customer (tạo/cập nhật)
   * 2. SalesOrder (tất cả dòng: I, S, V...)
   * 3. SalesInvoice (chỉ dòng productType = 'S')
   * 4. GxtInvoice (S → detail, I → ndetail)
   */
  private async executeServiceOrderFlow(
    orderData: any,
    docCode: string,
  ): Promise<any> {
    try {
      this.logger.log(
        `[ServiceOrderFlow] Bắt đầu xử lý đơn dịch vụ ${docCode}`,
      );

      const sales = orderData.sales || [];
      if (sales.length === 0) {
        throw new Error(`Đơn hàng ${docCode} không có sale item nào`);
      }

      // Step 1: Tạo/cập nhật Customer
      if (orderData.customer?.code) {
        await this.fastApiInvoiceFlowService.createOrUpdateCustomer({
          ma_kh: SalesUtils.normalizeMaKh(orderData.customer.code),
          ten_kh: orderData.customer.name || '',
          dia_chi: orderData.customer.address || undefined,
          dien_thoai:
            orderData.customer.mobile || orderData.customer.phone || undefined,
          so_cccd: orderData.customer.idnumber || undefined,
          ngay_sinh: orderData.customer?.birthday
            ? ConvertUtils.formatDateYYYYMMDD(orderData.customer.birthday)
            : undefined,
          gioi_tinh: orderData.customer.sexual || undefined,
        });
      }

      // Build invoice data cho tất cả sales (dùng để tạo SalesOrder)
      const invoiceData =
        await this.salesPayloadService.buildFastApiInvoiceData(orderData);

      // Step 2: Tạo SalesOrder cho TẤT CẢ dòng (I, S, V...)
      this.logger.log(
        `[ServiceOrderFlow] Tạo SalesOrder cho ${sales.length} dòng`,
      );
      const salesOrderResult =
        await this.fastApiInvoiceFlowService.createSalesOrder({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });

      // Validate Sales Order Result
      const isSoSuccess =
        (Array.isArray(salesOrderResult) &&
          salesOrderResult.length > 0 &&
          salesOrderResult[0].status === 1) ||
        (salesOrderResult && salesOrderResult.status === 1);

      if (!isSoSuccess) {
        const message =
          Array.isArray(salesOrderResult) && salesOrderResult[0]?.message
            ? salesOrderResult[0].message
            : salesOrderResult?.message || 'Tạo Sales Order thất bại';
        const error: any = new Error(message);
        error.response = { data: salesOrderResult }; // Attach response for logging
        throw error;
      }

      // Step 3: Tạo SalesInvoice CHỈ cho các dòng dịch vụ
      // Logic: Dòng dịch vụ là dòng có svc_code (item được dùng trong dịch vụ)
      // hoặc productType = 'S' (dịch vụ thuần túy)
      const serviceLines = sales.filter((s: any) => {
        // Fallback: Check productType = 'S' (dịch vụ thuần túy)
        const productType = s.productType?.toUpperCase()?.trim();
        if (productType === 'S') {
          return true;
        }

        // Không phải dòng dịch vụ
        return false;
      });

      let salesInvoiceResult: any = null;
      if (serviceLines.length > 0) {
        this.logger.log(
          `[ServiceOrderFlow] Tạo SalesInvoice cho ${serviceLines.length} dòng dịch vụ (productType = 'S')`,
        );

        // Build invoice data chỉ cho service lines
        const serviceInvoiceData =
          await this.salesPayloadService.buildFastApiInvoiceDataForServiceLines(
            orderData,
            serviceLines,
          );

        salesInvoiceResult =
          await this.fastApiInvoiceFlowService.createSalesInvoice({
            ...serviceInvoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || serviceInvoiceData.ong_ba || '',
          });

        const isSiSuccess =
          (Array.isArray(salesInvoiceResult) &&
            salesInvoiceResult.length > 0 &&
            salesInvoiceResult[0].status === 1) ||
          (salesInvoiceResult && salesInvoiceResult.status === 1);

        if (!isSiSuccess) {
          const message =
            Array.isArray(salesInvoiceResult) && salesInvoiceResult[0]?.message
              ? salesInvoiceResult[0].message
              : salesInvoiceResult?.message || 'Tạo Sales Invoice thất bại';
          const error: any = new Error(message);
          error.response = { data: salesInvoiceResult };
          throw error;
        }
      } else {
        this.logger.log(
          `[ServiceOrderFlow] Không có dòng dịch vụ (productType = 'S'), bỏ qua SalesInvoice`,
        );
      }

      let cashioResult: any = {
        cashReceiptResults: [],
        creditAdviceResults: [],
      };

      try {
        const paymentDataList =
          await this.paymentService.findPaymentByDocCode(docCode);
        if (paymentDataList && paymentDataList.length > 0) {
          this.logger.log(
            `[Cashio] Found ${paymentDataList.length} payment records for order ${docCode}. Processing...`,
          );
          for (const paymentData of paymentDataList) {
            await this.fastApiInvoiceFlowService.processCashioPayment(
              paymentData,
            );
          }
          // Note: Since we are using the new sync flow, we don't return specific cashReceipt/creditAdvice results here
          // But we can Log success
          this.logger.log(
            `[Cashio] Successfully triggered payment sync for order ${docCode}`,
          );
        } else {
          this.logger.log(
            `[Cashio] No payment records found for order ${docCode}`,
          );
        }
      } catch (err) {
        this.logger.error(
          `[Cashio] Error processing payment sync for order ${docCode}: ${err?.message || err}`,
        );
      }

      if (salesInvoiceResult) {
        // Xử lý payment (Phiếu chi tiền mặt) nếu có mã kho
        // CHỈ xử lý cho đơn có docSourceType = ORDER_RETURN hoặc SALE_RETURN
        try {
          // Kiểm tra docSourceType - chỉ xử lý payment cho ORDER_RETURN hoặc SALE_RETURN
          const firstSale =
            orderData.sales && orderData.sales.length > 0
              ? orderData.sales[0]
              : null;
          const docSourceTypeRaw =
            firstSale?.docSourceType || orderData.docSourceType || '';
          const docSourceType = docSourceTypeRaw
            ? String(docSourceTypeRaw).trim().toUpperCase()
            : '';

          if (
            docSourceType === 'ORDER_RETURN' ||
            docSourceType === 'SALE_RETURN'
          ) {
            const docCodesForStockTransfer =
              StockTransferUtils.getDocCodesForStockTransfer([docCode]);
            const stockTransfers = await this.stockTransferRepository.find({
              where: { soCode: In(docCodesForStockTransfer) },
            });
            const stockCodes = Array.from(
              new Set(stockTransfers.map((st) => st.stockCode).filter(Boolean)),
            );

            if (stockCodes.length > 0) {
              this.logger.log(
                `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (02. Làm dịch vụ, docSourceType: ${docSourceType}) với ${stockCodes.length} mã kho`,
              );
              const paymentResult =
                await this.fastApiInvoiceFlowService.processPayment(
                  docCode,
                  orderData,
                  invoiceData,
                  stockCodes,
                );

              if (
                paymentResult.paymentResults &&
                paymentResult.paymentResults.length > 0
              ) {
                this.logger.log(
                  `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`,
                );
              }
              if (
                paymentResult.debitAdviceResults &&
                paymentResult.debitAdviceResults.length > 0
              ) {
                this.logger.log(
                  `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`,
                );
              }
            } else {
              this.logger.debug(
                `[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`,
              );
            }
          } else {
            this.logger.debug(
              `[Payment] Đơn hàng ${docCode} (02. Làm dịch vụ) có docSourceType = "${docSourceType}", không phải ORDER_RETURN/SALE_RETURN, bỏ qua payment API`,
            );
          }
        } catch (paymentError: any) {
          // Log lỗi nhưng không fail toàn bộ flow
          this.logger.warn(
            `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${paymentError?.message || paymentError}`,
          );
        }
      }

      // Step 4: Tạo GxtInvoice (nếu có productType = 'S')
      let gxtInvoiceResult: any = null;
      if (serviceLines.length > 0) {
        // Tìm các dòng productType=I
        const exportLines = sales.filter((s: any) => {
          const productType = s?.productType?.toUpperCase()?.trim();
          return productType === 'I';
        });

        // Chỉ tạo GxtInvoice nếu có cả I (xuất) và S (nhập)
        if (exportLines.length > 0) {
          const gxtData = await this.salesPayloadService.buildGxtInvoiceData(
            orderData,
            serviceLines,
            exportLines,
          );

          try {
            gxtInvoiceResult =
              await this.fastApiInvoiceFlowService.createGxtInvoice({
                ...gxtData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || gxtData.ong_ba || '',
              });
          } catch (gxtError: any) {
            this.logger.warn(
              `[ServiceOrderFlow] Tạo GxtInvoice thất bại: ${gxtError?.message || gxtError}`,
            );
          }
        }
      }

      // Lưu kết quả vào bảng kê hóa đơn
      // Status = 1 chỉ khi salesInvoice thành công (vì salesInvoice là quan trọng nhất cho dịch vụ)
      const responseStatus = salesInvoiceResult ? 1 : 0;
      const responseMessage = salesInvoiceResult
        ? 'Tạo sales order và sales invoice thành công (02. Làm dịch vụ)'
        : 'Tạo sales order và sales invoice thất bại (02. Làm dịch vụ)';

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
        maKh: orderData.customer?.code || invoiceData.ma_kh || '',
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: salesInvoiceResult?.guid || null,
        fastApiResponse: JSON.stringify({
          salesOrder: salesOrderResult,
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
        }),
      });

      // Đánh dấu đơn hàng là đã xử lý
      await this.markOrderAsProcessed(docCode);

      return {
        success: true,
        message: responseMessage,
        result: {
          salesOrder: salesOrderResult,
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
        },
      };
    } catch (error: any) {
      this.logger.error(
        `[ServiceOrderFlow] Lỗi khi xử lý đơn dịch vụ ${docCode}: ${error?.message || error}`,
      );

      // Lưu lỗi vào bảng kê hóa đơn
      const invoiceData = await this.salesPayloadService
        .buildFastApiInvoiceData(orderData)
        .catch(() => ({
          ma_dvcs: orderData.branchCode || '',
          ma_kh: SalesUtils.normalizeMaKh(orderData.customer?.code),
          ong_ba: orderData.customer?.name || '',
          ngay_ct: orderData.docDate ? new Date(orderData.docDate) : new Date(),
        }));

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs || orderData.branchCode || '',
        maKh:
          invoiceData.ma_kh ||
          SalesUtils.normalizeMaKh(orderData.customer?.code),
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: 0,
        message: error?.message || 'Tạo hóa đơn dịch vụ thất bại',
        guid: null,
        fastApiResponse: JSON.stringify(error?.response?.data || error),
      });

      throw error;
    }
  }

  /**
   * Tạo stock transfer từ STOCK_TRANSFER data
   */
  async createStockTransfer(createDto: any): Promise<any> {
    try {
      // Group theo doccode để xử lý từng phiếu
      const transferMap = new Map<string, any[]>();

      for (const item of createDto.data) {
        if (!transferMap.has(item.doccode)) {
          transferMap.set(item.doccode, []);
        }
        transferMap.get(item.doccode)!.push(item);
      }

      const results: Array<{
        doccode: string;
        success: boolean;
        result?: any;
        error?: string;
      }> = [];

      for (const [doccode, items] of transferMap.entries()) {
        try {
          // Lấy item đầu tiên để lấy thông tin chung
          const firstItem = items[0];

          // Join với order nếu có so_code
          let orderData: any = null;
          if (firstItem.so_code) {
            try {
              orderData = await this.findByOrderCode(firstItem.so_code);
            } catch (error) {}
          }

          // Build FastAPI stock transfer data
          const stockTransferData =
            await this.salesPayloadService.buildStockTransferData(
              items,
              orderData,
            );

          // Submit to FastAPI
          const result =
            await this.fastApiService.submitStockTransfer(stockTransferData);

          results.push({
            doccode,
            success: true,
            result,
          });
        } catch (error: any) {
          this.logger.error(
            `Error creating stock transfer for ${doccode}: ${error?.message || error}`,
          );
          results.push({
            doccode,
            success: false,
            error: error?.message || 'Unknown error',
          });
        }
      }

      return {
        success: true,
        results,
        total: results.length,
        successCount: results.filter((r) => r.success).length,
        failedCount: results.filter((r) => !r.success).length,
      };
    } catch (error: any) {
      this.logger.error(
        `Error creating stock transfers: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Xử lý đơn hàng có đuôi _X (ví dụ: SO45.01574458_X)
   * Gọi API salesOrder với action: 1
   * Cả đơn có _X và đơn gốc (bỏ _X) đều sẽ có action = 1
   */
  private async handleSaleOrderWithUnderscoreX(
    orderData: any,
    docCode: string,
    action: number,
  ): Promise<any> {
    // Đơn có đuôi _X → Gọi API salesOrder với action: 1
    const invoiceData =
      await this.salesPayloadService.buildFastApiInvoiceData(orderData);
    function removeSuffixX(code: string): string {
      return code.endsWith('_X') ? code.slice(0, -2) : code;
    }
    const docCodeWithoutX = removeSuffixX(docCode);

    // Gọi API salesOrder với action = 1 (không cần tạo/cập nhật customer)
    let result: any;
    const data = {
      ...invoiceData,
      dien_giai: docCodeWithoutX,
      so_ct: docCodeWithoutX,
      ma_kho: orderData?.maKho || '',
    };
    try {
      result = await this.fastApiInvoiceFlowService.createSalesOrder(
        {
          ...data,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        },
        action,
      ); // action = 1 cho đơn hàng có đuôi _X

      // Lưu vào bảng kê hóa đơn
      const responseStatus =
        Array.isArray(result) && result.length > 0 && result[0].status === 1
          ? 1
          : 0;
      const apiMessage =
        Array.isArray(result) && result.length > 0
          ? result[0].message || ''
          : '';
      const shouldUseApiMessage =
        apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
      let responseMessage = '';
      if (responseStatus === 1) {
        responseMessage = shouldUseApiMessage
          ? `Tạo đơn hàng thành công cho đơn hàng ${docCode}. ${apiMessage}`
          : `Tạo đơn hàng thành công cho đơn hàng ${docCode}`;
      } else {
        responseMessage = shouldUseApiMessage
          ? `Tạo đơn hàng thất bại cho đơn hàng ${docCode}. ${apiMessage}`
          : `Tạo đơn hàng thất bại cho đơn hàng ${docCode}`;
      }
      const responseGuid =
        Array.isArray(result) &&
        result.length > 0 &&
        Array.isArray(result[0].guid)
          ? result[0].guid[0]
          : Array.isArray(result) && result.length > 0
            ? result[0].guid
            : null;

      // Xử lý cashio payment (Phiếu thu tiền mặt/Giấy báo có) nếu salesOrder thành công
      let cashioResult: any = null;
      let paymentResult: any = null;
      if (responseStatus === 1) {
        this.logger.log(
          `[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (đơn có đuôi _X)`,
        );
        cashioResult = {
          cashReceiptResults: [],
          creditAdviceResults: [],
        };

        try {
          const paymentDataList =
            await this.paymentService.findPaymentByDocCode(docCode);
          if (paymentDataList && paymentDataList.length > 0) {
            this.logger.log(
              `[Cashio] Found ${paymentDataList.length} payment records for order ${docCode}. Processing...`,
            );
            for (const paymentData of paymentDataList) {
              await this.fastApiInvoiceFlowService.processCashioPayment(
                paymentData,
              );
            }
            this.logger.log(
              `[Cashio] Successfully triggered payment sync for order ${docCode}`,
            );
          } else {
            this.logger.log(
              `[Cashio] No payment records found for order ${docCode}`,
            );
          }
        } catch (err) {
          this.logger.error(
            `[Cashio] Error processing payment sync for order ${docCode}: ${err?.message || err}`,
          );
        }

        if (
          cashioResult.cashReceiptResults &&
          cashioResult.cashReceiptResults.length > 0
        ) {
          this.logger.log(
            `[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
          );
        }
        if (
          cashioResult.creditAdviceResults &&
          cashioResult.creditAdviceResults.length > 0
        ) {
          this.logger.log(
            `[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
          );
        }

        // Xử lý Payment (Phiếu chi tiền mặt/Giấy báo nợ) cho đơn hủy (_X) - cho phép không có mã kho
        try {
          // Kiểm tra có stock transfer không
          const docCodesForStockTransfer =
            StockTransferUtils.getDocCodesForStockTransfer([docCode]);
          const stockTransfers = await this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          });
          const stockCodes = Array.from(
            new Set(stockTransfers.map((st) => st.stockCode).filter(Boolean)),
          );

          // Cho đơn _X: Gọi payment ngay cả khi không có mã kho (đơn hủy không có khái niệm xuất kho)
          const allowWithoutStockCodes = stockCodes.length === 0;

          if (allowWithoutStockCodes || stockCodes.length > 0) {
            this.logger.log(
              `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (đơn có đuôi _X) - ${allowWithoutStockCodes ? 'không có mã kho' : `với ${stockCodes.length} mã kho`}`,
            );
            paymentResult = await this.fastApiInvoiceFlowService.processPayment(
              docCode,
              orderData,
              invoiceData,
              stockCodes,
              allowWithoutStockCodes, // Cho phép gọi payment ngay cả khi không có mã kho
            );

            if (
              paymentResult.paymentResults &&
              paymentResult.paymentResults.length > 0
            ) {
              this.logger.log(
                `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
              );
            }
            if (
              paymentResult.debitAdviceResults &&
              paymentResult.debitAdviceResults.length > 0
            ) {
              this.logger.log(
                `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
              );
            }
          }
        } catch (paymentError: any) {
          // Log lỗi nhưng không fail toàn bộ flow
          this.logger.warn(
            `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode} (đơn có đuôi _X): ${paymentError?.message || paymentError}`,
          );
        }
      }

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: responseGuid || null,
        fastApiResponse: JSON.stringify({
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        }),
      });

      return {
        success: responseStatus === 1,
        message: responseMessage,
        result: {
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        },
      };
    } catch (error: any) {
      // Lấy thông báo lỗi chính xác từ Fast API response
      let errorMessage = 'Tạo đơn hàng thất bại';

      if (error?.response?.data) {
        const errorData = error.response.data;
        if (Array.isArray(errorData) && errorData.length > 0) {
          errorMessage =
            errorData[0].message || errorData[0].error || errorMessage;
        } else if (errorData.message) {
          errorMessage = errorData.message;
        } else if (errorData.error) {
          errorMessage = errorData.error;
        } else if (typeof errorData === 'string') {
          errorMessage = errorData;
        }
      } else if (error?.message) {
        errorMessage = error.message;
      }

      // Format error message
      const shouldUseApiMessage =
        errorMessage && errorMessage.trim().toUpperCase() !== 'OK';
      const formattedErrorMessage = shouldUseApiMessage
        ? `Tạo đơn hàng thất bại cho đơn hàng ${docCode}. ${errorMessage}`
        : `Tạo đơn hàng thất bại cho đơn hàng ${docCode}`;

      // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: 0,
        message: formattedErrorMessage,
        guid: null,
        fastApiResponse: JSON.stringify(error?.response?.data || error),
      });

      this.logger.error(
        `SALE_ORDER with _X suffix creation failed for order ${docCode}: ${formattedErrorMessage}`,
      );

      return {
        success: false,
        message: formattedErrorMessage,
        result: error?.response?.data || error,
      };
    }
  }

  private async handleSaleReturnFlow(
    orderData: any,
    docCode: string,
  ): Promise<any> {
    // Kiểm tra xem có stock transfer không
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
    });

    // Case 1: Có stock transfer → Gọi API salesReturn
    if (stockTransfers && stockTransfers.length > 0) {
      return this.executeInvoiceAction(
        docCode,
        orderData,
        'Tạo hàng bán trả lại',
        async () => {
          // Build salesReturn data
          const salesReturnStockTransfers = stockTransfers.filter(
            (stockTransfer) => stockTransfer.doctype === 'SALE_RETURN',
          );
          const salesReturnData =
            await this.salesPayloadService.buildSalesReturnData(
              orderData,
              salesReturnStockTransfers,
            );

          // Gọi API salesReturn (không cần tạo/cập nhật customer)
          const result =
            await this.fastApiInvoiceFlowService.createSalesReturn(
              salesReturnData,
            );

          const responseStatus =
            Array.isArray(result) && result.length > 0 && result[0].status === 1
              ? 1
              : 0;
          const apiMessage =
            Array.isArray(result) && result.length > 0 ? result[0].message : '';
          const shouldAppendApiMessage =
            apiMessage && apiMessage.trim().toUpperCase() !== 'OK';

          let responseMessage = '';
          if (responseStatus === 1) {
            responseMessage = shouldAppendApiMessage
              ? `Tạo hàng bán trả lại thành công cho đơn hàng ${docCode}. ${apiMessage}`
              : `Tạo hàng bán trả lại thành công cho đơn hàng ${docCode}`;
          } else {
            responseMessage = shouldAppendApiMessage
              ? `Tạo hàng bán trả lại thất bại cho đơn hàng ${docCode}. ${apiMessage}`
              : `Tạo hàng bán trả lại thất bại cho đơn hàng ${docCode}`;
          }

          const responseGuid =
            Array.isArray(result) &&
            result.length > 0 &&
            Array.isArray(result[0].guid)
              ? result[0].guid[0]
              : Array.isArray(result) && result.length > 0
                ? result[0].guid
                : null;

          // Xử lý Payment (Phiếu chi tiền mặt) nếu có mã kho
          if (responseStatus === 1) {
            try {
              const stockCodes = Array.from(
                new Set(
                  stockTransfers.map((st) => st.stockCode).filter(Boolean),
                ),
              );

              if (stockCodes.length > 0) {
                // Build invoiceData để dùng cho payment (tương tự như các case khác)
                const invoiceData =
                  await this.salesPayloadService.buildFastApiInvoiceData(
                    orderData,
                  );

                this.logger.log(
                  `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (SALE_RETURN) với ${stockCodes.length} mã kho`,
                );
                const paymentResult =
                  await this.fastApiInvoiceFlowService.processPayment(
                    docCode,
                    orderData,
                    invoiceData,
                    stockCodes,
                  );

                if (
                  paymentResult.paymentResults &&
                  paymentResult.paymentResults.length > 0
                ) {
                  this.logger.log(
                    `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (SALE_RETURN)`,
                  );
                }
                if (
                  paymentResult.debitAdviceResults &&
                  paymentResult.debitAdviceResults.length > 0
                ) {
                  this.logger.log(
                    `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (SALE_RETURN)`,
                  );
                }
              } else {
                this.logger.debug(
                  `[Payment] Đơn hàng ${docCode} (SALE_RETURN) không có mã kho, bỏ qua payment API`,
                );
              }
            } catch (paymentError: any) {
              // Log lỗi nhưng không fail toàn bộ flow
              this.logger.warn(
                `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode} (SALE_RETURN): ${paymentError?.message || paymentError}`,
              );
            }
          }

          return {
            result,
            status: responseStatus,
            message: responseMessage,
            guid: responseGuid,
          };
        },
        false, // shouldMarkProcessed = false
      );
    }

    // Case 2: Không có stock transfer → Không xử lý (bỏ qua)
    // SALE_RETURN không có stock transfer không cần xử lý
    await this.saveFastApiInvoice({
      docCode,
      maDvcs: orderData.branchCode || '',
      maKh: orderData.customer?.code || '',
      tenKh: orderData.customer?.name || '',
      ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
      status: 0,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
      guid: null,
      fastApiResponse: undefined,
    });

    return {
      success: false,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
      result: null,
    };
  }
} // End class
