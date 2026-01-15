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
import { SalesQueryService } from './sales-query.service';
import * as SalesFormattingUtils from '../../utils/sales-formatting.utils';

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
            maDvcs: orderData.branchCode || '',
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
            maDvcs: orderData.branchCode || '',
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

        const invoiceData = await this.buildFastApiInvoiceData(orderData);

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

        const invoiceData = await this.buildFastApiInvoiceData(orderData);

        // 2. Create Sales Order
        const soResult = await this.fastApiInvoiceFlowService.createSalesOrder({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });

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
          throw error;
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
    const formattedSales = await Promise.all(
      enrichedSalesWithDepartment.map(async (sale) => {
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

        const formattedSale = await SalesFormattingUtils.formatSaleForFrontend(
          sale,
          loyaltyProduct,
          department,
          calculatedFields,
          orderForFormatting,
          this.categoriesService,
          this.loyaltyService,
          stockTransfers,
        );

        // Thêm promotion info nếu có
        const promCode = sale.promCode;
        const promotion =
          promCode && promotionsByCode[promCode]
            ? promotionsByCode[promCode]
            : null;

        return {
          ...formattedSale,
          promotion,
          promotionDisplayCode: SalesUtils.getPromotionDisplayCode(promCode),
        };
      }),
    );

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
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Step 2: Tạo SalesOrder cho TẤT CẢ dòng (I, S, V...)
      this.logger.log(
        `[ServiceOrderFlow] Tạo SalesOrder cho ${sales.length} dòng`,
      );
      await this.fastApiInvoiceFlowService.createSalesOrder({
        ...invoiceData,
        customer: orderData.customer,
        ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
      });

      // Step 3: Tạo SalesInvoice CHỈ cho productType = 'S'
      const serviceLines = sales.filter((s: any) => {
        const productType = s.producttype.toUpperCase().trim();
        return productType === 'S';
      });

      let salesInvoiceResult: any = null;
      if (serviceLines.length > 0) {
        this.logger.log(
          `[ServiceOrderFlow] Tạo SalesInvoice cho ${serviceLines.length} dòng dịch vụ (productType = 'S')`,
        );

        // Build invoice data chỉ cho service lines
        const serviceInvoiceData =
          await this.buildFastApiInvoiceDataForServiceLines(
            orderData,
            serviceLines,
          );

        salesInvoiceResult =
          await this.fastApiInvoiceFlowService.createSalesInvoice({
            ...serviceInvoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || serviceInvoiceData.ong_ba || '',
          });
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
          const productType = s.producttype.toUpperCase().trim();
          return productType === 'I';
        });

        // Chỉ tạo GxtInvoice nếu có cả I (xuất) và S (nhập)
        if (exportLines.length > 0) {
          const gxtData = await this.buildGxtInvoiceData(
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
          salesOrder: 'success',
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
          salesOrder: 'success',
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
      const invoiceData = await this.buildFastApiInvoiceData(orderData).catch(
        () => ({
          ma_dvcs: orderData.branchCode || '',
          ma_kh: SalesUtils.normalizeMaKh(orderData.customer?.code),
          ong_ba: orderData.customer?.name || '',
          ngay_ct: orderData.docDate ? new Date(orderData.docDate) : new Date(),
        }),
      );

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
   * Build invoice data chỉ cho service lines (productType = 'S')
   */
  private async buildFastApiInvoiceDataForServiceLines(
    orderData: any,
    serviceLines: any[],
  ): Promise<any> {
    // Tạo orderData mới chỉ chứa service lines
    const serviceOrderData = {
      ...orderData,
      sales: serviceLines,
    };

    // Dùng lại logic buildFastApiInvoiceData nhưng với orderData đã filter
    return await this.buildFastApiInvoiceData(serviceOrderData);
  }

  /**
   * Build GxtInvoice data (Phiếu tạo gộp – xuất tách)
   * - detail: các dòng productType = 'I' (xuất)
   * - ndetail: các dòng productType = 'S' (nhập)
   */
  private async buildGxtInvoiceData(
    orderData: any,
    importLines: any[],
    exportLines: any[],
  ): Promise<any> {
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      if (isNaN(d.getTime())) {
        throw new Error('Invalid date');
      }
      return d.toISOString();
    };

    let docDate: Date;
    if (orderData.docDate instanceof Date) {
      docDate = orderData.docDate;
    } else if (typeof orderData.docDate === 'string') {
      docDate = new Date(orderData.docDate);
      if (isNaN(docDate.getTime())) {
        docDate = new Date();
      }
    } else {
      docDate = new Date();
    }

    const ngayCt = formatDateISO(docDate);
    const ngayLct = formatDateISO(docDate);

    const firstSale = orderData.sales?.[0] || {};
    const maDvcs =
      firstSale?.department?.ma_dvcs ||
      firstSale?.department?.ma_dvcs_ht ||
      orderData.customer?.brand ||
      orderData.branchCode ||
      '';

    // Helper để build detail/ndetail item
    const buildLineItem = async (sale: any, index: number): Promise<any> => {
      const toNumber = (value: any, defaultValue: number = 0): number => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        const num = Number(value);
        return isNaN(num) ? defaultValue : num;
      };

      const toString = (value: any, defaultValue: string = ''): string => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        return String(value);
      };

      const limitString = (value: string, maxLength: number): string => {
        if (!value) return '';
        const str = String(value);
        return str.length > maxLength ? str.substring(0, maxLength) : str;
      };

      const qty = toNumber(sale.qty, 0);
      const giaBan = toNumber(sale.giaBan, 0);
      const tienHang = toNumber(
        sale.tienHang || sale.linetotal || sale.revenue,
        0,
      );
      const giaNt2 = giaBan > 0 ? giaBan : qty > 0 ? tienHang / qty : 0;
      const tienNt2 = qty * giaNt2;

      // Lấy materialCode từ Loyalty API
      const materialCode =
        SalesUtils.getMaterialCode(sale, sale.product) || sale.itemCode || '';
      const dvt = toString(
        sale.product?.dvt || sale.product?.unit || sale.dvt,
        'Cái',
      );
      const maLo = toString(sale.maLo || sale.ma_lo, '');
      const maBp = toString(
        sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
        '',
      );

      return {
        ma_kho_n: firstSale?.maKho || '',
        ma_kho_x: firstSale?.maKho || '',
        ma_vt: limitString(materialCode, 16),
        dvt: limitString(dvt, 32),
        ma_lo: limitString(maLo, 16),
        so_luong: Math.abs(qty), // Lấy giá trị tuyệt đối
        gia_nt2: Number(giaNt2),
        tien_nt2: Number(tienNt2),
        ma_nx: 'NX01', // Fix cứng theo yêu cầu
        ma_bp: limitString(maBp, 8),
        dong: index + 1, // Số thứ tự dòng tăng dần (1, 2, 3...)
        dong_vt_goc: 1, // Dòng vật tư gốc luôn là 1
      };
    };

    // Build detail (xuất - productType = 'I')
    const detail = await Promise.all(
      exportLines.map((sale, index) => buildLineItem(sale, index)),
    );

    // Build ndetail (nhập - productType = 'S')
    const ndetail = await Promise.all(
      importLines.map((sale, index) => buildLineItem(sale, index)),
    );

    // Lấy kho nhập và kho xuất (có thể cần map từ branch/department)
    // Tạm thời dùng branchCode làm kho mặc định
    const maKhoN = firstSale?.maKho || '';
    const maKhoX = firstSale?.maKho || '';

    return {
      ma_dvcs: maDvcs,
      ma_kho_n: maKhoN,
      ma_kho_x: maKhoX,
      ong_ba: orderData.customer?.name || '',
      ma_gd: '2', // 1 = Tạo gộp, 2 = Xuất tách (có thể thay đổi theo rule)
      ngay_ct: ngayCt,
      ngay_lct: ngayLct,
      so_ct: orderData.docCode || '',
      dien_giai: orderData.docCode || '',
      action: 0, // 0: Mới, Sửa; 1: Xóa
      detail: detail,
      ndetail: ndetail,
    };
  }

  /**
   * Build invoice data cho Fast API (format mới)
   */
  private async buildFastApiInvoiceData(orderData: any): Promise<any> {
    try {
      // 1. Initialize and validate date
      const docDate = this.parseInvoiceDate(orderData.docDate);
      const ngayCt = this.formatDateKeepLocalDay(docDate);
      const ngayLct = ngayCt;

      const allSales = orderData.sales || [];
      if (allSales.length === 0) {
        throw new Error(
          `Đơn hàng ${orderData.docCode} không có sale item nào, bỏ qua không đồng bộ`,
        );
      }

      // 2. Determine order type (from first sale)
      const { isThuong: isNormalOrder } = InvoiceLogicUtils.getOrderTypes(
        allSales[0]?.ordertypeName || allSales[0]?.ordertype || '',
      );

      // 3. Load supporting data
      const { stockTransferMap, transDate } =
        await this.getInvoiceStockTransferMap(orderData.docCode, isNormalOrder);
      const cardSerialMap = await this.getInvoiceCardSerialMap(
        orderData.docCode,
      );

      // 4. Transform sales to details
      const detail = await Promise.all(
        allSales.map((sale: any, index: number) =>
          this.mapSaleToInvoiceDetail(sale, index, orderData, {
            isNormalOrder,
            stockTransferMap,
            cardSerialMap,
          }),
        ),
      );

      // 5. Build summary (cbdetail)
      const cbdetail = this.buildInvoiceCbDetail(detail);

      // 6. Assemble final payload
      return this.assembleInvoicePayload(orderData, detail, cbdetail, {
        ngayCt,
        ngayLct,
        transDate,
        maBp: detail[0]?.ma_bp || '',
      });
    } catch (error: any) {
      this.logInvoiceError(error, orderData);
      throw new Error(
        `Failed to build invoice data: ${error?.message || error}`,
      );
    }
  }

  /**
   * Build salesReturn data cho Fast API (Hàng bán trả lại)
   * Tương tự như buildFastApiInvoiceData nhưng có thêm các field đặc biệt cho salesReturn
   */
  private async buildSalesReturnData(
    orderData: any,
    stockTransfers: StockTransfer[],
  ): Promise<any> {
    try {
      // Sử dụng lại logic từ buildFastApiInvoiceData để build detail
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Format ngày theo ISO 8601
      const formatDateISO = (date: Date | string): string => {
        const d = typeof date === 'string' ? new Date(date) : date;
        if (isNaN(d.getTime())) {
          throw new Error('Invalid date');
        }
        return d.toISOString();
      };

      // Lấy ngày hóa đơn gốc (ngay_ct0) - có thể lấy từ sale đầu tiên hoặc orderData
      const firstSale = orderData.sales?.[0] || {};
      let ngayCt0: string | null = null;
      let soCt0: string | null = null;

      // Tìm hóa đơn gốc từ stock transfer hoặc sale
      // Nếu có stock transfer, có thể lấy từ soCode hoặc docCode
      if (stockTransfers && stockTransfers.length > 0) {
        const firstStockTransfer = stockTransfers.find(
          (stockTransfer) => stockTransfer.doctype === 'SALE_RETURN',
        );
        // soCode thường là mã đơn hàng gốc
        soCt0 = firstStockTransfer?.soCode || orderData.docCode || null;
        // Ngày có thể lấy từ stock transfer hoặc orderData
        if (firstStockTransfer?.transDate) {
          ngayCt0 = formatDateISO(firstStockTransfer?.transDate);
        } else if (orderData?.docDate) {
          ngayCt0 = formatDateISO(orderData?.docDate);
        }
      } else {
        // Nếu không có stock transfer, lấy từ orderData
        soCt0 = orderData.docCode || null;
        if (orderData?.docDate) {
          ngayCt0 = formatDateISO(orderData.docDate);
        }
      }

      // Format ngày hiện tại
      let docDate: Date;
      if (orderData.docDate instanceof Date) {
        docDate = orderData.docDate;
      } else if (typeof orderData.docDate === 'string') {
        docDate = new Date(orderData.docDate);
        if (isNaN(docDate.getTime())) {
          docDate = new Date();
        }
      } else {
        docDate = new Date();
      }

      const ngayCt = formatDateISO(docDate);
      const ngayLct = formatDateISO(docDate);

      // Lấy ma_dvcs
      const maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';

      // Lấy so_seri
      const soSeri =
        firstSale?.kyHieu ||
        firstSale?.branchCode ||
        orderData.branchCode ||
        'DEFAULT';

      // Gom số lượng trả lại theo mã vật tư
      const stockQtyMap = new Map<string, number>();

      (stockTransfers || [])
        .filter((st) => st.doctype === 'SALE_RETURN')
        .forEach((st) => {
          const maVt = st.materialCode;
          const qty = Number(st.qty || 0);

          if (!maVt || qty === 0) return;

          stockQtyMap.set(maVt, (stockQtyMap.get(maVt) || 0) + qty);
        });

      // Build detail từ invoiceData.detail, chỉ giữ các field cần thiết cho salesReturn
      const detail = (invoiceData.detail || [])
        .map((item: any, index: number) => {
          const soLuongFromStock = stockQtyMap.get(item.ma_vt) || 0;

          const detailItem: any = {
            // Field bắt buộc
            ma_vt: item.ma_vt,
            dvt: item.dvt,
            ma_kho: item.ma_kho,

            so_luong: soLuongFromStock,

            gia_ban: item.gia_ban,
            tien_hang: item.gia_ban * soLuongFromStock,

            // Field tài khoản
            tk_dt: item.tk_dt || '511',
            tk_gv: item.tk_gv || '632',

            // Field khuyến mãi
            is_reward_line: item.is_reward_line || 0,
            is_bundle_reward_line: item.is_bundle_reward_line || 0,
            km_yn: item.km_yn || 0,

            // CK
            ck01_nt: item.ck01_nt || 0,
            ck02_nt: item.ck02_nt || 0,
            ck03_nt: item.ck03_nt || 0,
            ck04_nt: item.ck04_nt || 0,
            ck05_nt: item.ck05_nt || 0,
            ck06_nt: item.ck06_nt || 0,
            ck07_nt: item.ck07_nt || 0,
            ck08_nt: item.ck08_nt || 0,
            ck09_nt: item.ck09_nt || 0,
            ck10_nt: item.ck10_nt || 0,
            ck11_nt: item.ck11_nt || 0,
            ck12_nt: item.ck12_nt || 0,
            ck13_nt: item.ck13_nt || 0,
            ck14_nt: item.ck14_nt || 0,
            ck15_nt: item.ck15_nt || 0,
            ck16_nt: item.ck16_nt || 0,
            ck17_nt: item.ck17_nt || 0,
            ck18_nt: item.ck18_nt || 0,
            ck19_nt: item.ck19_nt || 0,
            ck20_nt: item.ck20_nt || 0,
            ck21_nt: item.ck21_nt || 0,
            ck22_nt: item.ck22_nt || 0,

            // Thuế
            dt_tg_nt: item.dt_tg_nt || 0,
            ma_thue: item.ma_thue || '',
            tien_thue: item.tien_thue || 0,

            ma_bp: item.ma_bp,
            loai_gd: item.loai_gd || '01',
            dong: index + 1,
            id_goc_so: item.id_goc_so || 0,
            id_goc_ngay: item.id_goc_ngay || formatDateISO(new Date()),
          };

          return detailItem;
        })
        .filter(Boolean); // ❗ bỏ các dòng không có stock transfer

      // Build payload, chỉ thêm các field không null
      const salesReturnPayload: any = {
        ma_dvcs: maDvcs,
        ma_kh: invoiceData.ma_kh,
        ong_ba: invoiceData.ong_ba,
        ma_gd: '1', // Mã giao dịch (mặc định 1 - Hàng bán trả lại)
        tk_co: '131', // Tài khoản có (mặc định 131)
        ngay_lct: ngayLct,
        ngay_ct: ngayCt,
        so_ct: orderData.docCode || '',
        so_seri: soSeri,
        ma_nt: 'VND',
        ty_gia: 1.0,
        ma_kenh: 'ONLINE', // Mã kênh (mặc định ONLINE)
        detail: detail,
      };

      // Chỉ thêm các field optional nếu có giá trị
      if (firstSale?.maCa) {
        salesReturnPayload.ma_ca = firstSale.maCa;
      }
      if (soCt0) {
        salesReturnPayload.so_ct0 = soCt0;
      }
      if (ngayCt0) {
        salesReturnPayload.ngay_ct0 = ngayCt0;
      }
      if (orderData.docCode) {
        salesReturnPayload.dien_giai = orderData.docCode;
      }

      return salesReturnPayload;
    } catch (error: any) {
      this.logger.error(
        `Error building sales return data: ${error?.message || error}`,
      );
      throw new Error(
        `Failed to build sales return data: ${error?.message || error}`,
      );
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
          const stockTransferData = await this.buildStockTransferData(
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
    const invoiceData = await this.buildFastApiInvoiceData(orderData);
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
          const salesReturnData = await this.buildSalesReturnData(
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
                  await this.buildFastApiInvoiceData(orderData);

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

  /**
   * Build FastAPI stock transfer data từ STOCK_TRANSFER items
   */
  private async buildStockTransferData(
    items: any[],
    orderData: any,
  ): Promise<any> {
    const firstItem = items[0];

    // Lấy ma_dvcs từ order hoặc branch_code
    let maDvcs = '';
    if (orderData) {
      const firstSale = orderData.sales?.[0];
      maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';
    }
    if (!maDvcs) {
      maDvcs = firstItem.branch_code || '';
    }

    // Lấy ma_kh từ order và normalize (bỏ prefix "NV" nếu có)
    const maKh = SalesUtils.normalizeMaKh(orderData?.customer?.code);

    // Map iotype sang ma_nx (mã nhập xuất)
    // iotype: 'O' = xuất, 'I' = nhập
    // ma_nx: có thể là '1111' cho xuất, '1112' cho nhập (cần xác nhận với FastAPI)
    const getMaNx = (iotype: string): string => {
      if (iotype === 'O') {
        return '1111'; // Xuất nội bộ
      } else if (iotype === 'I') {
        return '1112'; // Nhập nội bộ
      }
      return '1111'; // Default
    };

    // Build detail items
    const detail = await Promise.all(
      items.map(async (item, index) => {
        // Fetch trackSerial và trackBatch từ Loyalty API để xác định dùng ma_lo hay so_serial
        let dvt = 'Cái'; // Default
        let trackSerial: boolean | null = null;
        let trackBatch: boolean | null = null;
        let productTypeFromLoyalty: string | null = null;

        try {
          const product = await this.productItemRepository.findOne({
            where: { maERP: item.item_code },
          });
          if (product?.dvt) {
            dvt = product.dvt;
          }
          // Fetch từ Loyalty API để lấy dvt, trackSerial, trackBatch và productType
          const loyaltyProduct = await this.loyaltyService.checkProduct(
            item.item_code,
          );
          if (loyaltyProduct) {
            if (loyaltyProduct?.unit) {
              dvt = loyaltyProduct.unit;
            }
            trackSerial = loyaltyProduct.trackSerial === true;
            trackBatch = loyaltyProduct.trackBatch === true;
            productTypeFromLoyalty =
              loyaltyProduct?.productType ||
              loyaltyProduct?.producttype ||
              null;
          }
        } catch (error) {}

        const productTypeUpper = productTypeFromLoyalty
          ? String(productTypeFromLoyalty).toUpperCase().trim()
          : null;

        // Debug log để kiểm tra trackSerial và trackBatch
        if (index === 0) {
        }

        // Xác định có dùng ma_lo hay so_serial dựa trên trackSerial và trackBatch từ Loyalty API
        const useBatch = SalesUtils.shouldUseBatch(trackBatch, trackSerial);

        let maLo: string | null = null;
        let soSerial: string | null = null;

        if (useBatch) {
          // trackBatch = true → dùng ma_lo với giá trị batchserial
          const batchSerial = item.batchserial || null;
          if (batchSerial) {
            // Vẫn cần productType để quyết định cắt bao nhiêu ký tự
            if (productTypeUpper === 'TPCN') {
              // Nếu productType là "TPCN", cắt lấy 8 ký tự cuối
              maLo =
                batchSerial.length >= 8 ? batchSerial.slice(-8) : batchSerial;
            } else if (
              productTypeUpper === 'SKIN' ||
              productTypeUpper === 'GIFT'
            ) {
              // Nếu productType là "SKIN" hoặc "GIFT", cắt lấy 4 ký tự cuối
              maLo =
                batchSerial.length >= 4 ? batchSerial.slice(-4) : batchSerial;
            } else {
              // Các trường hợp khác → giữ nguyên toàn bộ
              maLo = batchSerial;
            }
          } else {
            maLo = null;
          }
          soSerial = null;
        } else {
          // trackSerial = true và trackBatch = false → dùng so_serial, không set ma_lo
          maLo = null;
          soSerial = item.batchserial || null;
        }

        return {
          ma_vt: item.item_code,
          dvt: dvt,
          so_serial: soSerial,
          ma_kho: item.stock_code,
          so_luong: Math.abs(item.qty), // Lấy giá trị tuyệt đối
          gia_nt: 0, // Stock transfer thường không có giá
          tien_nt: 0, // Stock transfer thường không có tiền
          ma_lo: maLo,
          px_gia_dd: 0, // Mặc định 0
          ma_nx: getMaNx(item.iotype),
          ma_vv: null,
          ma_bp:
            orderData?.sales?.[0]?.department?.ma_bp ||
            item.branch_code ||
            null,
          so_lsx: null,
          ma_sp: null,
          ma_hd: null,
          ma_phi: null,
          ma_ku: null,
          ma_phi_hh: null,
          ma_phi_ttlk: null,
          tien_hh_nt: 0,
          tien_ttlk_nt: 0,
        };
      }),
    );

    // Format date
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      return d.toISOString();
    };

    const transDate = new Date(firstItem.transdate);
    const ngayCt = formatDateISO(transDate);
    const ngayLct = formatDateISO(transDate);

    // Lấy ma_nx từ item đầu tiên (tất cả items trong cùng 1 phiếu nên có cùng iotype)
    const maNx = getMaNx(firstItem.iotype);

    return {
      action: 0, // Thêm action field giống như salesInvoice
      ma_dvcs: maDvcs,
      ma_kh: maKh,
      ong_ba: orderData?.customer?.name || null,
      ma_gd: '1', // Mã giao dịch: 1
      ma_nx: maNx, // Thêm ma_nx vào header
      ngay_ct: ngayCt,
      so_ct: firstItem.doccode,
      ma_nt: 'VND',
      ty_gia: 1.0,
      dien_giai: firstItem.doc_desc || null,
      detail: detail,
    };
  }

  async cutCode(input: string): Promise<string> {
    return input?.split('-')[0] || '';
  }

  // ==================== HELPERS ====================

  private toNumber(value: any, defaultValue: number = 0): number {
    if (value === null || value === undefined || value === '')
      return defaultValue;
    const num = Number(value);
    return isNaN(num) ? defaultValue : num;
  }

  private toString(value: any, defaultValue: string = ''): string {
    return value === null || value === undefined || value === ''
      ? defaultValue
      : String(value);
  }

  private limitString(value: string, maxLength: number): string {
    if (!value) return '';
    const str = String(value);
    return str.length > maxLength ? str.substring(0, maxLength) : str;
  }

  /**
   * Helper terse wrapper for limitString(toString(value, def), max)
   */
  private val(
    value: any,
    maxLength: number,
    defaultValue: string = '',
  ): string {
    return this.limitString(this.toString(value, defaultValue), maxLength);
  }

  private formatDateISO(date: Date): string {
    if (isNaN(date.getTime())) throw new Error('Invalid date');
    return date.toISOString();
  }

  private formatDateKeepLocalDay(date: Date): string {
    if (isNaN(date.getTime())) throw new Error('Invalid date');

    const pad = (n: number) => String(n).padStart(2, '0');

    return (
      date.getFullYear() +
      '-' +
      pad(date.getMonth() + 1) +
      '-' +
      pad(date.getDate()) +
      'T' +
      pad(date.getHours()) +
      ':' +
      pad(date.getMinutes()) +
      ':' +
      pad(date.getSeconds()) +
      '.000Z'
    );
  }

  private parseInvoiceDate(inputDate: any): Date {
    let docDate: Date;
    if (inputDate instanceof Date) {
      docDate = inputDate;
    } else if (typeof inputDate === 'string') {
      docDate = new Date(inputDate);
      if (isNaN(docDate.getTime())) docDate = new Date();
    } else {
      docDate = new Date();
    }

    const minDate = new Date('1753-01-01T00:00:00');
    const maxDate = new Date('9999-12-31T23:59:59');
    if (docDate < minDate || docDate > maxDate) {
      throw new Error(
        `Date out of range for SQL Server: ${docDate.toISOString()}`,
      );
    }
    return docDate;
  }

  private async getInvoiceStockTransferMap(
    docCode: string,
    isNormalOrder: boolean,
  ) {
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const allStockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    const stockTransferMap = new Map<
      string,
      { st?: StockTransfer[]; rt?: StockTransfer[] }
    >();
    let transDate: Date | null = null;

    if (isNormalOrder && allStockTransfers.length > 0) {
      transDate = allStockTransfers[0].transDate || null;
      const itemCodes = Array.from(
        new Set(
          allStockTransfers
            .map((st) => st.itemCode)
            .filter((c): c is string => !!c && c.trim() !== ''),
        ),
      );

      const loyaltyMap = new Map<string, any>();
      if (itemCodes.length > 0) {
        const products = await this.loyaltyService.fetchProducts(itemCodes);
        products.forEach((p, c) => loyaltyMap.set(c, p));
      }

      allStockTransfers.forEach((st) => {
        const materialCode =
          st.materialCode || loyaltyMap.get(st.itemCode)?.materialCode;
        if (!materialCode) return;
        const key = `${st.soCode || st.docCode || docCode}_${materialCode}`;

        if (!stockTransferMap.has(key)) stockTransferMap.set(key, {});
        const m = stockTransferMap.get(key)!;
        if (st.docCode.startsWith('ST') || Number(st.qty || 0) < 0) {
          if (!m.st) m.st = [];
          m.st.push(st);
        } else if (st.docCode.startsWith('RT') || Number(st.qty || 0) > 0) {
          if (!m.rt) m.rt = [];
          m.rt.push(st);
        }
      });
    }
    return { stockTransferMap, allStockTransfers, transDate };
  }

  private async getInvoiceCardSerialMap(
    docCode: string,
  ): Promise<Map<string, string>> {
    const map = new Map<string, string>();
    const [dataCard] = await this.n8nService.fetchCardData(docCode);
    if (Array.isArray(dataCard?.data)) {
      for (const card of dataCard.data) {
        if (!card?.service_item_name || !card?.serial) continue;
        const product = await this.loyaltyService.checkProduct(
          card.service_item_name,
        );
        if (product) map.set(product.materialCode, card.serial);
      }
    }
    return map;
  }

  private async calculateInvoiceAmounts(
    sale: any,
    orderData: any,
    allocationRatio: number,
    isNormalOrder: boolean,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const headerOrderTypes = InvoiceLogicUtils.getOrderTypes(
      orderData.sales?.[0]?.ordertypeName ||
        orderData.sales?.[0]?.ordertype ||
        '',
    );

    const amounts: any = {
      tienThue: this.toNumber(sale.tienThue, 0),
      dtTgNt: this.toNumber(sale.dtTgNt, 0),
      ck01_nt: this.toNumber(
        sale.other_discamt || sale.chietKhauMuaHangGiamGia,
        0,
      ),
      ck02_nt:
        this.toNumber(sale.disc_tm, 0) > 0
          ? this.toNumber(sale.disc_tm, 0)
          : this.toNumber(sale.chietKhauCkTheoChinhSach, 0),
      ck03_nt: this.toNumber(
        sale.chietKhauMuaHangCkVip || sale.grade_discamt,
        0,
      ),
      ck04_nt: this.toNumber(
        sale.chietKhauThanhToanCoupon || sale.chietKhau09,
        0,
      ),
      ck05_nt:
        this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0
          ? this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0)
          : 0,
      ck07_nt: this.toNumber(sale.chietKhauVoucherDp2, 0),
      ck08_nt: this.toNumber(sale.chietKhauVoucherDp3, 0),
    };

    // Fill others with default 0 or from sale fields
    for (let i = 9; i <= 22; i++) {
      if (i === 11) continue; // ck11 handled separately
      const key = `ck${i.toString().padStart(2, '0')}_nt`;
      const saleKey = `chietKhau${i.toString().padStart(2, '0')}`;
      amounts[key] = this.toNumber(sale[saleKey] || sale[key], 0);
    }
    amounts.ck06_nt = 0;

    // ck11 (ECOIN) logic
    let ck11_nt = this.toNumber(
      sale.chietKhauThanhToanTkTienAo || sale.chietKhau11,
      0,
    );
    if (
      ck11_nt === 0 &&
      this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0 &&
      orderData.cashioData
    ) {
      const ecoin = orderData.cashioData.find(
        (c: any) => c.fop_syscode === 'ECOIN',
      );
      if (ecoin?.total_in) ck11_nt = this.toNumber(ecoin.total_in, 0);
    }
    amounts.ck11_nt = ck11_nt;

    // Allocation
    if (
      isNormalOrder &&
      allocationRatio !== 1 &&
      allocationRatio > 0 &&
      !orderTypes.isDoiDiem &&
      !headerOrderTypes.isDoiDiem
    ) {
      Object.keys(amounts).forEach((k) => {
        if (k.endsWith('_nt') || k === 'tienThue' || k === 'dtTgNt') {
          amounts[k] *= allocationRatio;
        }
      });
    }

    if (orderTypes.isDoiDiem || headerOrderTypes.isDoiDiem) amounts.ck05_nt = 0;

    // promCode logic
    let promCode = sale.promCode || sale.prom_code || null;

    if (promCode && typeof promCode === 'string' && promCode.trim() !== '') {
      const trimmed = promCode.trim();
      // Special logic for PRMN: transform to RMN, no suffix, no cutCode
      if (trimmed.toUpperCase().startsWith('PRMN')) {
        promCode = trimmed.replace(/^PRMN/i, 'RMN');
      } else {
        // Old logic: cutCode + suffix
        promCode = await this.cutCode(promCode);
        if (sale.productType === 'I') {
          promCode = promCode + '.I';
        } else if (sale.productType === 'S') {
          promCode = promCode + '.S';
        } else if (sale.productType === 'V') {
          promCode = promCode + '.V';
        }
      }
    } else {
      promCode = null;
    }

    amounts.promCode = promCode;

    return amounts;
  }

  private async resolveInvoicePromotionCodes(
    sale: any,
    orderData: any,
    giaBan: number,
    promCode: string | null,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const isTangHang =
      Math.abs(giaBan) < 0.01 &&
      Math.abs(this.toNumber(sale.linetotal || sale.revenue, 0)) < 0.01;
    const maDvcs = this.toString(
      sale.department?.ma_dvcs || sale.department?.ma_dvcs_ht || '',
      '',
    );
    const productType =
      sale.productType ||
      sale.product?.productType ||
      sale.product?.producttype ||
      '';
    const productTypeUpper = String(productType).toUpperCase().trim();

    return InvoiceLogicUtils.resolvePromotionCodes({
      sale,
      orderTypes,
      isTangHang,
      maDvcs,
      productTypeUpper,
      promCode: sale.promCode || sale.prom_code, // Pass RAW code to let Utils handle PRMN logic consistently
    });
  }

  private resolveInvoiceAccounts(
    sale: any,
    loyaltyProduct: any,
    giaBan: number,
    maCk01: string | null,
    maCtkmTangHang: string | null,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const isTangHang =
      Math.abs(giaBan) < 0.01 &&
      Math.abs(this.toNumber(sale.linetotal || sale.revenue, 0)) < 0.01;

    return InvoiceLogicUtils.resolveAccountingAccounts({
      sale,
      loyaltyProduct,
      orderTypes,
      isTangHang,
      hasMaCtkm: !!(maCk01 || maCtkmTangHang),
      hasMaCtkmTangHang: !!maCtkmTangHang,
    });
  }

  private resolveInvoiceLoaiGd(sale: any, loyaltyProduct: any = null): string {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertype || sale.ordertypeName || '',
    );
    return InvoiceLogicUtils.resolveLoaiGd({
      sale,
      orderTypes,
      loyaltyProduct,
    });
  }

  private async resolveInvoiceBatchSerial(
    sale: any,
    saleMaterialCode: string,
    cardSerialMap: Map<string, string>,
    stockTransferMap: Map<string, any>,
    docCode: string,
    loyaltyProduct: any,
  ) {
    let batchSerial: string | null = null;
    if (saleMaterialCode) {
      const sts = stockTransferMap.get(`${docCode}_${saleMaterialCode}`)?.st;
      if (sts?.[0]?.batchSerial) batchSerial = sts[0].batchSerial;
    }

    return InvoiceLogicUtils.resolveBatchSerial({
      batchSerialFromST: batchSerial,
      trackBatch: loyaltyProduct?.trackBatch === true,
      trackSerial: loyaltyProduct?.trackSerial === true,
    });
  }

  private calculateInvoiceQty(
    sale: any,
    docCode: string,
    saleMaterialCode: string,
    isNormalOrder: boolean,
    stockTransferMap: Map<string, any>,
  ) {
    let qty = this.toNumber(sale.qty, 0);
    const saleQty = this.toNumber(sale.qty, 0);
    let allocationRatio = 1;

    if (isNormalOrder && saleMaterialCode) {
      const key = `${docCode}_${saleMaterialCode}`;
      const firstSt = stockTransferMap.get(key)?.st?.[0];
      if (firstSt && saleQty !== 0) {
        qty = Math.abs(Number(firstSt.qty || 0));
        allocationRatio = qty / saleQty;
      }
    }
    return { qty, saleQty, allocationRatio };
  }

  private calculateInvoicePrices(
    sale: any,
    qty: number,
    allocationRatio: number,
    isNormalOrder: boolean,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    return InvoiceLogicUtils.calculatePrices({
      sale,
      orderTypes,
      allocationRatio,
      qtyFromStock: qty,
    });
  }

  private resolveInvoiceMaKhHeader(orderData: any): string {
    let maKh = SalesUtils.normalizeMaKh(orderData.customer?.code);
    const firstSale = orderData.sales?.[0];
    const { isTachThe } = InvoiceLogicUtils.getOrderTypes(
      firstSale?.ordertype || firstSale?.ordertypeName || '',
    );

    if (isTachThe && Array.isArray(orderData.sales)) {
      const saleWithIssue =
        orderData.sales.find(
          (s: any) => Number(s.qty || 0) < 0 && s.issuePartnerCode,
        ) || orderData.sales.find((s: any) => s.issuePartnerCode);
      if (saleWithIssue) {
        maKh = SalesUtils.normalizeMaKh(saleWithIssue.issuePartnerCode);
      }
    }
    return maKh;
  }

  private async resolveInvoiceMaKho(
    sale: any,
    saleMaterialCode: string,
    stockTransferMap: Map<string, any>,
    docCode: string,
    maBp: string,
    isTachThe: boolean,
  ): Promise<string> {
    let maKhoFromST: string | null = null;
    if (saleMaterialCode) {
      const sts = stockTransferMap.get(`${docCode}_${saleMaterialCode}`)?.st;
      if (sts?.[0]?.stockCode) maKhoFromST = sts[0].stockCode;
    }

    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertype || sale.ordertypeName || '',
    );

    const maKho = InvoiceLogicUtils.resolveMaKho({
      maKhoFromST,
      maKhoFromSale: sale.maKho || null,
      maBp,
      orderTypes,
    });

    const maKhoMap = await this.categoriesService.mapWarehouseCode(maKho);
    return maKhoMap || maKho || '';
  }

  private fillInvoiceChietKhauFields(
    detailItem: any,
    amounts: any,
    sale: any,
    orderData: any,
    loyaltyProduct: any,
  ) {
    for (let i = 1; i <= 22; i++) {
      const idx = i.toString().padStart(2, '0');
      const key = `ck${idx}_nt`;
      const maKey = `ma_ck${idx}`;
      detailItem[key] = Number(amounts[key] || 0);

      // Special ma_ck logic
      if (i === 2) {
        // 02. Chiết khấu theo chính sách (Bán buôn)
        const isWholesale =
          sale.type_sale === 'WHOLESALE' || sale.type_sale === 'WS';
        const distTm = detailItem.ck02_nt;

        // Bỏ check channel_code vì dữ liệu không có sẵn trong entity
        if (isWholesale && distTm > 0) {
          detailItem[maKey] = this.val(
            InvoiceLogicUtils.resolveWholesalePromotionCode({
              groupProductType: loyaltyProduct?.productType,
              productTypeCode: loyaltyProduct?.materialCode,
              distTm: distTm,
            }),
            32,
          );
        } else {
          detailItem[maKey] = this.val(sale.maCk02 || '', 32);
        }
      } else if (i === 3) {
        const brand = orderData.customer?.brand || orderData.brand || '';
        detailItem[maKey] = this.val(
          SalesCalculationUtils.calculateMuaHangCkVip(
            sale,
            sale.product,
            brand,
          ),
          32,
        );
      } else if (i === 4) {
        detailItem[maKey] = this.val(
          detailItem.ck04_nt > 0 || sale.thanhToanCoupon
            ? sale.maCk04 || 'COUPON'
            : '',
          32,
        );
      } else if (i === 5) {
        const { isDoiDiem } = InvoiceLogicUtils.getOrderTypes(
          sale.ordertype || sale.ordertypeName,
        );
        const { isDoiDiem: isDoiDiemHeader } = InvoiceLogicUtils.getOrderTypes(
          orderData.sales?.[0]?.ordertype ||
            orderData.sales?.[0]?.ordertypeName ||
            '',
        );

        if (isDoiDiem || isDoiDiemHeader) {
          detailItem[maKey] = '';
        } else if (detailItem.ck05_nt > 0) {
          // Note: using logic from buildFastApiInvoiceData
          detailItem[maKey] = this.val(
            InvoiceLogicUtils.resolveVoucherCode({
              sale: {
                ...sale,
                customer: sale.customer || orderData.customer,
              },
              customer: null, // Resolution happens inside resolveVoucherCode
              brand: orderData.customer?.brand || orderData.brand || '',
            }),
            32,
            sale.maCk05 || 'VOUCHER',
          );
        }
      } else if (i === 7) {
        detailItem[maKey] = this.val(sale.voucherDp2 ? 'VOUCHER_DP2' : '', 32);
      } else if (i === 8) {
        detailItem[maKey] = this.val(sale.voucherDp3 ? 'VOUCHER_DP3' : '', 32);
      } else if (i === 11) {
        detailItem[maKey] = this.val(
          detailItem.ck11_nt > 0 || sale.thanhToanTkTienAo
            ? sale.maCk11 ||
                SalesUtils.generateTkTienAoLabel(
                  orderData.docDate,
                  orderData.customer?.brand ||
                    orderData.sales?.[0]?.customer?.brand,
                )
            : '',
          32,
        );
      } else {
        // Default mapping for other ma_ck fields
        if (i !== 1) {
          const saleMaKey = `maCk${idx}`;
          detailItem[maKey] = this.val(sale[saleMaKey] || '', 32);
        }
      }
    }
  }

  private buildInvoiceCbDetail(detail: any[]) {
    return detail.map((item: any) => {
      let tongChietKhau = 0;
      for (let i = 1; i <= 22; i++) {
        tongChietKhau += Number(
          item[`ck${i.toString().padStart(2, '0')}_nt`] || 0,
        );
      }

      return {
        ma_vt: item.ma_vt || '',
        dvt: item.dvt || '',
        so_luong: Number(item.so_luong || 0),
        ck_nt: Number(tongChietKhau),
        gia_nt: Number(item.gia_ban || 0),
        tien_nt: Number(item.tien_hang || 0),
      };
    });
  }

  private async mapSaleToInvoiceDetail(
    sale: any,
    index: number,
    orderData: any,
    context: any,
  ): Promise<any> {
    const { isNormalOrder, stockTransferMap, cardSerialMap } = context;
    const saleMaterialCode =
      sale.product?.materialCode ||
      sale.product?.maVatTu ||
      sale.product?.maERP;

    // 1. Qty & Allocation
    const { qty, allocationRatio } = this.calculateInvoiceQty(
      sale,
      orderData.docCode,
      saleMaterialCode,
      isNormalOrder,
      stockTransferMap,
    );

    // 2. Prices
    const { giaBan, tienHang, tienHangGoc } = this.calculateInvoicePrices(
      sale,
      qty,
      allocationRatio,
      isNormalOrder,
    );

    // 3. Amounts (Discounts, Tax, Subsidy)
    const amounts = await this.calculateInvoiceAmounts(
      sale,
      orderData,
      allocationRatio,
      isNormalOrder,
    );

    // 4. Resolve Codes & Accounts
    const materialCode =
      SalesUtils.getMaterialCode(sale, sale.product) || sale.itemCode;
    const loyaltyProduct = await this.loyaltyService.checkProduct(materialCode);

    const { maCk01, maCtkmTangHang } = await this.resolveInvoicePromotionCodes(
      sale,
      orderData,
      giaBan,
      amounts.promCode,
    );

    const { tkChietKhau, tkChiPhi, maPhi } = this.resolveInvoiceAccounts(
      sale,
      loyaltyProduct,
      giaBan,
      maCk01,
      maCtkmTangHang,
    );

    // 5. Build Detail Item
    const maBp = this.val(
      sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
      8,
    );
    const loaiGd = this.resolveInvoiceLoaiGd(sale, loyaltyProduct);
    const { maLo, soSerial } = await this.resolveInvoiceBatchSerial(
      sale,
      saleMaterialCode,
      cardSerialMap,
      stockTransferMap,
      orderData.docCode,
      loyaltyProduct,
    );

    const detailItem: any = {
      tk_chiet_khau: this.val(tkChietKhau, 16),
      tk_chi_phi: this.val(tkChiPhi, 16),
      ma_phi: this.val(maPhi, 16),
      tien_hang: Number(sale.qty) * Number(sale.giaBan),
      so_luong: Number(sale.qty),
      ma_kh_i: this.val(sale.issuePartnerCode, 16),
      ma_vt: this.val(
        loyaltyProduct?.materialCode || sale.product?.maVatTu || '',
        16,
      ),
      dvt: this.val(
        sale.product?.dvt || sale.product?.unit || sale.dvt,
        32,
        'Cái',
      ),
      loai: this.val(sale.loai || sale.cat1, 2),
      loai_gd: this.val(loaiGd, 2),
      ma_ctkm_th: this.val(maCtkmTangHang, 32),
    };

    const finalMaKho = await this.resolveInvoiceMaKho(
      sale,
      saleMaterialCode,
      stockTransferMap,
      orderData.docCode,
      maBp,
      loaiGd === '11' || loaiGd === '12',
    );
    if (finalMaKho && finalMaKho.trim() !== '') {
      detailItem.ma_kho = this.limitString(finalMaKho, 16);
    }

    Object.assign(detailItem, {
      gia_ban: Number(giaBan),
      is_reward_line: sale.isRewardLine ? 1 : 0,
      is_bundle_reward_line: sale.isBundleRewardLine ? 1 : 0,
      km_yn:
        maCtkmTangHang === 'TT DAU TU'
          ? 0
          : Math.abs(giaBan) < 0.01 && Math.abs(tienHang) < 0.01
            ? 1
            : 0,
      dong_thuoc_goi: this.val(sale.dongThuocGoi, 32),
      trang_thai: this.val(sale.trangThai, 32),
      barcode: this.val(sale.barcode, 32),
      ma_ck01: this.val(maCk01, 32),
      dt_tg_nt: Number(amounts.dtTgNt),
      tien_thue: Number(amounts.tienThue),
      ma_thue: this.val(sale.maThue, 8, '00'),
      thue_suat: Number(this.toNumber(sale.thueSuat, 0)),
      tk_thue: this.val(sale.tkThueCo, 16),
      tk_cpbh: this.val(sale.tkCpbh, 16),
      ma_bp: maBp,
      ma_the: this.val(cardSerialMap.get(saleMaterialCode), 256),
      dong: index + 1,
      id_goc_ngay: sale.idGocNgay
        ? this.formatDateISO(new Date(sale.idGocNgay))
        : this.formatDateISO(new Date()),
      id_goc: this.val(sale.idGoc, 70),
      id_goc_ct: this.val(sale.idGocCt, 16),
      id_goc_so: Number(this.toNumber(sale.idGocSo, 0)),
      id_goc_dv: this.val(sale.idGocDv, 8),
      ma_combo: this.val(sale.maCombo, 16),
      ma_nx_st: this.val(sale.ma_nx_st, 32),
      ma_nx_rt: this.val(sale.ma_nx_rt, 32),
      ...(soSerial && soSerial.trim() !== ''
        ? { so_serial: this.limitString(soSerial, 64) }
        : maLo && maLo.trim() !== ''
          ? { ma_lo: this.limitString(maLo, 16) }
          : {}),
    });

    this.fillInvoiceChietKhauFields(
      detailItem,
      amounts,
      sale,
      orderData,
      loyaltyProduct,
    );

    return detailItem;
  }

  private assembleInvoicePayload(
    orderData: any,
    detail: any[],
    cbdetail: any[],
    context: any,
  ) {
    const { ngayCt, ngayLct, transDate, maBp } = context;
    const firstSale = orderData.sales?.[0];

    return {
      action: 0,
      ma_dvcs:
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '',
      ma_kh: this.resolveInvoiceMaKhHeader(orderData),
      ong_ba: orderData.customer?.name || null,
      ma_gd: '1',
      ma_tt: null,
      ma_ca: firstSale?.maCa || null,
      hinh_thuc: '0',
      dien_giai: orderData.docCode || null,
      ngay_lct: ngayLct,
      ngay_ct: ngayCt,
      so_ct: orderData.docCode || '',
      so_seri: orderData.branchCode || 'DEFAULT',
      ma_nt: 'VND',
      ty_gia: 1.0,
      ma_bp: maBp,
      tk_thue_no: '131111',
      ma_kenh: 'ONLINE',
      loai_gd: firstSale ? this.resolveInvoiceLoaiGd(firstSale, null) : '01',
      trans_date: transDate
        ? this.formatDateKeepLocalDay(new Date(transDate))
        : null,
      detail,
      cbdetail,
    };
  }

  private logInvoiceError(error: any, orderData: any) {
    this.logger.error(
      `Error building Fast API invoice data: ${error?.message || error}`,
    );
    this.logger.error(`Error stack: ${error?.stack || 'No stack trace'}`);
    this.logger.error(
      `Order data: ${JSON.stringify({
        docCode: orderData?.docCode,
        docDate: orderData?.docDate,
        salesCount: orderData?.sales?.length,
        customer: orderData?.customer
          ? { code: orderData.customer.code, name: orderData.customer.name }
          : null,
      })}`,
    );
  }
} // End class
