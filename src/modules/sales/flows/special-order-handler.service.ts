import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { Sale } from '../../../entities/sale.entity';
import { ProductItem } from '../../../entities/product-item.entity';
import { Invoice } from '../../../entities/invoice.entity';
import { InvoiceItem } from '../../../entities/invoice-item.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { FastApiInvoiceFlowService } from '../../../services/fast-api-invoice-flow.service';
import { N8nService } from '../../../services/n8n.service';
import { SalesPayloadService } from '../invoice/sales-payload.service';
import { SalesQueryService } from '../services/sales-query.service';
import { InvoicePersistenceService } from '../invoice/invoice-persistence.service';
import { PaymentService } from '../../payment/payment.service';
import { forwardRef, Inject } from '@nestjs/common';
import * as SalesUtils from '../../../utils/sales.utils';
import * as ConvertUtils from '../../../utils/convert.utils';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import {
  DOC_SOURCE_TYPES,
  PRODUCT_TYPES,
  STATUS,
  ORDER_TYPES,
} from '../constants/sales-invoice.constants';

@Injectable()
export class SpecialOrderHandlerService {
  private readonly logger = new Logger(SpecialOrderHandlerService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    @InjectRepository(InvoiceItem)
    private invoiceItemRepository: Repository<InvoiceItem>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private n8nService: N8nService,
    private salesPayloadService: SalesPayloadService,
    private salesQueryService: SalesQueryService,
    private invoicePersistenceService: InvoicePersistenceService,
    @Inject(forwardRef(() => PaymentService))
    private paymentService: PaymentService,
  ) {}

  /**
   * Helper xử lý các đơn hàng đặc biệt (Đổi điểm, Tặng sinh nhật, Đầu tư...)
   * Chỉ tạo Sales Order, không tạo Sales Invoice
   */
  async handleStandardSpecialOrder(
    orderData: any,
    docCode: string,
    description: string,
    beforeAction?: () => Promise<void>,
    afterEnrichmentAction?: (enrichedOrder: any) => Promise<void>,
    createInvoice: boolean = false,
  ): Promise<any> {
    this.logger.log(`[SpecialOrder] Bắt đầu xử lý ${description}: ${docCode}`);
    try {
      if (beforeAction) {
        await beforeAction();
      }

      // Explode sales by Stock Transfers
      const [enrichedOrder] =
        await this.salesQueryService.enrichOrdersWithCashio([orderData]);

      // [FIX] Execute logic AFTER enrichment (e.g. N8n mapping) to overwrite ST values
      if (afterEnrichmentAction) {
        await afterEnrichmentAction(enrichedOrder);
      }

      const invoiceData =
        await this.salesPayloadService.buildFastApiInvoiceData(enrichedOrder);

      // Call createSalesOrder
      const result = await this.fastApiInvoiceFlowService.createSalesOrder({
        ...invoiceData,
        customer: orderData.customer,
        ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
      });

      let responseStatus =
        Array.isArray(result) &&
        result.length > 0 &&
        result[0].status === STATUS.SUCCESS
          ? STATUS.SUCCESS
          : STATUS.FAILED;
      let apiMessage =
        Array.isArray(result) && result.length > 0 && result[0].message
          ? result[0].message
          : '';

      const fastApiResponse: any = { salesOrder: result };

      // [NEW] Option to create Sales Invoice immediately after Sales Order
      if (responseStatus === STATUS.SUCCESS && createInvoice) {
        try {
          this.logger.log(
            `[SpecialOrder] Creating Sales Invoice for ${docCode}...`,
          );
          const invoiceResult =
            await this.fastApiInvoiceFlowService.createSalesInvoice({
              ...invoiceData,
              customer: orderData.customer,
              ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
            });
          fastApiResponse.salesInvoice = invoiceResult;

          // Check invoice result status
          const invStatus =
            Array.isArray(invoiceResult) && invoiceResult.length > 0
              ? invoiceResult[0].status
              : invoiceResult && invoiceResult.status;

          if (invStatus === 1) {
            // 1 is Success for Invoice API
            apiMessage += ' | Invoice Created';
          } else {
            apiMessage += ` | Invoice Failed: ${Array.isArray(invoiceResult) && invoiceResult.length > 0 ? invoiceResult[0].message : invoiceResult?.message || 'Unknown'}`;
            // Optional: Mark overall status as warning or failed depending on strictness
            // keeping SUCCESS if SO succeeded, but warning in message
          }
        } catch (invError: any) {
          this.logger.error(
            `[SpecialOrder] Failed to create Sales Invoice for ${docCode}: ${invError.message}`,
          );
          apiMessage += ` | Invoice Error: ${invError.message}`;
        }
      }

      const responseMessage =
        responseStatus === STATUS.SUCCESS
          ? `${description} thành công: ${apiMessage}`
          : `${description} thất bại: ${apiMessage}`;

      const responseGuid =
        Array.isArray(result) && result.length > 0 && result[0].guid
          ? Array.isArray(result[0].guid)
            ? result[0].guid[0]
            : result[0].guid
          : null;

      return {
        result: { salesOrder: result },
        status: responseStatus,
        message: responseMessage,
        guid: responseGuid,
        fastApiResponse: fastApiResponse,
      };
    } catch (error: any) {
      this.logger.error(
        `[SpecialOrder] Lỗi khi xử lý ${description} ${docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Xử lý đơn Tách thẻ (08. Tách thẻ) - có thêm logic fetch card data
   */
  async handleTachTheOrder(orderData: any, docCode: string): Promise<any> {
    return this.handleStandardSpecialOrder(
      orderData,
      docCode,
      ORDER_TYPES.CARD_SEPARATION,
      undefined, // No beforeAction
      async (enrichedOrder) => {
        // Gọi API get_card để lấy issue_partner_code cho đơn "08. Tách thẻ"
        // [FIX] Apply to ENRICHED sales (After explosion)
        try {
          const cardResponse =
            await this.n8nService.fetchCardDataWithRetry(docCode);
          const cardData = this.n8nService.parseCardData(cardResponse);
          this.n8nService.mapIssuePartnerCodeToSales(
            enrichedOrder.sales || [],
            cardData,
          );
          this.logger.log(
            `[SpecialOrder] Successfully enriched sales with N8n Card Data (After Explosion)`,
          );
        } catch (e) {
          // Ignore error as per original logic, but log it
          this.logger.warn(
            `[SpecialOrder] Failed to n8n enrich: ${e?.message || e}`,
          );
        }
      },
      true, // [FIX] Force create Invoice for Split Card
    );
  }

  /**
   * Xử lý flow tạo hóa đơn cho đơn hàng dịch vụ (02. Làm dịch vụ)
   * Flow:
   * 1. Customer (tạo/cập nhật)
   * 2. SalesOrder (tất cả dòng: I, S, V...)
   * 3. SalesInvoice (chỉ dòng productType = 'S')
   * 4. GxtInvoice (S → detail, I → ndetail)
   */
  async executeServiceOrderFlow(orderData: any, docCode: string): Promise<any> {
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
          so_cccd: orderData.customer.idnumber || undefined,
          ngay_sinh: orderData.customer?.birthday
            ? ConvertUtils.formatDateYYYYMMDD(orderData.customer.birthday)
            : undefined,
          gioi_tinh: orderData.customer.sexual || undefined,
        });
      }

      // Step 2 & 3: Tạo SalesOrder & SalesInvoice CHỈ cho các dòng dịch vụ (S)
      // Logic: Dòng dịch vụ là dòng có svc_code hoặc productType = 'S'
      const serviceLines = sales.filter((s: any) => {
        const productType = s.productType?.toUpperCase()?.trim();
        return productType === PRODUCT_TYPES.SERVICE;
      });

      if (serviceLines.length === 0) {
        throw new Error(`Đơn dịch vụ ${docCode} không có dòng dịch vụ (S) nào`);
      }

      // Rebuild payload SPECIFICALLY for service lines
      const serviceOrderData = {
        ...orderData,
        sales: serviceLines,
      };

      const serviceInvoiceData =
        await this.salesPayloadService.buildFastApiInvoiceData(
          serviceOrderData,
        );

      // Step 2: Tạo SalesOrder (Chỉ dòng S)
      this.logger.log(
        `[ServiceOrderFlow] Tạo SalesOrder cho ${serviceLines.length} dòng dịch vụ (S)`,
      );
      const salesOrderResult =
        await this.fastApiInvoiceFlowService.createSalesOrder({
          ...serviceInvoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || serviceInvoiceData.ong_ba || '',
        });

      // Validate Sales Order Result
      const isSoSuccess =
        (Array.isArray(salesOrderResult) &&
          salesOrderResult.length > 0 &&
          salesOrderResult[0].status === STATUS.SUCCESS) ||
        (salesOrderResult && salesOrderResult.status === STATUS.SUCCESS);

      if (!isSoSuccess) {
        const message =
          Array.isArray(salesOrderResult) && salesOrderResult[0]?.message
            ? salesOrderResult[0].message
            : salesOrderResult?.message || 'Tạo Sales Order thất bại';
        const error: any = new Error(message);
        error.response = { data: salesOrderResult };
        throw error;
      }

      // Step 3: Tạo SalesInvoice (Chỉ dòng S - dùng chung Payload)
      this.logger.log(
        `[ServiceOrderFlow] Tạo SalesInvoice cho ${serviceLines.length} dòng dịch vụ (S)`,
      );

      // Declare variable for scope visibility in return statement
      let salesInvoiceResult: any = null;

      salesInvoiceResult =
        await this.fastApiInvoiceFlowService.createSalesInvoice({
          ...serviceInvoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || serviceInvoiceData.ong_ba || '',
        });

      const isSiSuccess =
        (Array.isArray(salesInvoiceResult) &&
          salesInvoiceResult.length > 0 &&
          salesInvoiceResult[0].status === STATUS.SUCCESS) ||
        (salesInvoiceResult && salesInvoiceResult.status === STATUS.SUCCESS);

      if (!isSiSuccess) {
        const message =
          Array.isArray(salesInvoiceResult) && salesInvoiceResult[0]?.message
            ? salesInvoiceResult[0].message
            : salesInvoiceResult?.message || 'Tạo Sales Invoice thất bại';
        const error: any = new Error(message);
        error.response = { data: salesInvoiceResult };
        throw error;
      }

      const paymentErrors: string[] = [];
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
      } catch (err: any) {
        const msg = `[Cashio Error] ${err?.message || err}`;
        this.logger.error(
          `[Cashio] Error processing payment sync for order ${docCode}: ${msg}`,
        );
        paymentErrors.push(msg);
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
            docSourceType === DOC_SOURCE_TYPES.ORDER_RETURN ||
            docSourceType === DOC_SOURCE_TYPES.SALE_RETURN
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
                  serviceInvoiceData,
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
          const msg = `[Stock Payment Error] ${paymentError?.message || paymentError}`;
          this.logger.warn(
            `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${msg}`,
          );
          paymentErrors.push(msg);
        }
      }

      // Step 4: Tạo GxtInvoice (nếu có productType = 'S')
      let gxtInvoiceResult: any = null;
      if (serviceLines.length > 0) {
        // Tìm các dòng productType=I
        const exportLines = sales.filter((s: any) => {
          const productType = s?.productType?.toUpperCase()?.trim();
          return productType === PRODUCT_TYPES.ITEM_EXPORT;
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

      // 6. Build Result
      const isPaymentSuccess = paymentErrors.length === 0;
      const responseStatus =
        salesInvoiceResult && isSiSuccess && isPaymentSuccess
          ? STATUS.SUCCESS
          : STATUS.FAILED;

      let responseMessage = '';
      if (responseStatus === STATUS.SUCCESS) {
        responseMessage =
          'Tạo sales order và sales invoice thành công (02. Làm dịch vụ)';
      } else {
        const parts: string[] = [];
        if (!isSiSuccess) parts.push('Lỗi tạo Sales Invoice');
        if (!isPaymentSuccess)
          parts.push(`Lỗi thanh toán: ${paymentErrors.join('; ')}`);
        responseMessage =
          parts.join('. ') || 'Xử lý thất bại (02. Làm dịch vụ)';
      }

      return {
        result: {
          salesOrder: salesOrderResult,
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
          paymentErrors,
        },
        status: responseStatus,
        message: responseMessage,
        guid: salesInvoiceResult?.guid || null,
        fastApiResponse: {
          salesOrder: salesOrderResult,
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
          errors: paymentErrors,
        },
      };
    } catch (error: any) {
      this.logger.error(
        `[ServiceOrderFlow] Lỗi khi xử lý đơn dịch vụ ${docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }
}
