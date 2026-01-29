import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { FastApiInvoiceFlowService } from '../../../services/fast-api-invoice-flow.service';
import { SalesPayloadService } from '../invoice/sales-payload.service';
import { SalesQueryService } from '../services/sales-query.service';
import { PaymentService } from '../../payment/payment.service';
import { forwardRef, Inject } from '@nestjs/common';
import { N8nService } from '../../../services/n8n.service';
import * as SalesUtils from '../../../utils/sales.utils';
import * as ConvertUtils from '../../../utils/convert.utils';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import { STATUS } from '../constants/sales-invoice.constants';
import axios from 'axios';

@Injectable()
export class NormalOrderHandlerService {
  private readonly logger = new Logger(NormalOrderHandlerService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private salesPayloadService: SalesPayloadService,
    private salesQueryService: SalesQueryService,
    private n8nService: N8nService,
    @Inject(forwardRef(() => PaymentService))
    private paymentService: PaymentService,
  ) {}

  /**
   * Helper xử lý đơn thường và đơn bán tài khoản
   */
  async handleNormalOrder(
    orderData: any,
    docCode: string,
  ): Promise<{
    result: any;
    status: number;
    message: string;
    guid?: string;
    fastApiResponse?: any;
  }> {
    this.logger.log(`[NormalOrder] Bắt đầu xử lý đơn thường: ${docCode}`);

    // 1. Create/Update Customer
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
        brand: orderData.sourceCompany || orderData.brand, // [NEW] Pass brand/sourceCompany
      });
    }

    // Explode sales by Stock Transfers (1 sale with 2 STs → 2 exploded sales)
    const [enrichedOrder] = await this.salesQueryService.enrichOrdersWithCashio(
      [orderData],
    );

    // [FIX] N8n Integration for Card Data (Enrichment source of truth)
    // Applied AFTER explosion to ensure we overwrite any Stock Transfer duplicates
    // [FIX] Check orderData.sales (Raw) for OrderType, as exploded sales might be minimalist and miss orderTypeName
    const isTachThe = orderData.sales?.some((s: any) =>
      SalesUtils.isTachTheOrder(s.ordertypeName),
    );

    if (isTachThe) {
      this.logger.log(
        `[NormalOrder] Detected Tách Thẻ order ${docCode}, fetching Card Data from N8n (After Explosion)...`,
      );
      try {
        const cardResponse =
          await this.n8nService.fetchCardDataWithRetry(docCode);
        const cardData = this.n8nService.parseCardData(cardResponse);
        if (cardData && cardData.length > 0) {
          // Use the CONSUMPTION logic to map data to enriched sales
          this.n8nService.mapIssuePartnerCodeToSales(
            enrichedOrder.sales,
            cardData,
          );
          this.logger.log(
            `[NormalOrder] Successfully enriched sales with N8n Card Data`,
          );
        } else {
          this.logger.warn(`[NormalOrder] N8n returned no card data`);
        }
      } catch (err) {
        this.logger.error(`[NormalOrder] Failed to enrich from N8n: ${err}`);
      }
    }

    const invoiceData =
      await this.salesPayloadService.buildFastApiInvoiceData(enrichedOrder);

    // 2. Create Sales Order (User Request: Run BOTH Order and Invoice)
    let soResult: any = null;
    try {
      soResult = await this.fastApiInvoiceFlowService.createSalesOrder({
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
          responseMessage.toLowerCase().includes('pk_d81') ||
          responseMessage.toLowerCase().includes('duplicate'));

      if (isDuplicateError) {
        this.logger.warn(
          `Sales Order ${docCode} đã tồn tại. Tiếp tục tạo Invoice.`,
        );
        soResult = {
          status: 1, // Mock success
          message: 'Sales Order already exists',
        };
      } else {
        // Nếu lỗi khác (không phải duplicate), có thể log và tiếp tục hoặc throw
        // User yêu cầu "đồng bộ chạy cả", nếu SO lỗi thì có thể ảnh hưởng SI?
        // Tạm thời log error và flow tiếp tục để đảm bảo Invoice (quan trọng hơn) vẫn chạy
        this.logger.error(
          `Lỗi tạo Sales Order ${docCode}: ${responseMessage}. Vẫn tiếp tục tạo Invoice.`,
        );
        soResult = {
          status: 0,
          message: error?.message || 'Create Sales Order Failed',
        };
      }
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
        this.logger.warn(
          `Đơn hàng ${docCode} đã tồn tại trong Fast API. Tiếp tục xử lý Payment.`,
        );
        // Mock success result to allow flow to continue
        siResult = [
          {
            status: STATUS.SUCCESS,
            message: 'Đã tồn tại (Duplicate) - Proceeding to Payment',
            guid: null, // Cannot retrieve GUID easily from duplicate error, but simpler for retryFlow
          },
        ];
      } else {
        // Real failure
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
    }

    const isSiSuccess =
      (Array.isArray(siResult) &&
        siResult.length > 0 &&
        siResult[0].status === STATUS.SUCCESS) ||
      (siResult && siResult.status === STATUS.SUCCESS);

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

    // 4 & 5. Payment Processing (Cashio & Stock)
    const paymentErrors: string[] = [];
    const cashioResult = {
      cashReceiptResults: [],
      creditAdviceResults: [],
    };
    let paymentResult: any = null;

    // 4. Cashio Payment
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
    } catch (err: any) {
      const msg = `[Cashio Error] ${err?.message || err}`;
      this.logger.error(
        `[Cashio] Error processing payment sync for order ${docCode}: ${msg}`,
      );
      paymentErrors.push(msg);
    }

    // 5. Payment (Stock)
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
    } catch (e: any) {
      const msg = `[Stock Payment Error] ${e?.message || e}`;
      this.logger.warn(`[Payment] warning: ${msg}`);
      // Only treat as error if it's critical? User seems to care about "Cấu hình thanh toán" which comes from here roughly or step 4.
      // processPayment uses findPaymentMethodByCode too.
      paymentErrors.push(msg);
    }

    // 6. Build Result
    // If there are payment errors, we treat the WHOLE process as FAILED to allow retry.
    const isPaymentSuccess = paymentErrors.length === 0;

    const responseStatus =
      isPaymentSuccess && isSiSuccess ? STATUS.SUCCESS : STATUS.FAILED;

    let responseMessage = '';
    if (responseStatus === STATUS.SUCCESS) {
      responseMessage = 'Tạo hóa đơn thành công';
      // Add note if it was a duplicate retry
      if (
        Array.isArray(siResult) &&
        siResult[0]?.message?.includes('Duplicate')
      ) {
        responseMessage = 'Tạo hóa đơn thành công (Đã tồn tại trước đó)';
      }
    } else {
      // Build error message
      const parts: string[] = [];
      if (!isSiSuccess) parts.push('Lỗi tạo Invoice');
      if (!isPaymentSuccess)
        parts.push(`Lỗi thanh toán: ${paymentErrors.join('; ')}`);
      responseMessage = parts.join('. ') || 'Xử lý thất bại';
    }

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
        paymentErrors,
      },
      status: responseStatus,
      message: responseMessage,
      guid: responseGuid,
      fastApiResponse: {
        salesOrder: soResult,
        salesInvoice: siResult,
        cashio: cashioResult,
        payment: paymentResult,
        errors: paymentErrors,
      },
    };
  }
}
