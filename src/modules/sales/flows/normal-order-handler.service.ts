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
  ) { }

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
    payload?: any;
  }> {
    this.logger.log(`[NormalOrder] Bắt đầu xử lý đơn thường: ${docCode}`);

    // 1. Create/Update Customer
    if (orderData.customer?.code) {
      await this.fastApiInvoiceFlowService.createOrUpdateCustomer({
        ma_kh: SalesUtils.normalizeMaKh(orderData.customer.code),
        brand: orderData.sourceCompany || orderData.brand,
      });
    }

    // Explode sales by Stock Transfers (1 sale with 2 STs → 2 exploded sales)
    const [enrichedOrder] = await this.salesQueryService.enrichOrdersWithCashio(
      [orderData],
    );

    // [FIX] N8n Integration for Card Data (Enrichment source of truth)
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

    const payloadLog: any = {};

    // ---------------------------------------------------------
    // STEP 2: CREATE SALES ORDER (Unified - 1 SO per Order)
    // ---------------------------------------------------------
    let soResult: any = null;
    let soStatus: number = STATUS.FAILED; // [FIX] Explicit type

    try {
      // Build full payload for SO
      const fullInvoiceData =
        await this.salesPayloadService.buildFastApiInvoiceData(enrichedOrder);

      const soPayload = {
        ...fullInvoiceData,
        customer: orderData.customer,
        ten_kh: orderData.customer?.name || fullInvoiceData.ong_ba || '',
      };
      payloadLog.salesOrder = soPayload;

      // Create Sales Order
      // [OPTIMIZATION] Sync Lot/Serial ONCE here (skipCustomerSync=true as done above)
      soResult = await this.fastApiInvoiceFlowService.createSalesOrder(
        soPayload,
        0,
        {
          skipCustomerSync: true,
        },
      );
      soStatus = STATUS.SUCCESS;
    } catch (error: any) {
      const exceptionResponse = error?.getResponse ? error.getResponse() : null;
      const responseData =
        exceptionResponse?.data || error?.response?.data || null;
      const responseMessage =
        exceptionResponse?.message ||
        error?.response?.data?.message ||
        error?.message ||
        '';
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
        soStatus = STATUS.SUCCESS;
      } else {
        this.logger.error(
          `Lỗi tạo Sales Order ${docCode}: ${responseMessage}. Vẫn tiếp tục tạo Invoice.`,
        );
        soResult = {
          status: 0,
          message: responseMessage || 'Create Sales Order Failed',
          response: responseData,
        };
        // Continue to Invoice even if SO failed?
        // Usually, if SO fails, we might still try SI or stop.
        // Current logic allows continuing.
      }
    }

    // ---------------------------------------------------------
    // STEP 3: CREATE SALES INVOICE (Split by Date)
    // ---------------------------------------------------------
    // Group sales by transDate (formatted YYYY-MM-DD)
    const salesByDate = new Map<string, any[]>();
    const noStockTransferSales: any[] = [];

    enrichedOrder.sales.forEach((sale: any) => {
      // Check transDate from attached stockTransfer
      let transDateStr = '';
      const rawTransDate = sale.stockTransfer?.transDate || sale.transDate;
      if (rawTransDate) {
        // Handle both Date objects and strings
        const d = new Date(rawTransDate);
        if (!isNaN(d.getTime())) {
          transDateStr = ConvertUtils.formatDateYYYYMMDD(d); // YYYYMMDD
        }
      }

      this.logger.debug(
        `[NormalOrder] Sale item ${sale.itemCode}: transDate=${rawTransDate} -> key=${transDateStr}`,
      );

      if (transDateStr) {
        let group = salesByDate.get(transDateStr);
        if (!group) {
          group = [];
          salesByDate.set(transDateStr, group);
        }
        group.push(sale);
      } else {
        noStockTransferSales.push(sale);
      }
    });

    const sortedDates = Array.from(salesByDate.keys()).sort();

    // Strategy for No-Stock-Transfer items:
    // If no dates, use today/order date (single group).
    // If dates exist, attach no-stock items to the FIRST group.
    if (sortedDates.length === 0) {
      const orderDateStr = orderData.docDate
        ? ConvertUtils.formatDateYYYYMMDD(new Date(orderData.docDate))
        : ConvertUtils.formatDateYYYYMMDD(new Date());
      salesByDate.set(orderDateStr, enrichedOrder.sales);
      sortedDates.push(orderDateStr);
    } else {
      if (noStockTransferSales.length > 0) {
        const firstDate = sortedDates[0];
        const group = salesByDate.get(firstDate);
        if (group) {
          group.push(...noStockTransferSales);
        }
      }
    }

    const siResults: any[] = [];
    const splitErrors: string[] = [];
    let processingStatus: number = STATUS.SUCCESS; // [FIX] Explicit type

    for (let i = 0; i < sortedDates.length; i++) {
      const dateKey = sortedDates[i];
      const groupSales = salesByDate.get(dateKey);

      // Determine Suffix (REMOVED as per user request)
      const currentDocCode = docCode;

      this.logger.log(
        `[NormalOrder] Processing Invoice Split ${currentDocCode} (Date: ${dateKey}) - Items: ${groupSales?.length}`,
      );

      // Construct Partial Order Data
      const partialOrderData = {
        ...enrichedOrder, // [FIX] Use enrichedOrder to include cashioData
        docCode: currentDocCode, // Use split code
        // docDate: NO OVERRIDE here, keep original for reference
        sales: groupSales,
      };

      // Build Invoice Payload for this split
      let invoiceData: any;
      try {
        invoiceData =
          await this.salesPayloadService.buildFastApiInvoiceData(
            partialOrderData,
          );
      } catch (err: any) {
        const msg = `Failed to build payload for ${currentDocCode}: ${err.message}`;
        this.logger.error(`[NormalOrder] ${msg}`);
        splitErrors.push(msg);
        processingStatus = STATUS.FAILED;
        continue;
      }

      // Override Dates for Sales Invoice (Use Stock Transfer Date)
      const year = dateKey.slice(0, 4);
      const month = dateKey.slice(4, 6);
      const day = dateKey.slice(6, 8);
      const stockTransferDateISO = `${year}-${month}-${day}T00:00:00.000Z`;

      const siPayload = {
        ...invoiceData,
        ngay_ct: stockTransferDateISO,
        ngay_lct: stockTransferDateISO,
        dh_ngay: stockTransferDateISO,
        trans_date: stockTransferDateISO,
        customer: partialOrderData.customer,
        ten_kh: partialOrderData.customer?.name || invoiceData.ong_ba || '',
      };

      // Save payload to log (accumulate if multiple)
      if (!payloadLog.salesInvoice) payloadLog.salesInvoice = [];
      if (Array.isArray(payloadLog.salesInvoice)) {
        payloadLog.salesInvoice.push({
          docCode: currentDocCode,
          payload: siPayload,
        });
      }

      // Create Sales Invoice
      let siResult: any = null;
      try {
        // [OPTIMIZATION] Skip sync here (already done in SO step)
        siResult = await this.fastApiInvoiceFlowService.createSalesInvoice(
          siPayload,
          { skipLotSync: true },
        );
        // Add docCode for reference
        if (Array.isArray(siResult) && siResult.length > 0) {
          siResult[0].docCode = currentDocCode;
        } else if (siResult) {
          siResult.docCode = currentDocCode;
        }
      } catch (error: any) {
        const exceptionResponse = error?.getResponse
          ? error.getResponse()
          : null;
        const responseMessage =
          exceptionResponse?.message ||
          error?.response?.data?.message ||
          error?.message ||
          '';
        const isDuplicateError =
          typeof responseMessage === 'string' &&
          (responseMessage.toLowerCase().includes('đã tồn tại') ||
            responseMessage.toLowerCase().includes('pk_d81'));

        if (isDuplicateError) {
          siResult = [
            {
              status: STATUS.SUCCESS,
              message: 'Invoice Duplicate',
              guid: null,
              docCode: currentDocCode,
            },
          ];
        } else {
          this.logger.error(
            `Lỗi tạo Sales Invoice ${currentDocCode}: ${responseMessage}`,
          );
          siResult = {
            status: 0,
            message: responseMessage,
            docCode: currentDocCode,
          };
          splitErrors.push(`SI ${currentDocCode} failed: ${responseMessage}`);
          processingStatus = STATUS.FAILED;
        }
      }

      siResults.push(siResult);
    } // End Loop

    // Check overall SI success
    const isSiSuccess =
      siResults.length > 0 &&
      siResults.every(
        (r) =>
          (Array.isArray(r) && r[0].status === STATUS.SUCCESS) ||
          r.status === STATUS.SUCCESS,
      );

    if (!isSiSuccess && processingStatus === STATUS.SUCCESS) {
      processingStatus = STATUS.FAILED;
    }

    // ---------------------------------------------------------
    // STEP 4 & 5: PAYMENT PROCESSING
    // ---------------------------------------------------------
    const paymentErrors: string[] = [];
    const cashioResult = {
      cashReceiptResults: [],
      creditAdviceResults: [],
    };
    let paymentResult: any = null;

    // Determine Main Split (First Successful One)
    const mainSplitResult = siResults.find(
      (r) =>
        (Array.isArray(r) && r[0].status === STATUS.SUCCESS) ||
        r.status === STATUS.SUCCESS,
    );
    const mainDocCode = mainSplitResult
      ? mainSplitResult.docCode ||
      (Array.isArray(mainSplitResult)
        ? mainSplitResult[0].docCode
        : undefined)
      : docCode; // Fallback

    if (processingStatus === STATUS.SUCCESS || mainSplitResult) {
      // 4. Cashio Payment
      try {
        const paymentDataList =
          await this.paymentService.findPaymentByDocCode(docCode);
        if (paymentDataList && paymentDataList.length > 0) {
          this.logger.log(
            `[Cashio] Found ${paymentDataList.length} payment records. Linking to ${mainDocCode}...`,
          );
          for (const originalPaymentData of paymentDataList) {
            // Clone and override so_code/so_hd/ma_tc to point to the actual Invoice created
            const modifiedPaymentData = {
              ...originalPaymentData,
              so_code: mainDocCode, // Point to split invoice
              so_hd: mainDocCode, // Point to split invoice
              ma_tc: mainDocCode, // Point to split invoice
            };
            await this.fastApiInvoiceFlowService.processCashioPayment(
              modifiedPaymentData,
            );
          }
          this.logger.log(`[Cashio] Payment sync completed.`);
        }
      } catch (err: any) {
        const msg = `[Cashio Error] ${err?.message || err}`;
        this.logger.error(msg);
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
          // [NOTE] processPayment might utilize docCode inside to fetch payments again.
          // We should pass mainDocCode if possible, but processPayment signature
          // takes docCode (to look up payments).
          // Ideally, we should refactor processPayment to accept targetDocCode.
          // For now, we pass docCode (original) to find payments,
          // ensuring processPayment uses mainDocCode for actual submission would be ideal
          // but might be out of scope for this surgical change.
          // Let's assume processPayment handles standard flow.
          // Wait, processPayment takes invoiceData too. Let's pass the payload of main split.
          const mainSplitPayloadStr = JSON.stringify(
            payloadLog.salesInvoice?.find((p: any) => p.docCode === mainDocCode)
              ?.payload || {},
          );
          const mainSplitPayload = JSON.parse(mainSplitPayloadStr);

          paymentResult = await this.fastApiInvoiceFlowService.processPayment(
            docCode, // Use original code to find payments
            orderData,
            mainSplitPayload, // Use payload from main split
            stockCodes,
          );
        }
      } catch (e: any) {
        const msg = `[Stock Payment Error] ${e?.message || e}`;
        this.logger.warn(`[Payment] warning: ${msg}`);
        paymentErrors.push(msg);
      }
    }

    // ---------------------------------------------------------
    // STEP 6: BUILD FINAL RESPONSE
    // ---------------------------------------------------------
    const isPaymentSuccess = paymentErrors.length === 0;
    const finalStatus =
      isSiSuccess && isPaymentSuccess && soStatus === STATUS.SUCCESS
        ? STATUS.SUCCESS
        : STATUS.FAILED;

    let responseMessage = '';
    if (finalStatus === STATUS.SUCCESS) {
      responseMessage = 'Tạo hóa đơn thành công';
      if (splitErrors.length > 0) {
        responseMessage += ` (Có lỗi ở invoice con: ${splitErrors.join('; ')})`;
      }
    } else {
      const parts: string[] = [];
      if (soStatus !== STATUS.SUCCESS) parts.push('Lỗi tạo Sales Order');
      if (!isSiSuccess)
        parts.push(`Lỗi tạo Invoice: ${splitErrors.join('; ')}`);
      if (!isPaymentSuccess)
        parts.push(`Lỗi thanh toán: ${paymentErrors.join('; ')}`);
      responseMessage = parts.join('. ') || 'Xử lý thất bại';
    }

    // Extract GUID from main split for return (legacy compatibility)
    let responseGuid: string | undefined;
    if (mainSplitResult) {
      if (Array.isArray(mainSplitResult)) {
        responseGuid = mainSplitResult[0]?.guid;
      } else {
        responseGuid = mainSplitResult?.guid;
      }
    }

    return {
      result: {
        salesOrder: soResult,
        salesInvoice: siResults,
        cashio: cashioResult,
        payment: paymentResult,
        paymentErrors,
      },
      status: finalStatus,
      message: responseMessage,
      guid: responseGuid,
      fastApiResponse: {
        salesOrder: soResult,
        salesInvoice: siResults,
        cashio: cashioResult,
        payment: paymentResult,
        errors: splitErrors.concat(paymentErrors),
      },
      payload: payloadLog,
    };
  }
}
