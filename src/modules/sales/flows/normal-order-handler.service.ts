import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { FastApiInvoiceFlowService } from '../../../services/fast-api-invoice-flow.service';
import { SalesPayloadService } from '../invoice/sales-payload.service';
import { SalesQueryService } from '../services/sales-query.service';
import { PaymentService } from '../../payment/payment.service';
import { forwardRef, Inject } from '@nestjs/common';
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
      });
    }

    // Explode sales by Stock Transfers (1 sale with 2 STs → 2 exploded sales)
    const [enrichedOrder] = await this.salesQueryService.enrichOrdersWithCashio(
      [orderData],
    );

    const invoiceData =
      await this.salesPayloadService.buildFastApiInvoiceData(enrichedOrder);

    // 2. Create Sales Order
    const soResult = await this.fastApiInvoiceFlowService.createSalesOrder({
      ...invoiceData,
      customer: orderData.customer,
      ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
    });

    const isSoSuccess =
      (Array.isArray(soResult) &&
        soResult.length > 0 &&
        soResult[0].status === STATUS.SUCCESS) ||
      (soResult && soResult.status === STATUS.SUCCESS);

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
          status: STATUS.SUCCESS,
          message: `Đơn hàng ${docCode} đã tồn tại trong Fast API`,
          result: error?.response?.data || {},
          fastApiResponse: error?.response?.data || {},
        };
      }
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
      siResult[0].status === STATUS.SUCCESS
        ? STATUS.SUCCESS
        : STATUS.FAILED;
    const responseMessage =
      responseStatus === STATUS.SUCCESS
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
  }
}
