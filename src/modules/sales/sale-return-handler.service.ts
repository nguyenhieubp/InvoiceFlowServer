import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { SalesPayloadService } from './sales-payload.service';
import { SalesQueryService } from './sales-query.service';
import { InvoicePersistenceService } from './invoice-persistence.service';
import { PaymentService } from '../payment/payment.service';
import { forwardRef, Inject } from '@nestjs/common';
import * as StockTransferUtils from '../../utils/stock-transfer.utils';
import { DOC_SOURCE_TYPES, STATUS, ACTION } from './sales-invoice.constants';

@Injectable()
export class SaleReturnHandlerService {
  private readonly logger = new Logger(SaleReturnHandlerService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private salesPayloadService: SalesPayloadService,
    private salesQueryService: SalesQueryService,
    private invoicePersistenceService: InvoicePersistenceService,
    @Inject(forwardRef(() => PaymentService))
    private paymentService: PaymentService,
  ) {}

  /**
   * Xử lý đơn hàng trả lại (SALE_RETURN)
   */
  async handleSaleReturnFlow(
    orderData: any,
    docCode: string,
  ): Promise<{
    result: any;
    status: number;
    message: string;
    guid?: string;
    fastApiResponse?: any;
  }> {
    this.logger.log(`[SaleReturn] Bắt đầu xử lý đơn trả lại: ${docCode}`);

    // Kiểm tra xem có stock transfer không
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
    });

    // Case 1: Có stock transfer → Gọi API salesReturn
    if (stockTransfers && stockTransfers.length > 0) {
      // Build salesReturn data
      const salesReturnStockTransfers = stockTransfers.filter(
        (stockTransfer) =>
          stockTransfer.doctype === DOC_SOURCE_TYPES.SALE_RETURN,
      );
      const salesReturnData =
        await this.salesPayloadService.buildSalesReturnData(
          orderData,
          salesReturnStockTransfers,
        );

      // Gọi API salesReturn (không cần tạo/cập nhật customer)
      const result =
        await this.fastApiInvoiceFlowService.createSalesReturn(salesReturnData);

      const responseStatus =
        Array.isArray(result) &&
        result.length > 0 &&
        result[0].status === STATUS.SUCCESS
          ? STATUS.SUCCESS
          : STATUS.FAILED;
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
            new Set(stockTransfers.map((st) => st.stockCode).filter(Boolean)),
          );

          if (stockCodes.length > 0) {
            // Build invoiceData để dùng cho payment (tương tự như các case khác)
            const invoiceData =
              await this.salesPayloadService.buildFastApiInvoiceData(orderData);

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
        fastApiResponse: result,
      };
    }

    // Case 2: Không có stock transfer → Không xử lý (bỏ qua)
    // SALE_RETURN không có stock transfer không cần xử lý
    return {
      result: null,
      status: 0,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
    };
  }

  /**
   * Xử lý đơn hàng có đuôi _X (ví dụ: SO45.01574458_X)
   * Gọi API salesOrder với action: 1
   * Cả đơn có _X và đơn gốc (bỏ _X) đều sẽ có action = 1
   */
  async handleSaleOrderWithUnderscoreX(
    orderData: any,
    docCode: string,
    action: number,
  ): Promise<any> {
    this.logger.log(
      `[SaleOrderWithX] Bắt đầu xử lý đơn có đuôi _X: ${docCode}, action: ${action}`,
    );

    // Explode sales by Stock Transfers
    const [enrichedOrder] = await this.salesQueryService.enrichOrdersWithCashio(
      [orderData],
    );

    // Đơn có đuôi _X → Gọi API salesOrder với action: 1
    const invoiceData =
      await this.salesPayloadService.buildFastApiInvoiceData(enrichedOrder);

    const docCodeWithoutX = this.removeSuffixX(docCode);

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

      return {
        success: responseStatus === 1,
        message: responseMessage,
        result: {
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        },
        status: responseStatus,
        guid: responseGuid,
        fastApiResponse: {
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

      this.logger.error(
        `SALE_ORDER with _X suffix creation failed for order ${docCode}: ${formattedErrorMessage}`,
      );

      throw new Error(formattedErrorMessage);
    }
  }

  /**
   * Helper: Remove suffix _X from docCode
   */
  private removeSuffixX(code: string): string {
    return code.endsWith('_X') ? code.slice(0, -2) : code;
  }
}
