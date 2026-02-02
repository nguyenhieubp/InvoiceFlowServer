import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { FastApiInvoice } from '../../../entities/fast-api-invoice.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { STATUS, DOC_SOURCE_TYPES } from '../constants/sales-invoice.constants';
import { FastApiInvoiceFlowService } from '../../../services/fast-api-invoice-flow.service';
import { SalesPayloadService } from '../invoice/sales-payload.service';
import { SalesQueryService } from '../services/sales-query.service';
import { PaymentService } from '../../payment/payment.service';
import { SalesInvoiceService } from '../invoice/sales-invoice.service';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import axios from 'axios';

@Injectable()
export class SaleReturnHandlerService {
  private readonly logger = new Logger(SaleReturnHandlerService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(FastApiInvoice) // [NEW] Inject FastApiInvoiceRepository
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private salesPayloadService: SalesPayloadService,
    private salesQueryService: SalesQueryService,
    @Inject(forwardRef(() => PaymentService))
    private paymentService: PaymentService,
    @Inject(forwardRef(() => SalesInvoiceService))
    private salesInvoiceService: SalesInvoiceService,
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
    payload?: any;
    maDvcs?: string;
    maKh?: string;
    tenKh?: string;
  }> {
    this.logger.log(`[SaleReturn] Bắt đầu xử lý đơn trả lại: ${docCode}`);

    // Payload Logging
    const payloadLog: any = {};

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

      payloadLog.salesReturn = salesReturnData;

      // [NEW] Tạo/cập nhật customer trước khi tạo đơn trả lại
      if (orderData.customer) {
        try {
          // Resolve brand from orderData (first sale line)
          const firstSale = orderData.sales?.[0];
          const brand = firstSale?.brand;

          const getCustomer = await axios.get(
            'https://n8n.vmt.vn/webhook/vmt/check_customer',
            {
              headers: {
                'Content-Type': 'application/json',
              },
              data: {
                partner_code: orderData.customer.code,
                source_company: brand,
              },
            },
          );

          const getCustomerData = getCustomer.data[0].data[0];

          const customerData = {
            ma_kh: getCustomerData.code,
            ten_kh: getCustomerData.name,
            dia_chi: getCustomerData.address_name,
            ngay_sinh: getCustomerData.birthday,
            so_cccd: getCustomerData.id_card_number,
            e_mail: getCustomerData.email,
            gioi_tinh: orderData.customer.sexual,
            brand: brand,
          };
          this.logger.log(
            `[SaleReturn] Tạo/cập nhật khách hàng: ${orderData.customer.code}`,
          );
          await this.fastApiInvoiceFlowService.createOrUpdateCustomer(
            customerData,
          );
        } catch (error) {
          this.logger.error(
            `[SaleReturn] Lỗi tạo khách hàng: ${error?.message}`,
            error?.stack,
          );
        }
      }

      // Gọi API salesReturn
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
        payload: payloadLog,
        maDvcs: salesReturnData?.ma_dvcs,
        maKh: salesReturnData?.ma_kh,
        tenKh: salesReturnData?.ong_ba,
      };
    }

    // Case 2: Không có stock transfer → Không xử lý (bỏ qua)
    // SALE_RETURN không có stock transfer không cần xử lý
    return {
      result: null,
      status: 0,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
      payload: payloadLog,
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

    // [NEW] 1. Create/Update Entry in fast_api_invoices with STATUS.PROCESSING (0)
    let fastApiInvoice = await this.fastApiInvoiceRepository.findOne({
      where: { docCode },
    });

    if (!fastApiInvoice) {
      fastApiInvoice = this.fastApiInvoiceRepository.create({
        docCode,
        maDvcs: orderData.maDvcs || orderData.branchCode || '',
        maKh: orderData.customer?.code || '',
        tenKh: orderData.customer?.name || '',
        ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
        status: STATUS.PROCESSING, // 2
        isManuallyCreated: false,
        lastErrorMessage: '',
        type: 'SALE_ORDER_X', // Helper type to distinguish
      });
    } else {
      fastApiInvoice.status = STATUS.PROCESSING;
      fastApiInvoice.lastErrorMessage = '';
      fastApiInvoice.maDvcs = orderData.maDvcs || orderData.branchCode || '';
      fastApiInvoice.maKh = orderData.customer?.code || '';
      fastApiInvoice.tenKh = orderData.customer?.name || '';
      fastApiInvoice.updatedAt = new Date();
    }
    await this.fastApiInvoiceRepository.save(fastApiInvoice);

    // Payload Logging
    const payloadLog: any = {};

    try {
      const docCodeWithoutX = this.removeSuffixX(docCode);

      const orderWithoutX =
        await this.salesInvoiceService.findByOrderCode(docCodeWithoutX);

      // Explode sales by Stock Transfers
      const [enrichedOrder] =
        await this.salesQueryService.enrichOrdersWithCashio([orderWithoutX]);

      // Đơn có đuôi _X → Gọi API salesOrder với action: 1
      const invoiceData =
        await this.salesPayloadService.buildFastApiInvoiceData(enrichedOrder);

      // [NEW] Update metadata from built invoiceData to ensure consistency with payload
      fastApiInvoice.maDvcs = invoiceData.ma_dvcs || fastApiInvoice.maDvcs;
      fastApiInvoice.maKh = invoiceData.ma_kh || fastApiInvoice.maKh;
      fastApiInvoice.tenKh =
        invoiceData.ten_kh || invoiceData.ong_ba || fastApiInvoice.tenKh;
      await this.fastApiInvoiceRepository.save(fastApiInvoice);

      // Gọi API salesOrder với action = 1 (không cần tạo/cập nhật customer)
      let result: any;
      const data = {
        ...invoiceData,
        dien_giai: docCodeWithoutX,
        so_ct: docCodeWithoutX,
        ma_kho: orderData?.maKho || '',
      };

      const soPayload = {
        ...data,
        customer: orderData.customer,
        ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
      };
      payloadLog.salesOrder = soPayload;

      result = await this.fastApiInvoiceFlowService.createSalesOrder(
        soPayload,
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

      // [NEW] 2. Update DB based on API Status
      if (responseStatus === 1) {
        responseMessage = shouldUseApiMessage
          ? `Tạo đơn hàng thành công cho đơn hàng ${docCode}. ${apiMessage}`
          : `Tạo đơn hàng thành công cho đơn hàng ${docCode}`;

        fastApiInvoice.status = STATUS.SUCCESS; // 1
        fastApiInvoice.guid = responseGuid;
        fastApiInvoice.lastErrorMessage = responseMessage;
        fastApiInvoice.fastApiResponse = JSON.stringify({
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        });
      } else {
        responseMessage = shouldUseApiMessage
          ? `Tạo đơn hàng thất bại cho đơn hàng ${docCode}. ${apiMessage}`
          : `Tạo đơn hàng thất bại cho đơn hàng ${docCode}`;

        fastApiInvoice.status = STATUS.FAILED; // 0
        fastApiInvoice.lastErrorMessage = responseMessage;
        fastApiInvoice.fastApiResponse = JSON.stringify({
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        });
      }
      // Save payload log
      fastApiInvoice.payload = JSON.stringify(payloadLog);
      await this.fastApiInvoiceRepository.save(fastApiInvoice);

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
        payload: payloadLog,
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

      // [NEW] 3. Update DB state to FAILED (2) on Exception
      try {
        fastApiInvoice.status = STATUS.FAILED; // 0
        fastApiInvoice.lastErrorMessage = formattedErrorMessage;

        // [NEW] Save detailed error to fastApiResponse
        const errorJson = {
          success: false,
          message: formattedErrorMessage,
          error: errorMessage,
          details: error?.response?.data || null,
          timestamp: new Date().toISOString(),
        };
        fastApiInvoice.fastApiResponse = JSON.stringify(errorJson);

        // Ensure payload is stringified
        if (payloadLog && typeof payloadLog === 'object') {
          fastApiInvoice.payload = JSON.stringify(payloadLog);
        } else {
          fastApiInvoice.payload = payloadLog;
        }

        await this.fastApiInvoiceRepository.save(fastApiInvoice);
      } catch (dbError) {
        this.logger.error(
          `Failed to update invoice status for ${docCode}`,
          dbError,
        );
      }

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
