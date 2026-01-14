import {
  Injectable,
  Logger,
  BadRequestException,
  Inject,
  forwardRef,
} from '@nestjs/common';
import { FastApiClientService } from './fast-api-client.service';
import { CategoriesService } from '../modules/categories/categories.service';
import { SyncService } from '../modules/sync/sync.service';
import { LoyaltyService } from './loyalty.service';
import { FastApiPayloadHelper } from './fast-api-payload.helper';
import { formatDateYYYYMMDD } from '../utils/convert.utils';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';
import { PaymentSyncLog } from '../entities/payment-sync-log.entity';

/**
 * Service quản lý tạo invoice trong Fast API
 * Luồng: Customer → Sales Invoice
 */
@Injectable()
export class FastApiInvoiceFlowService {
  private readonly logger = new Logger(FastApiInvoiceFlowService.name);

  constructor(
    private readonly fastApiService: FastApiClientService,
    private readonly categoriesService: CategoriesService,
    @Inject(forwardRef(() => SyncService))
    private readonly syncService: SyncService,
    private readonly loyaltyService: LoyaltyService,
    @InjectRepository(PaymentSyncLog)
    private readonly paymentSyncLogRepository: Repository<PaymentSyncLog>,
  ) {}

  /**
   * Tạo/cập nhật khách hàng trong Fast API
   * 2.1/ Danh mục khách hàng
   */
  async createOrUpdateCustomer(customerData: {
    ma_kh: string;
    ten_kh: string;
    dia_chi?: string;
    ngay_sinh?: string;
    so_cccd?: string;
    e_mail?: string;
    gioi_tinh?: string;
    dien_thoai?: string;
  }): Promise<any> {
    try {
      const result =
        await this.fastApiService.createOrUpdateCustomer(customerData);

      // Validate response: status = 1 mới là success
      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        if (firstItem.status !== 1) {
          const errorMessage =
            firstItem.message || 'Tạo/cập nhật customer thất bại';
          this.logger.error(
            `[Flow] Customer API trả về status = ${firstItem.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      } else if (
        result &&
        typeof result === 'object' &&
        result.status !== undefined
      ) {
        if (result.status !== 1) {
          const errorMessage =
            result.message || 'Tạo/cập nhật customer thất bại';
          this.logger.error(
            `[Flow] Customer API trả về status = ${result.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      }
      return result;
    } catch (error: any) {
      this.logger.warn(
        `[Flow] Customer creation failed but continuing: ${error?.message || error}`,
      );
      // Không throw error để không chặn luồng tạo invoice
      return null;
    }
  }

  /**
   * Tạo đơn hàng bán (salesOrder) trong Fast API
   * 2.3/ Đơn hàng bán
   * JSON body giống hóa đơn bán hàng (salesInvoice)
   * @param orderData - Dữ liệu đơn hàng
   * @param action - Action: 0 (mặc định) cho đơn hàng bán, 1 cho đơn hàng trả lại
   */
  async createSalesOrder(orderData: any, action: number = 0): Promise<any> {
    try {
      const cleanOrderData = FastApiPayloadHelper.buildCleanPayload(
        orderData,
        action,
      );
      const finalPayload =
        FastApiPayloadHelper.removeEmptyFields(cleanOrderData);
      const dataPromotion = finalPayload.detail.filter(
        (item) => item.ma_ck01 || item.ma_ctkm_th,
      );
      const uniquePromotions = new Map<string, any>();

      for (const item of dataPromotion) {
        if (!uniquePromotions.has(item.ma_ck01 || item.ma_ctkm_th)) {
          uniquePromotions.set(item.ma_ck01 || item.ma_ctkm_th, item);
        }
      }

      for (const item of uniquePromotions.values()) {
        // NOTE: ma_bp thay bằng dvcs theo yêu cầu BA
        const dataPayload = {
          ma_ctkm: item.ma_ck01 || item.ma_ctkm_th,
          ten_ctkm: item.ma_ck01 || item.ma_ctkm_th,
          ma_phi: item.ma_phi,
          tk_cpkm: item.tk_chi_phi,
          tk_ck: item.tk_chiet_khau,
        };

        const resultPromotion = await this.callPromotion(dataPayload);

        if (resultPromotion.code !== 1) {
          throw new BadRequestException(
            `Gọi promotion thất bại: ${item.ma_ck01 || item.ma_ctkm_th}`,
          );
        }
      }
      const result = await this.fastApiService.submitSalesOrder(finalPayload);
      // Validate response: status = 1 mới là success
      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        if (firstItem.status !== 1) {
          const errorMessage = firstItem.message || 'Tạo sales order thất bại';
          throw new BadRequestException(errorMessage);
        }
      } else if (
        result &&
        typeof result === 'object' &&
        result.status !== undefined
      ) {
        if (result.status !== 1) {
          const errorMessage = result.message || 'Tạo sales order thất bại';
          throw new BadRequestException(errorMessage);
        }
      }

      return result;
    } catch (error: any) {
      throw error;
    }
  }

  async callPromotion(promotionData: any): Promise<any> {
    try {
      return await this.fastApiService.callPromotion(promotionData);
    } catch (error: any) {
      this.logger.error(
        `[Flow] Failed to call promotion ${JSON.stringify(promotionData)}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Tạo hóa đơn bán hàng trong Fast API
   * 2.4/ Hóa đơn bán hàng
   * FAST 2.4
   */
  async createSalesInvoice(invoiceData: any): Promise<any> {
    try {
      // FIX: Validate mã CTKM với Loyalty API trước khi gửi lên Fast API
      // Helper function: cắt phần sau dấu "-" để lấy mã CTKM để check (ví dụ: "PRMN.020228-R510SOCOM" → "PRMN.020228")
      // Và chuyển đổi các mã VC label sang format có khoảng trắng (VCHB → VC HB, VCKM → VC KM, VCDV → VC DV)
      const getPromotionCodeToCheck = (
        promCode: string | null | undefined,
      ): string | null => {
        if (!promCode) return null;
        const trimmed = promCode.trim();
        if (trimmed === '') return null;
        // Cắt phần sau dấu "-" để lấy mã CTKM
        const parts = trimmed.split('-');
        let codeToCheck = parts[0] || trimmed;

        // Loại bỏ prefix "FBV TT " nếu có (ví dụ: "FBV TT VCHB" → "VCHB")
        codeToCheck = codeToCheck.replace(/^FBV\s+TT\s+/i, '');

        // Chuyển đổi các mã VC label sang format có khoảng trắng để match với Loyalty API
        // VCHB → VC HB, VCKM → VC KM, VCDV → VC DV
        codeToCheck = codeToCheck.replace(/VCHB/g, 'VC HB');
        codeToCheck = codeToCheck.replace(/VCKM/g, 'VC KM');
        codeToCheck = codeToCheck.replace(/VCDV/g, 'VC DV');

        return codeToCheck;
      };

      const validationErrors: string[] = [];
      const promotionCodes = new Set<string>();

      // Collect tất cả mã CTKM từ detail (ma_ck01, ma_ck02, ..., ma_ck22, ma_ctkm_th)
      if (invoiceData.detail && Array.isArray(invoiceData.detail)) {
        for (const item of invoiceData.detail) {
          // Collect ma_ctkm_th (mã CTKM tặng hàng) - không áp dụng getPromotionCodeToCheck
          if (
            item.ma_ctkm_th &&
            item.ma_ctkm_th.trim() !== '' &&
            item.ma_ctkm_th !== 'TT DAU TU'
          ) {
            const codeToCheck = item.ma_ctkm_th.trim();
            if (codeToCheck) {
              promotionCodes.add(codeToCheck);
            }
          }

          // Collect các mã CTKM mua hàng giảm giá (ma_ck01 đến ma_ck22)
          // Lưu ý: ma_ck05 (Thanh toán voucher) không phải promotion code nên không cần validate
          for (let i = 1; i <= 22; i++) {
            // Bỏ qua ma_ck05 (Thanh toán voucher) - không cần validate với Loyalty API
            if (i === 5) continue;

            const maCk = item[`ma_ck${i.toString().padStart(2, '0')}`];
            if (maCk && maCk.trim() !== '') {
              // Chỉ áp dụng getPromotionCodeToCheck cho ma_ck01
              const codeToCheck =
                i === 1 ? getPromotionCodeToCheck(maCk) : maCk.trim();
              if (codeToCheck) {
                promotionCodes.add(codeToCheck);
              }
            }
          }
        }
      }

      // Validate từng mã CTKM với Loyalty API (chỉ check phần trước dấu "-")
      for (const sale of invoiceData.detail) {
        const promotionData = {
          ma_ctkm: sale.ma_ck01 || sale.ma_ctkm_th || '',
          ten_ctkm: sale.ma_ck01 || sale.ma_ctkm_th || '',
          ma_phi: sale.ma_phi || '',
          tk_cpkm: sale.tk_chi_phi || '',
          tk_ck: sale.tk_chiet_khau || '',
        };
        if (sale.ma_ck01 || sale.ma_ctkm_th) {
          const resultPromotion =
            await this.categoriesService.createPromotionFromLoyaltyAPI(
              promotionData,
            );
          if (resultPromotion.status !== 1) {
            throw new BadRequestException(
              `Tạo promotion thất bại: ${sale.ma_ck01 || sale.ma_ctkm_th}`,
            );
          }
        }
      }

      // Nếu có lỗi validation, throw error và không gửi lên Fast API
      if (validationErrors.length > 0) {
        const errorMessage = `Mã khuyến mãi không hợp lệ:\n${validationErrors.join('\n')}`;
        this.logger.error(`[Flow] ${errorMessage}`);
        throw new BadRequestException(errorMessage);
      }

      // Build clean payload (giống salesOrder nhưng action luôn = 0)
      const cleanInvoiceData = FastApiPayloadHelper.buildCleanPayload(
        invoiceData,
        0,
        'saleInvoice',
      );
      const finalPayload =
        FastApiPayloadHelper.removeEmptyFields(cleanInvoiceData);

      const result = await this.fastApiService.submitSalesInvoice(finalPayload);

      // Validate response: status = 1 mới là success
      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        if (firstItem.status !== 1) {
          const errorMessage =
            firstItem.message || 'Tạo sales invoice thất bại';
          this.logger.error(
            `[Flow] Sales Invoice API trả về status = ${firstItem.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      } else if (
        result &&
        typeof result === 'object' &&
        result.status !== undefined
      ) {
        if (result.status !== 1) {
          const errorMessage = result.message || 'Tạo sales invoice thất bại';
          this.logger.error(
            `[Flow] Sales Invoice API trả về status = ${result.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      }

      this.logger.log(
        `[Flow] Sales invoice ${invoiceData.so_ct} created successfully`,
      );
      return result;
    } catch (error: any) {
      this.logger.error(
        `[Flow] Failed to create sales invoice ${invoiceData.so_ct}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Tạo hàng bán trả lại (salesReturn) trong Fast API
   * 2.15/ Hàng bán trả lại
   * FAST 2.15
   * Sử dụng cho SALE_RETURN có stock transfer
   */
  async createSalesReturn(salesReturnData: any): Promise<any> {
    this.logger.log(
      `[Flow] Creating sales return ${salesReturnData.so_ct || 'N/A'}...`,
    );
    try {
      const finalPayload = FastApiPayloadHelper.removeEmptyFields(
        salesReturnData,
        false,
      );

      const result = await this.fastApiService.submitSalesReturn(finalPayload);

      // Validate response: status = 1 mới là success
      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        if (firstItem.status !== 1) {
          const errorMessage = firstItem.message || 'Tạo sales return thất bại';
          this.logger.error(
            `[Flow] Sales Return API trả về status = ${firstItem.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      } else if (
        result &&
        typeof result === 'object' &&
        result.status !== undefined
      ) {
        if (result.status !== 1) {
          const errorMessage = result.message || 'Tạo sales return thất bại';
          this.logger.error(
            `[Flow] Sales Return API trả về status = ${result.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      }

      this.logger.log(
        `[Flow] Sales return ${salesReturnData.so_ct || 'N/A'} created successfully`,
      );
      return result;
    } catch (error: any) {
      this.logger.error(
        `[Flow] Failed to create sales return ${salesReturnData.so_ct || 'N/A'}: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `[Flow] Sales return error response status: ${error.response.status}`,
        );
        this.logger.error(
          `[Flow] Sales return error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Tạo phiếu tạo gộp – xuất tách (gxtInvoice) trong Fast API
   * Sử dụng cho đơn dịch vụ: detail (nhập - productType = 'S'), ndetail (xuất - productType = 'I')
   * FAST 2.10
   */
  async createGxtInvoice(gxtInvoiceData: any): Promise<any> {
    this.logger.log(
      `[Flow] Creating gxt invoice ${gxtInvoiceData.so_ct || 'N/A'}...`,
    );
    try {
      const finalPayload = FastApiPayloadHelper.removeEmptyFields(
        gxtInvoiceData,
        false,
      );

      const result = await this.fastApiService.submitGxtInvoice(finalPayload);

      // Validate response: status = 1 mới là success
      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        if (firstItem.status !== 1) {
          const errorMessage = firstItem.message || 'Tạo gxt invoice thất bại';
          this.logger.error(
            `[Flow] GxtInvoice API trả về status = ${firstItem.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      } else if (
        result &&
        typeof result === 'object' &&
        result.status !== undefined
      ) {
        if (result.status !== 1) {
          const errorMessage = result.message || 'Tạo gxt invoice thất bại';
          this.logger.error(
            `[Flow] GxtInvoice API trả về status = ${result.status}: ${errorMessage}`,
          );
          throw new BadRequestException(errorMessage);
        }
      }

      this.logger.log(
        `[Flow] GxtInvoice ${gxtInvoiceData.so_ct || 'N/A'} created successfully`,
      );
      return result;
    } catch (error: any) {
      this.logger.error(
        `[Flow] Failed to create gxt invoice ${gxtInvoiceData.so_ct || 'N/A'}: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `[Flow] GxtInvoice error response status: ${error.response.status}`,
        );
        this.logger.error(
          `[Flow] GxtInvoice error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Thực hiện tạo invoice - tạo Customer, salesOrder, sau đó tạo Sales Invoice
   */
  async executeFullInvoiceFlow(invoiceData: {
    ma_kh: string;
    ten_kh?: string;
    customer?: any;
    detail: Array<{
      ma_vt: string;
      ten_vt?: string;
      dvt?: string;
      [key: string]: any;
    }>;
    [key: string]: any;
  }): Promise<any> {
    this.logger.log(
      `[Flow] Starting invoice creation for order ${invoiceData.so_ct || 'N/A'}`,
    );

    try {
      // Step 1: Tạo/cập nhật Customer
      if (invoiceData.ma_kh) {
        await this.createOrUpdateCustomer({
          ma_kh: invoiceData.ma_kh,
          ten_kh: invoiceData.ten_kh || invoiceData.customer?.name || '',
          dia_chi: invoiceData.customer?.address || undefined,
          dien_thoai:
            invoiceData.customer?.mobile ||
            invoiceData.customer?.phone ||
            undefined,
          so_cccd: invoiceData.customer?.idnumber || undefined,
          ngay_sinh: invoiceData.customer?.birthday
            ? formatDateYYYYMMDD(invoiceData.customer.birthday)
            : undefined,
          gioi_tinh: invoiceData.customer?.sexual || undefined,
        });
      }

      // Step 2: Tạo salesOrder (đơn hàng bán)
      const resultSalesOrder = await this.createSalesOrder(invoiceData);
      if (!resultSalesOrder) {
        throw new Error('Failed to create sales order');
      }

      // Step 3: Tạo salesInvoice (hóa đơn bán hàng)
      const resultSalesInvoice = await this.createSalesInvoice(invoiceData);
      if (!resultSalesInvoice) {
        throw new Error('Failed to create sales invoice');
      }

      this.logger.log(
        `[Flow] Invoice creation completed successfully for order ${invoiceData.so_ct || 'N/A'}`,
      );
      return resultSalesInvoice;
    } catch (error: any) {
      this.logger.error(
        `[Flow] Invoice creation failed: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * THU TIỀN - XỬ LÝ CASHIO
   * UPDATE: Hình thức thanh toán
   * API: http://103.145.79.169:6688/Fast/paymentMethod
   * Xử lý cashio và gọi API cashReceipt hoặc creditAdvice nếu cần
   * Chỉ áp dụng cho đơn hàng "01. Thường"
   * Một đơn hàng có thể có nhiều phương thức thanh toán
   * @param docCode - Mã đơn hàng
   * @param orderData - Dữ liệu đơn hàng
   * @param invoiceData - Dữ liệu invoice đã build
   */
  /**
   * Xử lý payment (Phiếu chi tiền mặt/Giấy báo nợ)
   * Sử dụng endpoint /Fast/paymentMethod (Daily Sync & Manual Trigger)
   */
  async processCashioPayment(data: any): Promise<any> {
    try {
      if (!data.fop_syscode) {
        this.logger.warn(
          '[ProcessCashioPayment] Missing payment method code (fop_syscode)',
        );
        return null; // Skip if no code
      }

      // Map data from PaymentService to Fast API payload structure
      const payload = {
        httt: data.fop_syscode,
        so_pt: data.refno || data.so_code || '', // Ưu tiên refno, fallback về so_code
        ngay_pt: data.docdate,
        tien_pt: Number(data.total_in || 0),
        ma_nt: 'VND', // Default
        ty_gia: 1, // Default
        so_hd: data.so_code || '',
        ngay_hd: data.docDate,
        tien_hd: Number(data.revenue || 0),
        ma_bp: data.boPhan || data.branchCode || '',
        ma_dvcs_pt:
          data.fop_syscode === 'CASH'
            ? data.ma_dvcs_sale || ''
            : data.ma_dvcs_cashio || '',
        ma_dvcs_hd: data.ma_dvcs_sale || '',
        ma_ca: data.maCa || '',
        ma_kh: data.partnerCode || '',
        ma_kh2: data.ma_doi_tac_payment || '', // Đối tác
        ma_tc: data.refno || data.so_code || '', // Mã tham chiếu cũng fallback về so_code
        ky_han: data.period_code || '',
      };
      this.logger.log(
        `[ProcessCashioPayment] Submitting payment for ${payload.so_hd} (${payload.httt})`,
      );

      return await this.submitPaymentPayload(payload);
    } catch (error) {
      this.logger.error(
        `Failed to process cashio payment: ${error.message} - Data: ${JSON.stringify(
          data,
        )}`,
      );
      throw error;
    }
  }

  /**
   * Gửi thông tin hình thức thanh toán lên Fast API và lưu log audit
   * API: /Fast/paymentMethod
   */
  async submitPaymentPayload(payload: any): Promise<any> {
    try {
      const result = await this.fastApiService.submitPaymentMethod(payload);

      // Audit Logging
      try {
        const log = new PaymentSyncLog();
        log.docCode = payload.so_hd;
        log.docDate = payload.ngay_pt ? new Date(payload.ngay_pt) : new Date();
        log.requestPayload = JSON.stringify(payload);
        log.responsePayload = JSON.stringify(result);

        // Determine status
        let isSuccess = false;
        let errorMessage = null;

        if (Array.isArray(result) && result.length > 0) {
          // Case: Response is an array like [{ status: 1, message: 'OK' }]
          const firstItem = result[0];
          isSuccess = firstItem.status === 1;
          if (!isSuccess) {
            errorMessage = firstItem.message || 'Unknown error (status !== 1)';
          }
        } else if (result && typeof result === 'object') {
          // Case: Response is an object like { status: 1, success: true }
          isSuccess = result.status === 1 || result.success === true;
          if (!isSuccess) {
            errorMessage = result.message || result.error || 'Unknown error';
          }
        }

        log.status = isSuccess ? 'SUCCESS' : 'ERROR';
        log.errorMessage = errorMessage;

        await this.paymentSyncLogRepository.save(log);
      } catch (logErr) {
        this.logger.error(`Failed to save payment sync log: ${logErr}`);
      }

      // Validate response logic from original code
      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        if (firstItem.status !== 1) {
          throw new BadRequestException(
            firstItem.message || 'Submit payment method thất bại',
          );
        }
      } else if (result && result.status !== undefined) {
        if (result.status !== 1) {
          throw new BadRequestException(
            result.message || 'Submit payment method thất bại',
          );
        }
      }

      return result;
    } catch (error) {
      // Audit Logging for Exception
      try {
        const log = new PaymentSyncLog();
        log.docCode = payload.so_hd;
        log.docDate = payload.ngay_pt ? new Date(payload.ngay_pt) : new Date();
        log.requestPayload = JSON.stringify(payload);
        log.status = 'ERROR';
        log.errorMessage = error.message;
        await this.paymentSyncLogRepository.save(log);
      } catch (logErr) {
        this.logger.error(
          `Failed to save payment sync log (exception): ${logErr}`,
        );
      }

      this.logger.error(
        `[Flow] Failed to submit payment method: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Xử lý payment (Phiếu chi tiền mặt/Giấy báo nợ)
   * Trả tiền - Xử lý payment
   * Áp dụng cho các case sau:
   * 1. 01. Thường, 02. Làm dịch vụ, 04. Đổi DV, 07. Bán tài khoản: BẮT BUỘC phải có mã kho
   * 2. SALE_RETURN: BẮT BUỘC phải có mã kho
   * 3. Đơn có đuôi _X (hủy): Cho phép không có mã kho (đơn hủy chưa xuất kho)
   *
   * @param docCode - Mã đơn hàng
   * @param orderData - Dữ liệu đơn hàng
   * @param invoiceData - Dữ liệu invoice đã build
   * @param stockCodes - Danh sách mã kho (stockCode) từ stock transfers (có thể rỗng cho đơn hủy _X)
   * @param allowWithoutStockCodes - Cho phép gọi payment ngay cả khi không có stockCodes (CHỈ cho đơn hủy _X, mặc định false)
   */
  async processPayment(
    docCode: string,
    orderData: any,
    invoiceData: any,
    stockCodes: string[],
    allowWithoutStockCodes: boolean = false,
  ): Promise<{
    paymentResults?: any[];
    debitAdviceResults?: any[];
  }> {
    try {
      // Kiểm tra có mã kho không (trừ khi allowWithoutStockCodes = true cho đơn hủy)
      if ((!stockCodes || stockCodes.length === 0) && !allowWithoutStockCodes) {
        this.logger.debug(
          `[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`,
        );
        return {};
      }

      // Lấy cashio data theo soCode
      const cashioResult = await this.syncService.getCashio({
        page: 1,
        limit: 100,
        soCode: docCode,
      });

      if (
        !cashioResult.success ||
        !cashioResult.data ||
        cashioResult.data.length === 0
      ) {
        this.logger.debug(
          `[Payment] Không tìm thấy cashio data cho đơn hàng ${docCode}`,
        );
        return {};
      }

      // [NEW] Fetch department info to get ma_dvcs for payment method lookup
      const branchCodes = new Set<string>();
      cashioResult.data.forEach((row) => {
        if (row.branch_code) branchCodes.add(row.branch_code);
      });

      const departmentMap =
        branchCodes.size > 0
          ? await this.loyaltyService.fetchLoyaltyDepartments(
              Array.from(branchCodes),
            )
          : new Map();

      const paymentResults: any[] = [];
      const debitAdviceResults: any[] = [];

      // Xử lý tất cả các cashio records có total_out > 0
      for (const cashioData of cashioResult.data) {
        const totalOut = parseFloat(String(cashioData.total_out || '0'));

        // Chỉ xử lý nếu total_out > 0
        if (totalOut > 0) {
          // [NEW] Get dvcs from map
          const saleDept = departmentMap.get(cashioData.branch_code);
          const dvcs = saleDept?.ma_dvcs || '';

          // Trường hợp 1: fop_syscode = "CASH" và total_out > 0 → Gọi Payment (Phiếu chi tiền mặt)
          if (cashioData.fop_syscode === 'CASH') {
            try {
              this.logger.log(
                `[Payment] Phát hiện CASH payment cho đơn hàng ${docCode} (${cashioData.code}), total_out: ${totalOut}, gọi payment API`,
              );

              // Override ma_dvcs in invoiceData for CASH payload if needed, or pass explicitly
              // Here we create a modified copy of invoiceData or pass dvcs directly if helper supports it.
              // Helper buildPaymentPayload uses invoiceData.ma_dvcs.
              const invoiceDataWithCorrectDvcs = {
                ...invoiceData,
                ma_dvcs: dvcs || invoiceData.ma_dvcs,
              };

              const paymentPayload = FastApiPayloadHelper.buildPaymentPayload(
                cashioData,
                orderData,
                invoiceDataWithCorrectDvcs,
                null,
                '2', // loai_ct = 2 (Chi cho khách hàng)
              );

              const paymentResult =
                await this.fastApiService.submitPayment(paymentPayload);

              // Validate response: status = 1 mới là success
              if (Array.isArray(paymentResult) && paymentResult.length > 0) {
                const firstItem = paymentResult[0];
                if (firstItem.status !== 1) {
                  const errorMessage =
                    firstItem.message || 'Tạo payment thất bại';
                  this.logger.error(
                    `[Payment] Payment API trả về status = ${firstItem.status}: ${errorMessage}`,
                  );
                  throw new BadRequestException(errorMessage);
                }
              } else if (
                paymentResult &&
                typeof paymentResult === 'object' &&
                paymentResult.status !== undefined
              ) {
                if (paymentResult.status !== 1) {
                  const errorMessage =
                    paymentResult.message || 'Tạo payment thất bại';
                  this.logger.error(
                    `[Payment] Payment API trả về status = ${paymentResult.status}: ${errorMessage}`,
                  );
                  throw new BadRequestException(errorMessage);
                }
              }

              paymentResults.push({
                cashioCode: cashioData.code,
                result: paymentResult,
              });
            } catch (error: any) {
              // Nếu là BadRequestException từ validation, throw lại
              if (error instanceof BadRequestException) {
                throw error;
              }
              // Nếu là lỗi khác, log và throw
              const errorMessage = `Lỗi khi tạo payment cho cashio ${cashioData.code}: ${error?.message || error}`;
              this.logger.error(`[Payment] ${errorMessage}`);
              throw new BadRequestException(errorMessage);
            }
            continue;
          }

          // Trường hợp 2: fop_syscode != "CASH" → Kiểm tra payment method
          if (cashioData.fop_syscode && cashioData.fop_syscode !== 'CASH') {
            // [NEW] Get dvcs from map
            const saleDept = departmentMap.get(cashioData.branch_code);
            const dvcs = saleDept?.ma_dvcs || '';

            // Lấy payment method theo code
            const paymentMethod =
              await this.categoriesService.findPaymentMethodByCode(
                cashioData.fop_syscode,
                dvcs,
              );

            // Kiểm tra payment method có tồn tại không
            if (!paymentMethod) {
              const errorMessage = `Không tìm thấy payment method với code "${cashioData.fop_syscode}" cho cashio ${cashioData.code}`;
              this.logger.error(`[Payment] ${errorMessage}`);
              throw new BadRequestException(errorMessage);
            }

            // Kiểm tra documentType
            if (!paymentMethod.documentType) {
              const errorMessage = `Payment method "${cashioData.fop_syscode}" (${cashioData.code}) không có documentType`;
              this.logger.error(`[Payment] ${errorMessage}`);
              throw new BadRequestException(errorMessage);
            }

            // Chỉ xử lý nếu documentType = "Giấy báo nợ"
            if (paymentMethod.documentType === 'Giấy báo nợ') {
              try {
                this.logger.log(
                  `[Payment] Phát hiện non-CASH payment cho đơn hàng ${docCode} (${cashioData.code}), total_out: ${totalOut}, gọi debitAdvice API`,
                );

                const debitAdvicePayload =
                  FastApiPayloadHelper.buildDebitAdvicePayload(
                    cashioData,
                    orderData,
                    invoiceData,
                    paymentMethod,
                    '2', // loai_ct = 2 (Chi cho khách hàng)
                  );

                const debitAdviceResult =
                  await this.fastApiService.submitDebitAdvice(
                    debitAdvicePayload,
                  );

                // Validate response: status = 1 mới là success
                if (
                  Array.isArray(debitAdviceResult) &&
                  debitAdviceResult.length > 0
                ) {
                  const firstItem = debitAdviceResult[0];
                  if (firstItem.status !== 1) {
                    const errorMessage =
                      firstItem.message || 'Tạo debit advice thất bại';
                    this.logger.error(
                      `[Payment] Debit Advice API trả về status = ${firstItem.status}: ${errorMessage}`,
                    );
                    throw new BadRequestException(errorMessage);
                  }
                } else if (
                  debitAdviceResult &&
                  typeof debitAdviceResult === 'object' &&
                  debitAdviceResult.status !== undefined
                ) {
                  if (debitAdviceResult.status !== 1) {
                    const errorMessage =
                      debitAdviceResult.message || 'Tạo debit advice thất bại';
                    this.logger.error(
                      `[Payment] Debit Advice API trả về status = ${debitAdviceResult.status}: ${errorMessage}`,
                    );
                    throw new BadRequestException(errorMessage);
                  }
                }

                debitAdviceResults.push({
                  cashioCode: cashioData.code,
                  result: debitAdviceResult,
                });
              } catch (error: any) {
                // Nếu là BadRequestException từ validation, throw lại
                if (error instanceof BadRequestException) {
                  throw error;
                }
                // Nếu là lỗi khác, log và throw
                const errorMessage = `Lỗi khi tạo debit advice cho payment method "${cashioData.fop_syscode}" (${cashioData.code}): ${error?.message || error}`;
                this.logger.error(`[Payment] ${errorMessage}`);
                throw new BadRequestException(errorMessage);
              }
            } else {
              // Payment method có documentType nhưng không phải "Giấy báo nợ" → báo lỗi
              const errorMessage = `Payment method "${cashioData.fop_syscode}" (${cashioData.code}) có documentType = "${paymentMethod.documentType}", không phải "Giấy báo nợ"`;
              this.logger.error(`[Payment] ${errorMessage}`);
              throw new BadRequestException(errorMessage);
            }
          }
        }
      }

      // Trả về kết quả (có thể có nhiều kết quả)
      const result: any = {};
      if (paymentResults.length > 0) {
        result.paymentResults = paymentResults;
      }
      if (debitAdviceResults.length > 0) {
        result.debitAdviceResults = debitAdviceResults;
      }

      return result;
    } catch (error: any) {
      // Nếu là BadRequestException (lỗi mapping hoặc validation), throw lại để báo lỗi
      if (error instanceof BadRequestException) {
        this.logger.error(
          `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${error?.message || error}`,
        );
        throw error;
      }
      // Các lỗi khác (network, API, ...) vẫn log và throw để báo lỗi
      const errorMessage = `Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${error?.message || error}`;
      this.logger.error(`[Payment] ${errorMessage}`);
      throw new BadRequestException(errorMessage);
    }
  }

  /**
   * Xử lý warehouse receipt/release từ stock transfer (I/O kho)
   * Xử lý nhập xuất kho
   * @param stockTransfer - Dữ liệu stock transfer
   * @returns Kết quả từ API
   */
  async processWarehouseFromStockTransfer(stockTransfer: any): Promise<any> {
    // Kiểm tra doctype phải là "STOCK_IO"
    if (stockTransfer.doctype !== 'STOCK_IO') {
      throw new BadRequestException(
        `Không thể xử lý stock transfer có doctype = "${stockTransfer.doctype}". Chỉ chấp nhận doctype = "STOCK_IO".`,
      );
    }

    // Kiểm tra soCode phải là "null" (string) hoặc null
    if (stockTransfer.soCode !== 'null' && stockTransfer.soCode !== null) {
      throw new BadRequestException(
        `Không thể xử lý stock transfer có soCode = "${stockTransfer.soCode}". Chỉ chấp nhận soCode = "null" hoặc null.`,
      );
    }

    // Kiểm tra ioType phải là "I" hoặc "O"
    if (stockTransfer.ioType !== 'I' && stockTransfer.ioType !== 'O') {
      throw new BadRequestException(
        `Không thể xử lý stock transfer có ioType = "${stockTransfer.ioType}". Chỉ chấp nhận "I" (nhập) hoặc "O" (xuất).`,
      );
    }

    // Lấy ma_dvcs từ department API (ưu tiên), nếu không có thì fallback
    let department: any = null;
    if (stockTransfer.branchCode) {
      try {
        department = await this.categoriesService.getDepartmentFromLoyaltyAPI(
          stockTransfer.branchCode,
        );
      } catch (error: any) {
        this.logger.warn(
          `Không thể lấy department cho branchCode ${stockTransfer.branchCode}: ${error?.message || error}`,
        );
      }
    }

    const maDvcs = department?.ma_dvcs || department?.ma_dvcs_ht;
    if (!maDvcs) {
      throw new BadRequestException(
        `Không tìm thấy mã ĐVCS (ma_dvcs) cho chi nhánh ${stockTransfer.branchCode}. Vui lòng kiểm tra lại cấu hình Department/Loyalty API.`,
      );
    }

    // Gọi Customer API trước (Fast/Customer)
    if (stockTransfer.branchCode) {
      try {
        // Lấy tên từ department nếu có, nếu không thì dùng branchCode
        const tenKh =
          department?.name || department?.ten || stockTransfer.branchCode || '';
        await this.createOrUpdateCustomer({
          ma_kh: stockTransfer.branchCode,
          ten_kh: tenKh,
        });
        this.logger.log(
          `[Warehouse] Đã tạo/cập nhật customer ${stockTransfer.branchCode} trước khi xử lý warehouse`,
        );
      } catch (error: any) {
        // Log warning nhưng không throw error để không chặn luồng xử lý warehouse
        this.logger.warn(
          `[Warehouse] Không thể tạo/cập nhật customer ${stockTransfer.branchCode}: ${error?.message || error}`,
        );
      }
    }

    // Fetch material catalog từ Loyalty API (giống bên sale)
    let materialCatalog: any = null;
    const itemCodeToFetch = stockTransfer.itemCode;
    if (itemCodeToFetch) {
      try {
        materialCatalog =
          await this.loyaltyService.fetchProduct(itemCodeToFetch);
        if (materialCatalog) {
          this.logger.debug(
            `[Warehouse] Đã lấy material catalog cho itemCode ${itemCodeToFetch}`,
          );
        }
      } catch (error: any) {
        this.logger.warn(
          `Không thể lấy material catalog cho itemCode ${itemCodeToFetch}: ${error?.message || error}`,
        );
      }
    }

    // Lấy materialCode và unit từ material catalog (ưu tiên từ catalog, fallback từ stockTransfer)
    const materialCode =
      materialCatalog?.materialCode ||
      stockTransfer.materialCode ||
      stockTransfer.itemCode ||
      '';
    const unit = materialCatalog?.unit || '';

    // Map mã kho qua API warehouse-code-mappings (giống bên sale)
    let mappedStockCode = stockTransfer.stockCode || '';
    if (stockTransfer.stockCode) {
      try {
        const maMoi = await this.categoriesService.mapWarehouseCode(
          stockTransfer.stockCode,
        );
        // Nếu có maMoi (mapped = true) → dùng maMoi
        // Nếu không có maMoi (mapped = false) → dùng giá trị gốc từ stockTransfer
        mappedStockCode = maMoi || stockTransfer.stockCode;
      } catch (error: any) {
        // Nếu có lỗi khi gọi API mapping, fallback về giá trị gốc
        this.logger.warn(
          `Không thể map warehouse code ${stockTransfer.stockCode}: ${error?.message || error}`,
        );
        mappedStockCode = stockTransfer.stockCode;
      }
    }

    // Build payload từ stock transfer
    const payload = {
      ma_dvcs: maDvcs,
      ma_kh: stockTransfer.branchCode || '',
      ma_gd: '2', // Fix cứng ma_gd = 2
      ngay_ct: stockTransfer.transDate || new Date().toISOString(),
      so_ct: stockTransfer.docCode || '',
      ma_nt: 'VND',
      ty_gia: 1,
      dien_giai:
        stockTransfer.docDesc ||
        `Phiếu ${stockTransfer.ioType === 'I' ? 'nhập' : 'xuất'} kho ${stockTransfer.docCode}`,
      detail: [
        {
          ma_vt: materialCode,
          dvt: unit,
          so_serial: stockTransfer.batchSerial || '',
          ma_kho: mappedStockCode,
          so_luong: Math.abs(parseFloat(String(stockTransfer.qty || '0'))), // Lấy giá trị tuyệt đối
          gia_nt: 0,
          tien_nt: 0,
          ma_lo: stockTransfer.batchSerial || '',
          ma_nx: stockTransfer.lineInfo1 || '',
          ma_vv: '',
          so_lsx: '',
          ma_sp: stockTransfer.itemCode || '',
          ma_hd: '',
          // Các field chỉ có trong warehouseRelease
          ...(stockTransfer.ioType === 'O'
            ? {
                px_gia_dd: 0,
                ma_phi: '',
                ma_ku: '',
                ma_phi_hh: '',
                ma_phi_ttlk: '',
                tien_hh_nt: 0,
                tien_ttlk_nt: 0,
              }
            : {
                pn_gia_tb: 0,
              }),
        },
      ],
    };

    // Gọi API tương ứng (ioType đã được validate ở trên)
    let result: any;
    if (stockTransfer.ioType === 'I') {
      // Nhập kho
      this.logger.log(
        `[Warehouse] Tạo phiếu nhập kho cho ${stockTransfer.docCode}`,
      );
      result = await this.fastApiService.submitWarehouseReceipt(payload);
    } else {
      // Xuất kho (ioType = "O" - đã được validate ở trên)
      this.logger.log(
        `[Warehouse] Tạo phiếu xuất kho cho ${stockTransfer.docCode}`,
      );
      result = await this.fastApiService.submitWarehouseRelease(payload);
    }

    // Validate response: status = 1 mới là success
    if (Array.isArray(result) && result.length > 0) {
      const firstItem = result[0];
      if (firstItem.status !== 1) {
        const errorMessage =
          firstItem.message || 'Tạo phiếu warehouse thất bại';
        this.logger.error(
          `[Warehouse] Warehouse API trả về status = ${firstItem.status}: ${errorMessage}`,
        );
        throw new BadRequestException(errorMessage);
      }
    } else if (
      result &&
      typeof result === 'object' &&
      result.status !== undefined
    ) {
      if (result.status !== 1) {
        const errorMessage = result.message || 'Tạo phiếu warehouse thất bại';
        this.logger.error(
          `[Warehouse] Warehouse API trả về status = ${result.status}: ${errorMessage}`,
        );
        throw new BadRequestException(errorMessage);
      }
    }

    return result;
  }

  /**
   * Xử lý warehouse transfer từ stock transfer (điều chuyển kho)
   * Xử lý điều chuyển kho
   * @param stockTransfers - Mảng các stock transfer cùng docCode
   * @returns Kết quả từ API
   */
  async processWarehouseTransferFromStockTransfers(
    stockTransfers: any[],
  ): Promise<any> {
    if (!stockTransfers || stockTransfers.length === 0) {
      throw new BadRequestException('Không có stock transfer để xử lý');
    }

    // Lấy stock transfer đầu tiên để lấy thông tin chung (tất cả đều cùng docCode)
    const firstStockTransfer = stockTransfers[0];

    // Kiểm tra doctype phải là "STOCK_TRANSFER"
    if (firstStockTransfer.doctype !== 'STOCK_TRANSFER') {
      throw new BadRequestException(
        `Không thể xử lý stock transfer có doctype = "${firstStockTransfer.doctype}". Chỉ chấp nhận doctype = "STOCK_TRANSFER".`,
      );
    }

    // Kiểm tra relatedStockCode phải có
    if (
      !firstStockTransfer.relatedStockCode ||
      firstStockTransfer.relatedStockCode.trim() === ''
    ) {
      throw new BadRequestException(
        `Không thể xử lý stock transfer điều chuyển kho. relatedStockCode không được để trống.`,
      );
    }

    // Lấy ma_dvcs từ department API (ưu tiên), nếu không có thì fallback
    let department: any = null;
    if (firstStockTransfer.branchCode) {
      try {
        department = await this.categoriesService.getDepartmentFromLoyaltyAPI(
          firstStockTransfer.branchCode,
        );
      } catch (error: any) {
        this.logger.warn(
          `Không thể lấy department cho branchCode ${firstStockTransfer.branchCode}: ${error?.message || error}`,
        );
      }
    }

    const maDvcs = department?.ma_dvcs || department?.ma_dvcs_ht;
    if (!maDvcs) {
      throw new BadRequestException(
        `Không tìm thấy mã ĐVCS (ma_dvcs) cho chi nhánh ${firstStockTransfer.branchCode}. Vui lòng kiểm tra lại cấu hình Department/Loyalty API.`,
      );
    }
    const maBp = department?.ma_bp || '';

    // Gọi Customer API trước (Fast/Customer)
    if (firstStockTransfer.branchCode) {
      try {
        await this.createOrUpdateCustomer({
          ma_kh: firstStockTransfer.branchCode,
          ten_kh: firstStockTransfer.branchCode,
        });
        this.logger.log(
          `[Warehouse Transfer] Đã tạo/cập nhật customer ${firstStockTransfer.branchCode} trước khi xử lý warehouse transfer`,
        );
      } catch (error: any) {
        // Log warning nhưng không throw error để không chặn luồng xử lý warehouse transfer
        this.logger.warn(
          `[Warehouse Transfer] Không thể tạo/cập nhật customer ${firstStockTransfer.branchCode}: ${error?.message || error}`,
        );
      }
    }

    // Map mã kho xuất (ma_kho_x) - từ stockCode
    let mappedStockCodeX = firstStockTransfer.stockCode || '';
    if (firstStockTransfer.stockCode) {
      try {
        const maMoi = await this.categoriesService.mapWarehouseCode(
          firstStockTransfer.stockCode,
        );
        mappedStockCodeX = maMoi || firstStockTransfer.stockCode;
      } catch (error: any) {
        this.logger.warn(
          `Không thể map warehouse code ${firstStockTransfer.stockCode}: ${error?.message || error}`,
        );
        mappedStockCodeX = firstStockTransfer.stockCode;
      }
    }

    // Map mã kho nhập (ma_kho_n) - từ relatedStockCode
    let mappedStockCodeN = firstStockTransfer.relatedStockCode || '';
    if (firstStockTransfer.relatedStockCode) {
      try {
        const maMoi = await this.categoriesService.mapWarehouseCode(
          firstStockTransfer.relatedStockCode,
        );
        mappedStockCodeN = maMoi || firstStockTransfer.relatedStockCode;
      } catch (error: any) {
        this.logger.warn(
          `Không thể map warehouse code ${firstStockTransfer.relatedStockCode}: ${error?.message || error}`,
        );
        mappedStockCodeN = firstStockTransfer.relatedStockCode;
      }
    }

    // Build detail array từ các stock transfers
    const detail: any[] = [];
    for (const stockTransfer of stockTransfers) {
      // Fetch material catalog từ Loyalty API
      let materialCatalog: any = null;
      const itemCodeToFetch = stockTransfer.itemCode;
      if (itemCodeToFetch) {
        try {
          materialCatalog =
            await this.loyaltyService.fetchProduct(itemCodeToFetch);
          if (materialCatalog) {
            this.logger.debug(
              `[Warehouse Transfer] Đã lấy material catalog cho itemCode ${itemCodeToFetch}`,
            );
          }
        } catch (error: any) {
          this.logger.warn(
            `Không thể lấy material catalog cho itemCode ${itemCodeToFetch}: ${error?.message || error}`,
          );
        }
      }

      // Lấy materialCode và unit từ material catalog (ưu tiên từ catalog, fallback từ stockTransfer)
      const materialCode =
        materialCatalog?.materialCode ||
        stockTransfer.materialCode ||
        stockTransfer.itemCode ||
        '';
      const unit = materialCatalog?.unit || '';

      detail.push({
        ma_vt: materialCode,
        dvt: unit,
        so_serial: stockTransfer.batchSerial || '',
        so_luong: Math.abs(parseFloat(String(stockTransfer.qty || '0'))), // Lấy giá trị tuyệt đối
        gia_nt: 0,
        tien_nt: 0,
        ma_lo: stockTransfer.batchSerial || '',
        ma_bp: maBp,
        px_gia_dd: 0,
      });
    }

    // Build payload
    const payload = {
      ma_dvcs: maDvcs,
      ma_kho_n: mappedStockCodeN, // Kho nhập (từ relatedStockCode)
      ma_kho_x: mappedStockCodeX, // Kho xuất (từ stockCode)
      ma_gd: '3', // Fix cứng ma_gd = 3 (xuất điều chuyển)
      ngay_ct: firstStockTransfer.transDate || new Date().toISOString(),
      so_ct: firstStockTransfer.docCode || '',
      ma_nt: 'VND',
      ty_gia: 1,
      dien_giai:
        firstStockTransfer.docDesc ||
        `Phiếu điều chuyển kho ${firstStockTransfer.docCode}`,
      so_buoc: 2, // Mặc định 2
      detail,
    };

    // Gọi API warehouseTransfer
    this.logger.log(
      `[Warehouse Transfer] Tạo phiếu điều chuyển kho cho ${firstStockTransfer.docCode}`,
    );
    const result = await this.fastApiService.submitWarehouseTransfer(payload);

    // Validate response: status = 1 mới là success
    if (Array.isArray(result) && result.length > 0) {
      const firstItem = result[0];
      if (firstItem.status !== 1) {
        const errorMessage =
          firstItem.message || 'Tạo phiếu warehouse transfer thất bại';
        this.logger.error(
          `[Warehouse Transfer] Warehouse Transfer API trả về status = ${firstItem.status}: ${errorMessage}`,
        );
        throw new BadRequestException(errorMessage);
      }
    } else if (
      result &&
      typeof result === 'object' &&
      result.status !== undefined
    ) {
      if (result.status !== 1) {
        const errorMessage =
          result.message || 'Tạo phiếu warehouse transfer thất bại';
        this.logger.error(
          `[Warehouse Transfer] Warehouse Transfer API trả về status = ${result.status}: ${errorMessage}`,
        );
        throw new BadRequestException(errorMessage);
      }
    }

    return result;
  }

  async createPromotionFromLoyaltyAPI(promCode: string): Promise<any> {
    const promotion =
      await this.categoriesService.createPromotionFromLoyaltyAPI(promCode);
    if (!promotion || !promotion.code) {
      throw new BadRequestException(
        `Mã khuyến mãi "${promCode}" không tồn tại trên Loyalty API`,
      );
    }

    return promotion;
  }
}
