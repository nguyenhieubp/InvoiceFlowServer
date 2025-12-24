import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { FastApiService } from './fast-api.service';
import { CategoriesService } from '../modules/categories/categories.service';

/**
 * Service quản lý tạo invoice trong Fast API
 * Luồng: Customer → Sales Invoice
 */
@Injectable()
export class FastApiInvoiceFlowService {
  private readonly logger = new Logger(FastApiInvoiceFlowService.name);

  constructor(
    private readonly fastApiService: FastApiService,
    private readonly categoriesService: CategoriesService,
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
    this.logger.log(`[Flow] Creating/updating customer ${customerData.ma_kh}...`);
    try {
      const result = await this.fastApiService.createOrUpdateCustomer(customerData);
      this.logger.log(`[Flow] Customer ${customerData.ma_kh} created/updated successfully`);
      return result;
    } catch (error: any) {
      this.logger.warn(`[Flow] Customer creation failed but continuing: ${error?.message || error}`);
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
    this.logger.log(`[Flow] Creating sales order ${orderData.so_ct} with action=${action}...`);
    try {
      // Build payload giống như salesInvoice (JSON body giống hóa đơn bán hàng)
      // Sử dụng cùng logic build như createSalesInvoice
      const removeEmptyFields = (obj: any): any => {
        if (obj === null || obj === undefined) {
          return obj;
        }
        if (Array.isArray(obj)) {
          return obj.map(item => removeEmptyFields(item));
        }
        if (typeof obj === 'object') {
          const cleaned: any = {};
          for (const [key, value] of Object.entries(obj)) {
            // Giữ lại các giá trị: 0, false, empty array, date objects
            // Đặc biệt: giữ lại ma_lo và so_serial ngay cả khi null hoặc empty
            const shouldKeep = value !== null && value !== undefined && value !== '' 
              || key === 'ma_lo' || key === 'so_serial';
            if (shouldKeep) {
              cleaned[key] = removeEmptyFields(value);
            }
          }
          return cleaned;
        }
        return obj;
      };

      // Build payload giống như createSalesInvoice (JSON body giống hóa đơn bán hàng)
      // Loại bỏ các field không cần thiết khỏi payload trước khi gửi lên API
      // - product: không cần gửi lên salesOrder API
      // - customer: không cần gửi lên salesOrder API
      // - ten_kh: không cần thiết trong salesOrder API
      const cleanOrderData: any = {
        action: action, // 0 cho đơn hàng bán, 1 cho đơn hàng trả lại
        ma_dvcs: orderData.ma_dvcs,
        ma_kh: orderData.ma_kh,
        ong_ba: orderData.ong_ba ?? null,
        ma_gd: orderData.ma_gd ?? '1',
        ma_tt: orderData.ma_tt ?? null,
        ma_ca: orderData.ma_ca ?? null,
        hinh_thuc: orderData.hinh_thuc ?? '0',
        dien_giai: orderData.dien_giai ?? null,
        ngay_lct: orderData.ngay_lct,
        ngay_ct: orderData.ngay_ct,
        so_ct: orderData.so_ct,
        so_seri: orderData.so_seri,
        ma_nt: orderData.ma_nt ?? 'VND',
        ty_gia: typeof orderData.ty_gia === 'number' ? orderData.ty_gia : parseFloat(orderData.ty_gia) || 1.0,
        ma_bp: orderData.ma_bp,
        tk_thue_no: orderData.tk_thue_no ?? '131111',
        ma_kenh: orderData.ma_kenh ?? 'ONLINE',
        loai_gd: '01',
        detail: (orderData.detail || []).map((item: any) => {
          // Loại bỏ product và các field không cần thiết khỏi mỗi detail item
          // Nhưng giữ lại ma_lo và so_serial (có thể là null nhưng vẫn cần giữ)
          const { product, ...cleanItem } = item;
          // Đảm bảo ma_lo và so_serial được giữ lại (ngay cả khi null)
          const result: any = { ...cleanItem };
          // Nếu có ma_lo hoặc so_serial trong item gốc, giữ lại (kể cả null)
          if ('ma_lo' in item) {
            result.ma_lo = item.ma_lo;
          }
          if ('so_serial' in item) {
            result.so_serial = item.so_serial;
          }
          return result;
        }) || [],
        cbdetail: null,
      };

      const finalPayload = removeEmptyFields(cleanOrderData);
      
      const result = await this.fastApiService.submitSalesOrder(finalPayload);
      this.logger.log(`[Flow] Sales order ${orderData.so_ct} created successfully`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Failed to create sales order ${orderData.so_ct}: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(`[Flow] Sales order error response status: ${error.response.status}`);
        this.logger.error(`[Flow] Sales order error response data: ${JSON.stringify(error.response.data)}`);
      }
      throw error;
    }
  }

  /**
   * Tạo hóa đơn bán hàng trong Fast API
   * 2.4/ Hóa đơn bán hàng
   */
  async createSalesInvoice(invoiceData: any): Promise<any> {
    this.logger.log(`[Flow] Creating sales invoice ${invoiceData.so_ct}...`);
    try {
      // FIX: Validate mã CTKM với Loyalty API trước khi gửi lên Fast API
      // Helper function: cắt phần sau dấu "-" để lấy mã CTKM để check (ví dụ: "PRMN.020228-R510SOCOM" → "PRMN.020228")
      // Và chuyển đổi các mã VC label sang format có khoảng trắng (VCHB → VC HB, VCKM → VC KM, VCDV → VC DV)
      const getPromotionCodeToCheck = (promCode: string | null | undefined): string | null => {
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
          if (item.ma_ctkm_th && item.ma_ctkm_th.trim() !== '' && item.ma_ctkm_th !== 'TT DAU TU') {
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
              const codeToCheck = i === 1 ? getPromotionCodeToCheck(maCk) : maCk.trim();
              if (codeToCheck) {
                promotionCodes.add(codeToCheck);
              }
            }
          }
        }
      }

      // Validate từng mã CTKM với Loyalty API (chỉ check phần trước dấu "-")
      for (const promCode of promotionCodes) {
        try {
          const promotion = await this.categoriesService.getPromotionFromLoyaltyAPI(promCode);
          if (!promotion || !promotion.code) {
            validationErrors.push(`Mã khuyến mãi "${promCode}" không tồn tại trên Loyalty API`);
          }
        } catch (error: any) {
          // Nếu API trả về 404 hoặc không tìm thấy, coi như mã không tồn tại
          if (error?.response?.status === 404 || error?.message?.includes('404')) {
            validationErrors.push(`Mã khuyến mãi "${promCode}" không tồn tại trên Loyalty API`);
          } else {
            // Lỗi khác (network, timeout, etc.) - log nhưng không block
            this.logger.warn(`[Flow] Lỗi khi kiểm tra mã khuyến mãi "${promCode}": ${error?.message || error}`);
          }
        }
      }

      // Nếu có lỗi validation, throw error và không gửi lên Fast API
      if (validationErrors.length > 0) {
        const errorMessage = `Mã khuyến mãi không hợp lệ:\n${validationErrors.join('\n')}`;
        this.logger.error(`[Flow] ${errorMessage}`);
        throw new BadRequestException(errorMessage);
      }

      // Helper function để loại bỏ các field null, undefined, hoặc empty string
      // Nhưng giữ lại ma_lo và so_serial (có thể là null nhưng vẫn cần gửi lên API)
      const removeEmptyFields = (obj: any): any => {
        if (obj === null || obj === undefined) {
          return obj;
        }
        if (Array.isArray(obj)) {
          return obj.map(item => removeEmptyFields(item));
        }
        if (typeof obj === 'object') {
          const cleaned: any = {};
          for (const [key, value] of Object.entries(obj)) {
            // Giữ lại các giá trị: 0, false, empty array, date objects
            // Đặc biệt: giữ lại ma_lo và so_serial ngay cả khi null hoặc empty
            const shouldKeep = value !== null && value !== undefined && value !== '' 
              || key === 'ma_lo' || key === 'so_serial';
            if (shouldKeep) {
              cleaned[key] = removeEmptyFields(value);
            }
          }
          return cleaned;
        }
        return obj;
      };

      // menard - TTM
      // f3 - FBV
      // chando - CDV
      // labhair - LHV
      // yaman - BTH

      // Loại bỏ các field không cần thiết khỏi payload trước khi gửi lên API
      // - product: không cần gửi lên salesInvoice API
      // - customer: không cần gửi lên salesInvoice API
      // - ten_kh: không cần thiết trong salesInvoice API
      const cleanInvoiceData: any = {
        action: 0, // Luôn set = 0 cho API hóa đơn bán hàng
        ma_dvcs: invoiceData.ma_dvcs,
        ma_kh: invoiceData.ma_kh,
        ong_ba: invoiceData.ong_ba ?? null,
        ma_gd: invoiceData.ma_gd ?? '1',
        ma_tt: invoiceData.ma_tt ?? null,
        ma_ca: invoiceData.ma_ca ?? null,
        hinh_thuc: invoiceData.hinh_thuc ?? '0',
        dien_giai: invoiceData.dien_giai ?? null,
        ngay_lct: invoiceData.ngay_lct,
        ngay_ct: invoiceData.ngay_ct,
        so_ct: invoiceData.so_ct,
        so_seri: invoiceData.so_seri,
        ma_nt: invoiceData.ma_nt ?? 'VND',
        ty_gia: typeof invoiceData.ty_gia === 'number' ? invoiceData.ty_gia : parseFloat(invoiceData.ty_gia) || 1.0,
        ma_bp: invoiceData.ma_bp,
        tk_thue_no: invoiceData.tk_thue_no ?? '131111',
        ma_kenh: invoiceData.ma_kenh ?? 'ONLINE',
        loai_gd: '01',
        detail: invoiceData.detail?.map((item: any) => {
          // Loại bỏ product và các field không cần thiết khỏi mỗi detail item
          // Nhưng giữ lại ma_lo và so_serial (có thể là null nhưng vẫn cần giữ)
          const { product, ...cleanItem } = item;
          // Đảm bảo ma_lo và so_serial được giữ lại (ngay cả khi null)
          const result: any = { ...cleanItem };
          // Nếu có ma_lo hoặc so_serial trong item gốc, giữ lại (kể cả null)
          if ('ma_lo' in item) {
            result.ma_lo = item.ma_lo;
          }
          if ('so_serial' in item) {
            result.so_serial = item.so_serial;
          }
          return result;
        }) || [],
        cbdetail: null,
      };

      // Loại bỏ các field null, undefined, hoặc empty string trước khi gửi
      const finalPayload = removeEmptyFields(cleanInvoiceData);
      
      const result = await this.fastApiService.submitSalesInvoice(finalPayload);
      this.logger.log(`[Flow] Sales invoice ${invoiceData.so_ct} created successfully`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Failed to create sales invoice ${invoiceData.so_ct}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo hàng bán trả lại (salesReturn) trong Fast API
   * 2.15/ Hàng bán trả lại
   * Sử dụng cho SALE_RETURN có stock transfer
   */
  async createSalesReturn(salesReturnData: any): Promise<any> {
    this.logger.log(`[Flow] Creating sales return ${salesReturnData.so_ct || 'N/A'}...`);
    try {
      // Helper function để loại bỏ các field null, undefined, hoặc empty string
      const removeEmptyFields = (obj: any): any => {
    if (obj === null || obj === undefined) {
      return obj;
    }
    if (Array.isArray(obj)) {
          return obj.map(item => removeEmptyFields(item));
    }
    if (typeof obj === 'object') {
      const cleaned: any = {};
      for (const [key, value] of Object.entries(obj)) {
            // Giữ lại các giá trị: 0, false, empty array, date objects
            const shouldKeep = value !== null && value !== undefined && value !== '';
            if (shouldKeep) {
              cleaned[key] = removeEmptyFields(value);
            }
      }
      return cleaned;
    }
    return obj;
      };
      const finalPayload = removeEmptyFields(salesReturnData);
      
      const result = await this.fastApiService.submitSalesReturn(finalPayload);
      this.logger.log(`[Flow] Sales return ${salesReturnData.so_ct || 'N/A'} created successfully`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Failed to create sales return ${salesReturnData.so_ct || 'N/A'}: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(`[Flow] Sales return error response status: ${error.response.status}`);
        this.logger.error(`[Flow] Sales return error response data: ${JSON.stringify(error.response.data)}`);
      }
      throw error;
    }
  }

  /**
   * Tạo phiếu tạo gộp – xuất tách (gxtInvoice) trong Fast API
   * Sử dụng cho đơn dịch vụ: detail (nhập - productType = 'S'), ndetail (xuất - productType = 'I')
   */
  async createGxtInvoice(gxtInvoiceData: any): Promise<any> {
    this.logger.log(`[Flow] Creating gxt invoice ${gxtInvoiceData.so_ct || 'N/A'}...`);
    try {
      // Helper function để loại bỏ các field null, undefined, hoặc empty string
      const removeEmptyFields = (obj: any): any => {
        if (obj === null || obj === undefined) {
          return obj;
        }
        if (Array.isArray(obj)) {
          return obj.map(item => removeEmptyFields(item));
        }
        if (typeof obj === 'object') {
          const cleaned: any = {};
          for (const [key, value] of Object.entries(obj)) {
            // Giữ lại các giá trị: 0, false, empty array, date objects
            const shouldKeep = value !== null && value !== undefined && value !== '';
            if (shouldKeep) {
              cleaned[key] = removeEmptyFields(value);
            }
          }
          return cleaned;
        }
        return obj;
      };

      // Log payload để debug
      this.logger.debug(`[Flow] GxtInvoice payload: ${JSON.stringify(gxtInvoiceData, null, 2)}`);

      const finalPayload = removeEmptyFields(gxtInvoiceData);
      
      const result = await this.fastApiService.submitGxtInvoice(finalPayload);
      this.logger.log(`[Flow] GxtInvoice ${gxtInvoiceData.so_ct || 'N/A'} created successfully`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Failed to create gxt invoice ${gxtInvoiceData.so_ct || 'N/A'}: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(`[Flow] GxtInvoice error response status: ${error.response.status}`);
        this.logger.error(`[Flow] GxtInvoice error response data: ${JSON.stringify(error.response.data)}`);
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
    this.logger.log(`[Flow] Starting invoice creation for order ${invoiceData.so_ct || 'N/A'}`);

    try {
      // Step 1: Tạo/cập nhật Customer
      if (invoiceData.ma_kh) {
        await this.createOrUpdateCustomer({
          ma_kh: invoiceData.ma_kh,
          ten_kh: invoiceData.ten_kh || invoiceData.customer?.name || '',
          dia_chi: invoiceData.customer?.address || undefined,
          dien_thoai: invoiceData.customer?.mobile || invoiceData.customer?.phone || undefined,
          so_cccd: invoiceData.customer?.idnumber || undefined,
          ngay_sinh: invoiceData.customer?.birthday
            ? this.formatDateYYYYMMDD(invoiceData.customer.birthday)
            : undefined,
          gioi_tinh: invoiceData.customer?.sexual || undefined,
        });
      }

      // Step 2: Tạo salesOrder (đơn hàng bán)
      await this.createSalesOrder(invoiceData);

      // Step 3: Tạo salesInvoice (hóa đơn bán hàng)
      const result = await this.createSalesInvoice(invoiceData);

      this.logger.log(`[Flow] Invoice creation completed successfully for order ${invoiceData.so_ct || 'N/A'}`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Invoice creation failed: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Format date thành YYYYMMDD
   */
  private formatDateYYYYMMDD(date: Date | string): string {
    const d = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(d.getTime())) {
      return '';
    }
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${year}${month}${day}`;
  }
}

