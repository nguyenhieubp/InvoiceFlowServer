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
        action: invoiceData.action ?? 0,
        ma_dvcs: invoiceData.ma_dvcs,
        ma_kh: invoiceData.ma_kh,
        ong_ba: invoiceData.ong_ba ?? null,
        ma_gd: invoiceData.ma_gd ?? '2',
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
        loai_gd: invoiceData.loai_gd,
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
   * Helper function: Loại bỏ các field undefined, null, hoặc empty string
   */
  private cleanWarehousePayload(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }
    if (Array.isArray(obj)) {
      return obj.map(item => this.cleanWarehousePayload(item));
    }
    if (typeof obj === 'object') {
      const cleaned: any = {};
      for (const [key, value] of Object.entries(obj)) {
        // Giữ lại các giá trị: 0, false, date objects
        // Loại bỏ: undefined, null, empty string
        if (value === undefined || value === null) {
          // Bỏ qua undefined và null
          continue;
        }
        if (typeof value === 'string' && value === '') {
          // Bỏ qua empty string
          continue;
        }
        // Giữ lại tất cả giá trị khác (bao gồm 0, false, empty array, date objects)
        cleaned[key] = this.cleanWarehousePayload(value);
      }
      return cleaned;
    }
    return obj;
  }

  /**
   * Build dữ liệu warehouseRelease (xuất kho) từ invoiceData
   */
  private buildWarehouseReleaseData(invoiceData: any): any {
    const warehouseData: any = {
      ma_dvcs: invoiceData.ma_dvcs,
      ma_kh: invoiceData.ma_kh,
      ong_ba: invoiceData.ong_ba || invoiceData.ten_kh || '',
      ngay_ct: invoiceData.ngay_ct,
      so_ct: invoiceData.so_ct,
      dien_giai: invoiceData.dien_giai || `Xuất kho cho đơn hàng ${invoiceData.so_ct}`,
      detail: (invoiceData.detail || []).map((item: any) => {
        // Map các field từ invoiceData.detail sang warehouse detail
        // LƯU Ý: Các field dvt, ma_kho, ma_lo, ma_vt đã được tính đúng trong buildFastApiInvoiceData
        // từ Loyalty API và các nguồn chính xác, nên chỉ cần lấy trực tiếp từ item
        // Sử dụng gia_ban/tien_hang nếu không có gia_nt/tien_nt
        const giaNt = item.gia_nt || item.gia_ban || 0;
        const tienNt = item.tien_nt || item.tien_hang || 0;
        const pxGiaDd = item.px_gia_dd || item.gia_ban || giaNt || 0;
        
        const detailItem: any = {
          // ma_vt: Đã được lấy từ materialCode của Loyalty API trong buildFastApiInvoiceData
          ma_vt: item.ma_vt,
          // dvt: Đã được lấy từ sale.product?.dvt || sale.product?.unit || sale.dvt trong buildFastApiInvoiceData
          dvt: item.dvt,
          // ma_kho: Đã được tính từ calculateMaKho(ordertype, maBp) trong buildFastApiInvoiceData
          ma_kho: item.ma_kho,
          so_luong: item.so_luong,
          px_gia_dd: pxGiaDd,
          gia_nt: giaNt,
          tien_nt: tienNt,
          // Dùng ma_nx_st (ST*) cho warehouseRelease (xuất kho)
          ma_nx: item.ma_nx_st || item.ma_nx || '1111',
        };

        // Chỉ thêm các field nếu có giá trị (không undefined/null/empty)
        // ma_lo: Đã được tính từ serial value dựa trên trackBatch/trackSerial trong buildFastApiInvoiceData
        if (item.ma_lo) detailItem.ma_lo = item.ma_lo;
        // so_serial: Đã được tính từ serial value dựa trên trackBatch/trackSerial trong buildFastApiInvoiceData
        if (item.so_serial) detailItem.so_serial = item.so_serial;
        if (item.ma_vv) detailItem.ma_vv = item.ma_vv;
        if (item.ma_bp) detailItem.ma_bp = item.ma_bp;
        if (item.so_lsx) detailItem.so_lsx = item.so_lsx;
        if (item.ma_sp) detailItem.ma_sp = item.ma_sp;
        if (item.ma_hd) detailItem.ma_hd = item.ma_hd;
        if (item.ma_phi) detailItem.ma_phi = item.ma_phi;
        if (item.ma_ku) detailItem.ma_ku = item.ma_ku;
        if (item.ma_phi_hh) detailItem.ma_phi_hh = item.ma_phi_hh;
        if (item.ma_phi_ttlk) detailItem.ma_phi_ttlk = item.ma_phi_ttlk;
        if (item.tien_hh_nt !== undefined && item.tien_hh_nt !== null) detailItem.tien_hh_nt = item.tien_hh_nt;
        if (item.tien_ttlk_nt !== undefined && item.tien_ttlk_nt !== null) detailItem.tien_ttlk_nt = item.tien_ttlk_nt;

        return detailItem;
      }),
    };

    // Clean payload trước khi return
    return this.cleanWarehousePayload(warehouseData);
  }

  /**
   * Build dữ liệu warehouseReceipt (nhập kho) từ invoiceData
   */
  private buildWarehouseReceiptData(invoiceData: any): any {
    const warehouseData: any = {
      ma_dvcs: invoiceData.ma_dvcs,
      ma_kh: invoiceData.ma_kh,
      ong_ba: invoiceData.ong_ba || invoiceData.ten_kh || '',
      ngay_ct: invoiceData.ngay_ct,
      so_ct: invoiceData.so_ct,
      dien_giai: invoiceData.dien_giai || `Nhập kho cho đơn hàng ${invoiceData.so_ct}`,
      detail: (invoiceData.detail || []).map((item: any) => {
        // Map các field từ invoiceData.detail sang warehouse detail
        // LƯU Ý: Các field dvt, ma_kho, ma_lo, ma_vt đã được tính đúng trong buildFastApiInvoiceData
        // từ Loyalty API và các nguồn chính xác, nên chỉ cần lấy trực tiếp từ item
        // Sử dụng gia_ban/tien_hang nếu không có gia_nt/tien_nt
        const giaNt = item.gia_nt || item.gia_ban || 0;
        const tienNt = item.tien_nt || item.tien_hang || 0;
        
        const detailItem: any = {
          // ma_vt: Đã được lấy từ materialCode của Loyalty API trong buildFastApiInvoiceData
          ma_vt: item.ma_vt,
          // dvt: Đã được lấy từ sale.product?.dvt || sale.product?.unit || sale.dvt trong buildFastApiInvoiceData
          dvt: item.dvt,
          // ma_kho: Đã được tính từ calculateMaKho(ordertype, maBp) trong buildFastApiInvoiceData
          ma_kho: item.ma_kho,
          so_luong: item.so_luong,
          gia_nt: giaNt,
          tien_nt: tienNt,
          // Dùng ma_nx_rt (RT*) cho warehouseReceipt (nhập kho)
          ma_nx: item.ma_nx_rt || item.ma_nx || '1111',
        };

        // Chỉ thêm các field nếu có giá trị (không undefined/null/empty)
        // ma_lo: Đã được tính từ serial value dựa trên trackBatch/trackSerial trong buildFastApiInvoiceData
        if (item.ma_lo) detailItem.ma_lo = item.ma_lo;
        // so_serial: Đã được tính từ serial value dựa trên trackBatch/trackSerial trong buildFastApiInvoiceData
        if (item.so_serial) detailItem.so_serial = item.so_serial;
        if (item.ma_vv) detailItem.ma_vv = item.ma_vv;
        if (item.ma_bp) detailItem.ma_bp = item.ma_bp;
        if (item.so_lsx) detailItem.so_lsx = item.so_lsx;
        if (item.ma_sp) detailItem.ma_sp = item.ma_sp;
        if (item.ma_hd) detailItem.ma_hd = item.ma_hd;

        return detailItem;
      }),
    };

    // Clean payload trước khi return
    return this.cleanWarehousePayload(warehouseData);
  }

  /**
   * Gọi API warehouseRelease (xuất kho) với ioType: O
   */
  async createWarehouseRelease(invoiceData: any): Promise<any> {
    this.logger.log(`[Flow] Creating warehouse release for order ${invoiceData.so_ct || 'N/A'}`);
    try {
      const warehouseData = this.buildWarehouseReleaseData(invoiceData);
      const result = await this.fastApiService.submitWarehouseRelease(warehouseData, 'O');
      this.logger.log(`[Flow] Warehouse release created successfully for order ${invoiceData.so_ct || 'N/A'}`);
      return result;
    } catch (error: any) {
      this.logger.warn(`[Flow] Warehouse release failed but continuing: ${error?.message || error}`);
      // Không throw error để không chặn luồng tạo invoice
      return null;
    }
  }

  /**
   * Gọi API warehouseReceipt (nhập kho) với ioType: I
   */
  async createWarehouseReceipt(invoiceData: any): Promise<any> {
    this.logger.log(`[Flow] Creating warehouse receipt for order ${invoiceData.so_ct || 'N/A'}`);
    try {
      const warehouseData = this.buildWarehouseReceiptData(invoiceData);
      const result = await this.fastApiService.submitWarehouseReceipt(warehouseData, 'I');
      this.logger.log(`[Flow] Warehouse receipt created successfully for order ${invoiceData.so_ct || 'N/A'}`);
      return result;
    } catch (error: any) {
      this.logger.warn(`[Flow] Warehouse receipt failed but continuing: ${error?.message || error}`);
      // Không throw error để không chặn luồng tạo invoice
      return null;
    }
  }

  /**
   * Thực hiện tạo invoice - tạo Customer, warehouseRelease, warehouseReceipt, sau đó tạo Sales Invoice
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

      // Step 2: Tạo warehouseRelease (xuất kho) với ioType: O
      await this.createWarehouseRelease(invoiceData);

      // Step 3: Tạo warehouseReceipt (nhập kho) với ioType: I
      await this.createWarehouseReceipt(invoiceData);

      // Step 4: Tạo salesInvoice
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

