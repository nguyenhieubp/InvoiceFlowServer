import { Injectable, Logger } from '@nestjs/common';
import { FastApiService } from './fast-api.service';

/**
 * Service quản lý tạo invoice trong Fast API
 * Luồng: Customer → Sales Invoice
 */
@Injectable()
export class FastApiInvoiceFlowService {
  private readonly logger = new Logger(FastApiInvoiceFlowService.name);

  constructor(private readonly fastApiService: FastApiService) {}

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
   * Thực hiện tạo invoice - tạo Customer trước, sau đó tạo Sales Invoice
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

      // Step 2: Tạo salesInvoice
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

