import { Injectable, Logger } from '@nestjs/common';
import { FastApiService } from './fast-api.service';

/**
 * Service quản lý luồng tạo invoice trong Fast API
 * Luồng: Customer → Item → Lot → Site → salesInvoice
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
    this.logger.log(`[Flow] Step 1: Creating/updating customer ${customerData.ma_kh}...`);
    try {
      const result = await this.fastApiService.createOrUpdateCustomer(customerData);
      this.logger.log(`[Flow] Step 1: Customer ${customerData.ma_kh} created/updated successfully`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Step 1: Failed to create/update customer ${customerData.ma_kh}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo/cập nhật vật tư trong Fast API
   * 2.2/ Danh mục vật tư
   */
  async createOrUpdateItem(itemData: {
    ma_vt: string;
    ten_vt: string;
    ten_vt2?: string;
    dvt?: string;
    lo_yn?: number;
    nhieu_dvt?: number;
    loai_hh_dv?: string;
  }): Promise<any> {
    this.logger.log(`[Flow] Step 2: Creating/updating item ${itemData.ma_vt}...`);
    try {
      const result = await this.fastApiService.createOrUpdateItem(itemData);
      this.logger.log(`[Flow] Step 2: Item ${itemData.ma_vt} created/updated successfully`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Step 2: Failed to create/update item ${itemData.ma_vt}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo/cập nhật lô trong Fast API
   * 2.14/ Danh mục lô
   */
  async createOrUpdateLot(lotData: {
    ma_vt: string;
    ma_lo: string;
    ten_lo: string;
    ngay_nhap?: string | Date;
    ten_lo2?: string;
    ngay_sx?: string | Date;
    ngay_hhsd?: string | Date;
    ngay_hhbh?: string | Date;
    ghi_chu?: string;
    ma_phu?: string;
    active?: string;
    action?: string;
  }): Promise<any> {
    this.logger.log(`[Flow] Step 3: Creating/updating lot ${lotData.ma_lo} for item ${lotData.ma_vt}...`);
    try {
      const result = await this.fastApiService.createOrUpdateLot(lotData);
      this.logger.log(`[Flow] Step 3: Lot ${lotData.ma_lo} for item ${lotData.ma_vt} created/updated successfully`);
      this.logger.log(`================[Flow] Step 3: Lot API Response: ${JSON.stringify(result)}`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Step 3: Failed to create/update lot ${lotData.ma_lo} for item ${lotData.ma_vt}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo/cập nhật kho (Site) trong Fast API
   * 2.12/ Danh mục kho
   */
  async createOrUpdateSite(siteData: {
    ma_dvcs: string;
    ma_kho: string;
    ten_kho: string;
    ma_bp?: string;
  }): Promise<any> {
    this.logger.log(`[Flow] Step 4: Creating/updating site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs}...`);
    try {
      const result = await this.fastApiService.createOrUpdateSite(siteData);
      this.logger.log(`[Flow] Step 4: Site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs} created/updated successfully`);
      this.logger.log(`================[Flow] Step 4: Site API Response: ${JSON.stringify(result)}`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Step 4: Failed to create/update site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo hóa đơn bán hàng trong Fast API
   * 2.4/ Hóa đơn bán hàng
   */
  async createSalesInvoice(invoiceData: any): Promise<any> {
    this.logger.log(`[Flow] Step 5: Creating sales invoice ${invoiceData.so_ct}...`);
    try {
      // Loại bỏ các field không cần thiết khỏi payload trước khi gửi lên API
      // - product: chỉ dùng để tạo Item, không cần gửi lên salesInvoice API
      // - customer: chỉ dùng để tạo Customer, không cần gửi lên salesInvoice API
      // - ten_kh: không cần thiết trong salesInvoice API
      // - ma_nvbh: đã được loại bỏ khỏi payload
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
          const { product, ...cleanItem } = item;
          return cleanItem;
        }) || [],
        cbdetail: invoiceData.cbdetail ?? null,
      };
      
      const result = await this.fastApiService.submitSalesInvoice(cleanInvoiceData);
      this.logger.log(`[Flow] Step 5: Sales invoice ${invoiceData.so_ct} created successfully`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Step 5: Failed to create sales invoice ${invoiceData.so_ct}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Thực hiện toàn bộ luồng tạo invoice
   * 1. Tạo/cập nhật Customer
   * 2. Tạo/cập nhật tất cả Items trong detail
   * 3. Tạo/cập nhật tất cả Lots trong detail (nếu có ma_lo)
   * 4. Tạo/cập nhật tất cả Sites (kho) trong detail (nếu có ma_kho)
   * 5. Tạo salesInvoice
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
    this.logger.log(`[Flow] Starting full invoice flow for order ${invoiceData.so_ct || 'N/A'}`);

    try {
      // Step 1: Tạo/cập nhật Customer
      if (invoiceData.ma_kh) {
        try {
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
        } catch (error: any) {
          // Log warning nhưng không throw để không chặn luồng
          this.logger.warn(`[Flow] Step 1: Customer creation failed but continuing: ${error?.message || error}`);
        }
      }

      // Step 2: Tạo/cập nhật tất cả Items trong detail
      if (invoiceData.detail && Array.isArray(invoiceData.detail)) {
        const uniqueItems = new Map<string, any>();
        
        // Lấy unique items từ detail
        for (const item of invoiceData.detail) {
          if (item.ma_vt && !uniqueItems.has(item.ma_vt)) {
            uniqueItems.set(item.ma_vt, item);
          }
        }

        // Tạo/cập nhật từng item
        for (const [maVt, item] of uniqueItems) {
          try {
            // Lấy ten_vt từ product nếu có, nếu không thì dùng ma_vt
            const tenVt = item.ten_vt || item.product?.name || item.product?.invoiceName || item.ma_vt;
            
            await this.createOrUpdateItem({
              ma_vt: item.ma_vt,
              ten_vt: tenVt,
              dvt: item.dvt || item.product?.unit || undefined,
              lo_yn: item.lo_yn || 0,
              nhieu_dvt: item.nhieu_dvt || 0,
              loai_hh_dv: item.loai_hh_dv || item.product?.productType || undefined,
            });
          } catch (error: any) {
            // Log warning nhưng không throw để không chặn luồng
            this.logger.warn(`[Flow] Step 2: Item ${maVt} creation failed but continuing: ${error?.message || error}`);
          }
        }
      }

      // Step 3: Tạo/cập nhật tất cả Lots trong detail (nếu có ma_lo)
      this.logger.log(`[Flow] Step 3: Checking for lots in ${invoiceData.detail?.length || 0} detail items...`);
      
      if (invoiceData.detail && Array.isArray(invoiceData.detail)) {
        const uniqueLots = new Map<string, any>();
        
        // Debug: Log tất cả ma_lo trong detail (kể cả empty)
        this.logger.log(`[Flow] Step 3: All ma_lo values in detail: ${invoiceData.detail.map(i => `ma_vt:${i.ma_vt}, ma_lo:"${i.ma_lo || ''}"`).join(' | ')}`);
        
        const itemsWithLot = invoiceData.detail.filter(item => item.ma_lo && item.ma_lo.trim() !== '');
        this.logger.log(`[Flow] Step 3: Found ${itemsWithLot.length} items with non-empty ma_lo: ${itemsWithLot.map(i => `${i.ma_vt}:${i.ma_lo}`).join(', ')}`);
        
        // Lấy unique lots từ detail (ma_vt + ma_lo)
        for (const item of invoiceData.detail) {
          // Debug log cho từng item
          this.logger.log(`[Flow] Step 3: Checking item ma_vt:${item.ma_vt}, ma_lo:"${item.ma_lo || ''}", ma_lo.trim():"${item.ma_lo?.trim() || ''}"`);
          
          if (item.ma_vt && item.ma_lo && item.ma_lo.trim() !== '') {
            const lotKey = `${item.ma_vt}_${item.ma_lo}`;
            if (!uniqueLots.has(lotKey)) {
              uniqueLots.set(lotKey, {
                ma_vt: item.ma_vt,
                ma_lo: item.ma_lo,
                ten_lo: item.ten_lo || item.ma_lo, // Fallback về ma_lo nếu không có ten_lo
                ngay_nhap: item.ngay_nhap || undefined,
                ten_lo2: item.ten_lo2 || undefined,
                ngay_sx: item.ngay_sx || undefined,
                ngay_hhsd: item.ngay_hhsd || undefined,
                ngay_hhbh: item.ngay_hhbh || undefined,
                ghi_chu: item.ghi_chu || undefined,
                ma_phu: item.ma_phu || undefined,
                active: item.active || '0',
                action: item.action || '0',
              });
            }
          }
        }

        this.logger.log(`[Flow] Step 3: Found ${uniqueLots.size} unique lots to create/update`);

        // Tạo/cập nhật từng lot
        for (const [lotKey, lotData] of uniqueLots) {
          try {
            await this.createOrUpdateLot(lotData);
          } catch (error: any) {
            // Log warning nhưng không throw để không chặn luồng
            this.logger.warn(`[Flow] Step 3: Lot ${lotData.ma_lo} for item ${lotData.ma_vt} creation failed but continuing: ${error?.message || error}`);
          }
        }
        
        if (uniqueLots.size === 0) {
          this.logger.log(`[Flow] Step 3: No lots to create (all items have empty ma_lo)`);
        }
      } else {
        this.logger.log(`[Flow] Step 3: No detail items found, skipping lot creation`);
      }

      // Step 4: Tạo/cập nhật tất cả Sites (kho) trong detail (nếu có ma_kho)
      this.logger.log(`[Flow] Step 4: Checking for sites in ${invoiceData.detail?.length || 0} detail items...`);
      
      if (invoiceData.detail && Array.isArray(invoiceData.detail)) {
        const uniqueSites = new Map<string, any>();
        
        // Lấy unique sites từ detail (ma_dvcs + ma_kho)
        for (const item of invoiceData.detail) {
          if (item.ma_kho && item.ma_kho.trim() !== '' && invoiceData.ma_dvcs) {
            const siteKey = `${invoiceData.ma_dvcs}_${item.ma_kho}`;
            if (!uniqueSites.has(siteKey)) {
              // Lấy ma_bp từ item hoặc từ root level
              const maBp = item.ma_bp || invoiceData.ma_bp || undefined;
              
              uniqueSites.set(siteKey, {
                ma_dvcs: invoiceData.ma_dvcs,
                ma_kho: item.ma_kho,
                ten_kho: item.ten_kho || item.ma_kho, // Fallback về ma_kho nếu không có ten_kho
                ma_bp: maBp,
              });
            }
          }
        }

        this.logger.log(`[Flow] Step 4: Found ${uniqueSites.size} unique sites to create/update: ${Array.from(uniqueSites.keys()).join(', ')}`);

        // Tạo/cập nhật từng site
        for (const [siteKey, siteData] of uniqueSites) {
          try {
            await this.createOrUpdateSite(siteData);
          } catch (error: any) {
            // Log warning nhưng không throw để không chặn luồng
            this.logger.warn(`[Flow] Step 4: Site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs} creation failed but continuing: ${error?.message || error}`);
          }
        }
        
        if (uniqueSites.size === 0) {
          this.logger.log(`[Flow] Step 4: No sites to create (all items have empty ma_kho or no ma_dvcs)`);
        }
      } else {
        this.logger.log(`[Flow] Step 4: No detail items found, skipping site creation`);
      }

      // Step 5: Tạo salesInvoice
      const result = await this.createSalesInvoice(invoiceData);

      this.logger.log(`[Flow] Full invoice flow completed successfully for order ${invoiceData.so_ct || 'N/A'}`);
      return result;
    } catch (error: any) {
      this.logger.error(`[Flow] Full invoice flow failed: ${error?.message || error}`);
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

