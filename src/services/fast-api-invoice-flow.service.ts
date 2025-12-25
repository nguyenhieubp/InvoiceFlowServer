import { Injectable, Logger, BadRequestException, Inject, forwardRef } from '@nestjs/common';
import { FastApiService } from './fast-api.service';
import { CategoriesService } from '../modules/categories/categories.service';
import { SyncService } from './sync.service';
import { LoyaltyService } from './loyalty.service';

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
    @Inject(forwardRef(() => SyncService))
    private readonly syncService: SyncService,
    private readonly loyaltyService: LoyaltyService,
  ) { }

  /**
   * Helper: Loại bỏ các field null, undefined, hoặc empty string
   * Giữ lại ma_lo và so_serial ngay cả khi null hoặc empty
   */
  private removeEmptyFields(obj: any, keepMaLoAndSerial: boolean = true): any {
    if (obj === null || obj === undefined) {
      return obj;
    }
    if (Array.isArray(obj)) {
      return obj.map(item => this.removeEmptyFields(item, keepMaLoAndSerial));
    }
    if (typeof obj === 'object') {
      const cleaned: any = {};
      for (const [key, value] of Object.entries(obj)) {
        const shouldKeep = value !== null && value !== undefined && value !== ''
          || (keepMaLoAndSerial && (key === 'ma_lo' || key === 'so_serial'));
        if (shouldKeep) {
          cleaned[key] = this.removeEmptyFields(value, keepMaLoAndSerial);
        }
      }
      return cleaned;
    }
    return obj;
  }

  /**
   * Helper: Build clean payload cho salesOrder và salesInvoice
   */
  private buildCleanPayload(orderData: any, action: number = 0): any {
    return {
      action,
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
        const { product, ...cleanItem } = item;
        const result: any = { ...cleanItem };
        // Giữ lại ma_lo và so_serial (kể cả null)
        if ('ma_lo' in item) result.ma_lo = item.ma_lo;
        if ('so_serial' in item) result.so_serial = item.so_serial;
        return result;
      }),
      cbdetail: null,
    };
  }

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
      const cleanOrderData = this.buildCleanPayload(orderData, action);
      const finalPayload = this.removeEmptyFields(cleanOrderData);

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
   * FAST 2.4
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

      // Build clean payload (giống salesOrder nhưng action luôn = 0)
      const cleanInvoiceData = this.buildCleanPayload(invoiceData, 0);
      const finalPayload = this.removeEmptyFields(cleanInvoiceData);

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
   * FAST 2.15
   * Sử dụng cho SALE_RETURN có stock transfer
   */
  async createSalesReturn(salesReturnData: any): Promise<any> {
    this.logger.log(`[Flow] Creating sales return ${salesReturnData.so_ct || 'N/A'}...`);
    try {
      const finalPayload = this.removeEmptyFields(salesReturnData, false);

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
   * FAST 2.10
   */
  async createGxtInvoice(gxtInvoiceData: any): Promise<any> {
    this.logger.log(`[Flow] Creating gxt invoice ${gxtInvoiceData.so_ct || 'N/A'}...`);
    try {
      this.logger.debug(`[Flow] GxtInvoice payload: ${JSON.stringify(gxtInvoiceData, null, 2)}`);
      const finalPayload = this.removeEmptyFields(gxtInvoiceData, false);

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
      const resultSalesOrder = await this.createSalesOrder(invoiceData);
      if (!resultSalesOrder) {
        throw new Error('Failed to create sales order');
      }

      // Step 3: Tạo salesInvoice (hóa đơn bán hàng)
      const resultSalesInvoice = await this.createSalesInvoice(invoiceData);
      if (!resultSalesInvoice) {
        throw new Error('Failed to create sales invoice');
      }

      this.logger.log(`[Flow] Invoice creation completed successfully for order ${invoiceData.so_ct || 'N/A'}`);
      return resultSalesInvoice;
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

  /**
   * Format date thành ISO string cho Fast API (YYYY-MM-DDTHH:mm:ss)
   */
  private formatDateISO(date: Date | string): string {
    const d = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(d.getTime())) {
      return new Date().toISOString();
    }
    return d.toISOString();
  }

  /**
   * Xử lý cashio và gọi API cashReceipt hoặc creditAdvice nếu cần
   * Chỉ áp dụng cho đơn hàng "01. Thường"
   * Một đơn hàng có thể có nhiều phương thức thanh toán
   * @param docCode - Mã đơn hàng
   * @param orderData - Dữ liệu đơn hàng
   * @param invoiceData - Dữ liệu invoice đã build
   */
  async processCashioPayment(docCode: string, orderData: any, invoiceData: any): Promise<{
    cashReceiptResults?: any[];
    creditAdviceResults?: any[];
  }> {
    try {
      // Lấy cashio data theo soCode
      const cashioResult = await this.syncService.getCashio({
        page: 1,
        limit: 100,
        soCode: docCode,
      });

      if (!cashioResult.success || !cashioResult.data || cashioResult.data.length === 0) {
        this.logger.debug(`[Cashio] Không tìm thấy cashio data cho đơn hàng ${docCode}`);
        return {};
      }

      const cashReceiptResults: any[] = [];
      const creditAdviceResults: any[] = [];

      // Xử lý tất cả các cashio records (một đơn hàng có thể có nhiều phương thức thanh toán)
      for (const cashioData of cashioResult.data) {
        const totalIn = parseFloat(String(cashioData.total_in || '0'));

        // Trường hợp 1: fop_syscode = "CASH" và total_in > 0 → Gọi cashReceipt
        if (cashioData.fop_syscode === 'CASH' && totalIn > 0) {
          try {
            this.logger.log(`[Cashio] Phát hiện CASH payment cho đơn hàng ${docCode} (${cashioData.code}), gọi cashReceipt API`);

            const cashReceiptPayload = this.buildCashReceiptPayload(cashioData, orderData, invoiceData);
            const cashReceiptResult = await this.fastApiService.submitCashReceipt(cashReceiptPayload);

            cashReceiptResults.push({
              cashioCode: cashioData.code,
              result: cashReceiptResult,
            });
          } catch (error: any) {
            this.logger.warn(`[Cashio] Lỗi khi tạo cashReceipt cho ${cashioData.code}: ${error?.message || error}`);
          }
          continue;
        }

        // Trường hợp 2: fop_syscode != "CASH" → Kiểm tra payment method
        if (cashioData.fop_syscode && cashioData.fop_syscode !== 'CASH') {
          try {
            // Lấy payment method theo code
            const paymentMethod = await this.categoriesService.findPaymentMethodByCode(cashioData.fop_syscode);

            if (paymentMethod && paymentMethod.documentType === 'Giấy báo có') {

              const creditAdvicePayload = this.buildCreditAdvicePayload(cashioData, orderData, invoiceData, paymentMethod);
              const creditAdviceResult = await this.fastApiService.submitCreditAdvice(creditAdvicePayload);

              creditAdviceResults.push({
                cashioCode: cashioData.code,
                result: creditAdviceResult,
              });
            } else if (!paymentMethod || !paymentMethod.documentType) {
              this.logger.debug(`[Cashio] Payment method "${cashioData.fop_syscode}" (${cashioData.code}) không có documentType hoặc không phải "Giấy báo có", không gọi API nào`);
            }
          } catch (error: any) {
            this.logger.warn(`[Cashio] Lỗi khi xử lý payment method "${cashioData.fop_syscode}" cho ${cashioData.code}: ${error?.message || error}`);
          }
        }
      }

      // Trả về kết quả (có thể có nhiều kết quả)
      const result: any = {};
      if (cashReceiptResults.length > 0) {
        result.cashReceiptResults = cashReceiptResults;
      }
      if (creditAdviceResults.length > 0) {
        result.creditAdviceResults = creditAdviceResults;
      }

      return result;
    } catch (error: any) {
      // Log lỗi nhưng không throw để không chặn flow chính
      this.logger.warn(`[Cashio] Lỗi khi xử lý cashio payment cho đơn hàng ${docCode}: ${error?.message || error}`);
      return {};
    }
  }

  /**
   * Build payload cho cashReceipt API
   */
  private buildCashReceiptPayload(cashioData: any, orderData: any, invoiceData: any): any {
    const totalIn = parseFloat(String(cashioData.total_in || '0'));
    const docDate = cashioData.docdate || orderData.docDate || new Date();

    return {
      action: 0,
      ma_dvcs: invoiceData.ma_dvcs || cashioData.branch_code || '',
      ma_kh: invoiceData.ma_kh || cashioData.partner_code || '',
      ong_ba: orderData.customer?.name || cashioData.partner_name || invoiceData.ong_ba || '',
      loai_ct: '2', // Mặc định 2 - Thu của khách hàng
      dept_id: invoiceData.ma_bp || cashioData.branch_code || '',
      dien_giai: `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
      ngay_lct: this.formatDateISO(docDate),
      so_ct: orderData.docCode || invoiceData.so_ct || '',
      so_ct_tc: orderData.docCode || invoiceData.so_ct || '',
      ma_nt: 'VND',
      ty_gia: 1,
      ma_cp1: '',
      ma_cp2: '',
      httt: 'CASH',
      status: '0' as string,
      detail: [
        {
          ma_kh_i: invoiceData.ma_kh || cashioData.partner_code || '',
          tien: totalIn,
          dien_giai: cashioData.refno || `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
          ma_bp: invoiceData.ma_bp || cashioData.branch_code || '',
          ma_vv: '',
          ma_hd: '',
          ma_phi: '',
          ma_ku: '',
        },
      ],
    };
  }

  /**
   * Build payload cho creditAdvice API
   */
  private buildCreditAdvicePayload(cashioData: any, orderData: any, invoiceData: any, paymentMethod: any): any {
    const totalIn = parseFloat(String(cashioData.total_in || '0'));
    const docDate = cashioData.docdate || orderData.docDate || new Date();

    return {
      action: 0,
      ma_dvcs: invoiceData.ma_dvcs || cashioData.branch_code || '',
      ma_kh: invoiceData.ma_kh || cashioData.partner_code || '',
      ong_ba: orderData.customer?.name || cashioData.partner_name || invoiceData.ong_ba || '',
      loai_ct: '2', // Mặc định 2 - Thu của khách hàng
      dept_id: invoiceData.ma_bp || cashioData.branch_code || '',
      dien_giai: `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
      ngay_lct: this.formatDateISO(docDate),
      so_ct: orderData.docCode || invoiceData.so_ct || '',
      so_ct_tc: orderData.docCode || invoiceData.so_ct || '',
      ma_nt: 'VND',
      ty_gia: 1,
      ma_cp1: '',
      ma_cp2: '',
      httt: paymentMethod.code || cashioData.fop_syscode || '',
      status: '0',
      detail: [
        {
          ma_kh_i: invoiceData.ma_kh || cashioData.partner_code || '',
          tien: totalIn,
          dien_giai: cashioData.refno || paymentMethod.description || `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
          ma_bp: invoiceData.ma_bp || cashioData.branch_code || '',
          ma_vv: '',
          ma_hd: '',
          ma_phi: '',
          ma_ku: '',
        },
      ],
    };
  }

  /**
   * Xử lý warehouse receipt/release từ stock transfer
   * @param stockTransfer - Dữ liệu stock transfer
   * @returns Kết quả từ API
   */
  async processWarehouseFromStockTransfer(stockTransfer: any): Promise<any> {
    // Kiểm tra doctype phải là "STOCK_IO"
    if (stockTransfer.doctype !== 'STOCK_IO') {
      throw new BadRequestException(
        `Không thể xử lý stock transfer có doctype = "${stockTransfer.doctype}". Chỉ chấp nhận doctype = "STOCK_IO".`
      );
    }

    // Kiểm tra soCode phải là "null" (string) hoặc null
    if (stockTransfer.soCode !== 'null' && stockTransfer.soCode !== null) {
      throw new BadRequestException(
        `Không thể xử lý stock transfer có soCode = "${stockTransfer.soCode}". Chỉ chấp nhận soCode = "null" hoặc null.`
      );
    }

    // Kiểm tra ioType phải là "I" hoặc "O"
    if (stockTransfer.ioType !== 'I' && stockTransfer.ioType !== 'O') {
      throw new BadRequestException(
        `Không thể xử lý stock transfer có ioType = "${stockTransfer.ioType}". Chỉ chấp nhận "I" (nhập) hoặc "O" (xuất).`
      );
    }

    // Lấy ma_dvcs từ department API (ưu tiên), nếu không có thì fallback
    let department: any = null;
    if (stockTransfer.branchCode) {
      try {
        department = await this.categoriesService.getDepartmentFromLoyaltyAPI(stockTransfer.branchCode);
      } catch (error: any) {
        this.logger.warn(`Không thể lấy department cho branchCode ${stockTransfer.branchCode}: ${error?.message || error}`);
      }
    }

    const maDvcs = department?.ma_dvcs || department?.ma_dvcs_ht || '';

    // Fetch material catalog từ Loyalty API (giống bên sale)
    let materialCatalog: any = null;
    const itemCodeToFetch = stockTransfer.itemCode;
    if (itemCodeToFetch) {
      try {
        materialCatalog = await this.loyaltyService.fetchProduct(itemCodeToFetch);
        if (materialCatalog) {
          this.logger.debug(`[Warehouse] Đã lấy material catalog cho itemCode ${itemCodeToFetch}`);
        }
      } catch (error: any) {
        this.logger.warn(`Không thể lấy material catalog cho itemCode ${itemCodeToFetch}: ${error?.message || error}`);
      }
    }

    // Lấy materialCode và unit từ material catalog (ưu tiên từ catalog, fallback từ stockTransfer)
    const materialCode = materialCatalog?.materialCode || stockTransfer.materialCode || stockTransfer.itemCode || '';
    const unit = materialCatalog?.unit || '';

    // Map mã kho qua API warehouse-code-mappings (giống bên sale)
    let mappedStockCode = stockTransfer.stockCode || '';
    if (stockTransfer.stockCode) {
      try {
        const maMoi = await this.categoriesService.mapWarehouseCode(stockTransfer.stockCode);
        // Nếu có maMoi (mapped = true) → dùng maMoi
        // Nếu không có maMoi (mapped = false) → dùng giá trị gốc từ stockTransfer
        mappedStockCode = maMoi || stockTransfer.stockCode;
      } catch (error: any) {
        // Nếu có lỗi khi gọi API mapping, fallback về giá trị gốc
        this.logger.warn(`Không thể map warehouse code ${stockTransfer.stockCode}: ${error?.message || error}`);
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
      dien_giai: stockTransfer.docDesc || `Phiếu ${stockTransfer.ioType === 'I' ? 'nhập' : 'xuất'} kho ${stockTransfer.docCode}`,
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
          ...(stockTransfer.ioType === 'O' ? {
            px_gia_dd: 0,
            ma_phi: '',
            ma_ku: '',
            ma_phi_hh: '',
            ma_phi_ttlk: '',
            tien_hh_nt: 0,
            tien_ttlk_nt: 0,
          } : {
            pn_gia_tb: 0,
          }),
        },
      ],
    };

    // Gọi API tương ứng (ioType đã được validate ở trên)
    if (stockTransfer.ioType === 'I') {
      // Nhập kho
      this.logger.log(`[Warehouse] Tạo phiếu nhập kho cho ${stockTransfer.docCode}`);
      const result = await this.fastApiService.submitWarehouseReceipt(payload);
      return result;
    } else {
      // Xuất kho (ioType = "O" - đã được validate ở trên)
      this.logger.log(`[Warehouse] Tạo phiếu xuất kho cho ${stockTransfer.docCode}`);
      const result = await this.fastApiService.submitWarehouseRelease(payload);
      return result;
    }
  }
}

