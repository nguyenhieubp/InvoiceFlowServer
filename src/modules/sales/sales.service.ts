import { Injectable, InternalServerErrorException, Logger, NotFoundException, BadRequestException } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, Or, IsNull, Between } from 'typeorm';
import * as XLSX from 'xlsx-js-style';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { Invoice } from '../../entities/invoice.entity';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';
import { InvoiceService } from '../invoices/invoice.service';
import { ZappyApiService } from '../../services/zappy-api.service';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { CategoriesService } from '../categories/categories.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { InvoiceValidationService } from '../../services/invoice-validation.service';
import { Order, SaleItem } from '../../types/order.types';
import { CreateStockTransferDto, StockTransferItem } from '../../dto/create-stock-transfer.dto';
import { calculateVCType } from '../../utils/product.utils';

@Injectable()
export class SalesService {
  private readonly logger = new Logger(SalesService.name);

  /**
   * Validate integer value để tránh NaN
   */
  private validateInteger(value: any): number | undefined {
    if (value === null || value === undefined) {
      return undefined;
    }
    const num = Number(value);
    if (isNaN(num) || !isFinite(num)) {
      return undefined;
    }
    return Math.floor(num);
  }

  /**
   * Normalize mã khách hàng: Bỏ prefix "NV" nếu có
   * VD: "NV8480" => "8480", "KH123" => "KH123"
   */
  private normalizeMaKh(maKh: string | null | undefined): string {
    if (!maKh) return '';
    const trimmed = String(maKh).trim();
    // Bỏ prefix "NV" nếu có (case insensitive)
    if (trimmed.length > 2 && trimmed.substring(0, 2).toUpperCase() === 'NV') {
      return trimmed.substring(2);
    }
    return trimmed;
  }

  /**
   * Xử lý promotion code: cắt phần sau dấu "-" để lấy code hiển thị
   */
  private getPromotionDisplayCode(promCode: string | null | undefined): string | null {
    if (!promCode) return null;
    const parts = promCode.split('-');
    return parts[0] || promCode;
  }

  /**
   * Format voucher code để hiển thị/gửi lên API
   * - F3 (facialbar): giữ nguyên "FBV TT VCDV", "FBV TT VCHH" (không có khoảng trắng)
   * - Các brand khác: chuyển đổi "VCHB" → "VC HB", "VCKM" → "VC KM", "VCDV" → "VC DV"
   * @param maCk05Value - Giá trị từ calculateMaCk05()
   * @returns Format voucher code đã được chuyển đổi
   */
  private formatVoucherCode(maCk05Value: string | null): string | null {
    if (!maCk05Value) return null;

    // Riêng với F3 (facialbar): giữ nguyên format "FBV TT VCDV", "FBV TT VCHH" (không có khoảng trắng)
    if (maCk05Value.includes('FBV TT')) {
      return maCk05Value;
    }

    // Các brand khác: chuyển đổi VCHB → VC HB, VCKM → VC KM, VCDV → VC DV
    let formatted = maCk05Value;
    formatted = formatted.replace(/VCHB/g, 'VC HB');
    formatted = formatted.replace(/VCKM/g, 'VC KM');
    formatted = formatted.replace(/VCDV/g, 'VC DV');

    return formatted;
  }

  /**
   * Tính và trả về ma_ck05 (Thanh toán voucher) dựa trên productType và trackInventory
   * @param sale - Sale object (có thể có customer.brand)
   * @returns Loại VC: "VCDV" | "VCHB" | "VCKM" | "FBV TT VCDV" | "FBV TT VCHB" | null
   */
  private calculateMaCk05(sale: any): string | null {
    if (!sale) return null;

    const paidByVoucher = sale.paid_by_voucher_ecode_ecoin_bp ?? 0;
    const revenueValue = sale.revenue ?? 0;
    const linetotalValue = sale.linetotal ?? sale.tienHang ?? 0;

    // Nếu revenue = 0 và linetotal = 0 → không gắn nhãn
    if (revenueValue === 0 && linetotalValue === 0) {
      return null;
    }

    let brand = sale.brand || '';

    // Lấy productType và trackInventory từ sale hoặc product
    const productType = this.getProductType(sale);
    const isGift = sale.product.producttype === 'GIFT';
    const trackInventory = this.getTrackInventory(sale);

    if(brand === 'yaman') {
      if(productType === 'I') {
        return 'YVC.HB';
      }
      if(productType === 'S') {
        return 'YVC.DV';
      }
    }
    if(brand === 'facialbar') {
      if(productType === 'I') {
        return 'FBV TT VCDV';
      }
      if(productType === 'S') {
        return 'FBV TT VCHH';
      }
    }
    if(brand === 'labhair') {
      if(productType === 'I') {
        if(isGift) {
          return 'LHVTT.VCKM';
        }
        return 'LHVTT.VCDV';
      }
      if(productType === 'S') {
        return 'LHVTT.VCHB';
      }
    }
    if(brand === 'menard') {
      if(productType === 'I') {
        if(isGift) {
          return 'VC KM';
        }
        return 'VC DV';
      }
      if(productType === 'S') {
        return 'VC HB';
      }
    }
    return null;
  }

  /**
   * Map brand name sang brand code
   * menard → MN, f3 → FBV, chando → CDV, labhair → LHV, yaman → BTH
   */
  private mapBrandToCode(brand: string | null | undefined): string {
    if (!brand) return 'MN'; // Default

    const brandLower = brand.toLowerCase().trim();
    const brandMap: Record<string, string> = {
      'menard': 'MN',
      'f3': 'FBV',
      'facialbar': 'FBV',
      'chando': 'CDV',
      'labhair': 'LHV',
      'yaman': 'BTH',
    };

    return brandMap[brandLower] || 'MN'; // Default to MN
  }

  /**
   * Generate label cho "Thanh toán TK tiền ảo"
   * Format: YYMM{brand_code}.TKDV (ví dụ: 2511MN.TKDV)
   * - YY: 2 số cuối của năm từ docDate
   * - MM: Tháng từ docDate (2 số)
   * - {brand_code}: Brand code từ customer.brand (MN, FBV, CDV, LHV, BTH)
   */
  private generateTkTienAoLabel(orderData: any): string {
    // Lấy ngày từ docDate của order
    let docDate: Date;
    if (orderData.docDate instanceof Date) {
      docDate = orderData.docDate;
    } else if (typeof orderData.docDate === 'string') {
      docDate = new Date(orderData.docDate);
      if (isNaN(docDate.getTime())) {
        // Nếu không parse được, dùng ngày hiện tại
        docDate = new Date();
      }
    } else {
      // Fallback: dùng ngày hiện tại
      docDate = new Date();
    }

    const year = docDate.getFullYear();
    const month = docDate.getMonth() + 1; // getMonth() trả về 0-11

    // Lấy 2 số cuối của năm
    const yy = String(year).slice(-2);
    // Format tháng thành 2 số (01, 02, ..., 12)
    const mm = String(month).padStart(2, '0');

    // Ưu tiên lấy brand code từ customer.brand
    const brand = orderData.customer?.brand
      || orderData.sales?.[0]?.customer?.brand
      || '';

    // Map brand name sang brand code (menard → MN, f3 → FBV, etc.)
    const brandCode = this.mapBrandToCode(brand);

    return `${yy}${mm}${brandCode}.TKDV`;
  }


  /**
   * Lấy prefix từ ordertype để tính mã kho
   * - "L" cho: "02. Làm dịch vụ", "04. Đổi DV", "08. Tách thẻ", "Đổi thẻ KEEP->Thẻ DV"
   * - "B" cho: "01.Thường", "03. Đổi điểm", "05. Tặng sinh nhật", "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT", "Đổi vỏ"
   */
  private getOrderTypePrefix(ordertypeName: string | null | undefined): string | null {
    if (!ordertypeName) return null;

    const normalized = String(ordertypeName).trim();

    // Kho hàng làm (prefix L)
    const orderTypeLNames = [
      '02. Làm dịch vụ',
      '04. Đổi DV',
      '08. Tách thẻ',
      'Đổi thẻ KEEP->Thẻ DV',
      'LAM_DV',
      'DOI_VO_LAY_DV',
      'KEEP_TO_SVC',
      'LAM_THE_DV',
      'SUA_THE_DV',
      'DOI_THE_DV',
      'LAM_DV_LE',
      'LAM_THE_KEEP',
      'NOI_THE_KEEP',
      'RENAME_CARD',
    ];

    // Kho hàng bán (prefix B)
    const orderTypeBNames = [
      '01.Thường',
      '01. Thường',
      '03. Đổi điểm',
      '05. Tặng sinh nhật',
      '06. Đầu tư',
      '07. Bán tài khoản',
      '9. Sàn TMDT',
      'Đổi vỏ',
      'NORMAL',
      'KM_TRA_DL',
      'BIRTHDAY_PROM',
      'BP_TO_ITEM',
      'BAN_ECOIN',
      'SAN_TMDT',
      'SO_DL',
      'SO_HTDT_HB',
      'SO_HTDT_HK',
      'SO_HTDT_HL_CB',
      'SO_HTDT_HL_HB',
      'SO_HTDT_HL_KM',
      'SO_HTDT_HT',
      'ZERO_CTY',
      'ZERO_SHOP',
    ];

    if (orderTypeLNames.includes(normalized)) {
      return 'L';
    }

    if (orderTypeBNames.includes(normalized)) {
      return 'B';
    }

    return null;
  }

  /**
   * Tính mã kho từ ordertype + ma_bp (bộ phận)
   * Format: prefix + ma_bp (ví dụ: "L" + "MH10" = "LMH10", "B" + "MH10" = "BMH10")
   * @deprecated Không dùng nữa, dùng getMaKhoFromStockTransfer thay thế
   */
  private calculateMaKho(
    ordertype: string | null | undefined,
    maBp: string | null | undefined
  ): string | null {
    const prefix = this.getOrderTypePrefix(ordertype);
    if (!prefix || !maBp) {
      return null;
    }
    return prefix + maBp;
  }

  /**
   * Helper: Kiểm tra xem đơn hàng có phải "03. Đổi điểm" không
   */
  private isDoiDiemOrder(ordertype: string | null | undefined, ordertypeName: string | null | undefined): boolean {
    const ordertypeValue = ordertype || ordertypeName || '';
    return ordertypeValue.includes('03. Đổi điểm') ||
      ordertypeValue.includes('03.Đổi điểm') ||
      ordertypeValue.includes('03.  Đổi điểm');
  }

  /**
   * Helper: Kiểm tra xem đơn hàng có phải "04. Đổi DV" không
   */
  private isDoiDvOrder(ordertype: string | null | undefined, ordertypeName: string | null | undefined): boolean {
    const ordertypeValue = ordertype || ordertypeName || '';
    return ordertypeValue.includes('04. Đổi DV') ||
      ordertypeValue.includes('04.Đổi DV') ||
      ordertypeValue.includes('04.  Đổi DV');
  }

  /**
   * Helper: Kiểm tra xem đơn hàng có phải "05. Tặng sinh nhật" không
   */
  private isTangSinhNhatOrder(ordertype: string | null | undefined, ordertypeName: string | null | undefined): boolean {
    const ordertypeValue = ordertype || ordertypeName || '';
    return ordertypeValue.includes('05. Tặng sinh nhật') ||
      ordertypeValue.includes('05.Tặng sinh nhật') ||
      ordertypeValue.includes('05.  Tặng sinh nhật');
  }

  /**
   * Helper: Kiểm tra xem đơn hàng có phải "06. Đầu tư" không
   */
  private isDauTuOrder(ordertype: string | null | undefined, ordertypeName: string | null | undefined): boolean {
    const ordertypeValue = ordertype || ordertypeName || '';
    return ordertypeValue.includes('06. Đầu tư') ||
      ordertypeValue.includes('06.Đầu tư') ||
      ordertypeValue.includes('06.  Đầu tư');
  }

  /**
   * Helper: Kiểm tra xem đơn hàng có phải "08. Tách thẻ" không
   */
  private isTachTheOrder(ordertype: string | null | undefined, ordertypeName: string | null | undefined): boolean {
    const ordertypeValue = ordertype || ordertypeName || '';
    return ordertypeValue.includes('08. Tách thẻ') ||
      ordertypeValue.includes('08.Tách thẻ') ||
      ordertypeValue.includes('08.  Tách thẻ');
  }

  /**
   * Gọi API get_card để lấy issue_partner_code cho đơn "08. Tách thẻ"
   */
  private async fetchCardDataAndMapIssuePartnerCode(docCode: string, sales: any[]): Promise<void> {
    // Kiểm tra xem có sale nào là "08. Tách thẻ" không
    const hasTachThe = sales.some((s: any) =>
      this.isTachTheOrder(s.ordertype, s.ordertypeName)
    );

    if (!hasTachThe) {
      return; // Không phải đơn "08. Tách thẻ", không cần gọi API
    }

    try {
      const apiUrl = 'https://n8n.vmt.vn/webhook/vmt/get_card';
      const requestBody = { doccode: docCode };

      // API này dùng GET method nhưng có body
      let cardResponse: any;
      try {
        const response = await this.httpService.axiosRef.request({
          method: 'GET',
          url: apiUrl,
          headers: {
            'Content-Type': 'application/json',
          },
          data: requestBody,
          timeout: 30000,
        });
        cardResponse = response.data;
      } catch (getError: any) {
        // Nếu GET fail, thử POST như fallback
        if (getError?.response?.status === 404 || getError?.response?.status === 405) {
          try {
            const response = await this.httpService.axiosRef.post(
              apiUrl,
              requestBody,
              {
                headers: {
                  'Content-Type': 'application/json',
                },
                timeout: 30000,
              }
            );
            cardResponse = response.data;
          } catch (postError: any) {
            // Nếu cả POST cũng fail, return (không throw để không ảnh hưởng đến flow chính)
            return;
          }
        } else {
          return;
        }
      }

      // Parse response data
      let cardData: any[] = [];
      if (cardResponse && Array.isArray(cardResponse) && cardResponse.length > 0) {
        const firstItem = cardResponse[0];
        if (firstItem.data && Array.isArray(firstItem.data)) {
          cardData = firstItem.data;
        }
      }

      // Map issue_partner_code vào các sale
      if (cardData.length > 0) {
        sales.forEach((sale: any) => {
          const saleQty = Number(sale.qty || 0);

          if (saleQty < 0) {
            const negativeItem = cardData.find((item: any) => Number(item.qty || 0) < 0);
            if (negativeItem && negativeItem.issue_partner_code) {
              sale.issuePartnerCode = negativeItem.issue_partner_code;
            }
          } else if (saleQty > 0) {
            const positiveItem = cardData.find((item: any) => Number(item.qty || 0) > 0 && item.action === 'ADJUST');
            if (positiveItem && positiveItem.issue_partner_code) {
              sale.issuePartnerCode = positiveItem.issue_partner_code;
            } else {
              // Fallback: Tìm item có qty > 0 (không cần action = "ADJUST")
              const positiveItemFallback = cardData.find((item: any) => Number(item.qty || 0) > 0);
              if (positiveItemFallback && positiveItemFallback.issue_partner_code) {
                sale.issuePartnerCode = positiveItemFallback.issue_partner_code;
              }
            }
          }
        });
      }
    } catch (error: any) {
      // Tiếp tục (không throw để không ảnh hưởng đến flow chính)
      // Chỉ log error nếu thực sự cần thiết
    }
  }

  /**
   * Helper: Kiểm tra xem đơn hàng có phải "Đổi vỏ" không
   */
  private isDoiVoOrder(ordertype: string | null | undefined, ordertypeName: string | null | undefined): boolean {
    const ordertypeValue = ordertype || ordertypeName || '';
    return ordertypeValue.includes('Đổi vỏ');
  }

  /**
   * Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
   * Logic: Match stock transfer theo itemCode và soCode, lấy stockCode
   * Xử lý đặc biệt cho đơn trả lại (RT): chuyển RT -> SO hoặc RT -> ST để match
   * @param sale - Sale object có itemCode
   * @param docCode - Mã đơn hàng (docCode)
   * @param stockTransfers - Danh sách stock transfers
   * @param saleMaterialCode - MaterialCode từ Loyalty API (optional, để match chính xác hơn)
   * @param stockTransferMap - Map stock transfers theo key (optional, để tối ưu performance)
   * @returns Mã kho từ stockCode của stock transfer, hoặc fallback về sale.maKho hoặc sale.branchCode
   */
  private async getMaKhoFromStockTransfer(
    sale: any,
    docCode: string,
    stockTransfers: StockTransfer[],
    saleMaterialCode?: string | null,
    stockTransferMap?: Map<string, StockTransfer[]>
  ): Promise<string> {
    let matchedStockTransfer: StockTransfer | null = null;

    // Xử lý đặc biệt cho đơn trả lại (RT): chuyển RT -> SO (mã đơn mua gốc) hoặc RT -> ST (mã xuất kho)
    const isReturnOrder = docCode.startsWith('RT');
    let originalOrderCode: string | null = null;
    let stockOutDocCode: string | null = null;

    if (isReturnOrder) {
      // RT33.00121928_1 -> SO33.00121928 (chuyển RT thành SO, bỏ _1)
      originalOrderCode = docCode.replace(/^RT/, 'SO').replace(/_\d+$/, '');
      // RT33.00121928_1 -> ST33.00121928_1 (chuyển RT thành ST)
      stockOutDocCode = docCode.replace(/^RT/, 'ST');
    }

    // Ưu tiên match theo materialCode nếu có stockTransferMap
    if (saleMaterialCode && stockTransferMap) {
      // Thử match với docCode gốc
      let stockTransferKey = `${docCode}_${saleMaterialCode}`;
      let matchedTransfers = stockTransferMap.get(stockTransferKey) || [];

      // Nếu là đơn trả lại và không match được, thử với mã đơn gốc
      if (matchedTransfers.length === 0 && isReturnOrder && originalOrderCode) {
        stockTransferKey = `${originalOrderCode}_${saleMaterialCode}`;
        matchedTransfers = stockTransferMap.get(stockTransferKey) || [];
      }

      if (matchedTransfers.length > 0) {
        matchedStockTransfer = matchedTransfers[0];
      }
    }

    // Nếu không match được theo materialCode, match trực tiếp theo itemCode và soCode/docCode
    if (!matchedStockTransfer && sale.itemCode) {
      // Thử match với docCode gốc
      matchedStockTransfer = stockTransfers.find(
        (st) => st.soCode === docCode && st.itemCode === sale.itemCode
      ) || null;

      // Nếu là đơn trả lại và không match được, thử với mã đơn gốc (SO)
      if (!matchedStockTransfer && isReturnOrder && originalOrderCode) {
        matchedStockTransfer = stockTransfers.find(
          (st) => st.soCode === originalOrderCode && st.itemCode === sale.itemCode
        ) || null;
      }

      // Nếu vẫn không match được, thử match theo docCode của stock transfer (ST)
      if (!matchedStockTransfer && isReturnOrder && stockOutDocCode) {
        matchedStockTransfer = stockTransfers.find(
          (st) => st.docCode === stockOutDocCode && st.itemCode === sale.itemCode
        ) || null;
      }
    }

    // Lấy mã kho từ stockCode (Mã kho xuất) của stock transfer
    const stockCode = matchedStockTransfer?.stockCode || '';

    // Nếu không có stockCode, trả về rỗng
    if (!stockCode || stockCode.trim() === '') {
      return '';
    }

    // Map mã kho qua API warehouse-code-mappings
    try {
      const maMoi = await this.categoriesService.mapWarehouseCode(stockCode);

      // Nếu có maMoi (mapped = true) → dùng maMoi
      // Nếu không có maMoi (mapped = false) → dùng giá trị gốc từ stock_transfers
      return maMoi || stockCode;
    } catch (error: any) {
      // Nếu có lỗi khi gọi API mapping, fallback về giá trị gốc
      this.logger.error(`Error mapping warehouse code ${stockCode}: ${error?.message || error}`);
      return stockCode;
    }
  }

  /**
   * Lấy danh sách docCode cần fetch stock transfers
   * Xử lý đặc biệt cho đơn trả lại (RT): thêm mã đơn gốc (SO) vào danh sách
   * @param docCodes - Danh sách mã đơn hàng
   * @returns Danh sách docCode cần fetch (bao gồm cả mã đơn gốc nếu là đơn trả lại)
   */
  private getDocCodesForStockTransfer(docCodes: string[]): string[] {
    const result = new Set<string>();

    for (const docCode of docCodes) {
      result.add(docCode);

      // Nếu là đơn trả lại (RT), thêm mã đơn gốc (SO) vào danh sách
      if (docCode.startsWith('RT')) {
        // RT33.00121928_1 -> SO33.00121928 (chuyển RT thành SO, bỏ _1)
        const originalOrderCode = docCode.replace(/^RT/, 'SO').replace(/_\d+$/, '');
        result.add(originalOrderCode);
      }
    }

    return Array.from(result);
  }

  /**
   * Build stock transfer maps (stockTransferMap và stockTransferByDocCodeMap)
   * Xử lý đặc biệt cho đơn trả lại: thêm key với mã đơn trả lại (RT) nếu soCode là mã đơn gốc (SO)
   * @param stockTransfers - Danh sách stock transfers
   * @param loyaltyProductMap - Map để lấy materialCode từ itemCode
   * @param docCodes - Danh sách docCodes của orders (để xử lý đơn trả lại)
   * @returns Object chứa stockTransferMap và stockTransferByDocCodeMap
   */
  private buildStockTransferMaps(
    stockTransfers: StockTransfer[],
    loyaltyProductMap: Map<string, any>,
    docCodes: string[]
  ): {
    stockTransferMap: Map<string, StockTransfer[]>;
    stockTransferByDocCodeMap: Map<string, StockTransfer[]>;
  } {
    // Tạo map stock transfers theo docCode (của order) và materialCode (Mã hàng từ Loyalty API)
    // Match theo: soCode (Mã ĐH) = docCode (Số hóa đơn) VÀ materialCode (Mã hàng)
    // Xử lý đặc biệt cho đơn trả lại: thêm key với mã đơn trả lại (RT) nếu soCode là mã đơn gốc (SO)
    const stockTransferMap = new Map<string, StockTransfer[]>();
    const stockTransferByDocCodeMap = new Map<string, StockTransfer[]>();

    for (const transfer of stockTransfers) {
      // Lấy materialCode từ database hoặc từ Loyalty API
      const transferLoyaltyProduct = transfer.itemCode ? loyaltyProductMap.get(transfer.itemCode) : null;
      const materialCode = transfer.materialCode || transferLoyaltyProduct?.materialCode;

      if (!materialCode) {
        // Bỏ qua nếu không có materialCode (không match được)
        continue;
      }

      // Join theo soCode (của stock transfer) = docCode (của order) + materialCode (Mã hàng)
      const orderDocCode = transfer.soCode || transfer.docCode; // Ưu tiên soCode
      const key = `${orderDocCode}_${materialCode}`;

      // Thêm vào stockTransferMap
      if (!stockTransferMap.has(key)) {
        stockTransferMap.set(key, []);
      }
      stockTransferMap.get(key)!.push(transfer);

      // Thêm vào stockTransferByDocCodeMap
      if (!stockTransferByDocCodeMap.has(orderDocCode)) {
        stockTransferByDocCodeMap.set(orderDocCode, []);
      }
      stockTransferByDocCodeMap.get(orderDocCode)!.push(transfer);

      // Xử lý đặc biệt cho đơn trả lại: nếu soCode là mã đơn gốc (SO), thêm key với mã đơn trả lại (RT)
      // Ví dụ: soCode = SO33.00121928 -> thêm key RT33.00121928_1_materialCode
      if (orderDocCode.startsWith('SO') && docCodes.some(docCode => docCode.startsWith('RT'))) {
        // Tìm mã đơn trả lại tương ứng (SO33.00121928 -> RT33.00121928_1)
        for (const docCode of docCodes) {
          if (docCode.startsWith('RT')) {
            const originalOrderCode = docCode.replace(/^RT/, 'SO').replace(/_\d+$/, '');
            if (originalOrderCode === orderDocCode) {
              // Thêm key với mã đơn trả lại vào stockTransferMap
              const returnKey = `${docCode}_${materialCode}`;
              if (!stockTransferMap.has(returnKey)) {
                stockTransferMap.set(returnKey, []);
              }
              stockTransferMap.get(returnKey)!.push(transfer);

              // Thêm key với mã đơn trả lại vào stockTransferByDocCodeMap
              if (!stockTransferByDocCodeMap.has(docCode)) {
                stockTransferByDocCodeMap.set(docCode, []);
              }
              stockTransferByDocCodeMap.get(docCode)!.push(transfer);
            }
          }
        }
      }
    }

    return {
      stockTransferMap,
      stockTransferByDocCodeMap,
    };
  }

  /**
   * Normalize brand name: "facialbar" → "f3", giữ nguyên các brand khác
   */
  private normalizeBrand(brand: string | null | undefined): string {
    if (!brand) return '';
    let brandLower = brand.toLowerCase().trim();
    if (brandLower === 'facialbar') {
      brandLower = 'f3';
    }
    return brandLower;
  }

  /**
   * Lấy productType từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
   */
  private getProductType(sale: any, loyaltyProduct?: any): string | null {
    return loyaltyProduct?.productType ||
      loyaltyProduct?.producttype ||
      sale.productType ||
      sale.producttype ||
      sale.product?.productType ||
      sale.product?.producttype ||
      null;
  }

  /**
   * Lấy materialCode từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
   */
  private getMaterialCode(sale: any, loyaltyProduct?: any): string | null {
    return loyaltyProduct?.materialCode ||
      sale.product?.maVatTu ||
      sale.product?.materialCode ||
      sale.itemCode ||
      null;
  }

  /**
   * Lấy trackInventory từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
   */
  private getTrackInventory(sale: any, loyaltyProduct?: any): boolean | null {
    return loyaltyProduct?.trackInventory ??
      sale.trackInventory ??
      sale.product?.trackInventory ??
      null;
  }

  /**
   * Lấy trackSerial từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
   */
  private getTrackSerial(sale: any, loyaltyProduct?: any): boolean | null {
    return loyaltyProduct?.trackSerial ??
      sale.trackSerial ??
      sale.product?.trackSerial ??
      null;
  }

  /**
   * Tính muaHangCkVip/maCk03 dựa trên sale, loyaltyProduct và brand
   * Hàm chung được dùng cho cả calculateSaleFields và buildFastApiInvoiceData
   */
  private calculateMuaHangCkVip(
    sale: any,
    loyaltyProduct: any,
    brand: string | null | undefined,
    loggerContext: string = 'calculateMuaHangCkVip'
  ): string {
    const ck03_nt = Number(sale.chietKhauMuaHangCkVip || sale.grade_discamt || 0);
    if (ck03_nt <= 0) {
      return sale.muaHangCkVip || '';
    }

    // Normalize brand
    const brandLower = this.normalizeBrand(brand);

    // Lấy các giá trị cần thiết bằng helper functions
    const productType = this.getProductType(sale, loyaltyProduct);
    const materialCode = this.getMaterialCode(sale, loyaltyProduct);
    const code = sale.itemCode || null;
    const trackInventory = this.getTrackInventory(sale, loyaltyProduct);
    const trackSerial = this.getTrackSerial(sale, loyaltyProduct);

    // Logic cho F3
    if (brandLower === 'f3') {
      return productType === 'DIVU' ? 'FBV CKVIP DV' : 'FBV CKVIP SP';
    }


    const result = this.calculateVipType(productType, materialCode, code, trackInventory, trackSerial);


    return result;
  }

  /**
   * Tính VIP type dựa trên quy tắc (theo thứ tự ưu tiên):
   * 
   * 1. VIP DV MAT: Nếu productType == "DIVU"
   *    - Ví dụ: SPAMDV511 (productType: "DIVU", catName: "DVBDY", unit: "Lần")
   * 
   * 2. VIP VC MP: Nếu productType == "VOUC"
   *    - Ví dụ: E_VCM10.5TR_MDVK04 (productType: "VOUC", code có "E_" và "VC")
   * 
   * 3. VIP VC MP: Nếu materialCode bắt đầu bằng "E." HOẶC
   *               "VC" có trong materialCode/code (không phân biệt hoa thường) HOẶC
   *               (trackInventory == false VÀ trackSerial == true)
   * 
   * 4. VIP MP: Tất cả các trường hợp còn lại
   *    - Ví dụ: AUTO02 (productType: "MAKE", materialCode: "M00151", không có "VC")
   */
  private calculateVipType(
    productType: string | null | undefined,
    materialCode: string | null | undefined,
    code: string | null | undefined,
    trackInventory: boolean | null | undefined,
    trackSerial: boolean | null | undefined,
  ): string {
    // Rule 1: VIP DV MAT - Nếu productType == "DIVU"
    if (productType === 'DIVU') {
      return 'VIP DV MAT';
    }

    // Rule 2: VIP VC MP - Nếu productType == "VOUC"
    if (productType === 'VOUC') {
      return 'VIP VC MP';
    }

    // Rule 3: VIP VC MP - Kiểm tra các điều kiện khác
    const materialCodeStr = materialCode || '';
    const codeStr = code || '';
    // Kiểm tra "VC" trong materialCode hoặc code (không phân biệt hoa thường)
    const hasVC =
      materialCodeStr.toUpperCase().includes('VC') ||
      codeStr.toUpperCase().includes('VC');

    if (
      materialCodeStr.startsWith('E.') ||
      hasVC ||
      (trackInventory === false && trackSerial === true)
    ) {
      return 'VIP VC MP';
    }

    // Rule 4: VIP MP - Tất cả các trường hợp còn lại
    return 'VIP MP';
  }

  /**
   * Xác định nên dùng ma_lo hay so_serial dựa trên trackSerial và trackBatch từ Loyalty API
   * trackSerial: true → dùng so_serial
   * trackBatch: true → dùng ma_lo
   * Nếu cả hai đều true → ưu tiên trackBatch (dùng ma_lo)
   */
  private shouldUseBatch(trackBatch: boolean | null | undefined, trackSerial: boolean | null | undefined): boolean {
    // Nếu trackBatch = true → dùng ma_lo (ưu tiên)
    if (trackBatch === true) {
      return true;
    }
    // Nếu trackSerial = true và trackBatch = false → dùng so_serial
    if (trackSerial === true && trackBatch === false) {
      return false;
    }
    // Mặc định: nếu không có thông tin → dùng so_serial
    return false;
  }

  /**
   * Fetch products từ Loyalty API
   * @deprecated Sử dụng loyaltyService.fetchProducts() thay thế
   */
  private async fetchLoyaltyProducts(itemCodes: string[]): Promise<Map<string, any>> {
    return this.loyaltyService.fetchProducts(itemCodes);
  }

  /**
   * Fetch departments từ Loyalty API
   */
  private async fetchLoyaltyDepartments(branchCodes: string[]): Promise<Map<string, any>> {
    const departmentMap = new Map<string, any>();
    if (branchCodes.length === 0) return departmentMap;

    const departmentPromises = branchCodes.map(async (branchCode) => {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
          { headers: { accept: 'application/json' } },
        );
        const department = response?.data?.data?.items?.[0];
        return { branchCode, department };
      } catch (error) {
        this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
        return { branchCode, department: null };
      }
    });

    const results = await Promise.all(departmentPromises);
    results.forEach(({ branchCode, department }) => {
      if (department) {
        departmentMap.set(branchCode, department);
      }
    });

    return departmentMap;
  }

  /**
   * Tính toán các field phức tạp cho sale: maLo, maCtkmTangHang, muaHangCkVip
   */
  private calculateSaleFields(
    sale: any,
    loyaltyProduct: any,
    department: any,
    branchCode: string | null
  ): {
    maLo: string;
    maCtkmTangHang: string | null;
    muaHangCkVip: string;
    maKho: string | null;
    isTangHang: boolean;
    isDichVu: boolean;
    promCodeDisplay: string | null;
  } {
    const maBp = department?.ma_bp || branchCode || null;
    const maKho = this.calculateMaKho(sale.ordertype, maBp) || sale.maKho || branchCode || null;

    // Tính toán maCtkmTangHang
    // Lấy tienHang từ các nguồn (không dùng revenue vì revenue có thể khác 0 cho hàng tặng)
    const tienHang = Number(sale.tienHang || sale.linetotal || 0);
    const qty = Number(sale.qty || 0);
    let giaBan = Number(sale.giaBan || 0);
    if (giaBan === 0 && tienHang != null && qty > 0) {
      giaBan = tienHang / qty;
    }
    const revenue = Number(sale.revenue || 0);
    // Hàng tặng: giaBan = 0 và tienHang = 0 (revenue có thể > 0 nên không kiểm tra revenue)
    // Sử dụng Math.abs để tránh vấn đề với số thập phân nhỏ
    let isTangHang = Math.abs(giaBan) < 0.01 && Math.abs(tienHang) < 0.01;

    // Kiểm tra dịch vụ
    const ordertypeName = sale.ordertype || '';
    const isDichVu = ordertypeName.includes('02. Làm dịch vụ') ||
      ordertypeName.includes('04. Đổi DV') ||
      ordertypeName.includes('08. Tách thẻ') ||
      ordertypeName.includes('Đổi thẻ KEEP->Thẻ DV');

    // Kiểm tra đơn "03. Đổi điểm": không coi là hàng tặng (isTangHang = false) để không hiển thị "1" trong cột "Khuyến mãi"
    const isDoiDiem = this.isDoiDiemOrder(sale.ordertype, sale.ordertypeName);
    if (isDoiDiem) {
      isTangHang = false;
    }

    // Tính toán maCtkmTangHang TRƯỚC (cần dùng cho promCodeDisplay)
    // Lấy maCtkmTangHang từ sale (có thể đã có sẵn từ database hoặc tính toán trước đó)
    let maCtkmTangHang: string | null = sale.maCtkmTangHang ? String(sale.maCtkmTangHang).trim() : null;
    if (!maCtkmTangHang || maCtkmTangHang === '') {
      maCtkmTangHang = null;
    }
    // Nếu là đơn "03. Đổi điểm": set maCtkmTangHang theo ma_dvcs
    if (isDoiDiem && !maCtkmTangHang) {
      const maDvcs = department?.ma_dvcs || department?.ma_dvcs_ht || '';
      if (maDvcs === 'TTM' || maDvcs === 'AMA' || maDvcs === 'TSG') {
        maCtkmTangHang = 'TTM.KMDIEM';
      } else if (maDvcs === 'FBV') {
        maCtkmTangHang = 'FBV.KMDIEM';
      } else if (maDvcs === 'BTH') {
        maCtkmTangHang = 'BTH.KMDIEM';
      } else if (maDvcs === 'CDV') {
        maCtkmTangHang = 'CDV.KMDIEM';
      } else if (maDvcs === 'LHV') {
        maCtkmTangHang = 'LHV.KMDIEM';
      }
    }
    // Nếu chưa có maCtkmTangHang và là hàng tặng, tính toán nó
    if (isTangHang && !maCtkmTangHang) {
      if (ordertypeName.includes('06. Đầu tư') || ordertypeName.includes('06.Đầu tư')) {
        maCtkmTangHang = 'TT DAU TU';
      } else if (
        (ordertypeName.includes('01.Thường') || ordertypeName.includes('01. Thường')) ||
        (ordertypeName.includes('07. Bán tài khoản') || ordertypeName.includes('07.Bán tài khoản')) ||
        (ordertypeName.includes('9. Sàn TMDT') || ordertypeName.includes('9.Sàn TMDT'))
      ) {
        // Dùng promCode trực tiếp thay vì convertPromCodeToTangSp
        maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
      } else {
        maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
      }
      // Trim lại sau khi tính toán
      if (maCtkmTangHang) {
        maCtkmTangHang = maCtkmTangHang.trim();
      }
    }

    // Tính toán promCodeDisplay (giá trị hiển thị cho cột promCode) - SAU KHI đã tính maCtkmTangHang
    // Logic: Nếu là hàng tặng, không phải dịch vụ, và có maCtkmTangHang (không phải "TT DAU TU") → hiển thị "1"
    let promCodeDisplay: string | null = null;
    if (isTangHang && !isDichVu) {
      // Kiểm tra maCtkmTangHang: nếu có giá trị (từ database hoặc tính toán) và không phải "TT DAU TU" → hiển thị "1"
      const maCtkmTangHangStr = maCtkmTangHang ? String(maCtkmTangHang).trim() : '';
      if (maCtkmTangHangStr && maCtkmTangHangStr !== 'TT DAU TU') {
        promCodeDisplay = '1';
      }
    }

    // Tính toán muaHangCkVip - dùng hàm chung
    const customerBrand = sale.customer?.brand || null;
    const muaHangCkVip = this.calculateMuaHangCkVip(sale, loyaltyProduct, customerBrand, 'calculateSaleFields');

    // Tính toán maLo từ serial nếu chưa có
    let maLo = sale.maLo || '';
    if (!maLo) {
      const serial = sale.serial || '';
      if (serial) {
        const brand = sale.customer?.brand || '';
        const brandLower = this.normalizeBrand(brand);
        const underscoreIndex = serial.indexOf('_');
        if (underscoreIndex > 0 && underscoreIndex < serial.length - 1) {
          maLo = serial.substring(underscoreIndex + 1);
        } else {
          const productType = this.getProductType(sale, loyaltyProduct);
          const trackBatch = loyaltyProduct?.trackBatch === true || sale.trackInventory === true;
          if (trackBatch) {
            if (brandLower === 'f3') {
              maLo = serial;
            } else {
              const productTypeUpper = productType ? String(productType).toUpperCase().trim() : null;
              if (productTypeUpper === 'TPCN') {
                maLo = serial.length >= 8 ? serial.slice(-8) : serial;
              } else if (productTypeUpper === 'SKIN' || productTypeUpper === 'GIFT') {
                maLo = serial.length >= 4 ? serial.slice(-4) : serial;
              } else {
                maLo = serial.length >= 4 ? serial.slice(-4) : serial;
              }
            }
          }
        }
      }
    }

    return { maLo, maCtkmTangHang, muaHangCkVip, maKho, isTangHang, isDichVu, promCodeDisplay };
  }

  /**
   * Format sale object để trả về frontend
   */
  /**
   * Tính toán các field display phức tạp cho frontend
   */
  private calculateDisplayFields(sale: any, order: any, loyaltyProduct: any, department: any): {
    thanhToanCouponDisplay: string | null;
    chietKhauThanhToanCouponDisplay: number | null;
    thanhToanVoucherDisplay: string | null;
    chietKhauThanhToanVoucherDisplay: number | null;
    voucherDp1Display: string | null;
    chietKhauVoucherDp1Display: number | null;
    thanhToanTkTienAoDisplay: string | null;
    chietKhauThanhToanTkTienAoDisplay: number | null;
    soSerialDisplay: string | null;
    cucThueDisplay: string | null;
    tkDoanhThuDisplay: string | null;
    tkGiaVonDisplay: string | null;
  } {
    // Helper: Kiểm tra voucher dự phòng
    const isVoucherDuPhong = (brand: string, soSource: string | null | undefined, promCode: string | null | undefined, pkgCode: string | null | undefined): boolean => {
      const brandLower = this.normalizeBrand(brand);
      const isShopee = Boolean(soSource && String(soSource).toUpperCase() === 'SHOPEE');
      const hasPromCode = Boolean(promCode && promCode.trim() !== '');
      const hasPkgCode = Boolean(pkgCode && pkgCode.trim() !== '');
      if (brandLower === 'f3') {
        return isShopee;
      }
      return isShopee || (hasPromCode && !hasPkgCode);
    };

    // Helper: Kiểm tra ECOIN
    const hasEcoin = (orderData: any): boolean => {
      const chietKhauTkTienAo = orderData.cashioTotalIn ?? 0;
      const isEcoin = orderData.cashioFopSyscode === 'ECOIN';
      return chietKhauTkTienAo > 0 || (isEcoin && (orderData.cashioTotalIn ?? 0) > 0);
    };

    const brand = order?.customer?.brand || order?.brand || sale?.customer?.brand || '';
    const normalizedBrand = this.normalizeBrand(brand);

    // Kiểm tra đơn "03. Đổi điểm"
    const ordertypeNameForDisplayFields = sale.ordertypeName || sale.ordertype || '';
    const isDoiDiemForDisplayFields = this.isDoiDiemOrder(sale.ordertype, sale.ordertypeName);

    // thanhToanCouponDisplay
    const maCoupon = sale.maCk04 || (sale.thanhToanCoupon && sale.thanhToanCoupon > 0 ? 'COUPON' : null);
    const thanhToanCouponDisplay = maCoupon || null;

    // chietKhauThanhToanCouponDisplay
    const chietKhauCoupon = sale.chietKhauThanhToanCoupon ?? sale.chietKhau09 ?? 0;
    const chietKhauThanhToanCouponDisplay = chietKhauCoupon > 0 ? chietKhauCoupon : null;

    // thanhToanVoucherDisplay và chietKhauThanhToanVoucherDisplay
    // Nếu là đơn "03. Đổi điểm": set = null và 0
    let thanhToanVoucherDisplay: string | null = null;
    let chietKhauThanhToanVoucherDisplay: number | null = null;
    if (isDoiDiemForDisplayFields) {
      // Đơn "03. Đổi điểm": không hiển thị voucher
      thanhToanVoucherDisplay = null;
      chietKhauThanhToanVoucherDisplay = null;
    } else if (!hasEcoin(order)) {
      const chietKhauVoucherDp1 = Number(sale.chietKhauVoucherDp1 ?? 0) || 0;
      const pkgCode = sale.pkg_code || sale.pkgCode || null;
      const promCode = sale.promCode || null;
      const soSource = sale.order_source || sale.so_source || null;
      const paidByVoucher = Number(sale.paid_by_voucher_ecode_ecoin_bp ?? sale.chietKhauThanhToanVoucher ?? 0) || 0;
      const isVoucherDuPhongValue = isVoucherDuPhong(brand, soSource, promCode, pkgCode);

      if (chietKhauVoucherDp1 > 0 && !isVoucherDuPhongValue) {
        // Chuyển sang voucher chính
        const saleForVoucher = {
          ...sale,
          paid_by_voucher_ecode_ecoin_bp: chietKhauVoucherDp1,
          customer: order?.customer || sale.customer,
          brand: order?.customer?.brand || order?.brand || sale?.customer?.brand || sale?.brand,
          product: loyaltyProduct,
        };
        const maCk05Value = this.calculateMaCk05(saleForVoucher);
        thanhToanVoucherDisplay = this.formatVoucherCode(maCk05Value);
        chietKhauThanhToanVoucherDisplay = chietKhauVoucherDp1;
      } else if (isVoucherDuPhongValue && paidByVoucher > 0) {
        // Voucher dự phòng - không hiển thị
        thanhToanVoucherDisplay = null;
        chietKhauThanhToanVoucherDisplay = null;
      } else if (paidByVoucher > 0) {
        const saleForVoucher = {
          ...sale,
          paid_by_voucher_ecode_ecoin_bp: paidByVoucher,
          customer: order?.customer || sale.customer,
          brand: order?.customer?.brand || order?.brand || sale?.customer?.brand || sale?.brand,
          product: loyaltyProduct,
        };
        const maCk05Value = this.calculateMaCk05(saleForVoucher);
        thanhToanVoucherDisplay = this.formatVoucherCode(maCk05Value);
        chietKhauThanhToanVoucherDisplay = paidByVoucher;
      }
    }

    // voucherDp1Display và chietKhauVoucherDp1Display
    let voucherDp1Display: string | null = null;
    let chietKhauVoucherDp1Display: number | null = null;
    const pkgCode = sale.pkg_code || sale.pkgCode || null;
    const promCode = sale.promCode || null;
    const soSource = sale.order_source || sale.so_source || null;
    const paidByVoucher = sale.paid_by_voucher_ecode_ecoin_bp ?? 0;
    const chietKhauVoucherDp1Value = sale.chietKhauVoucherDp1 ?? 0;
    const isVoucherDuPhongValue = isVoucherDuPhong(brand, soSource, promCode, pkgCode);

    if (chietKhauVoucherDp1Value > 0 && !isVoucherDuPhongValue) {
      // Không hiển thị voucher dự phòng nếu không phải voucher dự phòng
      voucherDp1Display = null;
      chietKhauVoucherDp1Display = null;
    } else if (isVoucherDuPhongValue && (chietKhauVoucherDp1Value > 0 || paidByVoucher > 0)) {
      voucherDp1Display = 'VC CTKM SÀN';
      let chietKhauVoucherDp1Final = chietKhauVoucherDp1Value;
      if (chietKhauVoucherDp1Final === 0 && isVoucherDuPhongValue && paidByVoucher > 0) {
        chietKhauVoucherDp1Final = paidByVoucher;
      }
      chietKhauVoucherDp1Display = chietKhauVoucherDp1Final > 0 ? chietKhauVoucherDp1Final : null;
    }

    // thanhToanTkTienAoDisplay và chietKhauThanhToanTkTienAoDisplay
    let thanhToanTkTienAoDisplay: string | null = null;
    let chietKhauThanhToanTkTienAoDisplay: number | null = null;
    const chietKhauTkTienAo = sale.chietKhauThanhToanTkTienAo ?? 0;
    const vPaidForEcoin = sale.paid_by_voucher_ecode_ecoin_bp ?? 0;
    if (chietKhauTkTienAo > 0) {
      thanhToanTkTienAoDisplay = this.generateTkTienAoLabel(order);
      chietKhauThanhToanTkTienAoDisplay = chietKhauTkTienAo;
    } else if (vPaidForEcoin > 0 && order?.cashioData && Array.isArray(order.cashioData)) {
      const ecoinCashio = order.cashioData.find((c: any) => c.fop_syscode === 'ECOIN');
      if (ecoinCashio && ecoinCashio.total_in && parseFloat(String(ecoinCashio.total_in)) > 0) {
        thanhToanTkTienAoDisplay = this.generateTkTienAoLabel(order);
        chietKhauThanhToanTkTienAoDisplay = parseFloat(String(ecoinCashio.total_in)) || null;
      }
    }

    // soSerialDisplay
    let soSerialDisplay: string | null = null;
    const trackSerial = loyaltyProduct?.trackSerial === true;
    const trackBatch = loyaltyProduct?.trackBatch === true;
    const serial = sale.serial || '';
    if (serial && serial.indexOf('_') <= 0 && trackSerial && !trackBatch) {
      soSerialDisplay = serial || null;
    }

    // cucThueDisplay
    // Nếu sale.cucThue có giá trị, dùng nó
    // Nếu sale.cucThue là null, lấy từ department.ma_dvcs (tương tự cách lấy trong fast-api-invoice-flow.service.ts)
    // Nếu brand là 'f3' và không có giá trị nào, dùng 'FBV'
    let cucThueValue = sale.cucThue;
    if (!cucThueValue) {
      // Lấy ma_dvcs từ department làm fallback (ưu tiên ma_dvcs, sau đó ma_dvcs_ht)
      cucThueValue = department?.ma_dvcs || department?.ma_dvcs_ht || null;
    }
    const cucThueDisplay = cucThueValue;

    // tkDoanhThuDisplay và tkGiaVonDisplay
    const deptType = department?.type?.toLowerCase()?.trim();
    let tkDoanhThuDisplay = '-';
    let tkGiaVonDisplay = '-';
    if (deptType === 'bán lẻ') {
      tkDoanhThuDisplay = loyaltyProduct?.tkDoanhThuBanLe || '-';
      tkGiaVonDisplay = loyaltyProduct?.tkGiaVonBanLe || '-';
    } else if (deptType === 'bán buôn') {
      tkDoanhThuDisplay = loyaltyProduct?.tkDoanhThuBanBuon || '-';
      tkGiaVonDisplay = loyaltyProduct?.tkGiaVonBanBuon || '-';
    } else {
      tkDoanhThuDisplay = loyaltyProduct?.tkDoanhThuBanLe || loyaltyProduct?.tkDoanhThuBanBuon || '-';
      tkGiaVonDisplay = loyaltyProduct?.tkGiaVonBanLe || loyaltyProduct?.tkGiaVonBanBuon || '-';
    }

    return {
      thanhToanCouponDisplay,
      chietKhauThanhToanCouponDisplay,
      thanhToanVoucherDisplay,
      chietKhauThanhToanVoucherDisplay,
      voucherDp1Display,
      chietKhauVoucherDp1Display,
      thanhToanTkTienAoDisplay,
      chietKhauThanhToanTkTienAoDisplay,
      soSerialDisplay,
      cucThueDisplay,
      tkDoanhThuDisplay,
      tkGiaVonDisplay,
    };
  }

  /**
   * Format stock transfer cho frontend - đảm bảo materialCode được trả về
   * Frontend sẽ dùng materialCode để hiển thị "Mã SP" thay vì itemCode
   */
  private formatStockTransferForFrontend(st: StockTransfer): any {
    return {
      ...st,
      // Đảm bảo materialCode được trả về (đã được lưu trong DB khi sync)
      // Frontend sẽ dùng materialCode để hiển thị "Mã SP"
      materialCode: st.materialCode || null,
      // Giữ lại itemCode để backward compatibility
      itemCode: st.itemCode,
    };
  }

  private formatSaleForFrontend(
    sale: any,
    loyaltyProduct: any,
    department: any,
    calculatedFields: { maLo: string; maCtkmTangHang: string | null; muaHangCkVip: string; maKho: string | null; isTangHang: boolean; isDichVu: boolean; promCodeDisplay: string | null },
    order?: any
  ): any {
    // Kiểm tra đơn "03. Đổi điểm" trước khi tính toán giá
    const isDoiDiemForDisplay = this.isDoiDiemOrder(sale.ordertype, sale.ordertypeName);

    // Tính toán giaBan và tienHang
    // Nếu là đơn "03. Đổi điểm": set giaBan = 0 và tienHang = 0 (không tính lại)
    let tienHang = sale.tienHang || sale.linetotal || sale.revenue || 0;
    const qty = sale.qty || 0;
    let giaBan = sale.giaBan || 0;

    // Lưu giá bán gốc để kiểm tra điều kiện đặc biệt
    const giaBanGoc = giaBan;

    if (isDoiDiemForDisplay) {
      // Đơn "03. Đổi điểm": fix cứng giaBan = 0 và tienHang = 0
      giaBan = 0;
      tienHang = 0;
    } else {
      // Các đơn khác: tính giaBan nếu chưa có
      if (giaBan === 0 && tienHang != null && qty > 0) {
        giaBan = tienHang / qty;
      }
    }

    // Tính toán muaHangGiamGiaDisplay
    // Nếu là đơn "03. Đổi điểm": set muaHangGiamGiaDisplay = "TT DIEM DO"
    // Nếu không phải hàng tặng và không phải "03. Đổi điểm": dùng promCode
    const other_discamt = isDoiDiemForDisplay ? 0 : (sale.other_discamt ?? sale.chietKhauMuaHangGiamGia ?? 0);
    // KM VIP
    const isKmVip = sale.grade_discamt;
    // Lấy productType từ sale hoặc product
    const productType = sale.productType || null;
    const isTangSP = loyaltyProduct.productType;
    const productTypeUpper = productType ? String(productType).toUpperCase().trim() : null;
    // Kiểm tra "Chiết khấu mua hàng giảm giá" có giá trị không
    const hasChietKhauMuaHangGiamGia = other_discamt != null && other_discamt !== 0;

    let muaHangGiamGiaDisplay: string | null = null;
    let maCtkmTangHang = calculatedFields.maCtkmTangHang;
    // Nếu là đơn "03. Đổi điểm": set maCtkmTangHang và muaHangGiamGiaDisplay theo ma_dvcs
    if (isDoiDiemForDisplay) {
      if (department?.ma_dvcs === 'TTM' || department?.ma_dvcs === 'AMA' || department?.ma_dvcs === 'TSG') {
        maCtkmTangHang = 'TTM.KMDIEM';
      } else if (department?.ma_dvcs === 'FBV') {
        maCtkmTangHang = 'FBV.KMDIEM';
      } else if (department?.ma_dvcs === 'BTH') {
        maCtkmTangHang = 'BTH.KMDIEM';
      } else if (department?.ma_dvcs === 'CDV') {
        maCtkmTangHang = 'CDV.KMDIEM';
      } else if (department?.ma_dvcs === 'LHV') {
        maCtkmTangHang = 'LHV.KMDIEM';
      }
    } else if (!calculatedFields.isTangHang) {
      // Nếu không phải hàng tặng và không phải "03. Đổi điểm": dùng promCode
      muaHangGiamGiaDisplay = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
      if (muaHangGiamGiaDisplay && productTypeUpper === 'I') {
        muaHangGiamGiaDisplay = muaHangGiamGiaDisplay + '.I'
      } else if (muaHangGiamGiaDisplay && productTypeUpper === 'S') {
        muaHangGiamGiaDisplay = muaHangGiamGiaDisplay + '.S';
      } else if (muaHangGiamGiaDisplay && productTypeUpper === 'V') {
        muaHangGiamGiaDisplay = muaHangGiamGiaDisplay + '.V';
      }
    }

    // Tính toán các field display phức tạp
    const displayFields = this.calculateDisplayFields(sale, order || { customer: sale.customer, cashioData: sale.cashioData, cashioFopSyscode: sale.cashioFopSyscode, cashioTotalIn: sale.cashioTotalIn, brand: sale.brand }, loyaltyProduct, department);

    // Nếu là đơn "03. Đổi điểm": set other_discamt = 0 (chiết khấu mua hàng giảm giá)

    // Logic xử lý tkChietKhau, tkChiPhi, maPhi cho các đơn đặc biệt
    const ordertypeName = sale.ordertypeName || sale.ordertype || '';
    const isDoiVo = ordertypeName.toLowerCase().includes('đổi vỏ') || ordertypeName.toLowerCase().includes('doi vo');
    const isDoiDiem = isDoiDiemForDisplay || ordertypeName.toLowerCase().includes('đổi điểm') || ordertypeName.toLowerCase().includes('doi diem');
    const isDauTu = ordertypeName.includes('06. Đầu tư') || ordertypeName.includes('06.Đầu tư') || ordertypeName.toLowerCase().includes('đầu tư') || ordertypeName.toLowerCase().includes('dau tu');
    const isSinhNhat = ordertypeName.includes('05. Tặng sinh nhật') || ordertypeName.includes('05.Tặng sinh nhật') || ordertypeName.toLowerCase().includes('tặng sinh nhật') || ordertypeName.toLowerCase().includes('tang sinh nhat') || ordertypeName.toLowerCase().includes('sinh nhật') || ordertypeName.toLowerCase().includes('sinh nhat');
    const isThuong = ordertypeName.includes('01.Thường') || ordertypeName.includes('01. Thường') || ordertypeName.includes('01.Thường') || ordertypeName.toLowerCase().includes('thường') || ordertypeName.toLowerCase().includes('thuong');

    // Kiểm tra có mã CTKM không (promCode hoặc maCtkmTangHang)
    const hasMaCtkm = (sale.promCode && sale.promCode.trim() !== '') ||
      (maCtkmTangHang && maCtkmTangHang.trim() !== '') ||
      (sale.maCtkmTangHang && sale.maCtkmTangHang.trim() !== '');

    // Kiểm tra có mã CTKM tặng hàng không (chỉ maCtkmTangHang, không tính promCode)
    const hasMaCtkmTangHang = (maCtkmTangHang && maCtkmTangHang?.trim() !== '') ||
      (sale.maCtkmTangHang && sale.maCtkmTangHang.trim() !== '');

    // Kiểm tra giá bán = 0 (dùng giá bán gốc, trước khi tính lại)
    // Và kiểm tra khuyến mại = 1 (isTangHang = true)
    // Dùng Math.abs để tránh lỗi số thực
    const isGiaBanZero = Math.abs(giaBanGoc) < 0.01; // Dùng giá bán gốc

    // Kiểm tra có voucher không
    const hasVoucher = sale.paid_by_voucher_ecode_ecoin_bp;



    let tkChietKhau: string | null = null;
    let tkChiPhi: string | null = null;
    let maPhi: string | null = null;

    if (isDoiVo || isDoiDiem || isDauTu) {
      // Với đơn "Đổi vỏ", "Đổi điểm", "Đầu tư":
      tkChietKhau = null; // Để rỗng
      tkChiPhi = '64191';
      maPhi = '161010';
    } else if (isSinhNhat) {
      // Với đơn "Sinh nhật":
      tkChietKhau = null; // Để rỗng
      tkChiPhi = '64192';
      maPhi = '162010';
    } else if (isThuong && hasMaCtkmTangHang && isGiaBanZero && calculatedFields.isTangHang) {
      // Với đơn "Thường" có đơn giá = 0, Khuyến mại = 1, và có thông tin mã tại "Mã CTKM tặng hàng":
      tkChiPhi = '64191';
      maPhi = '161010';
      // tkChietKhau giữ nguyên (có thể được set bởi các điều kiện khác hoặc null)
      if (tkChietKhau === null) {
        tkChietKhau = sale.tkChietKhau || null;
      }
    } else if (isKmVip > 0 && productTypeUpper === 'I') {
      tkChietKhau = '521113';
    } else if (isKmVip > 0 && productTypeUpper === 'S') {
      tkChietKhau = '521132';
    }
    else if (hasVoucher > 0 && isTangSP === 'GIFT') {
      tkChietKhau = '5211631';
    }
    else if (hasVoucher > 0 && productTypeUpper === 'I') {
      tkChietKhau = '5211611';

    } else if (hasVoucher > 0 && productTypeUpper === 'S') {
      tkChietKhau = '5211621';
    }
    else if (hasChietKhauMuaHangGiamGia && productTypeUpper === 'S') {
      // Với đơn có "Chiết khấu mua hàng giảm giá" có giá trị và loại hàng hóa = S (Dịch vụ):
      tkChietKhau = '521131';
      tkChiPhi = sale.tkChiPhi || null;
      maPhi = sale.maPhi || null;
    } else if (isThuong && hasChietKhauMuaHangGiamGia && productTypeUpper === 'I') {
      // Với đơn "Thường" có giá trị tiền tại cột "Chiết khấu mua hàng giảm giá" và loại hàng hóa = I (Hàng hóa):
      tkChietKhau = '521111';
      tkChiPhi = sale.tkChiPhi || null;
      maPhi = sale.maPhi || null;
    } else if (isThuong && hasMaCtkm && !(hasMaCtkmTangHang && isGiaBanZero && calculatedFields.isTangHang)) {
      // Với đơn "Thường" có mã CTKM:
      // - Loại S (Dịch vụ): TK Chiết khấu = 521131
      // - Loại I (Hàng hóa): TK Chiết khấu = 521111
      if (productTypeUpper === 'S') {
        tkChietKhau = '521131';
      } else if (productTypeUpper === 'I') {
        tkChietKhau = '521111';
      } else {
        // Nếu không xác định được loại, mặc định là hàng hóa
        tkChietKhau = '521111';
      }
      tkChiPhi = sale.tkChiPhi || null;
      maPhi = sale.maPhi || null;
    } else {
      // Các đơn khác: lấy từ product hoặc sale nếu có
      tkChietKhau = loyaltyProduct?.tkChietKhau || sale.tkChietKhau || null;
      tkChiPhi = sale.tkChiPhi || null;
      maPhi = sale.maPhi || null;
    }

    if (sale.ordertypeName.includes('08. Tách thẻ')) {
      calculatedFields.maKho = 'B' + department?.ma_bp;
    }

    if (sale.ordertypeName.includes('03. Đổi điểm')) {
      calculatedFields.promCodeDisplay = '1';
    }

    return {
      ...sale,
      itemName: sale.itemName || loyaltyProduct?.name || null,
      maKho: calculatedFields.maKho,
      maCtkmTangHang: maCtkmTangHang,
      muaHangCkVip: calculatedFields.muaHangCkVip,
      maLo: calculatedFields.maLo,
      isTangHang: calculatedFields.isTangHang,
      isDichVu: calculatedFields.isDichVu,
      promCodeDisplay: calculatedFields.promCodeDisplay,
      muaHangGiamGiaDisplay: muaHangGiamGiaDisplay,
      other_discamt: other_discamt, // Set = 0 cho đơn "03. Đổi điểm"
      chietKhauMuaHangGiamGia: other_discamt, // Set = 0 cho đơn "03. Đổi điểm"
      giaBan: giaBan, // Đơn "03. Đổi điểm": = 0, các đơn khác: đã được tính toán
      tienHang: tienHang, // Đơn "03. Đổi điểm": = 0, các đơn khác: giữ nguyên từ sale
      linetotal: isDoiDiemForDisplay ? 0 : (sale.linetotal ?? tienHang), // Đơn "03. Đổi điểm": = 0
      promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
      // Đảm bảo ordertypeName được trả về (từ database hoặc từ ordertype nếu ordertypeName không có)
      ordertypeName: sale.ordertypeName || sale.ordertype || null,
      // issuePartnerCode cho đơn "08. Tách thẻ" (từ API get_card)
      issuePartnerCode: sale.issuePartnerCode || null,
      // partnerCode: Với đơn "08. Tách thẻ", ưu tiên issuePartnerCode
      partnerCode: (sale.ordertypeName || sale.ordertype || '').includes('08. Tách thẻ') && sale.issuePartnerCode
        ? sale.issuePartnerCode
        : sale.partnerCode || sale.partner_code || null,
      // Các field display từ calculateDisplayFields
      ...displayFields,
      // Ưu tiên productType từ Zappy API (I, S, V) trước Loyalty API (DIVU, VOUC, etc.)
      producttype: sale.productType || sale.producttype || loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
      productType: sale.productType || sale.producttype || loyaltyProduct?.productType || loyaltyProduct?.producttype || null,
      product: loyaltyProduct ? {
        ...loyaltyProduct,
        producttype: loyaltyProduct.producttype || loyaltyProduct.productType || null,
        productType: loyaltyProduct.productType || loyaltyProduct.producttype || null,
        dvt: loyaltyProduct.unit || null,
        maVatTu: loyaltyProduct.materialCode || sale.itemCode,
        trackInventory: loyaltyProduct.trackInventory ?? null,
        trackSerial: loyaltyProduct.trackSerial ?? null,
        trackBatch: loyaltyProduct.trackBatch ?? null,
      } : null,
      department: department,
      dvt: loyaltyProduct?.unit || sale.dvt || null,
      // Thêm các trường tkChietKhau, tkChiPhi, maPhi
      tkChietKhau: tkChietKhau,
      tkChiPhi: tkChiPhi,
      maPhi: maPhi,
    };
  }

  /**
   * Enrich orders với cashio data
   */
  private async enrichOrdersWithCashio(orders: any[]): Promise<any[]> {
    const docCodes = orders.map(o => o.docCode);
    if (docCodes.length === 0) return orders;

    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code IN (:...docCodes)', { docCodes })
      .orWhere('cashio.master_code IN (:...docCodes)', { docCodes })
      .getMany();

    const cashioMap = new Map<string, DailyCashio[]>();
    docCodes.forEach(docCode => {
      const matchingCashios = cashioRecords.filter(c =>
        c.so_code === docCode || c.master_code === docCode
      );
      if (matchingCashios.length > 0) {
        cashioMap.set(docCode, matchingCashios);
      }
    });

    // Fetch stock transfers để thêm thông tin stock transfer
    // Join theo soCode (của stock transfer) = docCode (của order)
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodes) },
    });

    const stockTransferMap = new Map<string, StockTransfer[]>();
    docCodes.forEach(docCode => {
      // Join theo soCode (của stock transfer) = docCode (của order)
      const matchingTransfers = stockTransfers.filter(st => st.soCode === docCode);
      if (matchingTransfers.length > 0) {
        stockTransferMap.set(docCode, matchingTransfers);
      }
    });

    return orders.map(order => {
      const cashioRecords = cashioMap.get(order.docCode) || [];
      const ecoinCashio = cashioRecords.find(c => c.fop_syscode === 'ECOIN');
      const voucherCashio = cashioRecords.find(c => c.fop_syscode === 'VOUCHER');
      const selectedCashio = ecoinCashio || voucherCashio || cashioRecords[0] || null;

      // Lấy thông tin stock transfer cho đơn hàng này
      const orderStockTransfers = stockTransferMap.get(order.docCode) || [];

      // Debug: Log tất cả stock transfers để kiểm tra
      if (orderStockTransfers.length > 0) {
        this.logger.debug(
          `[StockTransfer] Đơn hàng ${order.docCode}: Tìm thấy ${orderStockTransfers.length} records. ` +
          `Chi tiết: ${orderStockTransfers.map(st => `docCode=${st.docCode}, itemCode=${st.itemCode}, qty=${st.qty}, stockCode=${st.stockCode}`).join('; ')}`
        );
      }

      // Lọc chỉ lấy các stock transfer XUẤT KHO (SALE_STOCKOUT) với qty < 0
      // Bỏ qua các stock transfer nhập lại (RETURN) với qty > 0
      const stockOutTransfers = orderStockTransfers.filter((st) => {
        // Chỉ lấy các record có doctype = 'SALE_STOCKOUT' hoặc qty < 0 (xuất kho)
        const isStockOut = st.doctype === 'SALE_STOCKOUT' || Number(st.qty || 0) < 0;
        return isStockOut;
      });

      // Deduplicate stock transfers để tránh tính trùng
      // Group theo docCode + itemCode + stockCode + qty để đảm bảo chỉ tính một lần cho mỗi combination
      // (có thể có duplicate records trong database với id khác nhau nhưng cùng docCode, itemCode, stockCode, qty)
      const uniqueStockTransfersMap = new Map<string, StockTransfer>();
      stockOutTransfers.forEach((st) => {
        // Tạo key từ docCode + itemCode + stockCode + qty (giữ nguyên dấu âm để phân biệt ST và RT)
        // KHÔNG dùng Math.abs vì ST (qty=-11) và RT (qty=11) là 2 chứng từ khác nhau
        const qty = Number(st.qty || 0);
        const key = `${st.docCode || ''}_${st.itemCode || ''}_${st.stockCode || ''}_${qty}`;

        // Chỉ lưu nếu chưa có key này, hoặc nếu có thì giữ record có id (ưu tiên record có id)
        if (!uniqueStockTransfersMap.has(key)) {
          uniqueStockTransfersMap.set(key, st);
        } else {
          // Nếu đã có, chỉ thay thế nếu record hiện tại có id và record cũ không có id
          const existing = uniqueStockTransfersMap.get(key)!;
          if (st.id && !existing.id) {
            uniqueStockTransfersMap.set(key, st);
          }
        }
      });
      const uniqueStockTransfers = Array.from(uniqueStockTransfersMap.values());

      // Debug log nếu có duplicate hoặc có RT records bị loại bỏ
      if (orderStockTransfers.length > stockOutTransfers.length) {
        const returnCount = orderStockTransfers.length - stockOutTransfers.length;
        this.logger.debug(
          `[StockTransfer] Đơn hàng ${order.docCode}: Loại bỏ ${returnCount} records nhập lại (RETURN), chỉ tính ${stockOutTransfers.length} records xuất kho (ST)`
        );
      }
      if (stockOutTransfers.length > uniqueStockTransfers.length) {
        this.logger.warn(
          `[StockTransfer] Đơn hàng ${order.docCode}: ${stockOutTransfers.length} records xuất kho → ${uniqueStockTransfers.length} unique (đã loại bỏ ${stockOutTransfers.length - uniqueStockTransfers.length} duplicates)`
        );
      }

      // Tính tổng hợp thông tin stock transfer (chỉ tính từ unique records XUẤT KHO)
      // Lấy giá trị tuyệt đối của qty (vì qty xuất kho là số âm, nhưng số lượng xuất là số dương)
      const totalQty = uniqueStockTransfers.reduce((sum, st) => {
        const qty = Math.abs(Number(st.qty || 0));
        this.logger.debug(
          `[StockTransfer] Tính totalQty: docCode=${st.docCode}, qty=${st.qty}, abs(qty)=${qty}, sum=${sum} → ${sum + qty}`
        );
        return sum + qty;
      }, 0);

      this.logger.debug(
        `[StockTransfer] Đơn hàng ${order.docCode}: uniqueStockTransfers.length=${uniqueStockTransfers.length}, totalQty=${totalQty}`
      );

      const stockTransferSummary = {
        totalItems: uniqueStockTransfers.length, // Số dòng stock transfer xuất kho (sau khi deduplicate)
        totalQty: totalQty, // Tổng số lượng xuất kho (lấy giá trị tuyệt đối vì qty xuất kho là số âm)
        uniqueItems: new Set(uniqueStockTransfers.map(st => st.itemCode)).size, // Số sản phẩm khác nhau
        stockCodes: Array.from(new Set(uniqueStockTransfers.map(st => st.stockCode).filter(Boolean))), // Danh sách mã kho
        hasStockTransfer: uniqueStockTransfers.length > 0, // Có stock transfer xuất kho hay không
      };

      return {
        ...order,
        cashioData: cashioRecords.length > 0 ? cashioRecords : null,
        cashioFopSyscode: selectedCashio?.fop_syscode || null,
        cashioFopDescription: selectedCashio?.fop_description || null,
        cashioCode: selectedCashio?.code || null,
        cashioMasterCode: selectedCashio?.master_code || null,
        cashioTotalIn: selectedCashio?.total_in || null,
        cashioTotalOut: selectedCashio?.total_out || null,
        // Thông tin stock transfer
        stockTransferInfo: stockTransferSummary,
        stockTransfers: uniqueStockTransfers.length > 0
          ? uniqueStockTransfers.map(st => this.formatStockTransferForFrontend(st))
          : (order.stockTransfers || []).map((st: any) => this.formatStockTransferForFrontend(st)), // Format để trả về materialCode
      };
    });
  }

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(WarehouseProcessed)
    private warehouseProcessedRepository: Repository<WarehouseProcessed>,
    private invoiceService: InvoiceService,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private fastApiService: FastApiClientService,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private categoriesService: CategoriesService,
    private loyaltyService: LoyaltyService,
    private invoiceValidationService: InvoiceValidationService,
  ) { }

  /**
   * Lấy stock transfer theo id
   */
  async getStockTransferById(id: string): Promise<StockTransfer | null> {
    return await this.stockTransferRepository.findOne({
      where: { id },
    });
  }

  /**
   * Xử lý warehouse receipt/release/transfer từ stock transfer theo docCode
   */
  async processWarehouseFromStockTransferByDocCode(docCode: string): Promise<any> {
    // Lấy stock transfer đầu tiên theo docCode
    const stockTransfer = await this.stockTransferRepository.findOne({
      where: { docCode },
      order: { createdAt: 'ASC' },
    });

    if (!stockTransfer) {
      throw new NotFoundException(`Không tìm thấy stock transfer với docCode = "${docCode}"`);
    }

    return await this.processWarehouseFromStockTransfer(stockTransfer);
  }

  /**
   * Retry batch các warehouse processed failed theo date range
   */
  async retryWarehouseFailedByDateRange(dateFrom: string, dateTo: string): Promise<{
    success: boolean;
    message: string;
    totalProcessed: number;
    successCount: number;
    failedCount: number;
    errors: string[];
  }> {
    try {
      // Parse dates từ DDMMMYYYY
      const parseDate = (dateStr: string): Date => {
        const day = parseInt(dateStr.substring(0, 2));
        const monthStr = dateStr.substring(2, 5).toUpperCase();
        const year = parseInt(dateStr.substring(5, 9));
        const monthMap: Record<string, number> = {
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
        };
        const month = monthMap[monthStr] || 0;
        return new Date(year, month, day);
      };

      const fromDate = parseDate(dateFrom);
      const toDate = parseDate(dateTo);
      toDate.setHours(23, 59, 59, 999); // Set to end of day

      // Tìm tất cả warehouse processed failed trong khoảng thời gian
      const failedRecords = await this.warehouseProcessedRepository.find({
        where: {
          success: false,
          processedDate: Between(fromDate, toDate),
        },
        order: { processedDate: 'ASC' },
      });

      if (failedRecords.length === 0) {
        return {
          success: true,
          message: 'Không có record nào thất bại trong khoảng thời gian này',
          totalProcessed: 0,
          successCount: 0,
          failedCount: 0,
          errors: [],
        };
      }

      this.logger.log(`[Warehouse Batch Retry] Bắt đầu retry ${failedRecords.length} records từ ${dateFrom} đến ${dateTo}`);

      let successCount = 0;
      let failedCount = 0;
      const errors: string[] = [];

      // Retry từng record
      for (const record of failedRecords) {
        try {
          await this.processWarehouseFromStockTransferByDocCode(record.docCode);
          successCount++;
          this.logger.log(`[Warehouse Batch Retry] Retry thành công cho docCode: ${record.docCode}`);
        } catch (error: any) {
          failedCount++;
          const errorMsg = `docCode ${record.docCode}: ${error?.message || String(error)}`;
          errors.push(errorMsg);
          this.logger.error(`[Warehouse Batch Retry] Retry thất bại cho docCode: ${record.docCode} - ${errorMsg}`);
        }
      }

      const message = `Đã xử lý ${failedRecords.length} records: ${successCount} thành công, ${failedCount} thất bại`;

      return {
        success: failedCount === 0,
        message,
        totalProcessed: failedRecords.length,
        successCount,
        failedCount,
        errors: errors.slice(0, 10), // Chỉ trả về 10 lỗi đầu tiên
      };
    } catch (error: any) {
      this.logger.error(`[Warehouse Batch Retry] Lỗi khi retry batch: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Xử lý warehouse receipt/release/transfer từ stock transfer
   */
  async processWarehouseFromStockTransfer(stockTransfer: StockTransfer): Promise<any> {
    try {
      let result: any;
      let ioTypeForTracking: string;

      // Xử lý STOCK_TRANSFER với relatedStockCode
      if (stockTransfer.doctype === 'STOCK_TRANSFER' && stockTransfer.relatedStockCode) {
        // Lấy tất cả stock transfers cùng docCode
        const stockTransferList = await this.stockTransferRepository.find({
          where: { docCode: stockTransfer.docCode },
          order: { createdAt: 'ASC' },
        });

        // Gọi API warehouse transfer
        result = await this.fastApiInvoiceFlowService.processWarehouseTransferFromStockTransfers(stockTransferList);
        ioTypeForTracking = 'T'; // T = Transfer
      } else {
        // Xử lý STOCK_IO
        result = await this.fastApiInvoiceFlowService.processWarehouseFromStockTransfer(stockTransfer);
        ioTypeForTracking = stockTransfer.ioType;
      }

      // Kiểm tra result có status = 1 không để xác định success
      let isSuccess = false;
      let errorMessage: string | undefined = undefined;

      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        isSuccess = firstItem.status === 1;
        if (!isSuccess) {
          errorMessage = firstItem.message || 'Tạo phiếu warehouse thất bại';
        }
      } else if (result && typeof result === 'object' && result.status !== undefined) {
        isSuccess = result.status === 1;
        if (!isSuccess) {
          errorMessage = result.message || 'Tạo phiếu warehouse thất bại';
        }
      } else {
        // Nếu result không có format mong đợi, coi như thất bại
        isSuccess = false;
        errorMessage = 'Response không hợp lệ từ Fast API';
      }

      // Lưu vào bảng tracking với success đúng (upsert - update nếu đã tồn tại)
      try {
        // Tìm record đã tồn tại
        const existing = await this.warehouseProcessedRepository.findOne({
          where: { docCode: stockTransfer.docCode },
        });

        if (existing) {
          // Update record đã tồn tại
          // Nếu thành công, xóa errorMessage bằng cách update trực tiếp
          if (isSuccess) {
            await this.warehouseProcessedRepository.update(
              { docCode: stockTransfer.docCode },
              {
                ioType: ioTypeForTracking,
                processedDate: new Date(),
                result: JSON.stringify(result),
                success: isSuccess,
                errorMessage: null as any, // Set null để xóa errorMessage trong database
              }
            );
          } else {
            existing.ioType = ioTypeForTracking;
            existing.processedDate = new Date();
            existing.result = JSON.stringify(result);
            existing.success = isSuccess;
            existing.errorMessage = errorMessage;
            await this.warehouseProcessedRepository.save(existing);
          }
        } else {
          // Tạo mới nếu chưa tồn tại
          const warehouseProcessed = this.warehouseProcessedRepository.create({
            docCode: stockTransfer.docCode,
            ioType: ioTypeForTracking,
            processedDate: new Date(),
            result: JSON.stringify(result),
            success: isSuccess,
            ...(errorMessage && { errorMessage }),
          });
          await this.warehouseProcessedRepository.save(warehouseProcessed);
        }
        this.logger.log(`[Warehouse Manual] Đã lưu tracking cho docCode ${stockTransfer.docCode} với success = ${isSuccess}`);
      } catch (saveError: any) {
        this.logger.error(`[Warehouse Manual] Lỗi khi lưu tracking cho docCode ${stockTransfer.docCode}: ${saveError?.message || saveError}`);
        // Không throw error để không ảnh hưởng đến response chính
      }

      // Nếu không thành công, throw error để controller xử lý
      if (!isSuccess) {
        throw new BadRequestException(errorMessage || 'Tạo phiếu warehouse thất bại');
      }

      return result;
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      // Lấy result từ error response nếu có (để lưu vào database cho người dùng xem)
      let errorResult: any = null;
      if (error?.response?.data) {
        errorResult = error.response.data;
      } else if (error?.data) {
        errorResult = error.data;
      }

      // Lưu vào bảng tracking với success = false (upsert - update nếu đã tồn tại)
      try {
        const ioTypeForTracking = stockTransfer.doctype === 'STOCK_TRANSFER' ? 'T' : stockTransfer.ioType;

        // Tìm record đã tồn tại
        const existing = await this.warehouseProcessedRepository.findOne({
          where: { docCode: stockTransfer.docCode },
        });

        if (existing) {
          // Update record đã tồn tại
          existing.ioType = ioTypeForTracking;
          existing.processedDate = new Date();
          existing.errorMessage = errorMessage;
          existing.success = false;
          // Lưu result từ error response nếu có, nếu không thì giữ result cũ (nếu có)
          if (errorResult) {
            existing.result = JSON.stringify(errorResult);
          }
          // Nếu không có errorResult và existing cũng không có result, giữ null
          // Nếu existing đã có result, giữ nguyên
          await this.warehouseProcessedRepository.save(existing);
        } else {
          // Tạo mới nếu chưa tồn tại
          const warehouseProcessed = this.warehouseProcessedRepository.create({
            docCode: stockTransfer.docCode,
            ioType: ioTypeForTracking,
            processedDate: new Date(),
            errorMessage,
            success: false,
            // Lưu result từ error response nếu có
            ...(errorResult && { result: JSON.stringify(errorResult) }),
          });
          await this.warehouseProcessedRepository.save(warehouseProcessed);
        }
        this.logger.log(`[Warehouse Manual] Đã lưu tracking thất bại cho docCode ${stockTransfer.docCode}`);
      } catch (saveError: any) {
        this.logger.error(`[Warehouse Manual] Lỗi khi lưu tracking cho docCode ${stockTransfer.docCode}: ${saveError?.message || saveError}`);
      }

      // Throw error để controller xử lý
      throw error;
    }
  }

  async findAll(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
  }) {
    const { brand, isProcessed, page = 1, limit = 50 } = options;

    const query = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer')
      .orderBy('sale.docDate', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (brand) {
      query.andWhere('customer.brand = :brand', { brand });
    }

    if (isProcessed !== undefined) {
      query.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }

    const [data, total] = await query.getManyAndCount();

    // Thêm promotionDisplayCode vào mỗi sale
    const enrichedData = data.map((sale) => ({
      ...sale,
      promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
    }));

    return {
      data: enrichedData,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findAllOrders(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
    date?: string; // Format: DDMMMYYYY (ví dụ: 04DEC2025)
    dateFrom?: string; // Format: YYYY-MM-DD hoặc ISO string
    dateTo?: string; // Format: YYYY-MM-DD hoặc ISO string
    search?: string; // Search query để tìm theo docCode, customer name, code, mobile
    statusAsys?: boolean; // Filter theo statusAsys (true/false)
    export?: boolean; // Nếu true, trả về sales items riêng lẻ (không group, không paginate) để export Excel
  }) {
    const { brand, isProcessed, page = 1, limit = 50, date, dateFrom, dateTo, search, statusAsys, export: isExport } = options;

    // Nếu có search query, luôn search trong database (không dùng Zappy API)
    // Vì Zappy API chỉ lấy được một ngày, không hỗ trợ date range
    // Nếu có brand filter, luôn dùng database query (không dùng Zappy API) để tránh fetch tất cả products
    // Nếu có date parameter và không có search và không có brand, lấy dữ liệu từ Zappy API
    if (date && !search && !brand) {
      try {
        const orders = await this.zappyApiService.getDailySales(date);

        // Filter by brand nếu có
        let filteredOrders = orders;
        if (brand) {
          filteredOrders = orders.filter(
            (order) => order.customer.brand?.toLowerCase() === brand.toLowerCase()
          );
        }

        // Filter by search query nếu có
        if (search && search.trim() !== '') {
          const searchLower = search.trim().toLowerCase();
          filteredOrders = filteredOrders.filter(
            (order) =>
              order.docCode.toLowerCase().includes(searchLower) ||
              (order.customer?.name && order.customer.name.toLowerCase().includes(searchLower)) ||
              (order.customer?.code && order.customer.code.toLowerCase().includes(searchLower)) ||
              (order.customer?.mobile && order.customer.mobile.toLowerCase().includes(searchLower))
          );
        }

        // Phân trang TRƯỚC khi fetch products/departments để tránh fetch quá nhiều
        // Tính tổng số rows (sale items) từ tất cả orders
        let totalRows = 0;
        filteredOrders.forEach(order => {
          totalRows += (order.sales && order.sales.length > 0) ? order.sales.length : (order.totalItems > 0 ? order.totalItems : 1);
        });

        const paginationStartIndex = (page - 1) * limit;
        const paginationEndIndex = paginationStartIndex + limit;

        // Paginate orders dựa trên số rows
        let currentRowCount = 0;
        const paginatedOrders: typeof filteredOrders = [];

        for (const order of filteredOrders) {
          const orderRowCount = (order.sales && order.sales.length > 0) ? order.sales.length : (order.totalItems > 0 ? order.totalItems : 1);
          const orderStartRow = currentRowCount;
          const orderEndRow = currentRowCount + orderRowCount;

          // Nếu order này có overlap với phạm vi pagination, thêm vào
          if (orderEndRow > paginationStartIndex && orderStartRow < paginationEndIndex) {
            paginatedOrders.push(order);
          }

          currentRowCount += orderRowCount;

          // Nếu đã vượt quá phạm vi pagination, dừng lại
          if (currentRowCount >= paginationEndIndex) {
            break;
          }
        }

        // Fetch departments để tính maKho - CHỈ cho paginated orders
        const branchCodes = Array.from(
          new Set(
            paginatedOrders
              .flatMap((order) => order.sales || [])
              .map((sale) => sale.branchCode)
              .filter((code): code is string => !!code && code.trim() !== '')
          )
        );

        const departmentMap = new Map<string, any>();
        // Fetch parallel thay vì sequential
        if (branchCodes.length > 0) {
          const departmentPromises = branchCodes.map(async (branchCode) => {
            try {
              const response = await this.httpService.axiosRef.get(
                `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
                { headers: { accept: 'application/json' } },
              );
              const department = response?.data?.data?.items?.[0];
              return { branchCode, department };
            } catch (error) {
              this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
              return { branchCode, department: null };
            }
          });

          const departmentResults = await Promise.all(departmentPromises);
          departmentResults.forEach(({ branchCode, department }) => {
            if (department) {
              departmentMap.set(branchCode, department);
            }
          });
        }

        // Fetch products từ Loyalty API để lấy producttype - CHỈ cho paginated orders
        // BỎ QUA các sale có statusAsys = false (đơn lỗi) - không fetch từ Loyalty API
        const itemCodes = Array.from(
          new Set(
            paginatedOrders
              .flatMap((order) => order.sales || [])
              .filter((sale) => sale.statusAsys !== false) // Bỏ qua đơn lỗi
              .map((sale) => sale.itemCode)
              .filter((code): code is string => !!code && code.trim() !== '')
          )
        );

        // Fetch stock transfers theo soCode (mã đơn hàng) từ paginated orders
        // Join theo soCode (của stock transfer) = docCode (của order)
        // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
        const paginatedDocCodes = Array.from(new Set(paginatedOrders.map(order => order.docCode).filter(Boolean)));
        const docCodesForStockTransfer = this.getDocCodesForStockTransfer(paginatedDocCodes);
        const stockTransfers = docCodesForStockTransfer.length > 0
          ? await this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          })
          : [];

        // Fetch products từ Loyalty API sử dụng LoyaltyService (cho cả sales và stock transfers)
        const stockTransferItemCodes = Array.from(
          new Set(
            stockTransfers
              .map((st) => st.itemCode)
              .filter((code): code is string => !!code && code.trim() !== '')
          )
        );
        const allItemCodes = Array.from(new Set([...itemCodes, ...stockTransferItemCodes]));
        const loyaltyProductMap = await this.loyaltyService.fetchProducts(allItemCodes);

        // Build stock transfer maps (tổng hợp logic vào một chỗ)
        const { stockTransferMap, stockTransferByDocCodeMap } = this.buildStockTransferMaps(
          stockTransfers,
          loyaltyProductMap,
          paginatedDocCodes
        );

        // Thêm promotionDisplayCode, maKho, maCtkmTangHang, producttype và stock transfers vào các sales items
        const enrichedOrders = await Promise.all(paginatedOrders.map(async (order) => ({
          ...order,
          stockTransfers: (stockTransferByDocCodeMap.get(order.docCode) || []).map(st => this.formatStockTransferForFrontend(st)),
          sales: await Promise.all((order.sales || []).map(async (sale) => {
            const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
            const maBp = department?.ma_bp || sale.branchCode || null;
            const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;

            // Tính toán maCtkmTangHang: nếu là hàng tặng (giaBan = 0 và tienHang = 0 và revenue = 0)
            const tienHang = sale.tienHang || sale.linetotal || sale.revenue || 0;
            const qty = sale.qty || 0;
            let giaBan = sale.giaBan || 0;
            if (giaBan === 0 && tienHang != null && qty > 0) {
              giaBan = tienHang / qty;
            }
            const revenue = sale.revenue || 0;
            const isTangHang = giaBan === 0 && tienHang === 0 && revenue === 0;

            // Quy tắc: Nếu ordertype_name = "06. Đầu tư" và là hàng tặng → ma_ctkm_th = "TT DAU TU"
            let maCtkmTangHang: string | null = null;
            if (isTangHang) {
              const ordertypeName = sale.ordertype || '';
              if (ordertypeName.includes('06. Đầu tư') || ordertypeName.includes('06.Đầu tư')) {
                maCtkmTangHang = 'TT DAU TU';
              } else if (
                (ordertypeName.includes('01.Thường') || ordertypeName.includes('01. Thường')) ||
                (ordertypeName.includes('07. Bán tài khoản') || ordertypeName.includes('07.Bán tài khoản')) ||
                (ordertypeName.includes('9. Sàn TMDT') || ordertypeName.includes('9.Sàn TMDT'))
              ) {
                // Quy đổi prom_code sang TANGSP - lấy năm/tháng từ ngày đơn hàng
                const docDate = order.docDate || sale.docDate || sale.docdate;
                // Dùng promCode trực tiếp thay vì convertPromCodeToTangSp
                maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
              } else {
                // Các trường hợp khác: dùng promCode nếu có
                maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
              }
            }

            // Lấy stock transfers cho sale này (theo docCode + materialCode hoặc itemCode)
            // Match theo: soCode (Mã ĐH) = docCode (Số hóa đơn) VÀ materialCode (Mã hàng) hoặc itemCode
            const saleMaterialCode = loyaltyProduct?.materialCode;
            let saleStockTransfers: StockTransfer[] = [];
            if (saleMaterialCode) {
              const stockTransferKey = `${order.docCode}_${saleMaterialCode}`;
              saleStockTransfers = stockTransferMap.get(stockTransferKey) || [];
            }
            // Nếu không match được theo materialCode, match trực tiếp theo itemCode
            if (saleStockTransfers.length === 0 && sale.itemCode) {
              saleStockTransfers = stockTransfers.filter(
                (st) => st.soCode === order.docCode && st.itemCode === sale.itemCode
              );
            }

            // Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
            const maKho = await this.getMaKhoFromStockTransfer(sale, order.docCode, stockTransfers, saleMaterialCode, stockTransferMap);

            // Logic xử lý tkChietKhau, tkChiPhi, maPhi cho các đơn đặc biệt
            const ordertypeName = sale.ordertype || sale.ordertypeName || '';
            const isDoiVo = ordertypeName.toLowerCase().includes('đổi vỏ') || ordertypeName.toLowerCase().includes('doi vo');
            const isDoiDiem = ordertypeName.toLowerCase().includes('đổi điểm') || ordertypeName.toLowerCase().includes('doi diem');
            const isDauTu = ordertypeName.includes('06. Đầu tư') || ordertypeName.includes('06.Đầu tư') || ordertypeName.toLowerCase().includes('đầu tư') || ordertypeName.toLowerCase().includes('dau tu');
            const isSinhNhat = ordertypeName.includes('05. Tặng sinh nhật') || ordertypeName.includes('05.Tặng sinh nhật') || ordertypeName.toLowerCase().includes('tặng sinh nhật') || ordertypeName.toLowerCase().includes('tang sinh nhat') || ordertypeName.toLowerCase().includes('sinh nhật') || ordertypeName.toLowerCase().includes('sinh nhat');
            const isThuong = ordertypeName.includes('01.Thường') || ordertypeName.includes('01. Thường') || ordertypeName.includes('01.Thường') || ordertypeName.toLowerCase().includes('thường') || ordertypeName.toLowerCase().includes('thuong');

            // Kiểm tra có mã CTKM không (promCode hoặc maCtkmTangHang)
            const hasMaCtkm = (sale.promCode && sale.promCode.trim() !== '') ||
              (maCtkmTangHang && maCtkmTangHang.trim() !== '') ||
              (sale.maCtkmTangHang && sale.maCtkmTangHang.trim() !== '');

            // Kiểm tra có mã CTKM tặng hàng không (chỉ maCtkmTangHang, không tính promCode)
            const hasMaCtkmTangHang = (maCtkmTangHang && maCtkmTangHang.trim() !== '') ||
              (sale.maCtkmTangHang && sale.maCtkmTangHang.trim() !== '');

            // Kiểm tra giá bán = 0 (sau khi tính toán)
            const isGiaBanZero = giaBan === 0;

            // Lấy productType từ sale hoặc product
            const productType = sale.productType || sale.producttype || loyaltyProduct?.productType || loyaltyProduct?.producttype || null;
            const productTypeUpper = productType ? String(productType).toUpperCase().trim() : null;

            // Kiểm tra "Chiết khấu mua hàng giảm giá" có giá trị không
            const other_discamt = sale.other_discamt ?? sale.chietKhauMuaHangGiamGia ?? 0;
            const hasChietKhauMuaHangGiamGia = other_discamt != null && other_discamt !== 0;

            let tkChietKhau: string | null = null;
            let tkChiPhi: string | null = null;
            let maPhi: string | null = null;

            if (isDoiVo || isDoiDiem || isDauTu) {
              // Với đơn "Đổi vỏ", "Đổi điểm", "Đầu tư":
              tkChietKhau = null; // Để rỗng
              tkChiPhi = '64191';
              maPhi = '161010';
            } else if (isSinhNhat) {
              // Với đơn "Sinh nhật":
              tkChietKhau = null; // Để rỗng
              tkChiPhi = '64192';
              maPhi = '162010';
            } else if (isThuong && hasMaCtkmTangHang && isGiaBanZero && isTangHang) {
              // Với đơn "Thường" có đơn giá = 0, Khuyến mại = 1, và có thông tin mã tại "Mã CTKM tặng hàng":
              tkChiPhi = '64191';
              maPhi = '161010';
              // tkChietKhau giữ nguyên (có thể được set bởi các điều kiện khác hoặc null)
              if (tkChietKhau === null) {
                tkChietKhau = sale.tkChietKhau || null;
              }
            } else if (hasChietKhauMuaHangGiamGia && productTypeUpper === 'S') {
              // Với đơn có "Chiết khấu mua hàng giảm giá" có giá trị và loại hàng hóa = S (Dịch vụ):
              tkChietKhau = '521131';
              tkChiPhi = sale.tkChiPhi || null;
              maPhi = sale.maPhi || null;
            } else if (isThuong && hasChietKhauMuaHangGiamGia && productTypeUpper === 'I') {
              // Với đơn "Thường" có giá trị tiền tại cột "Chiết khấu mua hàng giảm giá" và loại hàng hóa = I (Hàng hóa):
              tkChietKhau = '521111';
              tkChiPhi = sale.tkChiPhi || null;
              maPhi = sale.maPhi || null;
            } else if (isThuong && hasMaCtkm && !(hasMaCtkmTangHang && isGiaBanZero && isTangHang)) {
              // Với đơn "Thường" có mã CTKM:
              // - Loại S (Dịch vụ): TK Chiết khấu = 521131
              // - Loại I (Hàng hóa): TK Chiết khấu = 521111
              if (productTypeUpper === 'S') {
                tkChietKhau = '521131';
              } else if (productTypeUpper === 'I') {
                tkChietKhau = '521111';
              } else {
                // Nếu không xác định được loại, mặc định là hàng hóa
                tkChietKhau = '521111';
              }
              tkChiPhi = sale.tkChiPhi || null;
              maPhi = sale.maPhi || null;
            } else {
              // Các đơn khác: lấy từ product hoặc sale nếu có
              tkChietKhau = loyaltyProduct?.tkChietKhau || sale.tkChietKhau || null;
              tkChiPhi = sale.tkChiPhi || null;
              maPhi = sale.maPhi || null;
            }

            return {
              ...sale,
              promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
              department: department,
              maKho: maKho,
              maCtkmTangHang: maCtkmTangHang,
              // Lấy producttype từ Loyalty API (không còn trong database)
              producttype: loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
              product: loyaltyProduct ? {
                ...loyaltyProduct,
                producttype: loyaltyProduct.producttype || loyaltyProduct.productType || null,
                // Đảm bảo productType từ Loyalty API được giữ lại
                productType: loyaltyProduct.productType || loyaltyProduct.producttype || null,
              } : (sale.product || null),
              stockTransfers: saleStockTransfers.map(st => this.formatStockTransferForFrontend(st)),
              // Thêm các trường tkChietKhau, tkChiPhi, maPhi
              tkChietKhau: tkChietKhau,
              tkChiPhi: tkChiPhi,
              maPhi: maPhi,
            };
          })) || [],
        })));

        return {
          data: enrichedOrders,
          total: totalRows, // Tổng số rows (sale items)
          page,
          limit,
          totalPages: Math.ceil(totalRows / limit),
        };
      } catch (error: any) {
        this.logger.error(`Error fetching orders from Zappy API: ${error?.message || error}`);
        // Fallback to database if Zappy API fails
      }
    }


    // Đếm tổng số sale items trước (để có total cho pagination)
    const countQuery = this.saleRepository
      .createQueryBuilder('sale')
      .select('COUNT(sale.id)', 'count');

    if (isProcessed !== undefined) {
      countQuery.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }

    // Thêm filter statusAsys vào count query
    if (statusAsys !== undefined) {
      countQuery.andWhere('sale.statusAsys = :statusAsys', { statusAsys });
    }

    // Luôn join với customer để có thể search hoặc export
    countQuery.leftJoin('sale.customer', 'customer');

    if (brand) {
      countQuery.andWhere('customer.brand = :brand', { brand });
    }

    // Thêm search query filter
    if (search && search.trim() !== '') {
      const searchPattern = `%${search.trim().toLowerCase()}%`;
      countQuery.andWhere(
        '(LOWER(sale.docCode) LIKE :search OR LOWER(customer.name) LIKE :search OR LOWER(customer.code) LIKE :search OR LOWER(customer.mobile) LIKE :search)',
        { search: searchPattern }
      );
    }

    // Date filter cho count query - PHẢI XỬ LÝ TRƯỚC KHI THỰC THI COUNT QUERY
    // Brand -> limit (30 ngày) nếu chưa có ngày
    // Brand -> date -> limit nếu có ngày
    let countHasDateFilter = false;
    if (dateFrom || dateTo) {
      countHasDateFilter = true;
      // Date range filter
      if (dateFrom && dateTo) {
        const startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        countQuery.andWhere('sale.docDate >= :dateFrom AND sale.docDate <= :dateTo', {
          dateFrom: startDate,
          dateTo: endDate,
        });
      } else if (dateFrom) {
        const startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        countQuery.andWhere('sale.docDate >= :dateFrom', { dateFrom: startDate });
      } else if (dateTo) {
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        countQuery.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }
    } else if (date) {
      countHasDateFilter = true;
      // Single date filter (format: DDMMMYYYY)
      const dateMatch = date.match(/^(\d{2})([A-Z]{3})(\d{4})$/i);
      if (dateMatch) {
        const [, day, monthStr, year] = dateMatch;
        const monthMap: { [key: string]: number } = {
          JAN: 0, FEB: 1, MAR: 2, APR: 3, MAY: 4, JUN: 5,
          JUL: 6, AUG: 7, SEP: 8, OCT: 9, NOV: 10, DEC: 11,
        };
        const month = monthMap[monthStr.toUpperCase()];
        if (month !== undefined) {
          const dateObj = new Date(parseInt(year), month, parseInt(day));
          const startOfDay = new Date(dateObj);
          startOfDay.setHours(0, 0, 0, 0);
          const endOfDay = new Date(dateObj);
          endOfDay.setHours(23, 59, 59, 999);
          countQuery.andWhere('sale.docDate >= :dateFrom AND sale.docDate <= :dateTo', {
            dateFrom: startOfDay,
            dateTo: endOfDay,
          });
        }
      }
    } else if (brand && !countHasDateFilter) {
      // Brand -> limit (30 ngày) nếu chưa có ngày
      const endDate = new Date();
      endDate.setHours(23, 59, 59, 999);
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - 30);
      startDate.setHours(0, 0, 0, 0);
      countQuery.andWhere('sale.docDate >= :dateFrom AND sale.docDate <= :dateTo', {
        dateFrom: startDate,
        dateTo: endDate,
      });
    }

    const totalResult = await countQuery.getRawOne();
    const totalSaleItems =

      parseInt(totalResult?.count || '0', 10);

    // Dùng getMany() để lấy đầy đủ entity data với customer relation
    const fullQuery = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer')
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.docCode', 'ASC')
      .addOrderBy('sale.id', 'ASC');

    // Apply filters
    if (isProcessed !== undefined) {
      fullQuery.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }
    if (statusAsys !== undefined) {
      fullQuery.andWhere('sale.statusAsys = :statusAsys', { statusAsys });
    }
    if (brand) {
      fullQuery.andWhere('customer.brand = :brand', { brand });
    }
    if (search && search.trim() !== '') {
      const searchPattern = `%${search.trim().toLowerCase()}%`;
      fullQuery.andWhere(
        '(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, \'\')) LIKE :search OR LOWER(COALESCE(customer.code, \'\')) LIKE :search OR LOWER(COALESCE(customer.mobile, \'\')) LIKE :search)',
        { search: searchPattern }
      );
    }

    // Apply date filters
    let hasDateFilter = false;
    if (dateFrom || dateTo) {
      hasDateFilter = true;
      if (dateFrom && dateTo) {
        const startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        fullQuery.andWhere('sale.docDate >= :dateFrom AND sale.docDate <= :dateTo', {
          dateFrom: startDate,
          dateTo: endDate,
        });
      } else if (dateFrom) {
        const startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        fullQuery.andWhere('sale.docDate >= :dateFrom', { dateFrom: startDate });
      } else if (dateTo) {
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        fullQuery.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }
    } else if (date) {
      hasDateFilter = true;
      const dateMatch = date.match(/^(\d{2})([A-Z]{3})(\d{4})$/i);
      if (dateMatch) {
        const [, day, monthStr, year] = dateMatch;
        const monthMap: { [key: string]: number } = {
          JAN: 0, FEB: 1, MAR: 2, APR: 3, MAY: 4, JUN: 5,
          JUL: 6, AUG: 7, SEP: 8, OCT: 9, NOV: 10, DEC: 11,
        };
        const month = monthMap[monthStr.toUpperCase()];
        if (month !== undefined) {
          const dateObj = new Date(parseInt(year), month, parseInt(day));
          const startOfDay = new Date(dateObj);
          startOfDay.setHours(0, 0, 0, 0);
          const endOfDay = new Date(dateObj);
          endOfDay.setHours(23, 59, 59, 999);
          fullQuery.andWhere('sale.docDate >= :dateFrom AND sale.docDate <= :dateTo', {
            dateFrom: startOfDay,
            dateTo: endOfDay,
          });
        }
      }
    } else if (brand && !hasDateFilter) {
      const endDate = new Date();
      endDate.setHours(23, 59, 59, 999);
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - 30);
      startDate.setHours(0, 0, 0, 0);
      fullQuery.andWhere('sale.docDate >= :dateFrom AND sale.docDate <= :dateTo', {
        dateFrom: startDate,
        dateTo: endDate,
      });
    }

    // Nếu export mode, không paginate, lấy tất cả
    // Nếu không phải export mode, phân trang bình thường
    if (!isExport) {
      const offset = (page - 1) * limit;
      fullQuery.skip(offset).take(limit);
    }

    const allSales = await fullQuery.getMany();


    // Nếu export mode, trả về sales items riêng lẻ (không group)
    if (isExport) {
      // Trả về đầy đủ entity data với customer relation
      const salesWithCustomer = allSales.map((sale) => {
        return {
          ...sale, // Trả về tất cả fields từ entity
          customer: sale.customer ? {
            code: sale.customer.code || sale.partnerCode || null,
            brand: sale.customer.brand || null,
            name: sale.customer.name || null,
            mobile: sale.customer.mobile || null,
          } : (sale.partnerCode ? {
            code: sale.partnerCode || null,
            brand: null,
            name: null,
            mobile: null,
          } : null),
        };
      });

      return {
        sales: salesWithCustomer,
        total: totalSaleItems,
      };
    }

    // Gộp theo docCode và lưu full sales data
    const orderMap = new Map<string, {
      docCode: string;
      docDate: Date;
      branchCode: string;
      docSourceType: string;
      customer: {
        code: string | null;
        brand?: string | null;
        name?: string | null;
        mobile?: string | null;
      } | null;
      totalRevenue: number;
      totalQty: number;
      totalItems: number;
      isProcessed: boolean;
      sales: any[];
      stockTransfers?: StockTransfer[];
    }>();

    // Đã đếm totalSaleItems từ count query ở trên
    // Lưu full sales data để enrich sau
    const allSalesData: any[] = [];
    for (const sale of allSales) {
      const docCode = sale.docCode;

      if (!orderMap.has(docCode)) {
        orderMap.set(docCode, {
          docCode: sale.docCode,
          docDate: sale.docDate,
          branchCode: sale.branchCode,
          docSourceType: sale.docSourceType,
          customer: sale.customer ? {
            code: sale.customer.code || sale.partnerCode || null,
            brand: sale.customer.brand || null,
            name: sale.customer.name || null,
            mobile: sale.customer.mobile || null,
          } : (sale.partnerCode ? {
            code: sale.partnerCode || null,
            brand: null,
            name: null,
            mobile: null,
          } : null),
          totalRevenue: 0,
          totalQty: 0,
          totalItems: 0,
          isProcessed: sale.isProcessed,
          sales: [],
        });
      }

      const order = orderMap.get(docCode)!;
      order.totalRevenue += Number(sale.revenue || 0);
      order.totalQty += Number(sale.qty || 0);
      order.totalItems += 1;

      // Nếu có ít nhất 1 sale chưa xử lý thì đơn hàng chưa xử lý
      if (!sale.isProcessed) {
        order.isProcessed = false;
      }

      // Lưu sale data để enrich sau
      allSalesData.push(sale);
    }

    // Fetch products và departments từ Loyalty API
    const itemCodes = Array.from(
      new Set(
        allSalesData
          .filter((sale) => sale.statusAsys !== false)
          .map((sale) => sale.itemCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );

    const branchCodes = Array.from(
      new Set(
        allSalesData
          .map((sale) => sale.branchCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );

    // Fetch stock transfers theo soCode (mã đơn hàng) - stock transfer có soCode = docCode của order
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodes = Array.from(new Set(allSalesData.map(sale => sale.docCode).filter(Boolean)));
    const docCodesForStockTransfer = this.getDocCodesForStockTransfer(docCodes);
    const stockTransfers = docCodesForStockTransfer.length > 0
      ? await this.stockTransferRepository.find({
        where: { soCode: In(docCodesForStockTransfer) },
      })
      : [];

    // Lấy itemCodes từ stock transfers để fetch materialCode từ Loyalty API
    const stockTransferItemCodes = Array.from(
      new Set(
        stockTransfers
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );
    const allItemCodes = Array.from(new Set([...itemCodes, ...stockTransferItemCodes]));

    // Fetch products và departments từ Loyalty API (cho cả sales và stock transfers)
    const [loyaltyProductMap, departmentMap] = await Promise.all([
      this.fetchLoyaltyProducts(allItemCodes),
      this.fetchLoyaltyDepartments(branchCodes),
    ]);

    // Build stock transfer maps (tổng hợp logic vào một chỗ)
    const { stockTransferMap, stockTransferByDocCodeMap } = this.buildStockTransferMaps(
      stockTransfers,
      loyaltyProductMap,
      docCodes
    );

    // Enrich sales với products, departments, stock transfers và tính toán các field phức tạp
    const enrichedSalesMap = new Map<string, any[]>();
    for (const sale of allSalesData) {
      const docCode = sale.docCode;
      if (!enrichedSalesMap.has(docCode)) {
        enrichedSalesMap.set(docCode, []);
      }

      const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
      const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;

      // Lấy stock transfers cho sale này (theo docCode + materialCode hoặc itemCode)
      // Match theo: soCode (Mã ĐH) = docCode (Số hóa đơn) VÀ materialCode (Mã hàng) hoặc itemCode
      const saleMaterialCode = loyaltyProduct?.materialCode;
      let saleStockTransfers: StockTransfer[] = [];
      if (saleMaterialCode) {
        const stockTransferKey = `${docCode}_${saleMaterialCode}`;
        saleStockTransfers = stockTransferMap.get(stockTransferKey) || [];
      }
      // Nếu không match được theo materialCode, match trực tiếp theo itemCode
      if (saleStockTransfers.length === 0 && sale.itemCode) {
        saleStockTransfers = stockTransfers.filter(
          (st) => st.soCode === docCode && st.itemCode === sale.itemCode
        );
      }

      // Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
      const maKhoFromStockTransfer = await this.getMaKhoFromStockTransfer(sale, docCode, stockTransfers, saleMaterialCode, stockTransferMap);

      const calculatedFields = this.calculateSaleFields(sale, loyaltyProduct, department, sale.branchCode);
      // Override maKho từ calculatedFields bằng maKho từ stock transfer
      calculatedFields.maKho = maKhoFromStockTransfer;

      const order = orderMap.get(sale.docCode);
      const enrichedSale = this.formatSaleForFrontend(sale, loyaltyProduct, department, calculatedFields, order);

      // Thêm stock transfers vào sale
      enrichedSale.stockTransfers = saleStockTransfers.map(st => this.formatStockTransferForFrontend(st));

      enrichedSalesMap.get(docCode)!.push(enrichedSale);
    }

    // Gắn enriched sales vào orders và thêm stock transfers tổng hợp cho order
    // Đồng thời gọi API get_card cho đơn "08. Tách thẻ" để lấy issue_partner_code
    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const order = orderMap.get(docCode);
      if (order) {
        // Gọi API get_card để lấy issue_partner_code cho đơn "08. Tách thẻ"
        await this.fetchCardDataAndMapIssuePartnerCode(docCode, sales);

        order.sales = sales;
        // Thêm tất cả stock transfers của đơn hàng này - format để trả về materialCode
        order.stockTransfers = (stockTransferByDocCodeMap.get(docCode) || []).map(st => this.formatStockTransferForFrontend(st));
      }
    }

    // Chuyển Map thành Array và sắp xếp
    const orders = Array.from(orderMap.values()).sort((a, b) => {
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });

    // Enrich orders với cashio data
    const enrichedOrders = await this.enrichOrdersWithCashio(orders);

    // Giới hạn số orders trả về
    const maxOrders = limit * 2;
    const limitedOrders = enrichedOrders.slice(0, maxOrders);

    return {
      data: limitedOrders,
      total: totalSaleItems,
      page,
      limit,
      totalPages: Math.ceil(totalSaleItems / limit),
    };
  }

  async getStatusAsys(
    statusAsys?: string,
    page?: number,
    limit?: number,
    brand?: string,
    dateFrom?: string,
    dateTo?: string,
    search?: string,
  ) {
    try {
      // Parse statusAsys: 'true' -> true, 'false' -> false, undefined/empty -> undefined
      let statusAsysValue: boolean | undefined;
      if (statusAsys === 'true') {
        statusAsysValue = true;
      } else if (statusAsys === 'false') {
        statusAsysValue = false;
      } else {
        statusAsysValue = undefined;
      }

      const pageNumber = page || 1;
      const limitNumber = limit || 10;
      const skip = (pageNumber - 1) * limitNumber;

      // Sử dụng QueryBuilder để hỗ trợ filter phức tạp
      let query = this.saleRepository.createQueryBuilder('sale');

      // Luôn leftJoinAndSelect customer để load relation (cần cho response)
      query = query.leftJoinAndSelect('sale.customer', 'customer');

      // Filter statusAsys
      if (statusAsysValue !== undefined) {
        query = query.andWhere('sale.statusAsys = :statusAsys', { statusAsys: statusAsysValue });
      }

      // Filter brand
      if (brand) {
        query = query.andWhere('customer.brand = :brand', { brand });
      }

      // Filter search (docCode, customer name, code, mobile)
      if (search && search.trim() !== '') {
        const searchPattern = `%${search.trim().toLowerCase()}%`;
        query = query.andWhere(
          '(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, \'\')) LIKE :search OR LOWER(COALESCE(customer.code, \'\')) LIKE :search OR LOWER(COALESCE(customer.mobile, \'\')) LIKE :search)',
          { search: searchPattern }
        );
      }

      // Filter date range - dùng Date object (TypeORM sẽ convert tự động)
      let startDate: Date | undefined;
      let endDate: Date | undefined;

      if (dateFrom) {
        startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        query = query.andWhere('sale.docDate >= :dateFrom', { dateFrom: startDate });
      }
      if (dateTo) {
        endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        query = query.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }

      // Tạo count query riêng (không dùng leftJoinAndSelect để tối ưu)
      const countQuery = this.saleRepository.createQueryBuilder('sale');

      // Apply cùng các filters như query chính nhưng chỉ dùng leftJoin (không Select)
      const needsCustomerJoin = brand || (search && search.trim() !== '');
      if (needsCustomerJoin) {
        countQuery.leftJoin('sale.customer', 'customer');
      }

      if (statusAsysValue !== undefined) {
        countQuery.andWhere('sale.statusAsys = :statusAsys', { statusAsys: statusAsysValue });
      }

      if (brand) {
        countQuery.andWhere('customer.brand = :brand', { brand });
      }

      if (search && search.trim() !== '') {
        const searchPattern = `%${search.trim().toLowerCase()}%`;
        countQuery.andWhere(
          '(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, \'\')) LIKE :search OR LOWER(COALESCE(customer.code, \'\')) LIKE :search OR LOWER(COALESCE(customer.mobile, \'\')) LIKE :search)',
          { search: searchPattern }
        );
      }

      if (startDate) {
        countQuery.andWhere('sale.docDate >= :dateFrom', { dateFrom: startDate });
      }

      if (endDate) {
        countQuery.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }

      // Count total
      const totalCount = await countQuery.getCount();

      // Apply pagination và order
      query = query
        .orderBy('sale.createdAt', 'DESC')
        .skip(skip)
        .take(limitNumber);

      const sales = await query.getMany();

      return {
        data: sales,
        total: totalCount,
        page: pageNumber,
        limit: limitNumber,
        totalPages: Math.ceil(totalCount / limitNumber),
      };
    } catch (error: any) {
      this.logger.error(`[getStatusAsys] Error: ${error?.message || error}`);
      this.logger.error(`[getStatusAsys] Stack: ${error?.stack || 'No stack trace'}`);
      throw error;
    }
  }

  /**
   * Đồng bộ lại đơn lỗi - check lại với Loyalty API
   * Nếu tìm thấy trong Loyalty, cập nhật itemCode (mã vật tư) và statusAsys = true
   * Xử lý theo batch từ database để tránh load quá nhiều vào memory
   */
  async syncErrorOrders(): Promise<{
    total: number;
    success: number;
    failed: number;
    updated: Array<{ id: string; docCode: string; itemCode: string; oldItemCode: string; newItemCode: string }>;
  }> {

    let successCount = 0;
    let failCount = 0;
    const updated: Array<{ id: string; docCode: string; itemCode: string; oldItemCode: string; newItemCode: string }> = [];

    // Cấu hình batch size
    const DB_BATCH_SIZE = 500; // Load 500 records từ DB mỗi lần
    const PROCESS_BATCH_SIZE = 100; // Xử lý 100 sales mỗi batch trong memory
    const CONCURRENT_LIMIT = 10; // Chỉ gọi 10 API cùng lúc để tránh quá tải

    // Helper function để xử lý một sale
    const processSale = async (sale: any): Promise<{ success: boolean; update?: { id: string; docCode: string; itemCode: string; oldItemCode: string; newItemCode: string } }> => {
      try {
        const itemCode = sale.itemCode || '';
        if (!itemCode) {
          return { success: false };
        }

        const product = await this.loyaltyService.checkProduct(itemCode);

        if (product && product.materialCode) {
          // Tìm thấy trong Loyalty - cập nhật
          const newItemCode = product.materialCode;
          const oldItemCode = itemCode;

          // Cập nhật sale
          await this.saleRepository.update(sale.id, {
            itemCode: newItemCode,
            statusAsys: true,
          });

          return {
            success: true,
            update: {
              id: sale.id,
              docCode: sale.docCode || '',
              itemCode: sale.itemCode || '',
              oldItemCode,
              newItemCode,
            },
          };
        }
        return { success: false };
      } catch (error: any) {
        this.logger.error(`[syncErrorOrders] ❌ Lỗi khi check sale ${sale.id}: ${error?.message || error}`);
        return { success: false };
      }
    };

    // Helper function để limit concurrent requests
    const processBatchConcurrent = async (sales: any[], limit: number) => {
      const results: Array<{ success: boolean; update?: any }> = [];
      for (let i = 0; i < sales.length; i += limit) {
        const batch = sales.slice(i, i + limit);
        const batchResults = await Promise.all(batch.map(sale => processSale(sale)));
        results.push(...batchResults);
      }
      return results;
    };

    // Xử lý từng batch từ database
    // Sau mỗi batch, query lại từ đầu vì các records đã xử lý (statusAsys = true) 
    // sẽ không còn trong query nữa, nên không cần cursor
    let processedCount = 0;
    let dbBatchNumber = 0;

    while (true) {
      dbBatchNumber++;

      // Load batch từ database (luôn query từ đầu, vì records đã xử lý sẽ không còn trong query)
      const dbBatch = await this.saleRepository.find({
        where: [
          { statusAsys: false },
          { statusAsys: IsNull() },
        ],
        order: { createdAt: 'DESC' },
        take: DB_BATCH_SIZE,
      });

      if (dbBatch.length === 0) {
        break; // Không còn records nào
      }


      // Xử lý batch này theo từng nhóm nhỏ
      for (let i = 0; i < dbBatch.length; i += PROCESS_BATCH_SIZE) {
        const processBatch = dbBatch.slice(i, i + PROCESS_BATCH_SIZE);
        const processBatchNumber = Math.floor(i / PROCESS_BATCH_SIZE) + 1;
        const totalProcessBatches = Math.ceil(dbBatch.length / PROCESS_BATCH_SIZE);


        // Xử lý batch với giới hạn concurrent
        const batchResults = await processBatchConcurrent(processBatch, CONCURRENT_LIMIT);

        // Cập nhật counters
        for (const result of batchResults) {
          if (result.success && result.update) {
            successCount++;
            updated.push(result.update);
          } else {
            failCount++;
          }
        }

        processedCount += processBatch.length;

        // Log progress - cập nhật totalCount vì có thể thay đổi khi có records mới
        const currentTotal = await this.saleRepository.count({
          where: [
            { statusAsys: false },
            { statusAsys: IsNull() },
          ],
        });
      }

      // Nếu batch nhỏ hơn DB_BATCH_SIZE, có nghĩa là đã hết records
      if (dbBatch.length < DB_BATCH_SIZE) {
        break;
      }
    }


    return {
      total: processedCount,
      success: successCount,
      failed: failCount,
      updated,
    };
  }

  /**
   * Đồng bộ lại một đơn hàng cụ thể - check lại với Loyalty API
   * Nếu tìm thấy trong Loyalty, cập nhật itemCode (mã vật tư) và statusAsys = true
   */
  async syncErrorOrderByDocCode(docCode: string): Promise<{
    success: boolean;
    message: string;
    updated: number;
    failed: number;
    details: Array<{ id: string; itemCode: string; oldItemCode: string; newItemCode: string }>;
  }> {

    // Lấy tất cả sales của đơn hàng có statusAsys = false, null, hoặc undefined
    // Sử dụng Or để match cả false, null, và undefined
    const errorSales = await this.saleRepository.find({
      where: [
        { docCode, statusAsys: false },
        { docCode, statusAsys: IsNull() },
      ],
    });

    if (errorSales.length === 0) {
      return {
        success: true,
        message: `Đơn hàng ${docCode} không có dòng nào cần đồng bộ`,
        updated: 0,
        failed: 0,
        details: [],
      };
    }


    let successCount = 0;
    let failCount = 0;
    const details: Array<{ id: string; itemCode: string; oldItemCode: string; newItemCode: string }> = [];

    // Check lại từng sale với Loyalty API
    for (const sale of errorSales) {
      try {
        const itemCode = sale.itemCode || '';
        if (!itemCode) {
          failCount++;
          continue;
        }

        // Check với Loyalty API - sử dụng LoyaltyService
        const product = await this.loyaltyService.checkProduct(itemCode);

        if (product && product.materialCode) {
          // Tìm thấy trong Loyalty - cập nhật
          const newItemCode = product.materialCode; // Mã vật tư từ Loyalty
          const oldItemCode = itemCode;

          // Cập nhật sale
          await this.saleRepository.update(sale.id, {
            itemCode: newItemCode,
            statusAsys: true, // Đánh dấu đã có trong Loyalty
          });

          successCount++;
          details.push({
            id: sale.id,
            itemCode: sale.itemCode || '',
            oldItemCode,
            newItemCode,
          });

        } else {
          // Vẫn không tìm thấy trong Loyalty
          failCount++;
          this.logger.warn(`[syncErrorOrderByDocCode] ❌ Sale ${sale.id} (${docCode}): itemCode ${itemCode} vẫn không tồn tại trong Loyalty`);
        }
      } catch (error: any) {
        failCount++;
        this.logger.error(`[syncErrorOrderByDocCode] ❌ Lỗi khi check sale ${sale.id}: ${error?.message || error}`);
      }
    }

    const message = successCount > 0
      ? `Đồng bộ thành công: ${successCount} dòng đã được cập nhật${failCount > 0 ? `, ${failCount} dòng vẫn lỗi` : ''}`
      : `Không có dòng nào được cập nhật. ${failCount} dòng vẫn không tìm thấy trong Loyalty API`;


    return {
      success: successCount > 0,
      message,
      updated: successCount,
      failed: failCount,
      details,
    };
  }

  async findOne(id: string) {
    const sale = await this.saleRepository.findOne({
      where: { id },
      relations: ['customer'],
    });

    if (!sale) {
      throw new NotFoundException(`Sale with ID ${id} not found`);
    }

    return sale;
  }

  async findByOrderCode(docCode: string) {
    // Lấy tất cả sales có cùng docCode (cùng đơn hàng)
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    if (sales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // Join với daily_cashio để lấy cashio data
    // Join dựa trên: cashio.so_code = docCode HOẶC cashio.master_code = docCode
    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code = :docCode', { docCode })
      .orWhere('cashio.master_code = :docCode', { docCode })
      .getMany();

    // Ưu tiên ECOIN, sau đó VOUCHER, sau đó các loại khác
    const ecoinCashio = cashioRecords.find(c => c.fop_syscode === 'ECOIN');
    const voucherCashio = cashioRecords.find(c => c.fop_syscode === 'VOUCHER');
    const selectedCashio = ecoinCashio || voucherCashio || cashioRecords[0] || null;

    // Lấy tất cả itemCode unique từ sales
    const itemCodes = Array.from(
      new Set(
        sales
          .map((sale) => sale.itemCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );

    // Load tất cả products một lần
    const products = itemCodes.length > 0
      ? await this.productItemRepository.find({
        where: { maERP: In(itemCodes) },
      })
      : [];

    // Tạo map để lookup nhanh
    const productMap = new Map<string, ProductItem>();
    products.forEach((product) => {
      if (product.maERP) {
        productMap.set(product.maERP, product);
      }
    });

    // Enrich sales với product information từ database
    const enrichedSales = sales.map((sale) => ({
      ...sale,
      product: sale.itemCode ? productMap.get(sale.itemCode) || null : null,
    }));

    // Fetch products từ Loyalty API cho các itemCode không có trong database hoặc không có dvt
    // BỎ QUA các sale có statusAsys = false (đơn lỗi) - không fetch từ Loyalty API
    const loyaltyProductMap = new Map<string, any>();
    // Filter itemCodes: chỉ fetch cho các sale không phải đơn lỗi
    const validItemCodes = itemCodes.filter(itemCode => {
      const sale = sales.find(s => s.itemCode === itemCode);
      return sale && sale.statusAsys !== false;
    });

    // Fetch products từ Loyalty API sử dụng LoyaltyService
    if (validItemCodes.length > 0) {
      const fetchedProducts = await this.loyaltyService.fetchProducts(validItemCodes);
      fetchedProducts.forEach((product, itemCode) => {
        loyaltyProductMap.set(itemCode, product);
      });
    }

    // Enrich sales với product từ Loyalty API (thêm dvt từ unit)
    const enrichedSalesWithLoyalty = enrichedSales.map((sale) => {
      const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
      const existingProduct = sale.product;

      // Nếu có product từ Loyalty API, merge thông tin (ưu tiên dvt từ Loyalty API)
      if (loyaltyProduct) {
        return {
          ...sale,
          // Lấy producttype từ Loyalty API (không còn trong database)
          producttype: loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
          product: {
            ...existingProduct,
            ...loyaltyProduct,
            // Map unit từ Loyalty API thành dvt
            dvt: loyaltyProduct.unit || existingProduct?.dvt || null,
            // Giữ lại các field từ database nếu có, chỉ dùng materialCode từ Loyalty API
            maVatTu: existingProduct?.maVatTu || loyaltyProduct.materialCode || sale.itemCode,
            maERP: existingProduct?.maERP || loyaltyProduct.materialCode || sale.itemCode,
            // Đảm bảo productType từ Loyalty API được giữ lại (ưu tiên productType, sau đó producttype)
            productType: loyaltyProduct.productType || loyaltyProduct.producttype || (existingProduct as any)?.productType || null,
            // Lấy producttype từ Loyalty API
            producttype: loyaltyProduct.producttype || loyaltyProduct.productType || (existingProduct as any)?.producttype || null,
          },
        };
      }

      return sale;
    });


    // Fetch departments để lấy ma_dvcs
    const branchCodes = Array.from(
      new Set(
        sales
          .map((sale) => sale.branchCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );

    const departmentMap = new Map<string, any>();
    // Fetch departments parallel để tối ưu performance
    if (branchCodes.length > 0) {
      const departmentPromises = branchCodes.map(async (branchCode) => {
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
            { headers: { accept: 'application/json' } },
          );
          const department = response?.data?.data?.items?.[0];
          return { branchCode, department };
        } catch (error) {
          this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
          return { branchCode, department: null };
        }
      });

      const departmentResults = await Promise.all(departmentPromises);
      departmentResults.forEach(({ branchCode, department }) => {
        if (department) {
          departmentMap.set(branchCode, department);
        }
      });
    }

    // Fetch stock transfers để lấy ma_nx (ST* và RT* từ stock transfer)
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer = this.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    // Sử dụng materialCode đã được lưu trong database (đã được đồng bộ từ Loyalty API khi sync)
    // Nếu chưa có materialCode trong database, mới fetch từ Loyalty API
    const stockTransferItemCodesWithoutMaterialCode = Array.from(
      new Set(
        stockTransfers
          .filter((st) => st.itemCode && !st.materialCode)
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );

    // Chỉ fetch materialCode cho các itemCode chưa có materialCode trong database
    const stockTransferLoyaltyMap = new Map<string, any>();
    if (stockTransferItemCodesWithoutMaterialCode.length > 0) {
      const fetchedStockTransferProducts = await this.loyaltyService.fetchProducts(stockTransferItemCodesWithoutMaterialCode);
      fetchedStockTransferProducts.forEach((product, itemCode) => {
        stockTransferLoyaltyMap.set(itemCode, product);
      });
    }

    // Tạo map: soCode_materialCode -> stock transfer (phân biệt ST và RT)
    // Match theo: Mã ĐH (soCode) = Số hóa đơn (docCode) VÀ Mã SP (itemCode) -> materialCode = Mã hàng (ma_vt)
    // Ưu tiên dùng materialCode đã lưu trong database, nếu chưa có thì lấy từ Loyalty API
    // Lưu ý: Dùng array để lưu tất cả stock transfers cùng key (tránh ghi đè khi có nhiều records giống nhau)
    const stockTransferMapBySoCodeAndMaterialCode = new Map<string, { st?: StockTransfer[]; rt?: StockTransfer[] }>();
    stockTransfers.forEach((st) => {
      // Ưu tiên dùng materialCode đã lưu trong database
      // Nếu chưa có thì lấy từ Loyalty API (đã fetch ở trên)
      const materialCode = st.materialCode || stockTransferLoyaltyMap.get(st.itemCode)?.materialCode;
      if (!materialCode) {
        // Bỏ qua nếu không có materialCode (không match được)
        return;
      }

      // Key: soCode_materialCode (Mã ĐH_Mã hàng từ Loyalty API)
      const soCode = st.soCode || st.docCode || docCode;
      const key = `${soCode}_${materialCode}`;

      if (!stockTransferMapBySoCodeAndMaterialCode.has(key)) {
        stockTransferMapBySoCodeAndMaterialCode.set(key, {});
      }
      const itemMap = stockTransferMapBySoCodeAndMaterialCode.get(key)!;
      // ST* - dùng array để lưu tất cả
      if (st.docCode.startsWith('ST')) {
        if (!itemMap.st) {
          itemMap.st = [];
        }
        itemMap.st.push(st);
      }
      // RT* - dùng array để lưu tất cả
      if (st.docCode.startsWith('RT')) {
        if (!itemMap.rt) {
          itemMap.rt = [];
        }
        itemMap.rt.push(st);
      }
    });

    // Enrich sales với department information và lấy maKho từ stock transfer
    const enrichedSalesWithDepartment = await Promise.all(enrichedSalesWithLoyalty.map(async (sale) => {
      const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
      const maBp = department?.ma_bp || sale.branchCode || null;

      // Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
      const saleLoyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
      const saleMaterialCode = saleLoyaltyProduct?.materialCode;
      const finalMaKho = await this.getMaKhoFromStockTransfer(sale, docCode, stockTransfers, saleMaterialCode);

      // Lấy ma_nx từ stock transfer (phân biệt ST và RT)
      // Match stock transfer để lấy ma_nx
      const matchedStockTransfer = stockTransfers.find(
        (st) => st.soCode === docCode && st.itemCode === sale.itemCode
      );
      const firstSt = matchedStockTransfer && matchedStockTransfer.docCode.startsWith('ST') ? matchedStockTransfer : null;
      const firstRt = matchedStockTransfer && matchedStockTransfer.docCode.startsWith('RT') ? matchedStockTransfer : null;

      return {
        ...sale,
        department: department,
        maKho: finalMaKho,
        // Thêm ma_nx từ stock transfer (lấy từ record đầu tiên)
        ma_nx_st: firstSt?.docCode || null, // ST* - mã nghiệp vụ từ stock transfer
        ma_nx_rt: firstRt?.docCode || null, // RT* - mã nghiệp vụ từ stock transfer
      };
    }));

    // Tính tổng doanh thu của đơn hàng
    const totalRevenue = sales.reduce((sum, sale) => sum + Number(sale.revenue), 0);
    const totalQty = sales.reduce((sum, sale) => sum + Number(sale.qty), 0);

    // Lấy thông tin chung từ sale đầu tiên
    const firstSale = sales[0];

    // Lấy thông tin khuyến mại từ Loyalty API cho các promCode trong đơn hàng
    // Fetch parallel để tối ưu performance
    const promotionsByCode: Record<string, any> = {};
    const uniquePromCodes = Array.from(
      new Set(
        sales
          .map((s) => s.promCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    if (uniquePromCodes.length > 0) {
      const promotionPromises = uniquePromCodes.map(async (promCode) => {
        try {
          // Gọi Loyalty API theo externalCode = promCode
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/promotions/item/external/${promCode}`,
            {
              headers: { accept: 'application/json' },
              timeout: 5000, // Timeout 5s để tránh chờ quá lâu
            },
          );

          const data = response?.data;
          return { promCode, data };
        } catch (error) {
          // Chỉ log error nếu không phải 404 (không tìm thấy promotion là bình thường)
          if ((error as any)?.response?.status !== 404) {
            this.logger.warn(
              `Lỗi khi lấy promotion cho promCode ${promCode}: ${(error as any)?.message || error}`,
            );
          }
          return { promCode, data: null };
        }
      });

      const promotionResults = await Promise.all(promotionPromises);
      promotionResults.forEach(({ promCode, data }) => {
        promotionsByCode[promCode] = {
          raw: data,
          main: data || null,
        };
      });
    }

    // Gắn promotion tương ứng vào từng dòng sale (chỉ để trả ra API, không lưu DB)
    // Và tính lại muaHangCkVip nếu chưa có hoặc cần override cho f3
    // Format sales giống findAllOrders để đảm bảo consistency với frontend
    // Format sales sau khi đã enrich promotion
    const formattedSales = enrichedSalesWithDepartment.map((sale) => {
      const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
      const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
      const calculatedFields = this.calculateSaleFields(sale, loyaltyProduct, department, sale.branchCode);
      const order = {
        customer: firstSale.customer || null,
        cashioData: selectedCashio,
        cashioFopSyscode: selectedCashio?.fop_syscode || null,
        cashioTotalIn: selectedCashio?.total_in || null,
        brand: firstSale.customer?.brand || null,
      };
      const formattedSale = this.formatSaleForFrontend(sale, loyaltyProduct, department, calculatedFields, order);

      // Thêm promotion info nếu có
      const promCode = sale.promCode;
      const promotion = promCode && promotionsByCode[promCode] ? promotionsByCode[promCode] : null;

      return {
        ...formattedSale,
        promotion,
        promotionDisplayCode: this.getPromotionDisplayCode(promCode),
      };
    });

    // Format customer object để match với frontend interface
    const formattedCustomer = firstSale.customer ? {
      ...firstSale.customer,
      // Map mobile -> phone nếu phone chưa có
      phone: firstSale.customer.phone || firstSale.customer.mobile || null,
      // Map address -> street nếu street chưa có
      street: firstSale.customer.street || firstSale.customer.address || null,
    } : null;

    return {
      docCode: firstSale.docCode,
      docDate: firstSale.docDate,
      branchCode: firstSale.branchCode,
      docSourceType: firstSale.docSourceType || (firstSale as any).docSourceType || null,
      customer: formattedCustomer,
      totalRevenue,
      totalQty,
      totalItems: sales.length,
      sales: formattedSales,
      promotions: promotionsByCode,
      // Cashio data từ join với daily_cashio
      cashioData: cashioRecords.length > 0 ? cashioRecords : null,
      cashioFopSyscode: selectedCashio?.fop_syscode || null,
      cashioFopDescription: selectedCashio?.fop_description || null,
      cashioCode: selectedCashio?.code || null,
      cashioMasterCode: selectedCashio?.master_code || null,
      cashioTotalIn: selectedCashio?.total_in || null,
      cashioTotalOut: selectedCashio?.total_out || null,
    };
  }

  async printOrder(docCode: string): Promise<any> {
    throw new Error('Print functionality has been removed');
  }


  /**
   * Lưu hóa đơn vào bảng kê hóa đơn (FastApiInvoice)
   */
  private async saveFastApiInvoice(data: {
    docCode: string;
    maDvcs?: string;
    maKh?: string;
    tenKh?: string;
    ngayCt?: Date;
    status: number;
    message?: string;
    guid?: string | null;
    fastApiResponse?: string;
  }): Promise<FastApiInvoice> {
    try {
      // Kiểm tra xem đã có chưa
      const existing = await this.fastApiInvoiceRepository.findOne({
        where: { docCode: data.docCode },
      });

      if (existing) {
        // Cập nhật record hiện có
        existing.status = data.status;
        existing.message = data.message || existing.message;
        existing.guid = data.guid || existing.guid;
        existing.fastApiResponse = data.fastApiResponse || existing.fastApiResponse;
        if (data.maDvcs) existing.maDvcs = data.maDvcs;
        if (data.maKh) existing.maKh = data.maKh;
        if (data.tenKh) existing.tenKh = data.tenKh;
        if (data.ngayCt) existing.ngayCt = data.ngayCt;

        const saved = await this.fastApiInvoiceRepository.save(existing);
        return Array.isArray(saved) ? saved[0] : saved;
      } else {
        // Tạo mới
        const fastApiInvoice = this.fastApiInvoiceRepository.create({
          docCode: data.docCode,
          maDvcs: data.maDvcs ?? null,
          maKh: data.maKh ?? null,
          tenKh: data.tenKh ?? null,
          ngayCt: data.ngayCt ?? new Date(),
          status: data.status,
          message: data.message ?? null,
          guid: data.guid ?? null,
          fastApiResponse: data.fastApiResponse ?? null,
        } as Partial<FastApiInvoice>);

        const saved = await this.fastApiInvoiceRepository.save(fastApiInvoice);
        return Array.isArray(saved) ? saved[0] : saved;
      }
    } catch (error: any) {
      this.logger.error(`Error saving FastApiInvoice for ${data.docCode}: ${error?.message || error}`);
      throw error;
    }
  }

  private async markOrderAsProcessed(docCode: string): Promise<void> {
    // Tìm tất cả các sale có cùng docCode
    const sales = await this.saleRepository.find({
      where: { docCode },
    });

    // Cập nhật isProcessed = true cho tất cả các sale
    if (sales.length > 0) {
      await this.saleRepository.update(
        { docCode },
        { isProcessed: true },
      );
    }
  }

  /**
   * Đánh dấu lại các đơn hàng đã có invoice là đã xử lý
   * Method này dùng để xử lý các invoice đã được tạo trước đó
   */
  async markProcessedOrdersFromInvoices(): Promise<{ updated: number; message: string }> {
    // Tìm tất cả các invoice đã được in (isPrinted = true)
    const invoices = await this.invoiceRepository.find({
      where: { isPrinted: true },
    });

    let updatedCount = 0;
    const processedDocCodes = new Set<string>();

    // Duyệt qua các invoice và tìm docCode từ key
    // Key có thể là docCode hoặc có format INV_xxx_xxx
    for (const invoice of invoices) {
      let docCode: string | null = null;

      // Thử 1: Key chính là docCode (cho các invoice mới)
      const salesByKey = await this.saleRepository.find({
        where: { docCode: invoice.key },
        take: 1,
      });
      if (salesByKey.length > 0) {
        docCode = invoice.key;
      } else {
        // Thử 2: Tìm trong printResponse xem có docCode không
        try {
          if (invoice.printResponse) {
            const printResponse = JSON.parse(invoice.printResponse);

            // Tìm trong Message (là JSON string chứa array)
            if (printResponse.Message) {
              try {
                const messageData = JSON.parse(printResponse.Message);
                if (Array.isArray(messageData) && messageData.length > 0) {
                  const data = messageData[0];
                  if (data.key) {
                    // Extract docCode từ key (format: SO52.00005808_X -> SO52.00005808)
                    const keyParts = data.key.split('_');
                    if (keyParts.length > 0) {
                      const potentialDocCode = keyParts[0];
                      const salesByPotentialKey = await this.saleRepository.find({
                        where: { docCode: potentialDocCode },
                        take: 1,
                      });
                      if (salesByPotentialKey.length > 0) {
                        docCode = potentialDocCode;
                      }
                    }
                  }
                }
              } catch (msgError) {
                // Message không phải JSON string, bỏ qua
              }
            }

            // Thử tìm trong Data nếu có
            if (!docCode && printResponse.Data && Array.isArray(printResponse.Data) && printResponse.Data.length > 0) {
              const data = printResponse.Data[0];
              if (data.key) {
                const keyParts = data.key.split('_');
                if (keyParts.length > 0) {
                  const potentialDocCode = keyParts[0];
                  const salesByPotentialKey = await this.saleRepository.find({
                    where: { docCode: potentialDocCode },
                    take: 1,
                  });
                  if (salesByPotentialKey.length > 0) {
                    docCode = potentialDocCode;
                  }
                }
              }
            }
          }
        } catch (error) {
          // Ignore parse errors
        }
      }

      // Nếu tìm thấy docCode, đánh dấu các sale là đã xử lý
      if (docCode && !processedDocCodes.has(docCode)) {
        const updateResult = await this.saleRepository.update(
          { docCode },
          { isProcessed: true },
        );
        if (updateResult.affected && updateResult.affected > 0) {
          updatedCount += updateResult.affected;
          processedDocCodes.add(docCode);
        }
      }
    }

    return {
      updated: updatedCount,
      message: `Đã đánh dấu ${processedDocCodes.size} đơn hàng là đã xử lý (${updatedCount} sale records)`,
    };
  }


  /**
   * Đồng bộ dữ liệu từ Zappy API và lưu vào database
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 04DEC2025)
   * @param brand - Brand name (f3, labhair, yaman, menard). Nếu không có thì dùng default
   * @returns Kết quả đồng bộ
   */
  async syncFromZappy(date: string, brand?: string): Promise<{
    success: boolean;
    message: string;
    ordersCount: number;
    salesCount: number;
    customersCount: number;
    errors?: string[];
  }> {

    try {
      // Lấy dữ liệu từ Zappy API
      const orders = await this.zappyApiService.getDailySales(date, brand);

      // Lấy dữ liệu cash/voucher từ get_daily_cash để enrich
      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date, brand);
      } catch (error) {
      }

      // Tạo map cash data theo so_code để dễ lookup
      const cashMapBySoCode = new Map<string, any[]>();
      cashData.forEach((cash) => {
        const soCode = cash.so_code || cash.master_code;
        if (soCode) {
          if (!cashMapBySoCode.has(soCode)) {
            cashMapBySoCode.set(soCode, []);
          }
          cashMapBySoCode.get(soCode)!.push(cash);
        }
      });

      if (orders.length === 0) {
        return {
          success: true,
          message: `Không có dữ liệu để đồng bộ cho ngày ${date}`,
          ordersCount: 0,
          salesCount: 0,
          customersCount: 0,
        };
      }

      let salesCount = 0;
      let customersCount = 0;
      const errors: string[] = [];

      // Collect tất cả branchCodes để fetch departments
      const branchCodes = Array.from(
        new Set(
          orders
            .map((o) => o.branchCode)
            .filter((code): code is string => !!code && code.trim() !== '')
        )
      );

      // Fetch departments để lấy company và map sang brand
      const departmentMap = new Map<string, { company?: string }>();
      for (const branchCode of branchCodes) {
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
            { headers: { accept: 'application/json' } },
          );
          const department = response?.data?.data?.items?.[0];
          if (department?.company) {
            departmentMap.set(branchCode, { company: department.company });
          }
        } catch (error) {
          this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
        }
      }

      // Map company sang brand
      const mapCompanyToBrand = (company: string | null | undefined): string => {
        if (!company) return '';
        const companyUpper = company.toUpperCase();
        const brandMap: Record<string, string> = {
          'F3': 'f3',
          'FACIALBAR': 'f3',
          'MENARD': 'menard',
          'CHANDO': 'chando',
          'LABHAIR': 'labhair',
          'YAMAN': 'yaman',
        };
        return brandMap[companyUpper] || company.toLowerCase();
      };

      // Xử lý từng order
      for (const order of orders) {
        try {
          // Lấy brand từ department.company
          const department = departmentMap.get(order.branchCode);
          const brandFromDepartment = department?.company
            ? mapCompanyToBrand(department.company)
            : order.customer.brand || '';

          // Tìm hoặc tạo customer
          let customer = await this.customerRepository.findOne({
            where: { code: order.customer.code },
          });

          if (!customer) {
            const newCustomer = this.customerRepository.create({
              code: order.customer.code,
              name: order.customer.name,
              brand: brandFromDepartment,
              mobile: order.customer.mobile,
              sexual: order.customer.sexual,
              idnumber: order.customer.idnumber,
              enteredat: order.customer.enteredat ? new Date(order.customer.enteredat) : null,
              crm_lead_source: order.customer.crm_lead_source,
              address: order.customer.address,
              province_name: order.customer.province_name,
              birthday: order.customer.birthday ? new Date(order.customer.birthday) : null,
              grade_name: order.customer.grade_name,
              branch_code: order.customer.branch_code,
            } as Partial<Customer>);
            customer = await this.customerRepository.save(newCustomer);
            customersCount++;
          } else {
            // Cập nhật thông tin customer nếu cần
            customer.name = order.customer.name || customer.name;
            customer.mobile = order.customer.mobile || customer.mobile;
            customer.grade_name = order.customer.grade_name || customer.grade_name;
            // Cập nhật brand từ department nếu có
            if (brandFromDepartment) {
              customer.brand = brandFromDepartment;
            }
            customer = await this.customerRepository.save(customer);
          }

          // Đảm bảo customer không null
          if (!customer) {
            const errorMsg = `Không thể tạo hoặc tìm customer với code ${order.customer.code}`;
            this.logger.error(errorMsg);
            errors.push(errorMsg);
            continue;
          }

          // Lấy cash/voucher data cho order này
          const orderCashData = cashMapBySoCode.get(order.docCode) || [];
          const voucherData = orderCashData.filter((cash) => cash.fop_syscode === 'VOUCHER');

          // Collect tất cả itemCodes từ order để fetch products từ Loyalty API và check 404
          // Đảm bảo trim ngay từ đầu để consistency
          const orderItemCodes = Array.from(
            new Set(
              (order.sales || [])
                .map((s) => s.itemCode?.trim())
                .filter((code): code is string => !!code && code !== '')
            )
          );

          // Fetch products từ Loyalty API để check sản phẩm không tồn tại (404)
          const notFoundItemCodes = new Set<string>();

          if (orderItemCodes.length > 0) {
            // Check products từ Loyalty API sử dụng LoyaltyService
            await Promise.all(
              orderItemCodes.map(async (trimmedItemCode) => {
                const product = await this.loyaltyService.checkProduct(trimmedItemCode);
                if (!product) {
                  notFoundItemCodes.add(trimmedItemCode);
                }
              }),
            );
          }

          // Xử lý từng sale trong order - LƯU TẤT CẢ, đánh dấu statusAsys = false nếu sản phẩm không tồn tại (404)
          if (order.sales && order.sales.length > 0) {
            for (const saleItem of order.sales) {
              try {
                // Bỏ qua các item có itemcode = "TRUTONKEEP"
                const itemCode = saleItem.itemCode?.trim();
                if (itemCode && itemCode.toUpperCase() === 'TRUTONKEEP') {
                  this.logger.log(`[SalesService] Bỏ qua sale item ${itemCode} (${saleItem.itemName || 'N/A'}) trong order ${order.docCode} - itemcode = TRUTONKEEP`);
                  continue;
                }

                // Kiểm tra xem sản phẩm có tồn tại trong Loyalty API không
                const isNotFound = itemCode && notFoundItemCodes.has(itemCode);
                // Set statusAsys: false nếu không tồn tại (404), true nếu tồn tại
                const statusAsys = !isNotFound;

                if (isNotFound) {
                  this.logger.warn(`[SalesService] Sale item ${itemCode} (${saleItem.itemName || 'N/A'}) trong order ${order.docCode} - Sản phẩm không tồn tại trong Loyalty API (404), sẽ lưu với statusAsys = false`);
                }

                // Lấy productType: Ưu tiên từ Zappy API (producttype), nếu không có thì lấy từ Loyalty API
                // Kiểm tra cả producttype (chữ thường) và productType (camelCase) từ Zappy API
                const productTypeFromZappy = saleItem.producttype || saleItem.productType || null;
                // Fetch productType từ Loyalty API nếu chưa có từ Zappy (đã có sẵn trong notFoundItemCodes check)
                let productTypeFromLoyalty: string | null = null;
                if (!productTypeFromZappy && itemCode && !notFoundItemCodes.has(itemCode)) {
                  try {
                    const loyaltyProduct = await this.loyaltyService.checkProduct(itemCode);
                    if (loyaltyProduct) {
                      productTypeFromLoyalty = loyaltyProduct.productType || loyaltyProduct.producttype || null;
                    }
                  } catch (error) {
                    // Ignore error, sẽ dùng null
                  }
                }
                const productType = productTypeFromZappy || productTypeFromLoyalty || null;


                // Kiểm tra xem sale đã tồn tại chưa
                // Với đơn "08. Tách thẻ": cần thêm qty vào điều kiện vì có thể có 2 dòng cùng itemCode nhưng qty khác nhau (-1 và 1)
                // Với các đơn khác: chỉ cần docCode + itemCode + customer
                const ordertypeName = saleItem.ordertype_name || saleItem.ordertype || '';
                const isTachThe = ordertypeName.includes('08. Tách thẻ') ||
                  ordertypeName.includes('08.Tách thẻ') ||
                  ordertypeName.includes('08.  Tách thẻ');



                // Enrich voucher data từ get_daily_cash
                let voucherRefno: string | undefined;
                let voucherAmount: number | undefined;
                if (voucherData.length > 0) {
                  // Lấy voucher đầu tiên (có thể có nhiều voucher)
                  const firstVoucher = voucherData[0];
                  voucherRefno = firstVoucher.refno;
                  voucherAmount = firstVoucher.total_in || 0;
                }

                // Tạo sale mới
                // Tính toán ordertypeName trước
                let finalOrderTypeNameForNew: string | undefined = undefined;
                if (saleItem.ordertype_name !== undefined && saleItem.ordertype_name !== null) {
                  if (typeof saleItem.ordertype_name === 'string') {
                    const trimmed = saleItem.ordertype_name.trim();
                    finalOrderTypeNameForNew = trimmed !== '' ? trimmed : undefined;
                  } else {
                    finalOrderTypeNameForNew = String(saleItem.ordertype_name).trim() || undefined;
                  }
                }
                // Log để debug
                this.logger.log(
                  `[SalesService] Tạo mới sale ${order.docCode}/${saleItem.itemCode}: ` +
                  `ordertype_name raw="${saleItem.ordertype_name}" (type: ${typeof saleItem.ordertype_name}), final="${finalOrderTypeNameForNew}"`
                );
                const newSale = this.saleRepository.create({
                  docCode: order.docCode,
                  docDate: new Date(order.docDate),
                  branchCode: order.branchCode,
                  docSourceType: order.docSourceType,
                  ordertype: saleItem.ordertype,
                  // Luôn lưu ordertypeName, kể cả khi là undefined (để lưu từ Zappy API)
                  // Nếu ordertypeName là empty string, set thành undefined
                  ordertypeName: finalOrderTypeNameForNew,
                  description: saleItem.description,
                  partnerCode: saleItem.partnerCode,
                  itemCode: saleItem.itemCode || '',
                  itemName: saleItem.itemName || '',
                  qty: saleItem.qty || 0,
                  revenue: saleItem.revenue || 0,
                  linetotal: saleItem.linetotal || saleItem.revenue || 0,
                  tienHang: saleItem.tienHang || saleItem.linetotal || saleItem.revenue || 0,
                  giaBan: saleItem.giaBan || 0,
                  promCode: saleItem.promCode,
                  serial: saleItem.serial,
                  soSerial: saleItem.serial,
                  disc_amt: saleItem.disc_amt,
                  grade_discamt: saleItem.grade_discamt,
                  other_discamt: saleItem.other_discamt,
                  chietKhauMuaHangGiamGia: saleItem.chietKhauMuaHangGiamGia,
                  paid_by_voucher_ecode_ecoin_bp: saleItem.paid_by_voucher_ecode_ecoin_bp,
                  maCa: saleItem.shift_code,
                  // Validate saleperson_id để tránh NaN
                  saleperson_id: this.validateInteger(saleItem.saleperson_id),
                  partner_name: saleItem.partner_name,
                  order_source: saleItem.order_source,
                  // Lưu mvc_serial vào maThe
                  maThe: saleItem.mvc_serial,
                  // Category fields
                  cat1: saleItem.cat1,
                  cat2: saleItem.cat2,
                  cat3: saleItem.cat3,
                  catcode1: saleItem.catcode1,
                  catcode2: saleItem.catcode2,
                  catcode3: saleItem.catcode3,
                  // Luôn lưu productType, kể cả khi là null (để lưu từ Zappy API)
                  // Nếu productType là empty string, set thành null
                  productType: productType && productType.trim() !== '' ? productType.trim() : null,
                  // Enrich voucher data từ get_daily_cash
                  voucherDp1: voucherRefno,
                  thanhToanVoucher: voucherAmount && voucherAmount > 0 ? voucherAmount : undefined,
                  customer: customer,
                  isProcessed: false,
                  statusAsys: statusAsys, // Set statusAsys: true nếu sản phẩm tồn tại, false nếu 404
                } as Partial<Sale>);
                await this.saleRepository.save(newSale);
                salesCount++;

              } catch (saleError: any) {
                const errorMsg = `Lỗi khi lưu sale ${order.docCode}/${saleItem.itemCode}: ${saleError?.message || saleError}`;
                this.logger.error(errorMsg);
                errors.push(errorMsg);
              }
            }
          }
        } catch (orderError: any) {
          const errorMsg = `Lỗi khi xử lý order ${order.docCode}: ${orderError?.message || orderError}`;
          this.logger.error(errorMsg);
          errors.push(errorMsg);
        }
      }


      return {
        success: errors.length === 0,
        message: `Đồng bộ thành công ${orders.length} đơn hàng cho ngày ${date}${brand ? ` (brand: ${brand})` : ''}`,
        ordersCount: orders.length,
        salesCount,
        customersCount,
        errors: errors.length > 0 ? errors : undefined,
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ từ Zappy API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Đồng bộ sale từ khoảng thời gian cho tất cả các nhãn
   * @param startDate - Ngày bắt đầu theo format DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc theo format DDMMMYYYY (ví dụ: 01DEC2025)
   * @returns Kết quả đồng bộ tổng hợp
   */
  async syncSalesByDateRange(startDate: string, endDate: string): Promise<{
    success: boolean;
    message: string;
    totalOrdersCount: number;
    totalSalesCount: number;
    totalCustomersCount: number;
    brandResults: Array<{
      brand: string;
      ordersCount: number;
      salesCount: number;
      customersCount: number;
      errors?: string[];
    }>;
    errors?: string[];
  }> {
    const brands = ['f3', 'labhair', 'yaman', 'menard'];
    const allErrors: string[] = [];
    const brandResults: Array<{
      brand: string;
      ordersCount: number;
      salesCount: number;
      customersCount: number;
      errors?: string[];
    }> = [];

    let totalOrdersCount = 0;
    let totalSalesCount = 0;
    let totalCustomersCount = 0;

    // Parse dates
    const parseDate = (dateStr: string): Date => {
      // Format: DDMMMYYYY (ví dụ: 01OCT2025)
      const day = parseInt(dateStr.substring(0, 2));
      const monthStr = dateStr.substring(2, 5).toUpperCase();
      const year = parseInt(dateStr.substring(5, 9));

      const monthMap: Record<string, number> = {
        'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3, 'MAY': 4, 'JUN': 5,
        'JUL': 6, 'AUG': 7, 'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
      };

      const month = monthMap[monthStr];
      if (month === undefined) {
        throw new Error(`Invalid month: ${monthStr}`);
      }

      return new Date(year, month, day);
    };

    const formatDate = (date: Date): string => {
      const day = date.getDate().toString().padStart(2, '0');
      const monthNames = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
      const month = monthNames[date.getMonth()];
      const year = date.getFullYear();
      return `${day}${month}${year}`;
    };

    try {
      const start = parseDate(startDate);
      const end = parseDate(endDate);

      // Lặp qua từng brand
      for (const brand of brands) {
        this.logger.log(`[syncSalesByDateRange] Bắt đầu đồng bộ brand: ${brand}`);
        let brandOrdersCount = 0;
        let brandSalesCount = 0;
        let brandCustomersCount = 0;
        const brandErrors: string[] = [];

        // Lặp qua từng ngày trong khoảng thời gian
        const currentDate = new Date(start);
        while (currentDate <= end) {
          const dateStr = formatDate(currentDate);
          try {
            this.logger.log(`[syncSalesByDateRange] Đồng bộ ${brand} - ngày ${dateStr}`);
            const result = await this.syncFromZappy(dateStr, brand);

            brandOrdersCount += result.ordersCount;
            brandSalesCount += result.salesCount;
            brandCustomersCount += result.customersCount;

            if (result.errors && result.errors.length > 0) {
              brandErrors.push(...result.errors.map(err => `[${dateStr}] ${err}`));
            }
          } catch (error: any) {
            const errorMsg = `[${brand}] Lỗi khi đồng bộ ngày ${dateStr}: ${error?.message || error}`;
            this.logger.error(errorMsg);
            brandErrors.push(errorMsg);
          }

          // Tăng ngày lên 1
          currentDate.setDate(currentDate.getDate() + 1);
        }

        totalOrdersCount += brandOrdersCount;
        totalSalesCount += brandSalesCount;
        totalCustomersCount += brandCustomersCount;

        brandResults.push({
          brand,
          ordersCount: brandOrdersCount,
          salesCount: brandSalesCount,
          customersCount: brandCustomersCount,
          errors: brandErrors.length > 0 ? brandErrors : undefined,
        });

        if (brandErrors.length > 0) {
          allErrors.push(...brandErrors);
        }

        this.logger.log(`[syncSalesByDateRange] Hoàn thành đồng bộ brand: ${brand} - ${brandOrdersCount} đơn, ${brandSalesCount} sale`);
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ thành công từ ${startDate} đến ${endDate}: ${totalOrdersCount} đơn hàng, ${totalSalesCount} sale, ${totalCustomersCount} khách hàng`,
        totalOrdersCount,
        totalSalesCount,
        totalCustomersCount,
        brandResults,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ sale theo khoảng thời gian: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo hóa đơn qua Fast API từ đơn hàng
   */
  async createInvoiceViaFastApi(docCode: string, forceRetry: boolean = false): Promise<any> {

    try {
      // Kiểm tra xem đơn hàng đã có trong bảng kê hóa đơn chưa (đã tạo thành công)
      // Nếu forceRetry = true, bỏ qua check này để cho phép retry
      if (!forceRetry) {
        const existingInvoice = await this.fastApiInvoiceRepository.findOne({
          where: { docCode },
        });

        if (existingInvoice && existingInvoice.status === 1) {
          return {
            success: true,
            message: `Đơn hàng ${docCode} đã được tạo hóa đơn thành công trước đó`,
            result: existingInvoice.fastApiResponse ? JSON.parse(existingInvoice.fastApiResponse) : null,
            alreadyExists: true,
          };
        }
      }

      // Lấy thông tin đơn hàng
      const orderData = await this.findByOrderCode(docCode);

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(`Order ${docCode} not found or has no sales`);
      }

      // ============================================
      // XỬ LÝ ĐƠN CÓ ĐUÔI _X
      // ============================================
      // Nếu đơn có đuôi _X → xử lý với action = 1
      if (this.hasUnderscoreX(docCode)) {
        return await this.handleSaleOrderWithUnderscoreX(orderData, docCode);
      }

      // Xử lý đơn gốc (không có _X)
      return await this.processSingleOrder(docCode, forceRetry);
    } catch (error: any) {
      this.logger.error(`Lỗi khi tạo hóa đơn cho ${docCode}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Xử lý một đơn hàng đơn lẻ (được gọi từ createInvoiceViaFastApi)
   */
  private async processSingleOrder(docCode: string, forceRetry: boolean = false): Promise<any> {
    try {
      // Kiểm tra xem đơn hàng đã có trong bảng kê hóa đơn chưa (đã tạo thành công)
      // Nếu forceRetry = true, bỏ qua check này để cho phép retry
      if (!forceRetry) {
        const existingInvoice = await this.fastApiInvoiceRepository.findOne({
          where: { docCode },
        });

        if (existingInvoice && existingInvoice.status === 1) {
          return {
            success: true,
            message: `Đơn hàng ${docCode} đã được tạo hóa đơn thành công trước đó`,
            result: existingInvoice.fastApiResponse ? JSON.parse(existingInvoice.fastApiResponse) : null,
            alreadyExists: true,
          };
        }
      }

      // Lấy thông tin đơn hàng
      const orderData = await this.findByOrderCode(docCode);

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(`Order ${docCode} not found or has no sales`);
      }

      // ============================================
      // BƯỚC 1: Kiểm tra docSourceType trước (ưu tiên cao nhất)
      // ============================================
      const firstSale = orderData.sales && orderData.sales.length > 0 ? orderData.sales[0] : null;
      const docSourceTypeRaw = firstSale?.docSourceType || orderData.docSourceType || '';
      const docSourceType = docSourceTypeRaw ? String(docSourceTypeRaw).trim().toUpperCase() : '';

      // Xử lý SALE_RETURN
      // Nhưng vẫn phải validate chỉ cho phép "01.Thường" và "01. Thường"
      if (docSourceType === 'SALE_RETURN') {
        // Validate chỉ cho phép "01.Thường" và "01. Thường"
        const validationResult = this.invoiceValidationService.validateOrderForInvoice({
          docCode,
          sales: orderData.sales,
        });
        if (!validationResult.success) {
          const errorMessage = validationResult.message || `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: undefined,
          });
          return {
            success: false,
            message: errorMessage,
            result: null,
          };
        }
        return await this.handleSaleReturnFlow(orderData, docCode);
      }

      // ============================================
      // BƯỚC 2: Validate điều kiện tạo hóa đơn TRƯỚC khi xử lý các case đặc biệt
      // ============================================
      // Validate chỉ cho phép "01.Thường" và "01. Thường" tạo hóa đơn
      // Các loại đơn đặc biệt (03. Đổi điểm, 04. Đổi DV, 05. Tặng sinh nhật) được xử lý riêng
      const sales = orderData.sales || [];
      const normalizeOrderType = (ordertypeName: string | null | undefined): string => {
        if (!ordertypeName) return '';
        return String(ordertypeName).trim().toLowerCase();
      };

      // Kiểm tra các loại đơn đặc biệt được phép xử lý
      const hasDoiDiemOrder = sales.some((s: any) =>
        this.isDoiDiemOrder(s.ordertype, s.ordertypeName)
      );
      const hasDoiDvOrder = sales.some((s: any) =>
        this.isDoiDvOrder(s.ordertype, s.ordertypeName)
      );
      const hasTangSinhNhatOrder = sales.some((s: any) =>
        this.isTangSinhNhatOrder(s.ordertype, s.ordertypeName)
      );
      const hasDauTuOrder = sales.some((s: any) =>
        this.isDauTuOrder(s.ordertype, s.ordertypeName)
      );
      const hasTachTheOrder = sales.some((s: any) =>
        this.isTachTheOrder(s.ordertype, s.ordertypeName)
      );
      const hasDoiVoOrder = sales.some((s: any) =>
        this.isDoiVoOrder(s.ordertype, s.ordertypeName)
      );
      const hasServiceOrder = sales.some((s: any) => {
        const normalized = normalizeOrderType(s.ordertypeName || s.ordertype);
        return normalized === '02. làm dịch vụ' || normalized === '02.làm dịch vụ';
      });

      // Nếu không phải các loại đơn đặc biệt được phép, validate chỉ cho phép "01.Thường"
      if (!hasDoiDiemOrder && !hasDoiDvOrder && !hasTangSinhNhatOrder && !hasDauTuOrder && !hasTachTheOrder && !hasDoiVoOrder && !hasServiceOrder) {
        const validationResult = this.invoiceValidationService.validateOrderForInvoice({
          docCode,
          sales: orderData.sales,
        });

        if (!validationResult.success) {
          const errorMessage = validationResult.message || `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: null,
          };
        }
      }

      // ============================================
      // BƯỚC 3: Xử lý các case đặc biệt (sau khi đã validate)
      // ============================================

      // Nếu là đơn dịch vụ, chạy flow dịch vụ
      if (hasServiceOrder) {
        return await this.executeServiceOrderFlow(orderData, docCode);
      }

      // Nếu là đơn "03. Đổi điểm", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDoiDiemOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult = await this.fastApiInvoiceFlowService.createSalesOrder({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          }, 0); // action = 0 cho đơn "03. Đổi điểm"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
              ...invoiceData,
              customer: orderData.customer,
              ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
            });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage = 'Tạo sales invoice thất bại (03. Đổi điểm)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage = errorData[0].message || errorData[0].error || salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(`03. Đổi điểm sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`);
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult ? 1 : (salesOrderResult ? 0 : 0);
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (03. Đổi điểm)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (03. Đổi điểm)'
              : 'Tạo sales order và sales invoice thất bại (03. Đổi điểm)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (03. Đổi điểm)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (03. Đổi điểm)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (03. Đổi điểm)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

          this.logger.error(`03. Đổi điểm order creation failed for order ${docCode}: ${errorMessage}`);

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data ? JSON.stringify(error.response.data) : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "04. Đổi DV", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDoiDvOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult = await this.fastApiInvoiceFlowService.createSalesOrder({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          }, 0); // action = 0 cho đơn "04. Đổi DV"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
              ...invoiceData,
              customer: orderData.customer,
              ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
            });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage = 'Tạo sales invoice thất bại (04. Đổi DV)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage = errorData[0].message || errorData[0].error || salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(`04. Đổi DV sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`);
          }

          // Bước 3: Xử lý cashio payment (Phiếu thu tiền mặt/Giấy báo có) nếu salesInvoice thành công
          let cashioResult: any = null;
          if (salesInvoiceResult) {
            this.logger.log(`[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (04. Đổi DV)`);
            cashioResult = await this.fastApiInvoiceFlowService.processCashioPayment(
              docCode,
              orderData,
              invoiceData,
            );

            if (cashioResult.cashReceiptResults && cashioResult.cashReceiptResults.length > 0) {
              this.logger.log(`[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (04. Đổi DV)`);
            }
            if (cashioResult.creditAdviceResults && cashioResult.creditAdviceResults.length > 0) {
              this.logger.log(`[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (04. Đổi DV)`);
            }

            // Xử lý payment (Phiếu chi tiền mặt) nếu có mã kho
            // CHỈ xử lý cho đơn có docSourceType = ORDER_RETURN hoặc SALE_RETURN
            try {
              // Kiểm tra docSourceType - chỉ xử lý payment cho ORDER_RETURN hoặc SALE_RETURN
              const firstSale = orderData.sales && orderData.sales.length > 0 ? orderData.sales[0] : null;
              const docSourceTypeRaw = firstSale?.docSourceType || orderData.docSourceType || '';
              const docSourceType = docSourceTypeRaw ? String(docSourceTypeRaw).trim().toUpperCase() : '';

              if (docSourceType === 'ORDER_RETURN' || docSourceType === 'SALE_RETURN') {
                const docCodesForStockTransfer = this.getDocCodesForStockTransfer([docCode]);
                const stockTransfers = await this.stockTransferRepository.find({
                  where: { soCode: In(docCodesForStockTransfer) },
                });
                const stockCodes = Array.from(new Set(stockTransfers.map(st => st.stockCode).filter(Boolean)));

                if (stockCodes.length > 0) {
                  this.logger.log(`[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (04. Đổi DV, docSourceType: ${docSourceType}) với ${stockCodes.length} mã kho`);
                  const paymentResult = await this.fastApiInvoiceFlowService.processPayment(
                    docCode,
                    orderData,
                    invoiceData,
                    stockCodes,
                  );

                  if (paymentResult.paymentResults && paymentResult.paymentResults.length > 0) {
                    this.logger.log(`[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (04. Đổi DV)`);
                  }
                  if (paymentResult.debitAdviceResults && paymentResult.debitAdviceResults.length > 0) {
                    this.logger.log(`[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (04. Đổi DV)`);
                  }
                } else {
                  this.logger.debug(`[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`);
                }
              } else {
                this.logger.debug(`[Payment] Đơn hàng ${docCode} (04. Đổi DV) có docSourceType = "${docSourceType}", không phải ORDER_RETURN/SALE_RETURN, bỏ qua payment API`);
              }
            } catch (paymentError: any) {
              // Log lỗi nhưng không fail toàn bộ flow
              this.logger.warn(`[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${paymentError?.message || paymentError}`);
            }
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult ? 1 : (salesOrderResult ? 0 : 0);
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (04. Đổi DV)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (04. Đổi DV)'
              : 'Tạo sales order và sales invoice thất bại (04. Đổi DV)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
              cashio: cashioResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (04. Đổi DV)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (04. Đổi DV)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
              cashio: cashioResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (04. Đổi DV)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

          this.logger.error(`04. Đổi DV order creation failed for order ${docCode}: ${errorMessage}`);

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data ? JSON.stringify(error.response.data) : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "05. Tặng sinh nhật", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasTangSinhNhatOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult = await this.fastApiInvoiceFlowService.createSalesOrder({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          }, 0); // action = 0 cho đơn "05. Tặng sinh nhật"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
              ...invoiceData,
              customer: orderData.customer,
              ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
            });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage = 'Tạo sales invoice thất bại (05. Tặng sinh nhật)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage = errorData[0].message || errorData[0].error || salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(`05. Tặng sinh nhật sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`);
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult ? 1 : (salesOrderResult ? 0 : 0);
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (05. Tặng sinh nhật)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (05. Tặng sinh nhật)'
              : 'Tạo sales order và sales invoice thất bại (05. Tặng sinh nhật)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (05. Tặng sinh nhật)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (05. Tặng sinh nhật)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (05. Tặng sinh nhật)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

          this.logger.error(`05. Tặng sinh nhật order creation failed for order ${docCode}: ${errorMessage}`);

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data ? JSON.stringify(error.response.data) : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "06. Đầu tư", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDauTuOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult = await this.fastApiInvoiceFlowService.createSalesOrder({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          }, 0); // action = 0 cho đơn "06. Đầu tư"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
              ...invoiceData,
              customer: orderData.customer,
              ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
            });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage = 'Tạo sales invoice thất bại (06. Đầu tư)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage = errorData[0].message || errorData[0].error || salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(`06. Đầu tư sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`);
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult ? 1 : (salesOrderResult ? 0 : 0);
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (06. Đầu tư)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (06. Đầu tư)'
              : 'Tạo sales order và sales invoice thất bại (06. Đầu tư)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (06. Đầu tư)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (06. Đầu tư)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (06. Đầu tư)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

          this.logger.error(`06. Đầu tư order creation failed for order ${docCode}: ${errorMessage}`);

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data ? JSON.stringify(error.response.data) : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "08. Tách thẻ", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasTachTheOrder) {
        // Gọi API get_card để lấy issue_partner_code cho đơn "08. Tách thẻ"
        await this.fetchCardDataAndMapIssuePartnerCode(docCode, orderData.sales || []);

        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult = await this.fastApiInvoiceFlowService.createSalesOrder({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          }, 0); // action = 0 cho đơn "08. Tách thẻ"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
              ...invoiceData,
              customer: orderData.customer,
              ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
            });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage = 'Tạo sales invoice thất bại (08. Tách thẻ)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage = errorData[0].message || errorData[0].error || salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(`08. Tách thẻ sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`);
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult ? 1 : (salesOrderResult ? 0 : 0);
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (08. Tách thẻ)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (08. Tách thẻ)'
              : 'Tạo sales order và sales invoice thất bại (08. Tách thẻ)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (08. Tách thẻ)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (08. Tách thẻ)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (08. Tách thẻ)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

          this.logger.error(`08. Tách thẻ order creation failed for order ${docCode}: ${errorMessage}`);

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data ? JSON.stringify(error.response.data) : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "Đổi vỏ", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDoiVoOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult = await this.fastApiInvoiceFlowService.createSalesOrder({
            ...invoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
          }, 0); // action = 0 cho đơn "Đổi vỏ"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
              ...invoiceData,
              customer: orderData.customer,
              ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
            });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage = 'Tạo sales invoice thất bại (Đổi vỏ)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage = errorData[0].message || errorData[0].error || salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(`Đổi vỏ sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`);
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult ? 1 : (salesOrderResult ? 0 : 0);
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (Đổi vỏ)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (Đổi vỏ)'
              : 'Tạo sales order và sales invoice thất bại (Đổi vỏ)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (Đổi vỏ)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (Đổi vỏ)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (Đổi vỏ)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

          this.logger.error(`Đổi vỏ order creation failed for order ${docCode}: ${errorMessage}`);

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data ? JSON.stringify(error.response.data) : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu không phải các loại đơn đặc biệt, chạy flow bình thường (01.Thường)
      // Validation đã được thực hiện ở trên, nên ở đây chỉ cần xử lý flow bình thường

      // Build invoice data
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Gọi API tạo đơn hàng
      let result: any;
      try {
        result = await this.fastApiInvoiceFlowService.executeFullInvoiceFlow({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });
      } catch (error: any) {
        // Lấy thông báo lỗi chính xác từ Fast API response
        let errorMessage = 'Tạo hóa đơn thất bại';

        if (error?.response?.data) {
          // Fast API trả về lỗi trong response.data
          const errorData = error.response.data;
          if (Array.isArray(errorData) && errorData.length > 0) {
            errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

        // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
        await this.saveFastApiInvoice({
          docCode,
          maDvcs: invoiceData.ma_dvcs,
          maKh: invoiceData.ma_kh,
          tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
          ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
          status: 0,
          message: errorMessage,
          guid: null,
          fastApiResponse: JSON.stringify(error?.response?.data || error),
        });

        this.logger.error(`Invoice creation failed for order ${docCode}: ${errorMessage}`);

        return {
          success: false,
          message: errorMessage,
          result: error?.response?.data || error,
        };
      }

      // FIX: Check response từ Fast API
      // Response thành công: [{ status: 1, message: "OK", guid: [...] }]
      // Response lỗi: [] hoặc [{ status: 0, message: "..." }]
      let isSuccess = false;
      let responseStatus = 0;
      let responseMessage = 'Tạo hóa đơn thất bại';
      let responseGuid: string | null = null;

      if (Array.isArray(result)) {
        if (result.length === 0) {
          // Mảng rỗng = thất bại
          isSuccess = false;
          responseStatus = 0;
          responseMessage = 'Fast API trả về mảng rỗng - tạo hóa đơn thất bại';
        } else {
          // Kiểm tra phần tử đầu tiên
          const firstItem = result[0];
          if (firstItem.status === 1) {
            // status === 1 = thành công
            isSuccess = true;
            responseStatus = 1;
            const apiMessage = firstItem.message || '';
            const shouldUseApiMessage = apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
            responseMessage = shouldUseApiMessage
              ? `Tạo hóa đơn thành công cho đơn hàng ${docCode}. ${apiMessage}`
              : `Tạo hóa đơn thành công cho đơn hàng ${docCode}`;
            responseGuid = Array.isArray(firstItem.guid) ? firstItem.guid[0] : firstItem.guid || null;
          } else {
            // status === 0 hoặc khác = lỗi
            isSuccess = false;
            responseStatus = firstItem.status ?? 0;
            const apiMessage = firstItem.message || firstItem.error || '';
            const shouldUseApiMessage = apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
            responseMessage = shouldUseApiMessage
              ? `Tạo hóa đơn thất bại cho đơn hàng ${docCode}. ${apiMessage}`
              : `Tạo hóa đơn thất bại cho đơn hàng ${docCode}`;
          }
        }
      } else if (result && typeof result === 'object') {
        // Nếu result không phải mảng
        if (result.status === 1) {
          isSuccess = true;
          responseStatus = 1;
          const apiMessage = result.message || '';
          const shouldUseApiMessage = apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
          responseMessage = shouldUseApiMessage
            ? `Tạo hóa đơn thành công cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hóa đơn thành công cho đơn hàng ${docCode}`;
          responseGuid = result.guid || null;
        } else {
          isSuccess = false;
          responseStatus = result.status ?? 0;
          const apiMessage = result.message || result.error || '';
          const shouldUseApiMessage = apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
          responseMessage = shouldUseApiMessage
            ? `Tạo hóa đơn thất bại cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hóa đơn thất bại cho đơn hàng ${docCode}`;
        }
      } else {
        // Fallback: không có result hoặc result không hợp lệ
        isSuccess = false;
        responseStatus = 0;
        responseMessage = 'Fast API không trả về response hợp lệ';
      }

      // Lưu vào bảng kê hóa đơn (cả thành công và thất bại)
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: responseGuid || null,
        fastApiResponse: JSON.stringify(result),
      });

      // Xử lý cashio payment (cho đơn hàng "01. Thường", "07. Bán tài khoản" và khi tạo invoice thành công)
      let cashioResult: any = null;
      if (isSuccess) {
        const firstSale = orderData.sales && orderData.sales.length > 0 ? orderData.sales[0] : null;
        const ordertypeName = firstSale?.ordertypeName || firstSale?.ordertype || '';
        const normalizedOrderType = String(ordertypeName).trim();
        const isNormalOrder = normalizedOrderType === '01.Thường' || normalizedOrderType === '01. Thường';
        const isBanTaiKhoanOrder = normalizedOrderType.includes('07. Bán tài khoản') || normalizedOrderType.includes('07.Bán tài khoản');

        if (isNormalOrder || isBanTaiKhoanOrder) {
          const orderTypeLabel = isNormalOrder ? '01. Thường' : '07. Bán tài khoản';
          this.logger.log(`[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (${orderTypeLabel})`);
          cashioResult = await this.fastApiInvoiceFlowService.processCashioPayment(
            docCode,
            orderData,
            invoiceData,
          );

          if (cashioResult.cashReceiptResults && cashioResult.cashReceiptResults.length > 0) {
            this.logger.log(`[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (${orderTypeLabel})`);
          }
          if (cashioResult.creditAdviceResults && cashioResult.creditAdviceResults.length > 0) {
            this.logger.log(`[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (${orderTypeLabel})`);
          }

          // Xử lý payment (Phiếu chi tiền mặt) nếu có mã kho
          // CHỈ xử lý cho đơn có docSourceType = ORDER_RETURN hoặc SALE_RETURN
          try {
            // Kiểm tra docSourceType - chỉ xử lý payment cho ORDER_RETURN hoặc SALE_RETURN
            const docSourceTypeRaw = firstSale?.docSourceType || orderData.docSourceType || '';
            const docSourceType = docSourceTypeRaw ? String(docSourceTypeRaw).trim().toUpperCase() : '';

            if (docSourceType === 'ORDER_RETURN' || docSourceType === 'SALE_RETURN') {
              const docCodesForStockTransfer = this.getDocCodesForStockTransfer([docCode]);
              const stockTransfers = await this.stockTransferRepository.find({
                where: { soCode: In(docCodesForStockTransfer) },
              });
              const stockCodes = Array.from(new Set(stockTransfers.map(st => st.stockCode).filter(Boolean)));

              if (stockCodes.length > 0) {
                this.logger.log(`[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (${orderTypeLabel}, docSourceType: ${docSourceType}) với ${stockCodes.length} mã kho`);
                const paymentResult = await this.fastApiInvoiceFlowService.processPayment(
                  docCode,
                  orderData,
                  invoiceData,
                  stockCodes,
                );

                if (paymentResult.paymentResults && paymentResult.paymentResults.length > 0) {
                  this.logger.log(`[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (${orderTypeLabel})`);
                }
                if (paymentResult.debitAdviceResults && paymentResult.debitAdviceResults.length > 0) {
                  this.logger.log(`[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (${orderTypeLabel})`);
                }
              } else {
                this.logger.debug(`[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`);
              }
            } else {
              this.logger.debug(`[Payment] Đơn hàng ${docCode} (${orderTypeLabel}) có docSourceType = "${docSourceType}", không phải ORDER_RETURN/SALE_RETURN, bỏ qua payment API`);
            }
          } catch (paymentError: any) {
            // Log lỗi nhưng không fail toàn bộ flow
            this.logger.warn(`[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${paymentError?.message || paymentError}`);
          }
        }
      }

      if (!isSuccess) {
        // Có lỗi từ Fast API
        this.logger.error(`Invoice creation failed for order ${docCode}: ${responseMessage}`);

        // Kiểm tra nếu là lỗi duplicate key - có thể đơn hàng đã tồn tại trong Fast API
        const isDuplicateError = responseMessage && (
          responseMessage.toLowerCase().includes('duplicate') ||
          responseMessage.toLowerCase().includes('primary key constraint') ||
          responseMessage.toLowerCase().includes('pk_d81')
        );

        if (isDuplicateError) {
          // Cập nhật status thành 1 (thành công) vì có thể đã tồn tại trong Fast API
          await this.saveFastApiInvoice({
            docCode,
            maDvcs: invoiceData.ma_dvcs,
            maKh: invoiceData.ma_kh,
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
            status: 1, // Coi như thành công vì đã tồn tại
            message: `Đơn hàng đã tồn tại trong Fast API: ${responseMessage}`,
            guid: responseGuid || null,
            fastApiResponse: JSON.stringify(result),
          });

          return {
            success: true,
            message: `Đơn hàng ${docCode} đã tồn tại trong Fast API (có thể đã được tạo trước đó)`,
            result,
            alreadyExists: true,
          };
        }

        return {
          success: false,
          message: responseMessage,
          result,
        };
      }

      // Đánh dấu đơn hàng là đã xử lý
      const markOrderAsProcessedResult = await this.markOrderAsProcessed(docCode);
      console.log('markOrderAsProcessedResult', markOrderAsProcessedResult);
      return {
        success: true,
        message: `Tạo hóa đơn ${docCode} thành công`,
        result,
      };
    } catch (error: any) {
      this.logger.error(`Error creating invoice for order ${docCode}: ${error?.message || error}`);
      this.logger.error(`Error stack: ${error?.stack || 'No stack trace'}`);

      throw error;
    }
  }

  /**
   * Xử lý flow tạo hóa đơn cho đơn hàng dịch vụ (02. Làm dịch vụ)
   * Flow:
   * 1. Customer (tạo/cập nhật)
   * 2. SalesOrder (tất cả dòng: I, S, V...)
   * 3. SalesInvoice (chỉ dòng productType = 'S')
   * 4. GxtInvoice (S → detail, I → ndetail)
   */
  private async executeServiceOrderFlow(orderData: any, docCode: string): Promise<any> {
    try {
      this.logger.log(`[ServiceOrderFlow] Bắt đầu xử lý đơn dịch vụ ${docCode}`);

      const sales = orderData.sales || [];
      if (sales.length === 0) {
        throw new Error(`Đơn hàng ${docCode} không có sale item nào`);
      }

      // Step 1: Tạo/cập nhật Customer
      if (orderData.customer?.code) {
        await this.fastApiInvoiceFlowService.createOrUpdateCustomer({
          ma_kh: this.normalizeMaKh(orderData.customer.code),
          ten_kh: orderData.customer.name || '',
          dia_chi: orderData.customer.address || undefined,
          dien_thoai: orderData.customer.mobile || orderData.customer.phone || undefined,
          so_cccd: orderData.customer.idnumber || undefined,
          ngay_sinh: orderData.customer.birthday
            ? this.formatDateYYYYMMDD(orderData.customer.birthday)
            : undefined,
          gioi_tinh: orderData.customer.sexual || undefined,
        });
      }

      // Build invoice data cho tất cả sales (dùng để tạo SalesOrder)
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Step 2: Tạo SalesOrder cho TẤT CẢ dòng (I, S, V...)
      this.logger.log(`[ServiceOrderFlow] Tạo SalesOrder cho ${sales.length} dòng`);
      await this.fastApiInvoiceFlowService.createSalesOrder({
        ...invoiceData,
        customer: orderData.customer,
        ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
      });

      // Step 3: Tạo SalesInvoice CHỈ cho productType = 'S'
      const serviceLines = sales.filter((s: any) => {
        const productType = (s.producttype).toUpperCase().trim();
        return productType === 'S';
      });

      let salesInvoiceResult: any = null;
      if (serviceLines.length > 0) {
        this.logger.log(`[ServiceOrderFlow] Tạo SalesInvoice cho ${serviceLines.length} dòng dịch vụ (productType = 'S')`);

        // Build invoice data chỉ cho service lines
        const serviceInvoiceData = await this.buildFastApiInvoiceDataForServiceLines(orderData, serviceLines);

        salesInvoiceResult = await this.fastApiInvoiceFlowService.createSalesInvoice({
          ...serviceInvoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || serviceInvoiceData.ong_ba || '',
        });
      } else {
        this.logger.log(`[ServiceOrderFlow] Không có dòng dịch vụ (productType = 'S'), bỏ qua SalesInvoice`);
      }

      // Step 3.5: Xử lý cashio payment (Phiếu thu tiền mặt/Giấy báo có) nếu salesInvoice thành công
      let cashioResult: any = null;
      if (salesInvoiceResult) {
        this.logger.log(`[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (02. Làm dịch vụ)`);
        cashioResult = await this.fastApiInvoiceFlowService.processCashioPayment(
          docCode,
          orderData,
          invoiceData,
        );

        if (cashioResult.cashReceiptResults && cashioResult.cashReceiptResults.length > 0) {
          this.logger.log(`[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`);
        }
        if (cashioResult.creditAdviceResults && cashioResult.creditAdviceResults.length > 0) {
          this.logger.log(`[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`);
        }

        // Xử lý payment (Phiếu chi tiền mặt) nếu có mã kho
        // CHỈ xử lý cho đơn có docSourceType = ORDER_RETURN hoặc SALE_RETURN
        try {
          // Kiểm tra docSourceType - chỉ xử lý payment cho ORDER_RETURN hoặc SALE_RETURN
          const firstSale = orderData.sales && orderData.sales.length > 0 ? orderData.sales[0] : null;
          const docSourceTypeRaw = firstSale?.docSourceType || orderData.docSourceType || '';
          const docSourceType = docSourceTypeRaw ? String(docSourceTypeRaw).trim().toUpperCase() : '';

          if (docSourceType === 'ORDER_RETURN' || docSourceType === 'SALE_RETURN') {
            const docCodesForStockTransfer = this.getDocCodesForStockTransfer([docCode]);
            const stockTransfers = await this.stockTransferRepository.find({
              where: { soCode: In(docCodesForStockTransfer) },
            });
            const stockCodes = Array.from(new Set(stockTransfers.map(st => st.stockCode).filter(Boolean)));

            if (stockCodes.length > 0) {
              this.logger.log(`[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (02. Làm dịch vụ, docSourceType: ${docSourceType}) với ${stockCodes.length} mã kho`);
              const paymentResult = await this.fastApiInvoiceFlowService.processPayment(
                docCode,
                orderData,
                invoiceData,
                stockCodes,
              );

              if (paymentResult.paymentResults && paymentResult.paymentResults.length > 0) {
                this.logger.log(`[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`);
              }
              if (paymentResult.debitAdviceResults && paymentResult.debitAdviceResults.length > 0) {
                this.logger.log(`[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`);
              }
            } else {
              this.logger.debug(`[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`);
            }
          } else {
            this.logger.debug(`[Payment] Đơn hàng ${docCode} (02. Làm dịch vụ) có docSourceType = "${docSourceType}", không phải ORDER_RETURN/SALE_RETURN, bỏ qua payment API`);
          }
        } catch (paymentError: any) {
          // Log lỗi nhưng không fail toàn bộ flow
          this.logger.warn(`[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${paymentError?.message || paymentError}`);
        }
      }

      // Step 4: Tạo GxtInvoice (S → detail, I → ndetail)
      const importLines = sales.filter((s: any) => {
        const productType = (s.producttype || s.productType || '').toUpperCase().trim();
        return productType === 'S';
      });

      const exportLines = sales.filter((s: any) => {
        const productType = (s.producttype || s.productType || '').toUpperCase().trim();
        return productType === 'I';
      });

      // Log để đảm bảo tất cả dòng đều được xử lý
      this.logger.log(
        `[ServiceOrderFlow] Tổng số dòng: ${sales.length}, ` +
        `Dòng S (nhập): ${importLines.length}, ` +
        `Dòng I (xuất): ${exportLines.length}, ` +
        `Dòng khác: ${sales.length - importLines.length - exportLines.length}`
      );

      let gxtInvoiceResult: any = null;
      if (importLines.length > 0 || exportLines.length > 0) {
        this.logger.log(
          `[ServiceOrderFlow] Tạo GxtInvoice: ${exportLines.length} dòng xuất (I) → detail, ${importLines.length} dòng nhập (S) → ndetail`
        );

        const gxtInvoiceData = await this.buildGxtInvoiceData(orderData, importLines, exportLines);

        // Log payload để verify
        this.logger.log(
          `[ServiceOrderFlow] GxtInvoice payload: detail có ${gxtInvoiceData.detail?.length || 0} dòng, ` +
          `ndetail có ${gxtInvoiceData.ndetail?.length || 0} dòng`
        );

        gxtInvoiceResult = await this.fastApiInvoiceFlowService.createGxtInvoice(gxtInvoiceData);
      } else {
        this.logger.log(`[ServiceOrderFlow] Không có dòng S hoặc I, bỏ qua GxtInvoice`);
      }

      // Lưu vào bảng kê hóa đơn
      const responseStatus = salesInvoiceResult ? 1 : 0;
      const responseMessage = salesInvoiceResult
        ? `Tạo hóa đơn dịch vụ ${docCode} thành công`
        : `Tạo SalesOrder thành công, không có dòng dịch vụ để tạo SalesInvoice`;

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: null,
        fastApiResponse: JSON.stringify({
          salesOrder: 'success',
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
        }),
      });

      // Đánh dấu đơn hàng là đã xử lý
      await this.markOrderAsProcessed(docCode);

      return {
        success: true,
        message: responseMessage,
        result: {
          salesOrder: 'success',
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
        },
      };
    } catch (error: any) {
      this.logger.error(`[ServiceOrderFlow] Lỗi khi xử lý đơn dịch vụ ${docCode}: ${error?.message || error}`);

      // Lưu lỗi vào bảng kê hóa đơn
      const invoiceData = await this.buildFastApiInvoiceData(orderData).catch(() => ({
        ma_dvcs: orderData.branchCode || '',
        ma_kh: this.normalizeMaKh(orderData.customer?.code),
        ong_ba: orderData.customer?.name || '',
        ngay_ct: orderData.docDate ? new Date(orderData.docDate) : new Date(),
      }));

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs || orderData.branchCode || '',
        maKh: invoiceData.ma_kh || this.normalizeMaKh(orderData.customer?.code),
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
        status: 0,
        message: error?.message || 'Tạo hóa đơn dịch vụ thất bại',
        guid: null,
        fastApiResponse: JSON.stringify(error?.response?.data || error),
      });

      throw error;
    }
  }

  /**
   * Build invoice data chỉ cho service lines (productType = 'S')
   */
  private async buildFastApiInvoiceDataForServiceLines(orderData: any, serviceLines: any[]): Promise<any> {
    // Tạo orderData mới chỉ chứa service lines
    const serviceOrderData = {
      ...orderData,
      sales: serviceLines,
    };

    // Dùng lại logic buildFastApiInvoiceData nhưng với orderData đã filter
    return await this.buildFastApiInvoiceData(serviceOrderData);
  }

  /**
   * Build GxtInvoice data (Phiếu tạo gộp – xuất tách)
   * - detail: các dòng productType = 'I' (xuất)
   * - ndetail: các dòng productType = 'S' (nhập)
   */
  private async buildGxtInvoiceData(orderData: any, importLines: any[], exportLines: any[]): Promise<any> {
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      if (isNaN(d.getTime())) {
        throw new Error('Invalid date');
      }
      return d.toISOString();
    };

    let docDate: Date;
    if (orderData.docDate instanceof Date) {
      docDate = orderData.docDate;
    } else if (typeof orderData.docDate === 'string') {
      docDate = new Date(orderData.docDate);
      if (isNaN(docDate.getTime())) {
        docDate = new Date();
      }
    } else {
      docDate = new Date();
    }

    const ngayCt = formatDateISO(docDate);
    const ngayLct = formatDateISO(docDate);

    const firstSale = orderData.sales?.[0] || {};
    const maDvcs =
      firstSale?.department?.ma_dvcs ||
      firstSale?.department?.ma_dvcs_ht ||
      orderData.customer?.brand ||
      orderData.branchCode ||
      '';

    // Helper để build detail/ndetail item
    const buildLineItem = async (sale: any, index: number): Promise<any> => {
      const toNumber = (value: any, defaultValue: number = 0): number => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        const num = Number(value);
        return isNaN(num) ? defaultValue : num;
      };

      const toString = (value: any, defaultValue: string = ''): string => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        return String(value);
      };

      const limitString = (value: string, maxLength: number): string => {
        if (!value) return '';
        const str = String(value);
        return str.length > maxLength ? str.substring(0, maxLength) : str;
      };

      const qty = toNumber(sale.qty, 0);
      const giaBan = toNumber(sale.giaBan, 0);
      const tienHang = toNumber(sale.tienHang || sale.linetotal || sale.revenue, 0);
      const giaNt2 = giaBan > 0 ? giaBan : (qty > 0 ? tienHang / qty : 0);
      const tienNt2 = qty * giaNt2;

      // Lấy materialCode từ Loyalty API
      const materialCode = this.getMaterialCode(sale, sale.product) || sale.itemCode || '';
      const dvt = toString(sale.product?.dvt || sale.product?.unit || sale.dvt, 'Cái');
      const maLo = toString(sale.maLo || sale.ma_lo, '');
      const maBp = toString(
        sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
        ''
      );

      return {
        ma_vt: limitString(materialCode, 16),
        dvt: limitString(dvt, 32),
        ma_lo: limitString(maLo, 16),
        so_luong: Math.abs(qty), // Lấy giá trị tuyệt đối
        gia_nt2: Number(giaNt2),
        tien_nt2: Number(tienNt2),
        ma_nx: 'NX01', // Fix cứng theo yêu cầu
        ma_bp: limitString(maBp, 8),
        dong: index + 1, // Số thứ tự dòng tăng dần (1, 2, 3...)
        dong_vt_goc: 1, // Dòng vật tư gốc luôn là 1
      };
    };

    // Build detail (xuất - productType = 'I')
    const detail = await Promise.all(exportLines.map((sale, index) => buildLineItem(sale, index)));

    // Build ndetail (nhập - productType = 'S')
    const ndetail = await Promise.all(importLines.map((sale, index) => buildLineItem(sale, index)));

    // Lấy kho nhập và kho xuất (có thể cần map từ branch/department)
    // Tạm thời dùng branchCode làm kho mặc định
    const maKhoN = orderData?.maKho || '';
    const maKhoX = orderData?.maKho || '';

    return {
      ma_dvcs: maDvcs,
      ma_kho_n: maKhoN,
      ma_kho_x: maKhoX,
      ong_ba: orderData.customer?.name || '',
      ma_gd: '2', // 1 = Tạo gộp, 2 = Xuất tách (có thể thay đổi theo rule)
      ngay_ct: ngayCt,
      ngay_lct: ngayLct,
      so_ct: orderData.docCode || '',
      dien_giai: orderData.docCode || '',
      action: 0, // 0: Mới, Sửa; 1: Xóa
      detail: detail,
      ndetail: ndetail,
    };
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
   * Build invoice data cho Fast API (format mới)
   */
  private async buildFastApiInvoiceData(orderData: any): Promise<any> {
    try {
      const toNumber = (value: any, defaultValue: number = 0): number => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        const num = Number(value);
        return isNaN(num) ? defaultValue : num;
      };

      // Format ngày theo ISO 8601 với milliseconds và Z
      const formatDateISO = (date: Date): string => {
        if (isNaN(date.getTime())) {
          throw new Error('Invalid date');
        }
        return date.toISOString();
      };

      // Format ngày
      let docDate: Date;
      if (orderData.docDate instanceof Date) {
        docDate = orderData.docDate;
      } else if (typeof orderData.docDate === 'string') {
        docDate = new Date(orderData.docDate);
        if (isNaN(docDate.getTime())) {
          docDate = new Date();
        }
      } else {
        docDate = new Date();
      }

      const minDate = new Date('1753-01-01T00:00:00');
      const maxDate = new Date('9999-12-31T23:59:59');
      if (docDate < minDate || docDate > maxDate) {
        throw new Error(`Date out of range for SQL Server: ${docDate.toISOString()}`);
      }

      const ngayCt = formatDateISO(docDate);
      const ngayLct = formatDateISO(docDate);

      // Lấy TẤT CẢ sales (giống logic printOrder - không filter)
      // Không filter theo dvt hay statusAsys - lấy tất cả như luồng tạo hóa đơn ban đầu
      const allSales = orderData.sales || [];

      // Nếu không có sale nào, throw error
      if (allSales.length === 0) {
        throw new Error(`Đơn hàng ${orderData.docCode} không có sale item nào, bỏ qua không đồng bộ`);
      }

      // Kiểm tra ordertype của đơn hàng (lấy từ sale đầu tiên)
      const firstSaleForOrderType = allSales[0];
      const ordertypeName = firstSaleForOrderType?.ordertypeName || firstSaleForOrderType?.ordertype || '';
      const normalizedOrderType = String(ordertypeName).trim();
      const isNormalOrder = normalizedOrderType === '01.Thường' || normalizedOrderType === '01. Thường';

      // Luôn fetch stock transfers để lấy mã kho (không chỉ cho "01.Thường")
      // Với đơn hàng "01.Thường": Lấy stock transfers để tính số lượng xuất kho
      // Với các đơn hàng khác: Dùng số lượng từ sale
      let stockTransferMapBySoCodeAndMaterialCode: Map<string, { st?: StockTransfer[]; rt?: StockTransfer[] }> = new Map();
      let allStockTransfers: StockTransfer[] = [];

      // Luôn fetch stock transfers để lấy mã kho
      const docCodesForStockTransfer = this.getDocCodesForStockTransfer([orderData.docCode]);
      allStockTransfers = await this.stockTransferRepository.find({
        where: { soCode: In(docCodesForStockTransfer) },
        order: { itemCode: 'ASC', createdAt: 'ASC' },
      });

      // Chỉ build map cho đơn hàng "01.Thường" (để tính số lượng)
      if (isNormalOrder) {
        const stockTransfers = allStockTransfers;

        // Fetch materialCode từ Loyalty API cho các itemCode trong stock transfers
        const stockTransferItemCodes = Array.from(
          new Set(
            stockTransfers
              .map((st) => st.itemCode)
              .filter((code): code is string => !!code && code.trim() !== '')
          )
        );

        const stockTransferLoyaltyMap = new Map<string, any>();
        if (stockTransferItemCodes.length > 0) {
          const fetchedProducts = await this.loyaltyService.fetchProducts(stockTransferItemCodes);
          fetchedProducts.forEach((product, itemCode) => {
            stockTransferLoyaltyMap.set(itemCode, product);
          });
        }

        // Tạo map: soCode_materialCode -> stock transfer (phân biệt ST và RT)
        stockTransfers.forEach((st) => {
          const materialCode = st.materialCode || stockTransferLoyaltyMap.get(st.itemCode)?.materialCode;
          if (!materialCode) {
            return;
          }

          const soCode = st.soCode || st.docCode || orderData.docCode;
          const key = `${soCode}_${materialCode}`;

          if (!stockTransferMapBySoCodeAndMaterialCode.has(key)) {
            stockTransferMapBySoCodeAndMaterialCode.set(key, {});
          }
          const itemMap = stockTransferMapBySoCodeAndMaterialCode.get(key)!;

          // ST* - xuất kho (qty < 0)
          if (st.docCode.startsWith('ST') || Number(st.qty || 0) < 0) {
            if (!itemMap.st) {
              itemMap.st = [];
            }
            itemMap.st.push(st);
          }
          // RT* - nhập lại (qty > 0)
          if (st.docCode.startsWith('RT') || Number(st.qty || 0) > 0) {
            if (!itemMap.rt) {
              itemMap.rt = [];
            }
            itemMap.rt.push(st);
          }
        });
      }

      // Xử lý từng sale với index để tính dong
      const detail = await Promise.all(allSales.map(async (sale: any, index: number) => {
        // Lấy materialCode từ sale (đã được enrich từ Loyalty API)
        const saleMaterialCode = sale.product?.materialCode || sale.product?.maVatTu || sale.product?.maERP;

        // Với đơn hàng "01.Thường": Lấy số lượng từ stock transfer xuất kho
        // Với các đơn hàng khác: Dùng số lượng từ sale
        let qty = toNumber(sale.qty, 0);
        const saleQty = toNumber(sale.qty, 0); // Lưu qty gốc từ sale để tính tỷ lệ phân bổ
        let allocationRatio = 1; // Tỷ lệ phân bổ (mặc định = 1 nếu không có stock transfer)

        if (isNormalOrder && saleMaterialCode) {
          const key = `${orderData.docCode}_${saleMaterialCode}`;
          const stockTransferInfo = stockTransferMapBySoCodeAndMaterialCode.get(key);
          const firstSt = stockTransferInfo?.st && stockTransferInfo.st.length > 0 ? stockTransferInfo.st[0] : null;

          if (firstSt && saleQty > 0) {
            // Lấy số lượng từ stock transfer xuất kho (lấy giá trị tuyệt đối vì qty xuất kho là số âm)
            qty = Math.abs(Number(firstSt.qty || 0));

            // Tính tỷ lệ phân bổ: qty (từ stock transfer) / saleQty (từ sale)
            // Ví dụ: mua 2 xuất 1 → tỷ lệ = 1/2 = 0.5
            allocationRatio = qty / saleQty;
          }
        }

        const tienHang = toNumber(sale.tienHang || sale.linetotal || sale.revenue, 0);
        let giaBan = toNumber(sale.giaBan, 0);

        // Kiểm tra đơn hàng "03. Đổi điểm" trước khi tính toán giá
        // Kiểm tra cả ordertype và ordertypeName
        const ordertypeNameForDoiDiem = sale.ordertypeName || '';
        const isDoiDiem = ordertypeNameForDoiDiem.includes('03. Đổi điểm') ||
          ordertypeNameForDoiDiem.includes('03.Đổi điểm') ||
          ordertypeNameForDoiDiem.includes('03.  Đổi điểm');
        const isDoiVo = sale.ordertypeName.toLowerCase().includes('đổi vỏ') || sale.ordertypeName.toLowerCase().includes('doi vo');
        const isDauTu = sale.ordertypeName.includes('06. Đầu tư') || sale.ordertypeName.includes('06.Đầu tư') || sale.ordertypeName.toLowerCase().includes('đầu tư') || sale.ordertypeName.toLowerCase().includes('dau tu');
        const isSinhNhat = sale.ordertypeName.includes('05. Tặng sinh nhật') || sale.ordertypeName.includes('05.Tặng sinh nhật') || sale.ordertypeName.toLowerCase().includes('tặng sinh nhật') || sale.ordertypeName.toLowerCase().includes('tang sinh nhat') || sale.ordertypeName.toLowerCase().includes('sinh nhật') || sale.ordertypeName.toLowerCase().includes('sinh nhat');
        const isThuong = sale.ordertypeName.includes('01.Thường') || sale.ordertypeName.includes('01. Thường') || sale.ordertypeName.includes('01.Thường') || sale.ordertypeName.toLowerCase().includes('thường') || sale.ordertypeName.toLowerCase().includes('thuong');
        // Nếu là đơn "03. Đổi điểm": set gia_ban = 0, tien_hang = 0
        if (isDoiDiem) {
          giaBan = 0;
        } else {
          // Tính gia_ban từ tien_hang và saleQty (luôn dùng qty từ sale để tính giá)
          if (giaBan === 0 && tienHang > 0 && saleQty > 0) {
            giaBan = tienHang / saleQty;
          }
        }

        // Với đơn hàng "01.Thường": Tính lại tien_hang = qty (từ stock transfer) * gia_ban (từ sale)
        // Với các đơn hàng khác: Giữ nguyên tien_hang từ sale
        // Nếu là đơn "03. Đổi điểm": set tien_hang = 0
        let tienHangGoc = isDoiDiem ? 0 : tienHang;
        if (!isDoiDiem && isNormalOrder && allocationRatio !== 1) {
          // Tính lại tien_hang = qty (từ stock transfer) * gia_ban (từ sale)
          if (qty > 0 && giaBan > 0) {
            tienHangGoc = qty * giaBan;
          } else {
            // Fallback: phân bổ theo tỷ lệ
            tienHangGoc = tienHang * allocationRatio;
          }
        }

        // Tính toán các chiết khấu (dùng let để có thể phân bổ lại cho "01.Thường")
        let ck01_nt = toNumber(sale.other_discamt || sale.chietKhauMuaHangGiamGia, 0);
        let ck02_nt = toNumber(sale.chietKhauCkTheoChinhSach, 0);
        let ck03_nt = toNumber(sale.chietKhauMuaHangCkVip || sale.grade_discamt, 0);

        // Tính VIP type nếu có chiết khấu VIP
        // Lấy brand từ orderData để phân biệt logic VIP
        const brand = orderData.customer?.brand || orderData.brand || '';
        const brandLower = this.normalizeBrand(brand);

        // Tính maCk03 - dùng hàm chung (lấy loyaltyProduct từ sale.product)
        const maCk03 = this.calculateMuaHangCkVip(sale, sale.product, brand, 'buildFastApiInvoiceData');
        // ma_ck04: Thanh toán coupon
        let ck04_nt = toNumber(sale.chietKhauThanhToanCoupon || sale.chietKhau09, 0);
        // ma_ck15: Voucher DP1 dự phòng - Ưu tiên kiểm tra trước
        let ck15_nt_voucherDp1 = toNumber(sale.chietKhauVoucherDp1, 0);
        const paidByVoucher = toNumber(sale.chietKhauThanhToanVoucher || sale.paid_by_voucher_ecode_ecoin_bp, 0);

        // Kiểm tra các điều kiện để xác định voucher dự phòng
        const pkgCode = (sale as any).pkg_code || (sale as any).pkgCode || null;
        let promCode = sale.promCode || sale.prom_code || null;
        promCode = await this.cutCode(promCode);
        if (sale.productType === 'I') {
          promCode = promCode + '.I';
        } else if (sale.productType === 'S') {
          promCode = promCode + '.S'
        } else if (sale.productType === 'V') {
          promCode = promCode + '.V'
        }
        const soSource = sale.order_source || (sale as any).so_source || null;

        const isShopee = soSource && String(soSource).toUpperCase() === 'SHOPEE';
        const hasPkgCode = pkgCode && pkgCode.trim() !== '';
        const hasPromCode = promCode && promCode.trim() !== '';

        // Xác định lại voucher dự phòng theo logic mới (để xử lý dữ liệu cũ đã sync với logic cũ)
        let isVoucherDuPhong = false;
        if (brandLower === 'f3') {
          // Với F3: Chỉ khi so_source = "SHOPEE" mới là voucher dự phòng
          isVoucherDuPhong = isShopee;
        } else {
          // Với các brand khác: SHOPEE hoặc (có prom_code và không có pkg_code)
          isVoucherDuPhong = isShopee || (hasPromCode && !hasPkgCode);
        }

        // Nếu có chietKhauVoucherDp1 > 0 nhưng theo logic mới không phải voucher dự phòng
        // → Chuyển sang voucher chính (dữ liệu cũ đã sync với logic cũ)
        // Lưu giá trị để chuyển sang voucher chính
        let voucherAmountToMove = 0;

        // ma_ck05: Thanh toán voucher chính
        // Chỉ map vào ck05_nt nếu không có voucher dự phòng (ck15_nt_voucherDp1 = 0)
        // Nếu có voucherAmountToMove (chuyển từ DP sang chính), dùng giá trị đó
        // Nếu không, dùng paidByVoucher
        // Nếu là đơn "03. Đổi điểm": bỏ ma_ck05 và ck05_nt (set = 0 và '')
        let ck05_nt = paidByVoucher > 0 ? paidByVoucher : 0;
        let maCk05Value: string | null = null;
        let formattedMaCk05: string | null = null;

        if (isDoiDiem) {
          ck15_nt_voucherDp1 = 0;
        };
        // Nếu là đơn "03. Đổi điểm": bỏ ma_ck05 và ck05_nt
        const isDoiDiemForCk05 = this.isDoiDiemOrder(sale.ordertype, sale.ordertypeName);

        if (isDoiDiem || isDoiDiemForCk05) {
          ck05_nt = 0;
          maCk05Value = null;
          formattedMaCk05 = null;
        } else {
          // Tính ma_ck05 giống frontend - truyền customer từ orderData nếu sale chưa có
          const saleWithCustomer = {
            ...sale,
            customer: sale.customer || orderData.customer,
            brand: sale.customer?.brand || orderData.customer?.brand  || ''
          };
          maCk05Value = this.calculateMaCk05(saleWithCustomer);
          formattedMaCk05 = maCk05Value;
        }
        const ck06_nt = 0; // Dự phòng 1 - không sử dụng (không phân bổ vì = 0)
        let ck07_nt = toNumber(sale.chietKhauVoucherDp2, 0);
        let ck08_nt = toNumber(sale.chietKhauVoucherDp3, 0);
        // Các chiết khấu từ 09-22 mặc định là 0
        let ck09_nt = toNumber(sale.chietKhau09, 0);
        let ck10_nt = toNumber(sale.chietKhau10, 0);
        // ck11_nt: Thanh toán TK tiền ảo
        // Chỉ map ECOIN nếu sale item có v_paid > 0 (từ paid_by_voucher_ecode_ecoin_bp hoặc chietKhauThanhToanTkTienAo)
        // Không map ECOIN cho items có v_paid = 0
        let ck11_nt = toNumber(sale.chietKhauThanhToanTkTienAo || sale.chietKhau11, 0);
        const vPaidForEcoin = toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0);

        // Chỉ lấy ECOIN từ cashioData nếu:
        // 1. Đã có chietKhauThanhToanTkTienAo > 0 (đã được lưu trong sync), HOẶC
        // 2. v_paid > 0 VÀ có ECOIN trong cashio
        if (ck11_nt === 0 && vPaidForEcoin > 0 && orderData.cashioData && Array.isArray(orderData.cashioData)) {
          const ecoinCashio = orderData.cashioData.find((c: any) => c.fop_syscode === 'ECOIN');
          if (ecoinCashio && ecoinCashio.total_in) {
            ck11_nt = toNumber(ecoinCashio.total_in, 0);
          }
        }
        let ck12_nt = toNumber(sale.chietKhau12, 0);
        let ck13_nt = toNumber(sale.chietKhau13, 0);
        let ck14_nt = toNumber(sale.chietKhau14, 0);
        let ck15_nt = ck15_nt_voucherDp1 > 0 ? ck15_nt_voucherDp1 : toNumber(sale.chietKhau15, 0);
        let ck16_nt = toNumber(sale.chietKhau16, 0);
        let ck17_nt = toNumber(sale.chietKhau17, 0);
        let ck18_nt = toNumber(sale.chietKhau18, 0);
        let ck19_nt = toNumber(sale.chietKhau19, 0);
        let ck20_nt = toNumber(sale.chietKhau20, 0);
        let ck21_nt = toNumber(sale.chietKhau21, 0);
        let ck22_nt = toNumber(sale.chietKhau22, 0);

        // Với đơn hàng "01.Thường": Phân bổ lại tất cả các khoản tiền theo tỷ lệ số lượng từ stock transfer
        // Tỷ lệ phân bổ = qty (từ stock transfer) / saleQty (từ sale)
        // Ví dụ: mua 2 xuất 1 → tỷ lệ = 1/2 = 0.5 → tất cả các khoản tiền nhân với 0.5
        // Lưu ý: Không phân bổ lại cho đơn "03. Đổi điểm" (đã set ck05_nt = 0)
        if (isNormalOrder && allocationRatio !== 1 && allocationRatio > 0 && !isDoiDiem && !isDoiDiemForCk05) {
          // Phân bổ lại tất cả các chiết khấu
          ck01_nt = ck01_nt * allocationRatio;
          ck02_nt = ck02_nt * allocationRatio;
          ck03_nt = ck03_nt * allocationRatio;
          ck04_nt = ck04_nt * allocationRatio;
          ck05_nt = ck05_nt * allocationRatio;
          ck07_nt = ck07_nt * allocationRatio;
          ck08_nt = ck08_nt * allocationRatio;
          ck09_nt = ck09_nt * allocationRatio;
          ck10_nt = ck10_nt * allocationRatio;
          ck11_nt = ck11_nt * allocationRatio;
          ck12_nt = ck12_nt * allocationRatio;
          ck13_nt = ck13_nt * allocationRatio;
          ck14_nt = ck14_nt * allocationRatio;
          ck15_nt = ck15_nt * allocationRatio;
          ck16_nt = ck16_nt * allocationRatio;
          ck17_nt = ck17_nt * allocationRatio;
          ck18_nt = ck18_nt * allocationRatio;
          ck19_nt = ck19_nt * allocationRatio;
          ck20_nt = ck20_nt * allocationRatio;
          ck21_nt = ck21_nt * allocationRatio;
          ck22_nt = ck22_nt * allocationRatio;

          this.logger.debug(
            `[Invoice] Đơn hàng ${orderData.docCode}, sale item ${sale.itemCode}: ` +
            `Phân bổ lại tất cả các khoản tiền theo tỷ lệ ${allocationRatio} (qty stock transfer=${qty} / qty sale=${saleQty})`
          );
        }

        // Tính tổng chiết khấu (sau khi đã phân bổ nếu có)
        const tongChietKhau = ck01_nt + ck02_nt + ck03_nt + ck04_nt + ck05_nt + ck06_nt + ck07_nt + ck08_nt +
          ck09_nt + ck10_nt + ck11_nt + ck12_nt + ck13_nt + ck14_nt + ck15_nt + ck16_nt +
          ck17_nt + ck18_nt + ck19_nt + ck20_nt + ck21_nt + ck22_nt;

        // Với đơn hàng "03. Đổi điểm": gia_ban và tien_hang luôn = 0, không tính lại
        // Với đơn hàng "01.Thường": tien_hang đã được tính từ qty (stock transfer) * giaBan ở trên
        // Với các đơn hàng khác: Tính tien_hang từ sale như bình thường
        if (!isDoiDiem && !isDoiDiemForCk05) {
          if (!isNormalOrder) {
            // tien_hang phải là giá gốc (trước chiết khấu)
            // Ưu tiên: mn_linetotal > linetotal > tienHang > (revenue + tongChietKhau)
            tienHangGoc = toNumber((sale as any).mn_linetotal || sale.linetotal || sale.tienHang, 0);
            if (tienHangGoc === 0) {
              // Nếu không có giá gốc, tính từ revenue + chiết khấu
              tienHangGoc = tienHang + tongChietKhau;
            }

            // Tính gia_ban: giá gốc (trước chiết khấu)
            // Nếu sale.giaBan đã có giá trị, dùng nó (đó là giá gốc)
            // Nếu không, tính từ tienHangGoc
            if (giaBan === 0 && qty > 0) {
              giaBan = tienHangGoc / qty;
            } else if (giaBan === 0 && tienHangGoc > 0 && qty > 0) {
              // Fallback: nếu không có chiết khấu, dùng tienHangGoc / qty
              giaBan = tienHangGoc / qty;
            }
          } else {
            // Với đơn hàng "01.Thường": tienHangGoc đã được tính từ qty * giaBan ở trên
            // Chỉ cần đảm bảo giaBan đã có giá trị
            if (giaBan === 0 && qty > 0 && tienHangGoc > 0) {
              giaBan = tienHangGoc / qty;
            }
          }
        }

        // Helper function để đảm bảo giá trị luôn là string, không phải null/undefined
        const toString = (value: any, defaultValue: string = ''): string => {
          if (value === null || value === undefined || value === '') {
            return defaultValue;
          }
          return String(value);
        };

        // Helper function để giới hạn độ dài string theo spec
        const limitString = (value: string, maxLength: number): string => {
          if (!value) return '';
          const str = String(value);
          return str.length > maxLength ? str.substring(0, maxLength) : str;
        };

        // Mỗi sale item xử lý riêng, không dùng giá trị mặc định chung
        // Lấy dvt từ product (đã được enrich từ Loyalty API) trước, sau đó mới lấy từ sale
        // Frontend: sale?.product?.dvt || sale?.dvt
        // Nếu không có thì dùng 'Cái' làm mặc định (Fast API yêu cầu field này phải có giá trị)
        const dvt = toString(sale.product?.dvt || sale.product?.unit || sale.dvt, 'Cái');

        // Lấy mã kho từ stock_transfers (chỉ lấy từ stock_transfers, không có thì để rỗng)
        // saleMaterialCode đã được khai báo ở trên (dòng 4234)
        // Tạo map đơn giản cho getMaKhoFromStockTransfer: docCode_materialCode -> StockTransfer[]
        const stockTransferMapForMaKho = new Map<string, StockTransfer[]>();
        if (saleMaterialCode) {
          const key = `${orderData.docCode}_${saleMaterialCode}`;
          const stockTransferInfo = stockTransferMapBySoCodeAndMaterialCode.get(key);
          if (stockTransferInfo?.st && stockTransferInfo.st.length > 0) {
            stockTransferMapForMaKho.set(key, stockTransferInfo.st);
          }
        }
        const maKho = await this.getMaKhoFromStockTransfer(sale, orderData.docCode, allStockTransfers, saleMaterialCode, stockTransferMapForMaKho);

        // Debug: Log maLo value từ sale
        if (index === 0) {
        }

        // Fetch trackSerial và trackBatch từ Loyalty API để xác định dùng ma_lo hay so_serial
        // Lấy materialCode từ Loyalty API (ưu tiên materialCode từ product, sau đó itemCode)
        const materialCode = this.getMaterialCode(sale, sale.product) || sale.itemCode;
        let trackSerial: boolean | null = null;
        let trackBatch: boolean | null = null;
        let trackInventory: boolean | null = null;
        let productTypeFromLoyalty: string | null = null;

        // Luôn fetch từ Loyalty API để lấy trackSerial, trackBatch, trackInventory và productType
        const loyaltyProduct = await this.loyaltyService.checkProduct(materialCode);
        if (loyaltyProduct) {
          trackSerial = loyaltyProduct.trackSerial === true;
          trackBatch = loyaltyProduct.trackBatch === true;
          trackInventory = loyaltyProduct.trackInventory === true;
          productTypeFromLoyalty = loyaltyProduct.productType || loyaltyProduct.producttype || null;
          // Update sale với thông tin từ Loyalty API
          sale.productType = productTypeFromLoyalty;
          sale.trackInventory = trackInventory;
          // Update sale.product với thông tin từ Loyalty API
          if (sale.product) {
            sale.product.productType = productTypeFromLoyalty;
            sale.product.producttype = productTypeFromLoyalty;
            sale.product.trackInventory = trackInventory;
          }
        }

        const productTypeUpper = productTypeFromLoyalty ? String(productTypeFromLoyalty).toUpperCase().trim() : null;

        // Lấy giá trị serial từ sale (tất cả đều lấy từ field "serial")
        const serialValue = toString(sale.serial || '', '');

        // Debug: Log trackSerial, trackBatch và serial để kiểm tra

        // Xác định có dùng ma_lo hay so_serial dựa trên trackSerial và trackBatch từ Loyalty API
        const useBatch = this.shouldUseBatch(trackBatch, trackSerial);

        // Lấy brand để phân biệt logic cho F3 (ma_lo)
        const brandForMaLo = orderData.customer?.brand || orderData.brand || '';
        let brandLowerForMaLo = (brandForMaLo || '').toLowerCase().trim();
        // Normalize: "facialbar" → "f3"
        if (brandLowerForMaLo === 'facialbar') {
          brandLowerForMaLo = 'f3';
        }

        // Xác định ma_lo và so_serial dựa trên trackSerial và trackBatch
        let maLo: string | null = null;
        let soSerial: string | null = null;

        if (useBatch) {
          // trackBatch = true → dùng ma_lo với giá trị serial
          if (serialValue && serialValue.trim() !== '') {
            // Với F3, lấy toàn bộ serial (không cắt, không xử lý)
            if (brandLowerForMaLo === 'f3') {
              maLo = serialValue;
            } else {
              // Kiểm tra nếu serial có dạng "XXX_YYYY" (có dấu gạch dưới), lấy phần sau dấu gạch dưới
              const underscoreIndex = serialValue.indexOf('_');
              if (underscoreIndex > 0 && underscoreIndex < serialValue.length - 1) {
                // Lấy phần sau dấu gạch dưới
                maLo = serialValue.substring(underscoreIndex + 1);
              } else {
                // Vẫn cần productType để quyết định cắt bao nhiêu ký tự
                if (productTypeUpper === 'TPCN') {
                  // Nếu productType là "TPCN", cắt lấy 8 ký tự cuối
                  maLo = serialValue.length >= 8 ? serialValue.slice(-8) : serialValue;
                } else if (productTypeUpper === 'SKIN' || productTypeUpper === 'GIFT') {
                  // Nếu productType là "SKIN" hoặc "GIFT", cắt lấy 4 ký tự cuối
                  maLo = serialValue.length >= 4 ? serialValue.slice(-4) : serialValue;
                } else {
                  // Các trường hợp khác → lấy 4 ký tự cuối (mặc định)
                  maLo = serialValue.length >= 4 ? serialValue.slice(-4) : serialValue;
                }
              }
            }
          } else {
            maLo = null;
          }
          soSerial = null;
        } else {
          // trackSerial = true và trackBatch = false
          // Nhưng vẫn cần kiểm tra xem có cần ma_lo không (nếu serial có dạng "XXX_YYYY")
          if (serialValue && serialValue.trim() !== '') {
            const underscoreIndex = serialValue.indexOf('_');
            if (underscoreIndex > 0 && underscoreIndex < serialValue.length - 1) {
              // Nếu serial có dạng "XXX_YYYY", dùng ma_lo với phần sau dấu gạch dưới
              maLo = serialValue.substring(underscoreIndex + 1);
              soSerial = null;
            } else {
              // Nếu không có dấu gạch dưới, dùng so_serial
              maLo = null;
              soSerial = serialValue;
            }
          } else {
            maLo = null;
            soSerial = null;
          }
        }

        // Log kết quả cuối cùng

        // Cảnh báo nếu không có serial nhưng trackSerial/trackBatch yêu cầu
        if (!serialValue || serialValue.trim() === '') {
          if (useBatch) {
          } else if (trackSerial) {
          }
        }

        const maThe = toString(sale.maThe || sale.mvc_serial, '');

        // loai_gd: Với đơn "04. Đổi DV" và "08. Tách thẻ":
        //   - Nếu số lượng âm (qty < 0) → loai_gd = '11'
        //   - Nếu số lượng dương (qty > 0) → loai_gd = '12'
        // Các đơn khác: dùng '01'
        const ordertypeNameForLoaiGd = sale.ordertype || sale.ordertypeName || '';
        const isDoiDv = this.isDoiDvOrder(sale.ordertype, sale.ordertypeName);
        const isTachThe = ordertypeNameForLoaiGd.includes('08. Tách thẻ') ||
          ordertypeNameForLoaiGd.includes('08.Tách thẻ') ||
          ordertypeNameForLoaiGd.includes('08.  Tách thẻ');
        let loaiGd = '01'; // Mặc định
        if (isDoiDv || isTachThe) {
          // Với đơn "04. Đổi DV" và "08. Tách thẻ", dùng số lượng gốc từ sale để xác định loai_gd
          const saleQtyForLoaiGd = toNumber(sale.qty, 0);
          if (saleQtyForLoaiGd < 0) {
            loaiGd = '11'; // Số lượng âm
          } else {
            loaiGd = '12'; // Số lượng dương
          }
        }

        const loai = toString(sale.loai || sale.cat1, '');

        // Lấy ma_bp - bắt buộc phải có giá trị
        const maBp = toString(
          sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
          ''
        );

        // Validate ma_bp - nếu vẫn empty thì log warning
        if (!maBp || maBp.trim() === '') {
        }

        // Xác định hàng tặng: giaBan = 0 và tienHang = 0
        let isTangHang = giaBan === 0 && tienHang === 0;

        // Các ordertype dịch vụ không được coi là hàng tặng (không set km_yn = 1)
        const ordertypeName = sale.ordertype || '';
        const isDichVu = ordertypeName.includes('02. Làm dịch vụ') ||
          ordertypeName.includes('04. Đổi DV') ||
          ordertypeName.includes('08. Tách thẻ') ||
          ordertypeName.includes('Đổi thẻ KEEP->Thẻ DV');
        if (isDichVu) {
          isTangHang = false;
        }

        // Tính toán ma_ctkm_th
        let maCtkmTangHang: string = '';
        if (isTangHang) {
          // Nếu đã có maCtkmTangHang từ findAllOrders (đã tính sẵn), dùng nó
          if (sale.maCtkmTangHang) {
            maCtkmTangHang = toString(sale.maCtkmTangHang, '');
          } else {
            // Nếu chưa có, tính toán lại
            const ordertypeName = sale.ordertype || '';
            if (ordertypeName.includes('06. Đầu tư') || ordertypeName.includes('06.Đầu tư')) {
              maCtkmTangHang = 'TT DAU TU';
            } else if (
              (ordertypeName.includes('01.Thường') || ordertypeName.includes('01. Thường')) ||
              (ordertypeName.includes('07. Bán tài khoản') || ordertypeName.includes('07.Bán tài khoản')) ||
              (ordertypeName.includes('9. Sàn TMDT') || ordertypeName.includes('9.Sàn TMDT'))
            ) {
              // Quy đổi prom_code sang TANGSP - lấy năm/tháng từ ngày đơn hàng
              // Dùng promCode trực tiếp thay vì convertPromCodeToTangSp
              maCtkmTangHang = toString(promCode);
              if (sale.productType === 'I') {
                maCtkmTangHang = maCtkmTangHang + '.I';
              } else if (sale.productType === 'S') {
                maCtkmTangHang = maCtkmTangHang + '.S';
              } else if (sale.productType === 'V') {
                maCtkmTangHang = maCtkmTangHang + '.V';
              }
            } else {
              // Các trường hợp khác: dùng promCode nếu có
              let promCode = await this.cutCode(sale.promCode || sale.prom_code || null);
              promCode = await this.cutCode(promCode);
              maCtkmTangHang = toString(promCode);
              if (sale.productType === 'I') {
                maCtkmTangHang = maCtkmTangHang + '.I';
              } else if (sale.productType === 'S') {
                maCtkmTangHang = maCtkmTangHang + '.S';
              } else if (sale.productType === 'V') {
                maCtkmTangHang = maCtkmTangHang + '.V';
              }
            }
          }
        } else {
          // Nếu không phải hàng tặng, dùng giá trị từ sale.maCtkmTangHang (nếu có)
          maCtkmTangHang = toString(sale.maCtkmTangHang, '');
        }

        // Nếu là hàng tặng, không set ma_ck01 (Mã CTKM mua hàng giảm giá)
        // Nếu là đơn "03. Đổi điểm": set ma_ck01 = "TT DIEM DO" và ck01_nt = 0
        // Nếu không phải hàng tặng và không phải "03. Đổi điểm", set ma_ck01 từ promCode như cũ
        let maCk01 = sale?.muaHangGiamGiaDisplay;
        // let maCk01 = isTangHang ? '' : (promCode ? promCode : '');
        if (isDoiDiem) {
          if (sale.cucThueDisplay === 'TTM' || sale.cucThueDisplay === 'AMA' || sale.cucThueDisplay === 'TSG') {
            maCtkmTangHang = 'TTM.KMDIEM';
          } else if (sale.cucThueDisplay === 'FBV') {
            maCtkmTangHang = 'FBV.KMDIEM';
          } else if (sale.cucThueDisplay === 'BTH') {
            maCtkmTangHang = 'BTH.KMDIEM';
          } else if (sale.cucThueDisplay === 'CDV') {
            maCtkmTangHang = 'CDV.KMDIEM';
          } else if (sale.cucThueDisplay === 'LHV') {
            maCtkmTangHang = 'LHV.KMDIEM';
          }
          ck01_nt = 0;
        }
        // else if (!isDoiDiem) {
        //   maCtkmTangHang = '';
        //   ck01_nt = 0;
        // }


        // Kiểm tra nếu ma_ctkm_th = "TT DAU TU" thì không set km_yn = 1
        const isTTDauTu = maCtkmTangHang && maCtkmTangHang.trim() === 'TT DAU TU';

        // Với đơn hàng "01.Thường": Phân bổ lại tiền thuế và tiền trợ giá theo tỷ lệ số lượng từ stock transfer
        let tienThue = toNumber(sale.tienThue, 0);
        let dtTgNt = toNumber(sale.dtTgNt, 0);

        if (isNormalOrder && allocationRatio !== 1 && allocationRatio > 0) {
          tienThue = tienThue * allocationRatio;
          dtTgNt = dtTgNt * allocationRatio;
        }

        let tkChietKhau: string | null = null;
        let tkChiPhi: string | null = null;
        let maPhi: string | null = null;
        // Lấy ma_vt từ materialCode (ưu tiên Loyalty API) - dùng lại materialCode đã fetch ở trên
        // materialCode đã được lấy từ getMaterialCode(sale) và fetch từ Loyalty API
        // Nếu có loyaltyProduct, ưu tiên dùng materialCode từ đó
        const finalMaterialCode = loyaltyProduct?.materialCode || sale.product?.maVatTu || '';

        //******************** */
        // Kiểm tra có mã CTKM tặng hàng không (chỉ maCtkmTangHang, không tính promCode)
        const hasMaCtkmTangHang = (maCtkmTangHang && maCtkmTangHang.trim() !== '') ||
          (sale.maCtkmTangHang && sale.maCtkmTangHang.trim() !== '');
        const isGiaBanZero = Math.abs(sale.giaBanGoc || 0) < 0.01;
        const isKmVip = toNumber(sale.kmVip, 0);

        const isTangSP = sale.tangSpDisplay || '';
        const hasVoucher = toNumber(sale.thanhToanVoucher, 0);
        const hasChietKhauMuaHangGiamGia = toNumber(sale.muaHangGiamGia, 0) > 0;
        const calculatedFields = await this.calculateSaleFields(sale, isDoiVo, isDoiDiem, isDauTu);
        const hasMaCtkm = (maCk01 && maCk01.trim() !== '') ||
          (sale.maCk01 && sale.maCk01.trim() !== '');


        if (isDoiVo || isDoiDiem || isDauTu) {
          // Với đơn "Đổi vỏ", "Đổi điểm", "Đầu tư":
          tkChietKhau = null; // Để rỗng
          tkChiPhi = '64191';
          maPhi = '161010';
        } else if (isSinhNhat) {
          // Với đơn "Sinh nhật":
          tkChietKhau = null; // Để rỗng
          tkChiPhi = '64192';
          maPhi = '162010';
        } else if (isThuong && hasMaCtkmTangHang && isGiaBanZero && calculatedFields.isTangHang) {
          // Với đơn "Thường" có đơn giá = 0, Khuyến mại = 1, và có thông tin mã tại "Mã CTKM tặng hàng":
          tkChiPhi = '64191';
          maPhi = '161010';
          // tkChietKhau giữ nguyên (có thể được set bởi các điều kiện khác hoặc null)
          if (tkChietKhau === null) {
            tkChietKhau = sale.tkChietKhau || null;
          }
        } else if (isKmVip > 0 && productTypeUpper === 'I') {
          tkChietKhau = '521113';
        } else if (isKmVip > 0 && productTypeUpper === 'S') {
          tkChietKhau = '521132';
        }
        else if (hasVoucher > 0 && isTangSP === 'GIFT') {
          tkChietKhau = '5211631';
        }
        else if (hasVoucher > 0 && productTypeUpper === 'I') {
          tkChietKhau = '5211611';

        } else if (hasVoucher > 0 && productTypeUpper === 'S') {
          tkChietKhau = '5211621';
        }
        else if (hasChietKhauMuaHangGiamGia && productTypeUpper === 'S') {
          // Với đơn có "Chiết khấu mua hàng giảm giá" có giá trị và loại hàng hóa = S (Dịch vụ):
          tkChietKhau = '521131';
          tkChiPhi = sale.tkChiPhi || null;
          maPhi = sale.maPhi || null;
        } else if (isThuong && hasChietKhauMuaHangGiamGia && productTypeUpper === 'I') {
          // Với đơn "Thường" có giá trị tiền tại cột "Chiết khấu mua hàng giảm giá" và loại hàng hóa = I (Hàng hóa):
          tkChietKhau = '521111';
          tkChiPhi = sale.tkChiPhi || null;
          maPhi = sale.maPhi || null;
        } else if (isThuong && hasMaCtkm && !(hasMaCtkmTangHang && isGiaBanZero && calculatedFields.isTangHang)) {
          // Với đơn "Thường" có mã CTKM:
          // - Loại S (Dịch vụ): TK Chiết khấu = 521131
          // - Loại I (Hàng hóa): TK Chiết khấu = 521111
          if (productTypeUpper === 'S') {
            tkChietKhau = '521131';
          } else if (productTypeUpper === 'I') {
            tkChietKhau = '521111';
          } else {
            // Nếu không xác định được loại, mặc định là hàng hóa
            tkChietKhau = '521111';
          }
          tkChiPhi = sale.tkChiPhi || null;
          maPhi = sale.maPhi || null;
        } else {
          // Các đơn khác: lấy từ product hoặc sale nếu có
          tkChietKhau = loyaltyProduct?.tkChietKhau || sale.tkChietKhau || null;
          tkChiPhi = sale.tkChiPhi || null;
          maPhi = sale.maPhi || null;
        }


        // if (isDoiVo || isDoiDiem || isDauTu) {
        //   // Với đơn "Đổi vỏ", "Đổi điểm", "Đầu tư":
        //   tkChietKhau = null; // Để rỗng
        //   tkChiPhi = '64191';
        //   maPhi = '161010';
        // } else if (isSinhNhat) {
        //   // Với đơn "Sinh nhật":
        //   tkChietKhau = null; // Để rỗng
        //   tkChiPhi = '64192';
        //   maPhi = '162010';
        // } else if (isThuong && hasMaCtkmTangHang && isGiaBanZero && calculatedFields.isTangHang) {
        //   // Với đơn "Thường" có đơn giá = 0, Khuyến mại = 1, và có thông tin mã tại "Mã CTKM tặng hàng":
        //   tkChiPhi = '64191';
        //   maPhi = '161010';
        //   // tkChietKhau giữ nguyên (có thể được set bởi các điều kiện khác hoặc null)
        //   if (tkChietKhau === null) {
        //     tkChietKhau = sale.tkChietKhau || null;
        //   }
        // } else if (isKmVip > 0 && productTypeUpper === 'I') {
        //   tkChietKhau = '521113';
        // } else if (isKmVip > 0 && productTypeUpper === 'S') {
        //   tkChietKhau = '521132';
        // }
        // else if (hasVoucher > 0 && isTangSP === 'GIFT') {
        //   tkChietKhau = '5211631';
        // }
        // else if (hasVoucher > 0 && productTypeUpper === 'I') {
        //   tkChietKhau = '5211611';

        // } else if (hasVoucher > 0 && productTypeUpper === 'S') {
        //   tkChietKhau = '5211621';
        // }
        // else if (hasChietKhauMuaHangGiamGia && productTypeUpper === 'S') {
        //   // Với đơn có "Chiết khấu mua hàng giảm giá" có giá trị và loại hàng hóa = S (Dịch vụ):
        //   tkChietKhau = '521131';
        //   tkChiPhi = sale.tkChiPhi || null;
        //   maPhi = sale.maPhi || null;
        // } else if (isThuong && hasChietKhauMuaHangGiamGia && productTypeUpper === 'I') {
        //   // Với đơn "Thường" có giá trị tiền tại cột "Chiết khấu mua hàng giảm giá" và loại hàng hóa = I (Hàng hóa):
        //   tkChietKhau = '521111';
        //   tkChiPhi = sale.tkChiPhi || null;
        //   maPhi = sale.maPhi || null;
        // } else if (isThuong && hasMaCtkm && !(hasMaCtkmTangHang && isGiaBanZero && calculatedFields.isTangHang)) {
        //   // Với đơn "Thường" có mã CTKM:
        //   // - Loại S (Dịch vụ): TK Chiết khấu = 521131
        //   // - Loại I (Hàng hóa): TK Chiết khấu = 521111
        //   if (productTypeUpper === 'S') {
        //     tkChietKhau = '521131';
        //   } else if (productTypeUpper === 'I') {
        //     tkChietKhau = '521111';
        //   } else {
        //     // Nếu không xác định được loại, mặc định là hàng hóa
        //     tkChietKhau = '521111';
        //   }
        //   tkChiPhi = sale.tkChiPhi || null;
        //   maPhi = sale.maPhi || null;
        // } else {
        //   // Các đơn khác: lấy từ product hoặc sale nếu có
        //   tkChietKhau = loyaltyProduct?.tkChietKhau || sale.tkChietKhau || null;
        //   tkChiPhi = sale.tkChiPhi || null;
        //   maPhi = sale.maPhi || null;
        // }


        // Build detail item, chỉ thêm ma_kho nếu có giá trị (không rỗng)
        const detailItem: any = {
          tk_chiet_khau: limitString(toString(tkChietKhau, ''), 16) || '',
          tk_chi_phi: limitString(toString(tkChiPhi, ''), 16) || '',
          ma_phi: limitString(toString(maPhi, ''), 16) || '',
          tien_hang: Number(sale.qty) * Number(sale.giaBan),
          so_luong: Number(sale.qty),
          ma_kh_i: limitString(toString(sale.issuePartnerCode, ''), 16),
          // ma_vt: Mã vật tư (String, max 16 ký tự) - Bắt buộc
          // Dùng materialCode từ Loyalty API (giống như sales invoice)
          ma_vt: limitString(toString(finalMaterialCode), 16),
          // dvt: Đơn vị tính (String, max 32 ký tự) - Bắt buộc
          dvt: limitString(dvt, 32),
          // loai: Loại (String, max 2 ký tự) - 07-phí,lệ phí; 90-giảm thuế (mặc định rỗng)
          loai: limitString(loai, 2),
          // ma_ctkm_th: Mã ctkm tặng hàng (String, max 32 ký tự)
          ma_ctkm_th: limitString(maCtkmTangHang, 32),
        };

        // Thêm ma_kho: Chỉ thêm khi có giá trị hợp lệ, nếu không có thì không thêm key vào payload
        // Ưu tiên: maKho từ stock transfer > sale.maKho > maBp > orderData.branchCode

        let finalMaKho = maKho || '';

        if (isTachThe) {
          finalMaKho = 'B' + maBp;
        }

        // Chỉ thêm ma_kho vào detail item khi có giá trị hợp lệ (không rỗng)
        // Nếu không có giá trị hợp lệ, không thêm key ma_kho vào payload
        if (finalMaKho && finalMaKho?.trim() !== '') {
          detailItem.ma_kho = limitString(finalMaKho, 16);
        }

        // Thêm các field còn lại
        Object.assign(detailItem, {
          // gia_ban: Giá bán (Decimal) - giá gốc trước chiết khấu
          gia_ban: Number(giaBan),
          // is_reward_line: is_reward_line (Int)
          is_reward_line: sale.isRewardLine ? 1 : 0,
          // is_bundle_reward_line: is_bundle_reward_line (Int)
          is_bundle_reward_line: sale.isBundleRewardLine ? 1 : 0,
          // km_yn: Khuyến mãi (Int)
          // - = 1 CHỈ KHI là hàng tặng (giaBan = 0 && tienHang = 0)
          // - KHÔNG set = 1 khi chỉ có promCode (promCode là mã CTKM mua hàng giảm giá, không phải hàng tặng)
          // - Nếu ma_ctkm_th = "TT DAU TU" thì km_yn = 0
          km_yn: (isTTDauTu ? 0 : (isTangHang ? 1 : 0)),
          // dong_thuoc_goi: dong_thuoc_goi (String, max 32 ký tự)
          dong_thuoc_goi: limitString(toString(sale.dongThuocGoi, ''), 32),
          // trang_thai: trang_thai (String, max 32 ký tự)
          trang_thai: limitString(toString(sale.trangThai, ''), 32),
          // barcode: Barcode (String, max 32 ký tự)
          barcode: limitString(toString(sale.barcode, ''), 32),
          // ma_ck01: Mã ctkm mua hàng giảm giá (String, max 32 ký tự)
          ma_ck01: limitString(maCk01, 32),
          // ck01_nt: Tiền (Decimal)
          ck01_nt: Number(ck01_nt),
          // ma_ck02: Mã ck theo chính sách (String, max 32 ký tự)
          ma_ck02: limitString(toString(sale.ckTheoChinhSach, ''), 32),
          // ck02_nt: Tiền (Decimal)
          ck02_nt: Number(ck02_nt),
          // ma_ck03: Mua hàng ck vip (String, max 32 ký tự)
          ma_ck03: limitString(toString(maCk03, ''), 32),
          // ck03_nt: Tiền (Decimal)
          ck03_nt: Number(ck03_nt),
          // ma_ck04: Thanh toán coupon (String, max 32 ký tự)
          ma_ck04: limitString((ck04_nt > 0 || sale.thanhToanCoupon) ? toString(sale.maCk04 || 'COUPON', '') : '', 32),
          // ck04_nt: Tiền (Decimal)
          ck04_nt: Number(ck04_nt),
          // ma_ck05: Thanh toán voucher (String, max 32 ký tự)
          // Nếu là đơn "03. Đổi điểm": luôn set ma_ck05 = '' và ck05_nt = 0
          // Nếu không phải "03. Đổi điểm": chỉ thêm ma_ck05 khi ck05_nt > 0
          ...(isDoiDiem || isDoiDiemForCk05
            ? { ma_ck05: '' }
            : (ck05_nt > 0 ? {
              ma_ck05: limitString(formattedMaCk05 || toString(sale.maCk05 || 'VOUCHER', ''), 32),
            } : {})
          ),
          // ck05_nt: Tiền (Decimal)
          ck05_nt: Number(ck05_nt),
          // ma_ck06: Dự phòng 1 (String, max 32 ký tự) - không sử dụng
          ma_ck06: '',
          // ck06_nt: Tiền (Decimal)
          ck06_nt: Number(ck06_nt),
          // ma_ck07: Dự phòng 2 (String, max 32 ký tự)
          ma_ck07: limitString(sale.voucherDp2 ? 'VOUCHER_DP2' : '', 32),
          // ck07_nt: Tiền (Decimal)
          ck07_nt: Number(ck07_nt),
          // ma_ck08: Dự phòng 3 (String, max 32 ký tự)
          ma_ck08: limitString(sale.voucherDp3 ? 'VOUCHER_DP3' : '', 32),
          // ck08_nt: Tiền (Decimal)
          ck08_nt: Number(ck08_nt),
          // ma_ck09: Chiết khấu hãng (String, max 32 ký tự)
          ma_ck09: limitString(toString(sale.maCk09, ''), 32),
          // ck09_nt: Tiền (Decimal)
          ck09_nt: Number(ck09_nt),
          // ma_ck10: Thưởng bằng hàng (String, max 32 ký tự)
          ma_ck10: limitString(toString(sale.maCk10, ''), 32),
          // ck10_nt: Tiền (Decimal)
          ck10_nt: Number(ck10_nt),
          // ma_ck11: Thanh toán TK tiền ảo (String, max 32 ký tự)
          // Format: YYMM{brand_code}.TKDV (ví dụ: 2510MN.TKDV)
          ma_ck11: limitString(
            (ck11_nt > 0 || sale.thanhToanTkTienAo)
              ? toString(sale.maCk11 || this.generateTkTienAoLabel(orderData), '')
              : '',
            32
          ),
          // ck11_nt: Tiền (Decimal)
          ck11_nt: Number(ck11_nt),
          // ma_ck12: CK thêm 1 (String, max 32 ký tự)
          ma_ck12: limitString(toString(sale.maCk12, ''), 32),
          // ck12_nt: Tiền (Decimal)
          ck12_nt: Number(ck12_nt),
          // ma_ck13: CK thêm 2 (String, max 32 ký tự)
          ma_ck13: limitString(toString(sale.maCk13, ''), 32),
          // ck13_nt: Tiền (Decimal)
          ck13_nt: Number(ck13_nt),
          // ma_ck14: CK thêm 3 (String, max 32 ký tự)
          ma_ck14: limitString(toString(sale.maCk14, ''), 32),
          // ck14_nt: Tiền (Decimal)
          ck14_nt: Number(ck14_nt),
          // ma_ck15: Voucher DP1 (String, max 32 ký tự)
          // Với F3, thêm prefix "FBV TT " trước "VC CTKM SÀN"
          ma_ck15: limitString(
            ck15_nt_voucherDp1 > 0
              ? (brandLower === 'f3' ? 'FBV TT VC CTKM SÀN' : 'VC CTKM SÀN')
              : toString(sale.maCk15, ''),
            32
          ),
          // ck15_nt: Tiền (Decimal)
          ck15_nt: Number(ck15_nt),
          // ma_ck16: Voucher DP2 (String, max 32 ký tự)
          ma_ck16: limitString(toString(sale.maCk16, ''), 32),
          // ck16_nt: Tiền (Decimal)
          ck16_nt: Number(ck16_nt),
          // ma_ck17: Voucher DP3 (String, max 32 ký tự)
          ma_ck17: limitString(toString(sale.maCk17, ''), 32),
          // ck17_nt: Tiền (Decimal)
          ck17_nt: Number(ck17_nt),
          // ma_ck18: Voucher DP4 (String, max 32 ký tự)
          ma_ck18: limitString(toString(sale.maCk18, ''), 32),
          // ck18_nt: Tiền (Decimal)
          ck18_nt: Number(ck18_nt),
          // ma_ck19: Voucher DP5 (String, max 32 ký tự)
          ma_ck19: limitString(toString(sale.maCk19, ''), 32),
          // ck19_nt: Tiền (Decimal)
          ck19_nt: Number(ck19_nt),
          // ma_ck20: Voucher DP6 (String, max 32 ký tự)
          ma_ck20: limitString(toString(sale.maCk20, ''), 32),
          // ck20_nt: Tiền (Decimal)
          ck20_nt: Number(ck20_nt),
          // ma_ck21: Voucher DP7 (String, max 32 ký tự)
          ma_ck21: limitString(toString(sale.maCk21, ''), 32),
          // ck21_nt: Tiền (Decimal)
          ck21_nt: Number(ck21_nt),
          // ma_ck22: Voucher DP8 (String, max 32 ký tự)
          ma_ck22: limitString(toString(sale.maCk22, ''), 32),
          // ck22_nt: Tiền (Decimal)
          ck22_nt: Number(ck22_nt),
          // dt_tg_nt: Tiền trợ giá (Decimal) - đã được phân bổ nếu là "01.Thường"
          dt_tg_nt: Number(dtTgNt),
          // ma_thue: Mã thuế (String, max 8 ký tự) - Bắt buộc
          // Nếu không có mã thuế, set thành "00" thay vì "10"
          ma_thue: limitString(toString(sale.maThue, '00'), 8),
          // thue_suat: Thuế suất (Decimal) - không phân bổ (tỷ lệ %)
          thue_suat: Number(toNumber(sale.thueSuat, 0)),
          // tien_thue: Tiền thuế (Decimal) - đã được phân bổ nếu là "01.Thường"
          tien_thue: Number(tienThue),
          // tk_thue: Tài khoản thuế (String, max 16 ký tự)
          tk_thue: limitString(toString(sale.tkThueCo, ''), 16),
          // tk_cpbh: Tài khoản chiết khấu km (String, max 16 ký tự)
          tk_cpbh: limitString(toString(sale.tkCpbh, ''), 16),
          // ma_bp: Mã bộ phận (String, max 8 ký tự) - Bắt buộc
          ma_bp: limitString(maBp, 8),
          // ma_the: Mã thẻ (String, max 256 ký tự)
          ma_the: limitString(maThe, 256),
          // ma_lo: Mã lô (String, max 16 ký tự)
          // so_serial: Số serial (String, max 64 ký tự)
          // Chỉ thêm ma_lo hoặc so_serial vào payload (không gửi cả hai, và chỉ gửi khi có giá trị)
          ...(soSerial && soSerial.trim() !== ''
            ? { so_serial: limitString(soSerial, 64) }
            : (maLo && maLo.trim() !== '' ? { ma_lo: limitString(maLo, 16) } : {})),
          // loai_gd: Loại giao dịch (String, max 2 ký tự) - Bắt buộc
          loai_gd: limitString(loaiGd, 2),
          // ma_combo: mã combo (String, max 16 ký tự)
          ma_combo: limitString(toString(sale.maCombo, ''), 16),
          // ma_nx_st: Mã nghiệp vụ (ST* từ stock transfer)
          ma_nx_st: limitString(toString(sale.ma_nx_st, ''), 32),
          // ma_nx_rt: Mã nghiệp vụ (RT* từ stock transfer)
          ma_nx_rt: limitString(toString(sale.ma_nx_rt, ''), 32),
          // id_goc: ID phiếu gốc (String, max 70 ký tự)
          id_goc: limitString(toString(sale.idGoc, ''), 70),
          // id_goc_ct: Số ct phiếu gốc (String, max 16 ký tự)
          id_goc_ct: limitString(toString(sale.idGocCt, ''), 16),
          // id_goc_so: Dòng phiếu gốc (Int)
          id_goc_so: Number(toNumber(sale.idGocSo, 0)),
          // dong: Dòng của phiếu (Int) - Bắt buộc, bắt đầu từ 1
          dong: index + 1,
          // id_goc_ngay: Ngày phiếu gốc (DateTime)
          id_goc_ngay: sale.idGocNgay ? formatDateISO(new Date(sale.idGocNgay)) : formatDateISO(new Date()),
          // id_goc_dv: Đơn vị phiếu gốc (String, max 8 ký tự)
          id_goc_dv: limitString(toString(sale.idGocDv, ''), 8),
        });

        return detailItem;
      }));


      // Validate sales array
      if (!orderData.sales || orderData.sales.length === 0) {
        throw new Error('Order has no sales items');
      }

      // Build cbdetail từ detail (tổng hợp thông tin sản phẩm)
      const cbdetail = detail.map((item: any) => {
        // Tính tổng chiết khấu từ tất cả các loại chiết khấu
        const tongChietKhau =
          Number(item.ck01_nt || 0) +
          Number(item.ck02_nt || 0) +
          Number(item.ck03_nt || 0) +
          Number(item.ck04_nt || 0) +
          Number(item.ck05_nt || 0) +
          Number(item.ck06_nt || 0) +
          Number(item.ck07_nt || 0) +
          Number(item.ck08_nt || 0) +
          Number(item.ck09_nt || 0) +
          Number(item.ck10_nt || 0) +
          Number(item.ck11_nt || 0) +
          Number(item.ck12_nt || 0) +
          Number(item.ck13_nt || 0) +
          Number(item.ck14_nt || 0) +
          Number(item.ck15_nt || 0) +
          Number(item.ck16_nt || 0) +
          Number(item.ck17_nt || 0) +
          Number(item.ck18_nt || 0) +
          Number(item.ck19_nt || 0) +
          Number(item.ck20_nt || 0) +
          Number(item.ck21_nt || 0) +
          Number(item.ck22_nt || 0);

        return {
          ma_vt: item.ma_vt || '',
          dvt: item.dvt || '',
          so_luong: Number(item.so_luong || 0),
          ck_nt: Number(tongChietKhau),
          gia_nt: Number(item.gia_ban || 0),
          tien_nt: Number(item.tien_hang || 0),
        };
      });

      const firstSale = orderData.sales[0];
      const maKenh = 'ONLINE'; // Fix mã kênh là ONLINE
      const soSeri = firstSale?.kyHieu || firstSale?.branchCode || orderData.branchCode || 'DEFAULT';

      // loai_gd: Tất cả đều dùng '01'
      const loaiGd = '01';

      // Lấy ma_dvcs từ department API (ưu tiên), nếu không có thì fallback
      const maDvcs = firstSale?.department?.ma_dvcs
        || firstSale?.department?.ma_dvcs_ht
        || orderData.customer?.brand
        || orderData.branchCode
        || '';

      // Với đơn "08. Tách thẻ": Ưu tiên dùng issue_partner_code làm ma_kh
      // Lấy từ dòng đầu tiên có issuePartnerCode, nếu không có thì dùng customer code mặc định
      let maKhForHeader = this.normalizeMaKh(orderData.customer?.code);
      const ordertypeNameForMaKh = firstSale?.ordertype || firstSale?.ordertypeName || '';
      const isTachTheForMaKh = ordertypeNameForMaKh.includes('08. Tách thẻ') ||
        ordertypeNameForMaKh.includes('08.Tách thẻ') ||
        ordertypeNameForMaKh.includes('08.  Tách thẻ');

      if (isTachTheForMaKh && orderData.sales && Array.isArray(orderData.sales)) {
        // Tìm dòng có issuePartnerCode
        // Ưu tiên: dòng qty > 0 (người nhận) trước, sau đó mới đến dòng qty < 0 (người chuyển)
        // Vì thông thường ma_kh header sẽ là của người nhận (người sở hữu thẻ mới)
        let saleWithIssuePartnerCode = orderData.sales.find((s: any) =>
          Number(s.qty || 0) < 0 && s.issuePartnerCode
        );

        // Nếu không tìm thấy dòng qty > 0, tìm dòng qty < 0
        if (!saleWithIssuePartnerCode) {
          saleWithIssuePartnerCode = orderData.sales.find((s: any) =>
            Number(s.qty || 0) < 0 && s.issuePartnerCode
          );
        }

        // Nếu vẫn không tìm thấy, lấy dòng đầu tiên có issuePartnerCode
        if (!saleWithIssuePartnerCode) {
          saleWithIssuePartnerCode = orderData.sales.find((s: any) => s.issuePartnerCode);
        }

        if (saleWithIssuePartnerCode && saleWithIssuePartnerCode.issuePartnerCode) {
          maKhForHeader = this.normalizeMaKh(saleWithIssuePartnerCode.issuePartnerCode);
        }
      }

      return {
        action: 0,
        ma_dvcs: maDvcs,
        ma_kh: maKhForHeader,
        ong_ba: orderData.customer?.name || null,
        ma_gd: '1',
        ma_tt: null,
        ma_ca: firstSale?.maCa || null,
        hinh_thuc: '0',
        dien_giai: orderData.docCode || null,
        ngay_lct: ngayLct,
        ngay_ct: ngayCt,
        so_ct: orderData.docCode || '',
        so_seri: soSeri,
        ma_nt: 'VND',
        ty_gia: 1.0,
        ma_bp: firstSale?.department?.ma_bp || firstSale?.branchCode || '',
        tk_thue_no: '131111',
        ma_kenh: maKenh,
        loai_gd: loaiGd,
        detail,
        cbdetail,
      };
    } catch (error: any) {
      this.logger.error(`Error building Fast API invoice data: ${error?.message || error}`);
      this.logger.error(`Error stack: ${error?.stack || 'No stack trace'}`);
      this.logger.error(`Order data: ${JSON.stringify({
        docCode: orderData?.docCode,
        docDate: orderData?.docDate,
        salesCount: orderData?.sales?.length,
        customer: orderData?.customer ? { code: orderData.customer.code, name: orderData.customer.name } : null,
      })}`);
      throw new Error(`Failed to build invoice data: ${error?.message || error}`);
    }
  }

  /**
   * Build salesReturn data cho Fast API (Hàng bán trả lại)
   * Tương tự như buildFastApiInvoiceData nhưng có thêm các field đặc biệt cho salesReturn
   */
  private async buildSalesReturnData(orderData: any, stockTransfers: StockTransfer[]): Promise<any> {
    try {
      // Sử dụng lại logic từ buildFastApiInvoiceData để build detail
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Format ngày theo ISO 8601
      const formatDateISO = (date: Date | string): string => {
        const d = typeof date === 'string' ? new Date(date) : date;
        if (isNaN(d.getTime())) {
          throw new Error('Invalid date');
        }
        return d.toISOString();
      };

      // Lấy ngày hóa đơn gốc (ngay_ct0) - có thể lấy từ sale đầu tiên hoặc orderData
      const firstSale = orderData.sales?.[0] || {};
      let ngayCt0: string | null = null;
      let soCt0: string | null = null;

      // Tìm hóa đơn gốc từ stock transfer hoặc sale
      // Nếu có stock transfer, có thể lấy từ soCode hoặc docCode
      if (stockTransfers && stockTransfers.length > 0) {
        const firstStockTransfer = stockTransfers[0];
        // soCode thường là mã đơn hàng gốc
        soCt0 = firstStockTransfer.soCode || orderData.docCode || null;
        // Ngày có thể lấy từ stock transfer hoặc orderData
        if (firstStockTransfer.transDate) {
          ngayCt0 = formatDateISO(firstStockTransfer.transDate);
        } else if (orderData.docDate) {
          ngayCt0 = formatDateISO(orderData.docDate);
        }
      } else {
        // Nếu không có stock transfer, lấy từ orderData
        soCt0 = orderData.docCode || null;
        if (orderData.docDate) {
          ngayCt0 = formatDateISO(orderData.docDate);
        }
      }

      // Format ngày hiện tại
      let docDate: Date;
      if (orderData.docDate instanceof Date) {
        docDate = orderData.docDate;
      } else if (typeof orderData.docDate === 'string') {
        docDate = new Date(orderData.docDate);
        if (isNaN(docDate.getTime())) {
          docDate = new Date();
        }
      } else {
        docDate = new Date();
      }

      const ngayCt = formatDateISO(docDate);
      const ngayLct = formatDateISO(docDate);

      // Lấy ma_dvcs
      const maDvcs = firstSale?.department?.ma_dvcs
        || firstSale?.department?.ma_dvcs_ht
        || orderData.customer?.brand
        || orderData.branchCode
        || '';

      // Lấy so_seri
      const soSeri = firstSale?.kyHieu || firstSale?.branchCode || orderData.branchCode || 'DEFAULT';

      // Build detail từ invoiceData.detail, chỉ giữ các field cần thiết cho salesReturn
      const detail = (invoiceData.detail || []).map((item: any, index: number) => {
        // Chỉ thêm tk_ck nếu có giá trị (không phải null/undefined/empty)
        const detailItem: any = {
          // Field bắt buộc
          ma_vt: item.ma_vt,
          dvt: item.dvt,
          ma_kho: item.ma_kho,
          so_luong: item.so_luong,
          gia_ban: item.gia_ban,
          tien_hang: item.tien_hang,
          // Field tài khoản
          tk_dt: item.tk_dt || '511', // Tài khoản trả lại (mặc định 511)
          tk_gv: item.tk_gv || '632', // Tài khoản giá vốn (mặc định 632)
          // Field khuyến mãi
          is_reward_line: item.is_reward_line || 0,
          is_bundle_reward_line: item.is_bundle_reward_line || 0,
          km_yn: item.km_yn || 0,
          // Field chiết khấu (ck01_nt đến ck22_nt)
          ck01_nt: item.ck01_nt || 0,
          ck02_nt: item.ck02_nt || 0,
          ck03_nt: item.ck03_nt || 0,
          ck04_nt: item.ck04_nt || 0,
          ck05_nt: item.ck05_nt || 0,
          ck06_nt: item.ck06_nt || 0,
          ck07_nt: item.ck07_nt || 0,
          ck08_nt: item.ck08_nt || 0,
          ck09_nt: item.ck09_nt || 0,
          ck10_nt: item.ck10_nt || 0,
          ck11_nt: item.ck11_nt || 0,
          ck12_nt: item.ck12_nt || 0,
          ck13_nt: item.ck13_nt || 0,
          ck14_nt: item.ck14_nt || 0,
          ck15_nt: item.ck15_nt || 0,
          ck16_nt: item.ck16_nt || 0,
          ck17_nt: item.ck17_nt || 0,
          ck18_nt: item.ck18_nt || 0,
          ck19_nt: item.ck19_nt || 0,
          ck20_nt: item.ck20_nt || 0,
          ck21_nt: item.ck21_nt || 0,
          ck22_nt: item.ck22_nt || 0,
          // Field thuế
          dt_tg_nt: item.dt_tg_nt || 0,
          ma_thue: item.ma_thue || '00',
          thue_suat: item.thue_suat || 0,
          tien_thue: item.tien_thue || 0,
          // Field bộ phận
          ma_bp: item.ma_bp,
          // Field loại giao dịch (cần thiết cho salesReturn)
          loai_gd: item.loai_gd || '01',
          // Field dòng (cần thiết cho salesReturn)
          dong: index + 1,
          // Field id gốc (cần thiết cho salesReturn)
          id_goc_so: item.id_goc_so || 0,
          // Field ngày gốc (cần thiết cho salesReturn)
          id_goc_ngay: item.id_goc_ngay || formatDateISO(new Date()),
        };

        // Chỉ thêm tk_ck nếu có giá trị (không phải null/undefined/empty)
        if (item.tk_ck && item.tk_ck.trim() !== '') {
          detailItem.tk_ck = item.tk_ck;
        }

        return detailItem;
      });

      // Build payload, chỉ thêm các field không null
      const salesReturnPayload: any = {
        ma_dvcs: maDvcs,
        ma_kh: invoiceData.ma_kh,
        ong_ba: invoiceData.ong_ba,
        ma_gd: '1', // Mã giao dịch (mặc định 1 - Hàng bán trả lại)
        tk_co: '131', // Tài khoản có (mặc định 131)
        ngay_lct: ngayLct,
        ngay_ct: ngayCt,
        so_ct: orderData.docCode || '',
        so_seri: soSeri,
        ma_nt: 'VND',
        ty_gia: 1.0,
        ma_kenh: 'ONLINE', // Mã kênh (mặc định ONLINE)
        detail: detail,
      };

      // Chỉ thêm các field optional nếu có giá trị
      if (firstSale?.maCa) {
        salesReturnPayload.ma_ca = firstSale.maCa;
      }
      if (soCt0) {
        salesReturnPayload.so_ct0 = soCt0;
      }
      if (ngayCt0) {
        salesReturnPayload.ngay_ct0 = ngayCt0;
      }
      if (orderData.docCode) {
        salesReturnPayload.dien_giai = orderData.docCode;
      }

      return salesReturnPayload;
    } catch (error: any) {
      this.logger.error(`Error building sales return data: ${error?.message || error}`);
      throw new Error(`Failed to build sales return data: ${error?.message || error}`);
    }
  }

  /**
   * Tạo stock transfer từ STOCK_TRANSFER data
   */
  async createStockTransfer(createDto: CreateStockTransferDto): Promise<any> {
    try {
      // Group theo doccode để xử lý từng phiếu
      const transferMap = new Map<string, StockTransferItem[]>();

      for (const item of createDto.data) {
        if (!transferMap.has(item.doccode)) {
          transferMap.set(item.doccode, []);
        }
        transferMap.get(item.doccode)!.push(item);
      }

      const results: Array<{
        doccode: string;
        success: boolean;
        result?: any;
        error?: string;
      }> = [];

      for (const [doccode, items] of transferMap.entries()) {
        try {
          // Lấy item đầu tiên để lấy thông tin chung
          const firstItem = items[0];

          // Join với order nếu có so_code
          let orderData: any = null;
          if (firstItem.so_code) {
            try {
              orderData = await this.findByOrderCode(firstItem.so_code);
            } catch (error) {
            }
          }

          // Build FastAPI stock transfer data
          const stockTransferData = await this.buildStockTransferData(items, orderData);

          // Submit to FastAPI
          const result = await this.fastApiService.submitStockTransfer(stockTransferData);

          results.push({
            doccode,
            success: true,
            result,
          });
        } catch (error: any) {
          this.logger.error(
            `Error creating stock transfer for ${doccode}: ${error?.message || error}`,
          );
          results.push({
            doccode,
            success: false,
            error: error?.message || 'Unknown error',
          });
        }
      }

      return {
        success: true,
        results,
        total: results.length,
        successCount: results.filter((r) => r.success).length,
        failedCount: results.filter((r) => !r.success).length,
      };
    } catch (error: any) {
      this.logger.error(`Error creating stock transfers: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Xử lý flow SALE_RETURN
   * Case 1: Có stock transfer → Gọi API salesReturn
   * Case 2: Không có stock transfer → Gọi API salesOrder với action=1
   */
  /**
   * Helper: Kiểm tra xem docCode có đuôi _X không (ví dụ: SO45.01574458_X)
   */
  private hasUnderscoreX(docCode: string): boolean {
    if (!docCode) return false;
    return docCode.trim().endsWith('_X');
  }

  /**
   * Helper: Lấy đơn gốc từ đơn có đuôi _X (ví dụ: SO45.01574458_X => SO45.01574458)
   */
  private getBaseDocCode(docCode: string): string {
    if (!docCode) return docCode;
    const trimmed = docCode.trim();
    if (trimmed.endsWith('_X')) {
      return trimmed.slice(0, -2); // Bỏ '_X' ở cuối
    }
    return trimmed;
  }

  /**
   * Xử lý đơn hàng có đuôi _X (ví dụ: SO45.01574458_X)
   * Gọi API salesOrder với action: 1
   * Cả đơn có _X và đơn gốc (bỏ _X) đều sẽ có action = 1
   */
  private async handleSaleOrderWithUnderscoreX(orderData: any, docCode: string): Promise<any> {
    // Đơn có đuôi _X → Gọi API salesOrder với action: 1
    const invoiceData = await this.buildFastApiInvoiceData(orderData);

    // Gọi API salesOrder với action = 1 (không cần tạo/cập nhật customer)
    let result: any;
    let data = { ...invoiceData, ma_kho: orderData?.maKho || '' }
    try {
      result = await this.fastApiInvoiceFlowService.createSalesOrder({
        ...data,
        customer: orderData.customer,
        ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
      }, 1); // action = 1 cho đơn hàng có đuôi _X

      // Lưu vào bảng kê hóa đơn
      const responseStatus = Array.isArray(result) && result.length > 0 && result[0].status === 1 ? 1 : 0;
      const apiMessage = Array.isArray(result) && result.length > 0 ? (result[0].message || '') : '';
      const shouldUseApiMessage = apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
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
      const responseGuid = Array.isArray(result) && result.length > 0 && Array.isArray(result[0].guid)
        ? result[0].guid[0]
        : (Array.isArray(result) && result.length > 0 ? result[0].guid : null);

      // Xử lý cashio payment (Phiếu thu tiền mặt/Giấy báo có) nếu salesOrder thành công
      let cashioResult: any = null;
      let paymentResult: any = null;
      if (responseStatus === 1) {
        this.logger.log(`[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (đơn có đuôi _X)`);
        cashioResult = await this.fastApiInvoiceFlowService.processCashioPayment(
          docCode,
          orderData,
          invoiceData,
        );

        if (cashioResult.cashReceiptResults && cashioResult.cashReceiptResults.length > 0) {
          this.logger.log(`[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`);
        }
        if (cashioResult.creditAdviceResults && cashioResult.creditAdviceResults.length > 0) {
          this.logger.log(`[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`);
        }

        // Xử lý Payment (Phiếu chi tiền mặt/Giấy báo nợ) cho đơn hủy (_X) - cho phép không có mã kho
        try {
          // Kiểm tra có stock transfer không
          const docCodesForStockTransfer = this.getDocCodesForStockTransfer([docCode]);
          const stockTransfers = await this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          });
          const stockCodes = Array.from(new Set(stockTransfers.map(st => st.stockCode).filter(Boolean)));

          // Cho đơn _X: Gọi payment ngay cả khi không có mã kho (đơn hủy không có khái niệm xuất kho)
          const allowWithoutStockCodes = stockCodes.length === 0;

          if (allowWithoutStockCodes || stockCodes.length > 0) {
            this.logger.log(`[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (đơn có đuôi _X) - ${allowWithoutStockCodes ? 'không có mã kho' : `với ${stockCodes.length} mã kho`}`);
            paymentResult = await this.fastApiInvoiceFlowService.processPayment(
              docCode,
              orderData,
              invoiceData,
              stockCodes,
              allowWithoutStockCodes, // Cho phép gọi payment ngay cả khi không có mã kho
            );

            if (paymentResult.paymentResults && paymentResult.paymentResults.length > 0) {
              this.logger.log(`[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`);
            }
            if (paymentResult.debitAdviceResults && paymentResult.debitAdviceResults.length > 0) {
              this.logger.log(`[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`);
            }
          }
        } catch (paymentError: any) {
          // Log lỗi nhưng không fail toàn bộ flow
          this.logger.warn(`[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode} (đơn có đuôi _X): ${paymentError?.message || paymentError}`);
        }
      }

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: responseGuid || null,
        fastApiResponse: JSON.stringify({
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        }),
      });

      return {
        success: responseStatus === 1,
        message: responseMessage,
        result: {
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
          errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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
      const shouldUseApiMessage = errorMessage && errorMessage.trim().toUpperCase() !== 'OK';
      const formattedErrorMessage = shouldUseApiMessage
        ? `Tạo đơn hàng thất bại cho đơn hàng ${docCode}. ${errorMessage}`
        : `Tạo đơn hàng thất bại cho đơn hàng ${docCode}`;

      // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
        status: 0,
        message: formattedErrorMessage,
        guid: null,
        fastApiResponse: JSON.stringify(error?.response?.data || error),
      });

      this.logger.error(`SALE_ORDER with _X suffix creation failed for order ${docCode}: ${formattedErrorMessage}`);

      return {
        success: false,
        message: formattedErrorMessage,
        result: error?.response?.data || error,
      };
    }
  }

  private async handleSaleReturnFlow(orderData: any, docCode: string): Promise<any> {
    // Kiểm tra xem có stock transfer không
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer = this.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
    });

    // Case 1: Có stock transfer → Gọi API salesReturn
    if (stockTransfers && stockTransfers.length > 0) {
      // Build salesReturn data
      const salesReturnData = await this.buildSalesReturnData(orderData, stockTransfers);

      // Gọi API salesReturn (không cần tạo/cập nhật customer)
      let result: any;
      try {
        result = await this.fastApiInvoiceFlowService.createSalesReturn(salesReturnData);

        // Lưu vào bảng kê hóa đơn
        const responseStatus = Array.isArray(result) && result.length > 0 && result[0].status === 1 ? 1 : 0;
        let responseMessage = '';
        const apiMessage = Array.isArray(result) && result.length > 0 ? result[0].message : '';
        const shouldAppendApiMessage = apiMessage && apiMessage.trim().toUpperCase() !== 'OK';

        if (responseStatus === 1) {
          responseMessage = shouldAppendApiMessage
            ? `Tạo hàng bán trả lại thành công cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hàng bán trả lại thành công cho đơn hàng ${docCode}`;
        } else {
          responseMessage = shouldAppendApiMessage
            ? `Tạo hàng bán trả lại thất bại cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hàng bán trả lại thất bại cho đơn hàng ${docCode}`;
        }
        const responseGuid = Array.isArray(result) && result.length > 0 && Array.isArray(result[0].guid)
          ? result[0].guid[0]
          : (Array.isArray(result) && result.length > 0 ? result[0].guid : null);

        // Xử lý Payment (Phiếu chi tiền mặt) nếu có mã kho
        if (responseStatus === 1) {
          try {
            const stockCodes = Array.from(new Set(stockTransfers.map(st => st.stockCode).filter(Boolean)));

            if (stockCodes.length > 0) {
              // Build invoiceData để dùng cho payment (tương tự như các case khác)
              const invoiceData = await this.buildFastApiInvoiceData(orderData);

              this.logger.log(`[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (SALE_RETURN) với ${stockCodes.length} mã kho`);
              const paymentResult = await this.fastApiInvoiceFlowService.processPayment(
                docCode,
                orderData,
                invoiceData,
                stockCodes,
              );

              if (paymentResult.paymentResults && paymentResult.paymentResults.length > 0) {
                this.logger.log(`[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (SALE_RETURN)`);
              }
              if (paymentResult.debitAdviceResults && paymentResult.debitAdviceResults.length > 0) {
                this.logger.log(`[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (SALE_RETURN)`);
              }
            } else {
              this.logger.debug(`[Payment] Đơn hàng ${docCode} (SALE_RETURN) không có mã kho, bỏ qua payment API`);
            }
          } catch (paymentError: any) {
            // Log lỗi nhưng không fail toàn bộ flow
            this.logger.warn(`[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode} (SALE_RETURN): ${paymentError?.message || paymentError}`);
          }
        }

        await this.saveFastApiInvoice({
          docCode,
          maDvcs: salesReturnData.ma_dvcs,
          maKh: salesReturnData.ma_kh,
          tenKh: orderData.customer?.name || salesReturnData.ong_ba || '',
          ngayCt: salesReturnData.ngay_ct ? new Date(salesReturnData.ngay_ct) : new Date(),
          status: responseStatus,
          message: responseMessage,
          guid: responseGuid || null,
          fastApiResponse: JSON.stringify(result),
        });

        return {
          success: responseStatus === 1,
          message: responseMessage,
          result: result,
        };
      } catch (error: any) {
        // Lấy thông báo lỗi chính xác từ Fast API response
        let errorMessage = 'Tạo hàng bán trả lại thất bại';

        if (error?.response?.data) {
          const errorData = error.response.data;
          if (Array.isArray(errorData) && errorData.length > 0) {
            errorMessage = errorData[0].message || errorData[0].error || errorMessage;
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

        // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
        await this.saveFastApiInvoice({
          docCode,
          maDvcs: salesReturnData.ma_dvcs,
          maKh: salesReturnData.ma_kh,
          tenKh: orderData.customer?.name || salesReturnData.ong_ba || '',
          ngayCt: salesReturnData.ngay_ct ? new Date(salesReturnData.ngay_ct) : new Date(),
          status: 0,
          message: errorMessage,
          guid: null,
          fastApiResponse: JSON.stringify(error?.response?.data || error),
        });

        this.logger.error(`SALE_RETURN order creation failed for order ${docCode}: ${errorMessage}`);

        return {
          success: false,
          message: errorMessage,
          result: error?.response?.data || error,
        };
      }
    }

    // Case 2: Không có stock transfer → Không xử lý (bỏ qua)
    // SALE_RETURN không có stock transfer không cần xử lý
    await this.saveFastApiInvoice({
      docCode,
      maDvcs: orderData.branchCode || '',
      maKh: orderData.customer?.code || '',
      tenKh: orderData.customer?.name || '',
      ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
      status: 0,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
      guid: null,
      fastApiResponse: undefined,
    });

    return {
      success: false,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
      result: null,
    };
  }

  /**
   * Build FastAPI stock transfer data từ STOCK_TRANSFER items
   */
  private async buildStockTransferData(
    items: StockTransferItem[],
    orderData: any,
  ): Promise<any> {
    const firstItem = items[0];

    // Lấy ma_dvcs từ order hoặc branch_code
    let maDvcs = '';
    if (orderData) {
      const firstSale = orderData.sales?.[0];
      maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';
    }
    if (!maDvcs) {
      maDvcs = firstItem.branch_code || '';
    }

    // Lấy ma_kh từ order và normalize (bỏ prefix "NV" nếu có)
    const maKh = this.normalizeMaKh(orderData?.customer?.code);

    // Map iotype sang ma_nx (mã nhập xuất)
    // iotype: 'O' = xuất, 'I' = nhập
    // ma_nx: có thể là '1111' cho xuất, '1112' cho nhập (cần xác nhận với FastAPI)
    const getMaNx = (iotype: string): string => {
      if (iotype === 'O') {
        return '1111'; // Xuất nội bộ
      } else if (iotype === 'I') {
        return '1112'; // Nhập nội bộ
      }
      return '1111'; // Default
    };

    // Build detail items
    const detail = await Promise.all(
      items.map(async (item, index) => {
        // Fetch trackSerial và trackBatch từ Loyalty API để xác định dùng ma_lo hay so_serial
        let dvt = 'Cái'; // Default
        let trackSerial: boolean | null = null;
        let trackBatch: boolean | null = null;
        let productTypeFromLoyalty: string | null = null;

        try {
          const product = await this.productItemRepository.findOne({
            where: { maERP: item.item_code },
          });
          if (product?.dvt) {
            dvt = product.dvt;
          }
          // Fetch từ Loyalty API để lấy dvt, trackSerial, trackBatch và productType
          const loyaltyProduct = await this.loyaltyService.checkProduct(item.item_code);
          if (loyaltyProduct) {
            if (loyaltyProduct?.unit) {
              dvt = loyaltyProduct.unit;
            }
            trackSerial = loyaltyProduct.trackSerial === true;
            trackBatch = loyaltyProduct.trackBatch === true;
            productTypeFromLoyalty = loyaltyProduct?.productType || loyaltyProduct?.producttype || null;
          }
        } catch (error) {
        }

        const productTypeUpper = productTypeFromLoyalty ? String(productTypeFromLoyalty).toUpperCase().trim() : null;

        // Debug log để kiểm tra trackSerial và trackBatch
        if (index === 0) {
        }

        // Xác định có dùng ma_lo hay so_serial dựa trên trackSerial và trackBatch từ Loyalty API
        const useBatch = this.shouldUseBatch(trackBatch, trackSerial);

        let maLo: string | null = null;
        let soSerial: string | null = null;

        if (useBatch) {
          // trackBatch = true → dùng ma_lo với giá trị batchserial
          const batchSerial = item.batchserial || null;
          if (batchSerial) {
            // Vẫn cần productType để quyết định cắt bao nhiêu ký tự
            if (productTypeUpper === 'TPCN') {
              // Nếu productType là "TPCN", cắt lấy 8 ký tự cuối
              maLo = batchSerial.length >= 8 ? batchSerial.slice(-8) : batchSerial;
            } else if (productTypeUpper === 'SKIN' || productTypeUpper === 'GIFT') {
              // Nếu productType là "SKIN" hoặc "GIFT", cắt lấy 4 ký tự cuối
              maLo = batchSerial.length >= 4 ? batchSerial.slice(-4) : batchSerial;
            } else {
              // Các trường hợp khác → giữ nguyên toàn bộ
              maLo = batchSerial;
            }
          } else {
            maLo = null;
          }
          soSerial = null;
        } else {
          // trackSerial = true và trackBatch = false → dùng so_serial, không set ma_lo
          maLo = null;
          soSerial = item.batchserial || null;
        }

        return {
          ma_vt: item.item_code,
          dvt: dvt,
          so_serial: soSerial,
          ma_kho: item.stock_code,
          so_luong: Math.abs(item.qty), // Lấy giá trị tuyệt đối
          gia_nt: 0, // Stock transfer thường không có giá
          tien_nt: 0, // Stock transfer thường không có tiền
          ma_lo: maLo,
          px_gia_dd: 0, // Mặc định 0
          ma_nx: getMaNx(item.iotype),
          ma_vv: null,
          ma_bp: orderData?.sales?.[0]?.department?.ma_bp || item.branch_code || null,
          so_lsx: null,
          ma_sp: null,
          ma_hd: null,
          ma_phi: null,
          ma_ku: null,
          ma_phi_hh: null,
          ma_phi_ttlk: null,
          tien_hh_nt: 0,
          tien_ttlk_nt: 0,
        };
      }),
    );

    // Format date
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      return d.toISOString();
    };

    const transDate = new Date(firstItem.transdate);
    const ngayCt = formatDateISO(transDate);
    const ngayLct = formatDateISO(transDate);

    // Lấy ma_nx từ item đầu tiên (tất cả items trong cùng 1 phiếu nên có cùng iotype)
    const maNx = getMaNx(firstItem.iotype);

    return {
      action: 0, // Thêm action field giống như salesInvoice
      ma_dvcs: maDvcs,
      ma_kh: maKh,
      ong_ba: orderData?.customer?.name || null,
      ma_gd: '1', // Mã giao dịch: 1
      ma_nx: maNx, // Thêm ma_nx vào header
      ngay_ct: ngayCt,
      so_ct: firstItem.doccode,
      ma_nt: 'VND',
      ty_gia: 1.0,
      dien_giai: firstItem.doc_desc || null,
      detail: detail,
    };
  }

  /**
   * Export orders to Excel file
   * Sử dụng getAllOrders với export=true để đảm bảo đồng bộ với UI
   * Khi getAllOrders thay đổi gì thì export Excel cũng tự động thay đổi theo
   */
  async exportOrders(options: {
    brand?: string;
    isProcessed?: boolean;
    date?: string;
    dateFrom?: string;
    dateTo?: string;
    search?: string;
    statusAsys?: boolean;
  }): Promise<Buffer> {
    const { brand, isProcessed, date, dateFrom, dateTo, search, statusAsys } = options;

    try {
      // Limit số records để tránh quá tải
      const MAX_EXPORT_RECORDS = 100000;

      // Gọi getAllOrders với export=true để lấy tất cả sales items (đồng bộ với UI)
      const result = await this.findAllOrders({
        brand,
        isProcessed,
        date,
        dateFrom,
        dateTo,
        search,
        statusAsys,
        export: true, // Export mode: trả về sales items riêng lẻ
      });

      const allSales = result.sales || [];
      const totalCount = result.total || 0;

      if (totalCount > MAX_EXPORT_RECORDS) {
        throw new BadRequestException(`Số lượng records quá lớn (${totalCount}). Vui lòng thêm filter để giảm số lượng (tối đa ${MAX_EXPORT_RECORDS} records)`);
      }

      if (allSales.length === 0) {
        throw new BadRequestException('Không có dữ liệu để xuất Excel');
      }


      // Debug: Đếm số đơn lỗi
      const errorCount = allSales.filter(s => s.statusAsys === false).length;

      // Enrich dữ liệu với products và departments (batch process)
      // Lấy tất cả itemCode và branchCode unique
      const itemCodes = Array.from(new Set(allSales.map(s => s.itemCode).filter(Boolean)));
      const branchCodes = Array.from(new Set(allSales.map(s => s.branchCode).filter(Boolean)));

      // Fetch products từ Loyalty API (chỉ cho các sale không phải đơn lỗi)
      const loyaltyProductMap = new Map<string, any>();
      const validItemCodes = itemCodes.filter(itemCode => {
        const sale = allSales.find(s => s.itemCode === itemCode);
        return sale && sale.statusAsys !== false;
      });

      // Fetch products từ Loyalty API sử dụng LoyaltyService
      if (validItemCodes.length > 0) {
        const fetchedProducts = await this.loyaltyService.fetchProducts(validItemCodes);
        fetchedProducts.forEach((product, itemCode) => {
          loyaltyProductMap.set(itemCode, product);
        });
      }

      // Fetch departments
      const departmentMap = new Map<string, any>();
      if (branchCodes.length > 0) {
        const departmentPromises = branchCodes.map(async (branchCode) => {
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
              { headers: { accept: 'application/json' } },
            );
            const department = response?.data?.data?.items?.[0];
            return { branchCode, department };
          } catch (error) {
            this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
            return { branchCode, department: null };
          }
        });
        const departmentResults = await Promise.all(departmentPromises);
        departmentResults.forEach(({ branchCode, department }) => {
          if (department) {
            departmentMap.set(branchCode, department);
          }
        });
      }

      // Prepare Excel data với các cột giống frontend (MAIN_COLUMNS)
      // Các cột theo thứ tự: partnerCode, docDate, docCode, kyHieu, description, itemCode, dvt, loai, promCode, maKho, maLo, qty, giaBan, tienHang, tyGia, maThue, tkNo, tkDoanhThu, tkGiaVon, cucThue, maThanhToan, vuViec, boPhan, trangThai, barcode, muaHangGiamGia, chietKhauMuaHangGiamGia, chietKhauCkTheoChinhSach, muaHangCkVip, chietKhauMuaHangCkVip, thanhToanCoupon, chietKhauThanhToanCoupon, thanhToanVoucher, chietKhauThanhToanVoucher, thanhToanTkTienAo, chietKhauThanhToanTkTienAo, voucherDp1, chietKhauVoucherDp1, maCtkmTangHang, maThe, soSerial
      const excelData = allSales.map((sale) => {
        // Sử dụng sale.product đã được enrich từ formatSaleForFrontend (ưu tiên)
        // Fallback về loyaltyProductMap nếu sale.product chưa có
        const saleProduct = (sale as any).product || null;
        const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
        const product = saleProduct || loyaltyProduct || null;
        const department = sale.branchCode ? departmentMap.get(sale.branchCode) : null;
        const brand = sale.customer?.brand || '';
        const brandLower = brand.toLowerCase().trim();
        const normalizedBrand = brandLower === 'facialbar' ? 'f3' : brandLower;

        // Tính toán các giá trị
        const tienHang = sale.tienHang || sale.linetotal || 0;
        const qty = sale.qty || 0;
        let giaBan = sale.giaBan || 0;
        if (giaBan === 0 && tienHang != null && qty > 0) {
          giaBan = tienHang / qty;
        }
        const revenue = sale.revenue || 0;
        const isTangHang = giaBan === 0 && tienHang === 0 && revenue === 0;

        // Tính maKho - ưu tiên từ sale.maKho đã được tính toán
        const maBp = department?.ma_bp || sale.branchCode || null;
        const maKho = sale.maKho || this.calculateMaKho(sale.ordertype || '', maBp) || sale.branchCode || '';

        // Tính maLo - ưu tiên từ sale.maLo đã được tính toán
        let maLo = sale.maLo || '';
        const serial = sale.serial || '';
        if (!maLo && serial) {
          const underscoreIndex = serial.indexOf('_');
          if (underscoreIndex > 0 && underscoreIndex < serial.length - 1) {
            maLo = serial.substring(underscoreIndex + 1);
          } else {
            const trackBatch = saleProduct?.trackBatch ?? loyaltyProduct?.trackBatch ?? product?.trackBatch ?? false;
            if (trackBatch === true) {
              if (normalizedBrand === 'f3') {
                maLo = serial;
              } else {
                const productType = saleProduct?.productType || saleProduct?.producttype ||
                  loyaltyProduct?.productType || loyaltyProduct?.producttype ||
                  product?.productType || product?.producttype || '';
                const productTypeUpper = productType.toUpperCase().trim();
                if (productTypeUpper === 'TPCN') {
                  maLo = serial.length >= 8 ? serial.slice(-8) : serial;
                } else {
                  maLo = serial.length >= 4 ? serial.slice(-4) : serial;
                }
              }
            }
          }
        }

        // Tính soSerial - ưu tiên logic từ sale đã được enrich
        let soSerial = '';
        const trackSerial = saleProduct?.trackSerial ?? loyaltyProduct?.trackSerial ?? product?.trackSerial ?? false;
        const trackBatch = saleProduct?.trackBatch ?? loyaltyProduct?.trackBatch ?? product?.trackBatch ?? false;
        if (trackSerial === true && trackBatch !== true) {
          if (serial && serial.indexOf('_') <= 0) {
            soSerial = serial;
          }
        }

        // Tính promCode (Khuyến mãi)
        let promCode = '';
        const ordertypeName = sale.ordertype || '';
        const isDichVu = ordertypeName.includes('02. Làm dịch vụ') ||
          ordertypeName.includes('04. Đổi DV') ||
          ordertypeName.includes('08. Tách thẻ') ||
          ordertypeName.includes('Đổi thẻ KEEP->Thẻ DV');
        const hasPromCode = sale.promCode && String(sale.promCode).trim() !== '';
        if (!isDichVu && isTangHang) {
          const hasMaThe = sale.maThe && String(sale.maThe).trim() !== '';
          let maCtkmTangHang = sale.maCtkmTangHang || '';
          if (!maCtkmTangHang && isTangHang) {
            if (ordertypeName.includes('06. Đầu tư') || ordertypeName.includes('06.Đầu tư')) {
              maCtkmTangHang = 'TT DAU TU';
            }
          }
          if (!hasMaThe && maCtkmTangHang !== 'TT DAU TU') {
            promCode = '1';
          }
        }

        // Tính maCtkmTangHang
        let maCtkmTangHang = sale.maCtkmTangHang || '';
        if (!maCtkmTangHang && isTangHang) {
          if (ordertypeName.includes('06. Đầu tư') || ordertypeName.includes('06.Đầu tư')) {
            maCtkmTangHang = 'TT DAU TU';
          } else if (
            ordertypeName.includes('01.Thường') || ordertypeName.includes('01. Thường') ||
            ordertypeName.includes('07. Bán tài khoản') || ordertypeName.includes('07.Bán tài khoản') ||
            ordertypeName.includes('9. Sàn TMDT') || ordertypeName.includes('9.Sàn TMDT')
          ) {
            // Quy đổi prom_code sang TANGSP - lấy năm/tháng từ ngày đơn hàng
            const docDate = sale.docDate || (sale as any).order?.docDate;
            // Dùng promCode trực tiếp thay vì convertPromCodeToTangSp
            maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || '';
          } else {
            maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || '';
          }
        }

        // Tính muaHangGiamGia
        let muaHangGiamGia = '';
        if (!isTangHang && sale.promCode) {
          muaHangGiamGia = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || '';
        }

        // Lấy dvt - giống logic trong formatSaleForFrontend: loyaltyProduct?.unit || sale.dvt
        // Ưu tiên: sale.product?.dvt (đã được enrich) > sale.dvt > product?.unit (từ Loyalty API)
        const dvt = saleProduct?.dvt || sale.dvt || product?.unit || loyaltyProduct?.unit || '';

        // Lấy maVatTu - giống logic trong formatSaleForFrontend: loyaltyProduct?.materialCode || sale.itemCode
        // Ưu tiên: sale.product?.maVatTu (đã được enrich) > product?.materialCode > sale.itemCode
        const maVatTu = saleProduct?.maVatTu || product?.materialCode || loyaltyProduct?.materialCode || sale.itemCode || '';

        // Lấy loai - loại đơn hàng (ordertypeName) như "01. Thường", "02. Làm dịch vụ", ...
        // Ưu tiên: sale.ordertypeName > sale.ordertype > sale.loai
        const loai = sale.ordertypeName || sale.ordertype || sale.loai || '';

        return {
          '* Mã khách': sale.partnerCode || sale.customer?.code || '',
          '* Ngày': sale.docDate ? new Date(sale.docDate).toLocaleDateString('vi-VN') : '',
          '* Số hóa đơn': sale.docCode || '',
          '* Ký hiệu': department?.branchcode || sale.branchCode || '',
          'Diễn giải': sale.docCode || '',
          '* Mã hàng': maVatTu,
          'Đvt': dvt,
          'Loại': loai,
          'Khuyến mãi': promCode,
          '* Mã kho': maKho,
          '* Mã lô': maLo,
          'Số lượng': qty,
          'Giá bán': giaBan,
          'Tiền hàng': tienHang,
          'Tỷ giá': sale.tyGia || 1,
          '* Mã thuế': sale.maThue,
          '* Tk nợ': sale.tkNo || '131',
          '* Tk doanh thu': sale.tkDoanhThu || '',
          '* Tk giá vốn': sale.tkGiaVon || '',
          '* Cục thuế': sale.cucThue || '',
          'Mã thanh toán': sale.maThanhToan || '',
          'Vụ việc': sale.vuViec || '',
          'Bộ phận': sale.boPhan || '',
          'Trạng thai': sale.isProcessed ? 'Đã xử lý' : 'Chưa xử lý',
          'Barcode': sale.barcode || '',
          'Mua hàng giảm giá': muaHangGiamGia,
          'Chiết khấu mua hàng giảm giá': sale.chietKhauMuaHangGiamGia || 0,
          'Chiết khấu ck theo chính sách': sale.chietKhauCkTheoChinhSach || 0,
          'Mua hàng CK VIP': sale.muaHangCkVip || '',
          'Chiết khấu mua hàng CK VIP': sale.chietKhauMuaHangCkVip || sale.grade_discamt || 0,
          'Thanh toán coupon': sale.thanhToanCoupon || '',
          'Chiết khấu thanh toán coupon': sale.chietKhauThanhToanCoupon || 0,
          'Thanh toán voucher': sale.thanhToanVoucher || '',
          'Chiết khấu thanh toán voucher': sale.chietKhauThanhToanVoucher || 0,
          'Thanh toán TK tiền ảo': sale.thanhToanTkTienAo || '',
          'Chiết khấu thanh toán TK tiền ảo': sale.chietKhauThanhToanTkTienAo || 0,
          'Voucher DP1': sale.voucherDp1 || '',
          'Chiết khấu Voucher DP1': sale.chietKhauVoucherDp1 || 0,
          'Mã CTKM tặng hàng': maCtkmTangHang,
          'Mã thẻ': sale.maThe || '',
          'Số serial': soSerial,
        };
      });

      // Create workbook
      const wb = XLSX.utils.book_new();
      const ws = XLSX.utils.json_to_sheet(excelData);

      // Style header row
      const range = XLSX.utils.decode_range(ws['!ref'] || 'A1');
      for (let col = range.s.c; col <= range.e.c; col++) {
        const colLetter = XLSX.utils.encode_col(col);
        const headerCellAddress = colLetter + '1';
        if (ws[headerCellAddress]) {
          ws[headerCellAddress].s = {
            fill: {
              fgColor: { rgb: 'E5E7EB' },
              patternType: 'solid',
            },
            font: {
              bold: true,
              color: { rgb: '000000' },
            },
            alignment: {
              horizontal: 'left',
              vertical: 'center'
            },
          };
        }
      }

      // Style data rows - bôi đỏ các dòng có statusAsys = false
      // range.s.r = 0 (header row), data rows bắt đầu từ row 1 (Excel row 2)
      for (let row = range.s.r + 1; row <= range.e.r; row++) {
        // saleIndex = row - range.s.r - 1 = row - 0 - 1 = row - 1
        // Row 1 (Excel row 2, data row đầu tiên) -> saleIndex = 0
        const saleIndex = row - range.s.r - 1;
        const sale = allSales[saleIndex];

        if (!sale) {
          this.logger.warn(`[exportOrders] No sale found for row ${row}, saleIndex ${saleIndex}`);
          continue;
        }

        // Kiểm tra statusAsys: false = đơn lỗi
        // Đảm bảo kiểm tra đúng: statusAsys phải là boolean false
        const statusAsysValue = sale.statusAsys;
        const isErrorRow = statusAsysValue === false;

        // Log để debug
        if (isErrorRow) {
        }

        for (let col = range.s.c; col <= range.e.c; col++) {
          const colLetter = XLSX.utils.encode_col(col);
          // Excel rows là 1-based, nên row 0-based + 1 = Excel row number
          const cellAddress = colLetter + (row + 1);
          const cell = ws[cellAddress];

          if (!cell) {
            continue;
          }

          if (!cell.s) {
            cell.s = {};
          }

          // Set background color: đỏ nhạt cho đơn lỗi
          if (isErrorRow) {
            // Màu đỏ nhạt: #FFE5E5 (RGB: 255, 229, 229)
            cell.s.fill = {
              fgColor: { rgb: 'FFE5E5' },
              patternType: 'solid',
            };
            cell.s.font = {
              color: { rgb: '000000' },
              bold: false,
            };

            // Log sau khi set style (chỉ log cột đầu tiên)
            if (col === range.s.c) {
            }
          } else {
            // Màu trắng cho các dòng bình thường
            cell.s.fill = {
              fgColor: { rgb: 'FFFFFF' },
              patternType: 'solid',
            };
            cell.s.font = {
              color: { rgb: '000000' },
              bold: false,
            };
          }

          cell.s.alignment = {
            horizontal: 'left',
            vertical: 'center'
          };
        }
      }

      // Set column widths
      const colWidths = [
        { wch: 15 }, // Mã đơn hàng
        { wch: 12 }, // Ngày đơn hàng
        { wch: 12 }, // Mã chi nhánh
        { wch: 12 }, // Loại đơn
        { wch: 15 }, // Mã khách hàng
        { wch: 25 }, // Tên khách hàng
        { wch: 12 }, // Số điện thoại
        { wch: 10 }, // Brand
        { wch: 15 }, // Mã sản phẩm
        { wch: 10 }, // Số lượng
        { wch: 12 }, // Giá bán
        { wch: 12 }, // Thành tiền
        { wch: 12 }, // Doanh thu
        { wch: 15 }, // Trạng thái xử lý
        { wch: 15 }, // Trạng thái sync
      ];
      ws['!cols'] = colWidths;

      // Add worksheet to workbook
      XLSX.utils.book_append_sheet(wb, ws, 'Đơn hàng');

      // Convert to buffer
      const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

      return buffer;
    } catch (error: any) {
      this.logger.error(`[exportOrders] Error: ${error?.message || error}`);
      this.logger.error(`[exportOrders] Stack: ${error?.stack || 'No stack trace'}`);
      if (error instanceof BadRequestException) {
        throw error;
      }
      throw new InternalServerErrorException(`Error exporting orders to Excel: ${error?.message || 'Unknown error'}`);
    }
  }

  async cutCode(input: string): Promise<string> {
    return input?.split('-')[0] || '';
  }

}
