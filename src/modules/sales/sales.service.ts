import { Injectable, InternalServerErrorException, Logger, NotFoundException, BadRequestException } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, Or, IsNull } from 'typeorm';
import * as XLSX from 'xlsx-js-style';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { Invoice } from '../../entities/invoice.entity';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { CheckFaceId } from '../../entities/check-face-id.entity';
import { InvoicePrintService } from '../../services/invoice-print.service';
import { InvoiceService } from '../../services/invoice.service';
import { ZappyApiService } from '../../services/zappy-api.service';
import { FastApiService } from '../../services/fast-api.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { CategoriesService } from '../categories/categories.service';
import { Order, SaleItem } from '../../types/order.types';
import { CreateStockTransferDto, StockTransferItem } from '../../dto/create-stock-transfer.dto';
import { ExplainFaceIdDto } from '../../dto/explain-faceid.dto';
import { calculateVCType } from '../../utils/product.utils';

@Injectable()
export class SalesService {
  private readonly logger = new Logger(SalesService.name);

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

    let cucThueValue = sale.cucThue || '';
    if (cucThueValue.includes('F3')) {
      cucThueValue = 'FBV';
    }

    // Lấy productType và trackInventory từ sale hoặc product
    const productType = sale.productType || sale.product?.productType || sale.product?.producttype || null;
    const trackInventory = sale.trackInventory ?? sale.product?.trackInventory ?? null;

    // Sử dụng logic VC mới dựa trên productType và trackInventory
    const vcType = calculateVCType(productType, trackInventory);

    let vcLabel: string | null = null;

    // Nếu có VC type từ logic mới, dùng nó
    if (vcType) {
      vcLabel = vcType;
    } else {
      // Fallback: Logic cũ dựa trên cat1 và itemCode (chỉ khi có paid_by_voucher)
      if (paidByVoucher <= 0) {
        return null;
      }

      const cat1Value = sale.cat1 || sale.catcode1 || sale.product?.cat1 || sale.product?.catcode1 || '';
      const itemCodeValue = sale.itemCode || '';

      // Tập hợp các nhãn sẽ hiển thị
      const labels: string[] = [];

      // VCDV: Nếu cat1 = "CHANDO" hoặc itemcode bắt đầu bằng "S" hoặc "H"
      if (cat1Value === 'CHANDO' || itemCodeValue.toUpperCase().startsWith('S') || itemCodeValue.toUpperCase().startsWith('H')) {
        labels.push('VCDV');
      }

      // VCHB: Nếu cat1 = "FACIALBAR" hoặc itemcode bắt đầu bằng "F" hoặc "V"
      if (cat1Value === 'FACIALBAR' || itemCodeValue.toUpperCase().startsWith('F') || itemCodeValue.toUpperCase().startsWith('V')) {
        labels.push('VCHB');
      }

      vcLabel = labels.length > 0 ? labels.join(' ') : null;
    }

    // Nếu không có label, trả về null
    if (!vcLabel) {
      return null;
    }

    // Normalize "VC KM" thành "VCKM" và "VC HB" thành "VC HB" (giữ nguyên để xử lý sau cho F3)
    let normalizedVcLabel = vcLabel;
    // Normalize "VC KM" thành "VCKM" cho tất cả các brand
    if (normalizedVcLabel.includes('VC KM')) {
      normalizedVcLabel = normalizedVcLabel.replace(/VC\s+KM/g, 'VCKM');
    }

    // Với F3, thêm prefix "FBV TT" trước VC label
    // Và chuyển tất cả VCHB hoặc VCHH thành VCHH
    if (cucThueValue === 'FBV') {
      let finalVcLabel = normalizedVcLabel;
      // Xử lý cả "VCHB" và "VC HB" (có khoảng trắng) - chuyển thành VCHH
      if (finalVcLabel.includes('VCHB') || finalVcLabel.includes('VC HB')) {
        // Thay thế "VC HB" trước, sau đó thay "VCHB"
        finalVcLabel = finalVcLabel.replace(/VC\s+HB/g, 'VCHH');
        finalVcLabel = finalVcLabel.replace(/VCHB/g, 'VCHH');
      }
      // Nếu có VCHH thì giữ nguyên (không cần thay thế)
      return `FBV TT ${finalVcLabel}`;
    }

    return normalizedVcLabel;
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
   * Quy đổi prom_code sang ma_ctkm_th cho trường hợp tặng sản phẩm
   * Quy tắc: PRMN.020255-R510ECOM → 2512MN.TANG SP (nếu đơn hàng tháng 12/2025)
   * - Từ "PRMN.020255": lấy 2 ký tự cuối của phần trước dấu chấm → "MN"
   * - Từ docDate: lấy năm và tháng (ví dụ: 2025-12-14 → "2512")
   * - Kết hợp: "2512MN.TANG SP"
   */
  private convertPromCodeToTangSp(promCode: string | null | undefined, docDate?: string | Date | null): string | null {
    if (!promCode || promCode.trim() === '') return null;

    const parts = promCode.split('-');
    if (parts.length < 2) return null;

    const part1 = parts[0].trim(); // "PRMN.020255"
    const part2 = parts[1].trim(); // "R510ECOM"

    // Lấy 2 ký tự cuối của phần trước dấu chấm từ part1
    const dotIndex = part1.indexOf('.');
    let mnPart = '';
    if (dotIndex > 0) {
      const beforeDot = part1.substring(0, dotIndex); // "PRMN"
      if (beforeDot.length >= 2) {
        mnPart = beforeDot.substring(beforeDot.length - 2); // "MN"
      }
    }

    // Lấy năm và tháng từ docDate (ngày đơn hàng)
    let yearMonth = '';
    if (docDate) {
      try {
        const date = typeof docDate === 'string' ? new Date(docDate) : docDate;
        if (!isNaN(date.getTime())) {
          const year = date.getFullYear();
          const month = date.getMonth() + 1; // getMonth() trả về 0-11
          // Lấy 2 số cuối của năm và tháng (ví dụ: 2025-12 → "2512")
          const yearLast2 = String(year).slice(-2);
          const monthStr = String(month).padStart(2, '0');
          yearMonth = `${yearLast2}${monthStr}`;
        }
      } catch (error) {
        // Nếu không parse được date, fallback về logic cũ
      }
    }

    // Nếu không có docDate hoặc parse thất bại, fallback về logic cũ (parse từ promCode)
    if (!yearMonth) {
      // Parse số từ part2: R510ECOM → tìm số 5 và 10
      // Quy tắc: 5 → 25, 10 → 10
      // Cần tìm "10" trước (2 chữ số), sau đó tìm "5" (1 chữ số)
      const numbers: string[] = [];
      const part2Upper = part2.toUpperCase();

      // Tìm tất cả số "10" trước (2 chữ số)
      let searchIndex = 0;
      while (searchIndex < part2Upper.length - 1) {
        const foundIndex = part2Upper.indexOf('10', searchIndex);
        if (foundIndex >= 0) {
          numbers.push('10');
          searchIndex = foundIndex + 2;
        } else {
          break;
        }
      }

      // Tìm tất cả số "5" (1 chữ số, nhưng không phải là phần của "10")
      searchIndex = 0;
      while (searchIndex < part2Upper.length) {
        if (part2Upper[searchIndex] === '5') {
          // Kiểm tra xem có phải là phần của "10" không (trước đó là "1" hoặc sau đó là "0")
          const isPartOf10 =
            (searchIndex > 0 && part2Upper[searchIndex - 1] === '1') ||
            (searchIndex < part2Upper.length - 1 && part2Upper[searchIndex + 1] === '0');
          if (!isPartOf10) {
            numbers.push('25'); // 5 → 25
          }
        }
        searchIndex++;
      }

      // Sắp xếp: 25 trước, 10 sau
      const sortedNumbers = numbers.sort((a, b) => {
        if (a === '25' && b === '10') return -1;
        if (a === '10' && b === '25') return 1;
        return 0;
      });

      yearMonth = sortedNumbers.join('');
    }

    // Kết hợp: số + MN + ".TANG SP" (có dấu cách giữa "TANG" và "SP" để khớp Loyalty)
    if (yearMonth && mnPart) {
      return `${yearMonth}${mnPart}.TANG SP`;
    }

    return null;
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
   * Tính VIP type dựa trên quy tắc:
   * - Nếu productType == "DIVU" → "VIP DV MAT"
   * - Nếu productType == "VOUC" → "VIP VC MP"
   * - Nếu materialCode bắt đầu bằng "E." hoặc "VC" có trong code hoặc (trackInventory == False và trackSerial == True) → "VIP VC MP"
   * - Ngược lại → "VIP MP"
   */
  private calculateVipType(
    productType: string | null | undefined,
    materialCode: string | null | undefined,
    code: string | null | undefined,
    trackInventory: boolean | null | undefined,
    trackSerial: boolean | null | undefined,
  ): string {
    // Nếu productType == "DIVU"
    if (productType === 'DIVU') {
      return 'VIP DV MAT';
    }

    // Nếu productType == "VOUC" → "VIP VC MP"
    if (productType === 'VOUC') {
      return 'VIP VC MP';
    }

    // Nếu materialCode bắt đầu bằng "E." hoặc "VC" có trong code hoặc (trackInventory == False và trackSerial == True)
    const materialCodeStr = materialCode || '';
    const codeStr = code || '';
    // Kiểm tra "VC" trong materialCode, code, hoặc itemCode (không phân biệt hoa thường)
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

    // Ngược lại
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
   */
  private async fetchLoyaltyProducts(itemCodes: string[]): Promise<Map<string, any>> {
    const productMap = new Map<string, any>();
    if (itemCodes.length === 0) return productMap;

    const productPromises = itemCodes.map(async (itemCode) => {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
          { headers: { accept: 'application/json' } },
        );
        const loyaltyProduct = response?.data?.data?.item || response?.data;
        return { itemCode, loyaltyProduct };
      } catch (error) {
        this.logger.warn(`Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
        return { itemCode, loyaltyProduct: null };
      }
    });
    
    const results = await Promise.all(productPromises);
    results.forEach(({ itemCode, loyaltyProduct }) => {
      if (loyaltyProduct) {
        productMap.set(itemCode, loyaltyProduct);
      }
    });

    return productMap;
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
    const isTangHang = Math.abs(giaBan) < 0.01 && Math.abs(tienHang) < 0.01;
    
    // Kiểm tra dịch vụ
    const ordertypeName = sale.ordertype || '';
    const isDichVu = ordertypeName.includes('02. Làm dịch vụ') ||
      ordertypeName.includes('04. Đổi DV') ||
      ordertypeName.includes('08. Tách thẻ') ||
      ordertypeName.includes('Đổi thẻ KEEP->Thẻ DV');
    
    // Tính toán maCtkmTangHang TRƯỚC (cần dùng cho promCodeDisplay)
    // Lấy maCtkmTangHang từ sale (có thể đã có sẵn từ database hoặc tính toán trước đó)
    let maCtkmTangHang: string | null = sale.maCtkmTangHang ? String(sale.maCtkmTangHang).trim() : null;
    if (!maCtkmTangHang || maCtkmTangHang === '') {
      maCtkmTangHang = null;
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
        const tangSpCode = this.convertPromCodeToTangSp(sale.promCode, sale.docDate);
        maCtkmTangHang = tangSpCode || this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
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

    // Tính toán muaHangCkVip nếu chưa có
    let muaHangCkVip = sale.muaHangCkVip || '';
    const ck03_nt = Number(sale.chietKhauMuaHangCkVip || sale.grade_discamt || 0);
    if (ck03_nt > 0 && !muaHangCkVip) {
      const customerBrand = sale.customer?.brand || null;
      let brandLower = (customerBrand || '').toLowerCase().trim();
      if (brandLower === 'facialbar') {
        brandLower = 'f3';
      }

      const productType = sale.productType || loyaltyProduct?.producttype || loyaltyProduct?.productType || null;
      if (brandLower === 'f3') {
        muaHangCkVip = productType === 'DIVU' ? 'FBV CKVIP DV' : 'FBV CKVIP SP';
      } else {
        const materialCode = loyaltyProduct?.materialCode || sale.itemCode || null;
        const code = sale.itemCode || null;
        const trackInventory = loyaltyProduct?.trackInventory ?? sale.trackInventory ?? null;
        const trackSerial = loyaltyProduct?.trackSerial ?? sale.trackSerial ?? null;
        muaHangCkVip = this.calculateVipType(productType, materialCode, code, trackInventory, trackSerial);
      }
    }

    // Tính toán maLo từ serial nếu chưa có
    let maLo = sale.maLo || '';
    if (!maLo) {
      const serial = sale.serial || '';
      if (serial) {
        const brand = sale.customer?.brand || '';
        const brandLower = (brand || '').toLowerCase().trim();
        const underscoreIndex = serial.indexOf('_');
        if (underscoreIndex > 0 && underscoreIndex < serial.length - 1) {
          maLo = serial.substring(underscoreIndex + 1);
        } else {
          const productType = sale.productType || loyaltyProduct?.producttype || loyaltyProduct?.productType || null;
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
      let brandLower = (brand || '').toLowerCase().trim();
      if (brandLower === 'facialbar') brandLower = 'f3';
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
    const brandLower = brand.toLowerCase().trim();
    const normalizedBrand = brandLower === 'facialbar' ? 'f3' : brandLower;

    // thanhToanCouponDisplay
    const maCoupon = sale.maCk04 || (sale.thanhToanCoupon && sale.thanhToanCoupon > 0 ? 'COUPON' : null);
    const thanhToanCouponDisplay = maCoupon || null;

    // chietKhauThanhToanCouponDisplay
    const chietKhauCoupon = sale.chietKhauThanhToanCoupon ?? sale.chietKhau09 ?? 0;
    const chietKhauThanhToanCouponDisplay = chietKhauCoupon > 0 ? chietKhauCoupon : null;

    // thanhToanVoucherDisplay và chietKhauThanhToanVoucherDisplay
    let thanhToanVoucherDisplay: string | null = null;
    let chietKhauThanhToanVoucherDisplay: number | null = null;
    if (!hasEcoin(order)) {
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
    const cucThueDisplay = sale.cucThue || (normalizedBrand === 'f3' ? 'FBV' : null);

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

  private formatSaleForFrontend(
    sale: any,
    loyaltyProduct: any,
    department: any,
    calculatedFields: { maLo: string; maCtkmTangHang: string | null; muaHangCkVip: string; maKho: string | null; isTangHang: boolean; isDichVu: boolean; promCodeDisplay: string | null },
    order?: any
  ): any {
    // Tính toán giaBan nếu chưa có
    const tienHang = sale.tienHang || sale.linetotal || sale.revenue || 0;
    const qty = sale.qty || 0;
    let giaBan = sale.giaBan || 0;
    if (giaBan === 0 && tienHang != null && qty > 0) {
      giaBan = tienHang / qty;
    }

    // Tính toán muaHangGiamGiaDisplay
    let muaHangGiamGiaDisplay: string | null = null;
    if (!calculatedFields.isTangHang) {
      muaHangGiamGiaDisplay = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
    }

    // Tính toán các field display phức tạp
    const displayFields = this.calculateDisplayFields(sale, order || { customer: sale.customer, cashioData: sale.cashioData, cashioFopSyscode: sale.cashioFopSyscode, cashioTotalIn: sale.cashioTotalIn, brand: sale.brand }, loyaltyProduct, department);

    return {
      ...sale,
      itemName: sale.itemName || loyaltyProduct?.name || null,
      maKho: calculatedFields.maKho,
      maCtkmTangHang: calculatedFields.maCtkmTangHang,
      muaHangCkVip: calculatedFields.muaHangCkVip,
      maLo: calculatedFields.maLo,
      isTangHang: calculatedFields.isTangHang,
      isDichVu: calculatedFields.isDichVu,
      promCodeDisplay: calculatedFields.promCodeDisplay,
      muaHangGiamGiaDisplay: muaHangGiamGiaDisplay,
      giaBan: giaBan, // Đảm bảo giaBan đã được tính toán
      promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
      // Các field display từ calculateDisplayFields
      ...displayFields,
      producttype: loyaltyProduct?.producttype || loyaltyProduct?.productType || sale.productType || null,
      productType: loyaltyProduct?.productType || loyaltyProduct?.producttype || sale.productType || null,
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

    return orders.map(order => {
      const cashioRecords = cashioMap.get(order.docCode) || [];
      const ecoinCashio = cashioRecords.find(c => c.fop_syscode === 'ECOIN');
      const voucherCashio = cashioRecords.find(c => c.fop_syscode === 'VOUCHER');
      const selectedCashio = ecoinCashio || voucherCashio || cashioRecords[0] || null;

      return {
        ...order,
        cashioData: cashioRecords.length > 0 ? cashioRecords : null,
        cashioFopSyscode: selectedCashio?.fop_syscode || null,
        cashioFopDescription: selectedCashio?.fop_description || null,
        cashioCode: selectedCashio?.code || null,
        cashioMasterCode: selectedCashio?.master_code || null,
        cashioTotalIn: selectedCashio?.total_in || null,
        cashioTotalOut: selectedCashio?.total_out || null,
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
    @InjectRepository(CheckFaceId)
    private checkFaceIdRepository: Repository<CheckFaceId>,
    private invoicePrintService: InvoicePrintService,
    private invoiceService: InvoiceService,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private fastApiService: FastApiService,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private categoriesService: CategoriesService,
  ) { }

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

        const loyaltyProductMap = new Map<string, any>();
        // Fetch parallel thay vì sequential
        if (itemCodes.length > 0) {
          const productPromises = itemCodes.map(async (itemCode) => {
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
              { headers: { accept: 'application/json' } },
            );
            const loyaltyProduct = response?.data?.data?.item || response?.data;
              return { itemCode, loyaltyProduct };
          } catch (error) {
            this.logger.warn(`Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
              return { itemCode, loyaltyProduct: null };
            }
          });
          
          const productResults = await Promise.all(productPromises);
          productResults.forEach(({ itemCode, loyaltyProduct }) => {
            if (loyaltyProduct) {
              loyaltyProductMap.set(itemCode, loyaltyProduct);
            }
          });
        }

        // Thêm promotionDisplayCode, maKho, maCtkmTangHang và producttype vào các sales items
        const enrichedOrders = paginatedOrders.map((order) => ({
          ...order,
          sales: order.sales?.map((sale) => {
            const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
            const maBp = department?.ma_bp || sale.branchCode || null;
            const calculatedMaKho = this.calculateMaKho(sale.ordertype, maBp);
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
                const tangSpCode = this.convertPromCodeToTangSp(sale.promCode, docDate);
                maCtkmTangHang = tangSpCode || this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
              } else {
                // Các trường hợp khác: dùng promCode nếu có
                maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || null;
              }
            }

            return {
              ...sale,
              promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
              department: department,
              maKho: calculatedMaKho || sale.maKho || sale.branchCode || null,
              maCtkmTangHang: maCtkmTangHang,
              // Lấy producttype từ Loyalty API (không còn trong database)
              producttype: loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
              product: loyaltyProduct ? {
                ...loyaltyProduct,
                producttype: loyaltyProduct.producttype || loyaltyProduct.productType || null,
                // Đảm bảo productType từ Loyalty API được giữ lại
                productType: loyaltyProduct.productType || loyaltyProduct.producttype || null,
              } : (sale.product || null),
            };
          }) || [],
        }));

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

    // Lấy tất cả sales với filter - CHỈ LẤY BASIC DATA, KHÔNG ENRICH (tối ưu performance)
    let query = this.saleRepository
      .createQueryBuilder('sale')
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.docCode', 'ASC');

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

    const [loyaltyProductMap, departmentMap] = await Promise.all([
      this.fetchLoyaltyProducts(itemCodes),
      this.fetchLoyaltyDepartments(branchCodes),
    ]);

    // Enrich sales với products, departments và tính toán các field phức tạp
    const enrichedSalesMap = new Map<string, any[]>();
    for (const sale of allSalesData) {
      const docCode = sale.docCode;
      if (!enrichedSalesMap.has(docCode)) {
        enrichedSalesMap.set(docCode, []);
      }

      const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
      const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
      const calculatedFields = this.calculateSaleFields(sale, loyaltyProduct, department, sale.branchCode);
      const order = orderMap.get(sale.docCode);
      const enrichedSale = this.formatSaleForFrontend(sale, loyaltyProduct, department, calculatedFields, order);

      enrichedSalesMap.get(docCode)!.push(enrichedSale);
    }

    // Gắn enriched sales vào orders
    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const order = orderMap.get(docCode);
      if (order) {
        order.sales = sales;
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
   */
  async syncErrorOrders(): Promise<{
    total: number;
    success: number;
    failed: number;
    updated: Array<{ id: string; docCode: string; itemCode: string; oldItemCode: string; newItemCode: string }>;
  }> {
    this.logger.log('[syncErrorOrders] Bắt đầu đồng bộ lại đơn lỗi...');

    // Lấy tất cả sales có statusAsys = false, null, hoặc undefined
    // Sử dụng Or để match cả false, null, và undefined
    const errorSales = await this.saleRepository.find({
      where: [
        { statusAsys: false },
        { statusAsys: IsNull() },
      ],
      order: { createdAt: 'DESC' },
    });

    this.logger.log(`[syncErrorOrders] Tìm thấy ${errorSales.length} đơn lỗi cần check lại`);

    let successCount = 0;
    let failCount = 0;
    const updated: Array<{ id: string; docCode: string; itemCode: string; oldItemCode: string; newItemCode: string }> = [];

    // Xử lý theo batch để tối ưu performance
    const BATCH_SIZE = 100; // Xử lý 100 sales mỗi batch
    const CONCURRENT_LIMIT = 10; // Chỉ gọi 10 API cùng lúc để tránh quá tải

    // Helper function để check product từ Loyalty API
    const checkProduct = async (itemCode: string): Promise<any> => {
      // Thử endpoint /products/code/ trước
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
          {
            headers: { accept: 'application/json' },
            timeout: 5000,
          },
        );
        // Parse response: endpoint /products/code/ trả về data.item
        const product = response?.data?.data?.item || response?.data?.data || response?.data;
        if (product) {
          return product;
        }
      } catch (error: any) {
        // Nếu 404, thử fallback
        if (error?.response?.status === 404) {
          // Thử fallback /products/old-code/
          try {
            const fallbackResponse = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/products/old-code/${encodeURIComponent(itemCode)}`,
              {
                headers: { accept: 'application/json' },
                timeout: 5000,
              },
            );
            // Parse response: endpoint /products/old-code/ trả về trực tiếp object
            return fallbackResponse?.data || null;
          } catch (fallbackError: any) {
            // Cả 2 endpoint đều không tìm thấy
            return null;
          }
        }
      }
      return null;
    };

    // Helper function để xử lý một sale
    const processSale = async (sale: any): Promise<{ success: boolean; update?: { id: string; docCode: string; itemCode: string; oldItemCode: string; newItemCode: string } }> => {
      try {
        const itemCode = sale.itemCode || '';
        if (!itemCode) {
          return { success: false };
        }

        const product = await checkProduct(itemCode);

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

    // Xử lý từng batch
    for (let i = 0; i < errorSales.length; i += BATCH_SIZE) {
      const batch = errorSales.slice(i, i + BATCH_SIZE);
      this.logger.log(`[syncErrorOrders] Đang xử lý batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(errorSales.length / BATCH_SIZE)} (${batch.length} sales)`);

      // Xử lý batch với giới hạn concurrent
      const batchResults = await processBatchConcurrent(batch, CONCURRENT_LIMIT);

      // Cập nhật counters
      for (const result of batchResults) {
        if (result.success && result.update) {
          successCount++;
          updated.push(result.update);
          this.logger.debug(`[syncErrorOrders] ✅ Cập nhật sale ${result.update.id} (${result.update.docCode}): ${result.update.oldItemCode} → ${result.update.newItemCode}`);
        } else {
          failCount++;
        }
      }

      // Log progress
      this.logger.log(`[syncErrorOrders] Tiến độ: ${Math.min(i + BATCH_SIZE, errorSales.length)}/${errorSales.length} (${successCount} thành công, ${failCount} thất bại)`);
    }

    this.logger.log(`[syncErrorOrders] Hoàn thành: ${successCount} thành công, ${failCount} thất bại`);

    return {
      total: errorSales.length,
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
    this.logger.log(`[syncErrorOrderByDocCode] Bắt đầu đồng bộ lại đơn ${docCode}...`);

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

    this.logger.log(`[syncErrorOrderByDocCode] Tìm thấy ${errorSales.length} dòng lỗi trong đơn ${docCode}`);

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

        // Check với Loyalty API - thử /products/code/ trước, nếu không có thì thử /products/old-code/
        let product: any = null;
        
        // Thử endpoint /products/code/ trước
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
            {
              headers: { accept: 'application/json' },
              timeout: 5000,
            },
          );
          // Parse response: endpoint /products/code/ trả về data.item
          product = response?.data?.data?.item || response?.data?.data || response?.data;
          if (product) {
            // Tìm thấy tại /products/code/
            this.logger.log(`[syncErrorOrderByDocCode] Tìm thấy sản phẩm ${itemCode} tại /products/code/`);
          }
        } catch (error: any) {
          // Nếu 404, thử fallback
          if (error?.response?.status === 404) {
            this.logger.warn(`[syncErrorOrderByDocCode] Sản phẩm không tìm thấy tại /products/code/: ${itemCode} (404), thử /products/old-code/...`);
          } else {
            // Lỗi khác 404 - không coi là not found
            this.logger.warn(`[syncErrorOrderByDocCode] Lỗi khi fetch product ${itemCode} từ /products/code/: ${error?.message || error?.response?.status || 'Unknown error'}`);
          }
        }

        // Nếu /products/code/ không tìm thấy, thử fallback /products/old-code/
        if (!product) {
          try {
            const fallbackResponse = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/products/old-code/${encodeURIComponent(itemCode)}`,
              {
                headers: { accept: 'application/json' },
                timeout: 5000,
              },
            );
            // Parse response: endpoint /products/old-code/ trả về trực tiếp object (không có wrapper data.item)
            product = fallbackResponse?.data || null;
            if (product) {
              // Tìm thấy tại fallback endpoint
              this.logger.log(`[syncErrorOrderByDocCode] Tìm thấy sản phẩm ${itemCode} tại /products/old-code/`);
            }
          } catch (fallbackError: any) {
            if (fallbackError?.response?.status === 404) {
              this.logger.warn(`[syncErrorOrderByDocCode] Sản phẩm không tìm thấy tại cả /products/code/ và /products/old-code/: ${itemCode}`);
            } else {
              this.logger.warn(`[syncErrorOrderByDocCode] Lỗi khi fetch product ${itemCode} từ /products/old-code/: ${fallbackError?.message || fallbackError?.response?.status || 'Unknown error'}`);
            }
          }
        }

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

          this.logger.log(`[syncErrorOrderByDocCode] ✅ Cập nhật sale ${sale.id} (${docCode}): ${oldItemCode} → ${newItemCode}`);
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

    this.logger.log(`[syncErrorOrderByDocCode] Hoàn thành đơn ${docCode}: ${successCount} thành công, ${failCount} thất bại`);

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
    
    if (validItemCodes.length > 0) {
      const productPromises = validItemCodes.map(async (itemCode) => {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
          { headers: { accept: 'application/json' } },
        );
        const loyaltyProduct = response?.data?.data?.item || response?.data;
          return { itemCode, loyaltyProduct };
      } catch (error) {
        this.logger.warn(`Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
          return { itemCode, loyaltyProduct: null };
        }
      });
      
      const productResults = await Promise.all(productPromises);
      productResults.forEach(({ itemCode, loyaltyProduct }) => {
        if (loyaltyProduct) {
          loyaltyProductMap.set(itemCode, loyaltyProduct);
        }
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

    // Enrich sales với department information và tính maKho
    const enrichedSalesWithDepartment = enrichedSalesWithLoyalty.map((sale) => {
      const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
      const maBp = department?.ma_bp || sale.branchCode || null;
      const calculatedMaKho = this.calculateMaKho(sale.ordertype, maBp);

      return {
        ...sale,
        department: department,
        maKho: calculatedMaKho || sale.maKho || sale.branchCode || null,
      };
    });

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
    const enrichedSalesWithPromotion = enrichedSalesWithDepartment.map((sale) => {
      const promCode = sale.promCode;
      const promotion =
        promCode && promotionsByCode[promCode]
          ? promotionsByCode[promCode]
          : null;

      // Tính lại muaHangCkVip nếu cần (giống logic trong buildFastApiInvoiceData)
      const ck03_nt = Number(sale.chietKhauMuaHangCkVip || sale.grade_discamt || 0);
      let muaHangCkVip = sale.muaHangCkVip || '';

      if (ck03_nt > 0) {
        // Lấy brand từ customer
        const brand = firstSale.customer?.brand || '';
        let brandLower = (brand || '').toLowerCase().trim();
        // Normalize: "facialbar" → "f3"
        if (brandLower === 'facialbar') {
          brandLower = 'f3';
        }

        // Nếu là f3, luôn tính lại theo logic cũ (override giá trị cũ nếu có)
        if (brandLower === 'f3') {
          const productType = sale.productType || sale.product?.productType || sale.product?.producttype || null;
          if (productType === 'DIVU') {
            muaHangCkVip = 'FBV CKVIP DV';
          } else {
            muaHangCkVip = 'FBV CKVIP SP';
          }
        } else if (!muaHangCkVip) {
          // Logic mới cho các brand khác - chỉ tính nếu chưa có
          const productType = sale.productType || sale.product?.productType || sale.product?.producttype || null;
          const materialCode = sale.product?.maVatTu || sale.product?.materialCode || sale.itemCode || null;
          const code = sale.itemCode || null;
          const trackInventory = (sale as any).trackInventory ?? sale.product?.trackInventory ?? null;
          const trackSerial = (sale as any).trackSerial ?? sale.product?.trackSerial ?? null;
          muaHangCkVip = this.calculateVipType(productType, materialCode, code, trackInventory, trackSerial);
        }
      }

      return {
        ...sale,
        promotion,
        promotionDisplayCode: this.getPromotionDisplayCode(promCode),
        muaHangCkVip: muaHangCkVip || sale.muaHangCkVip, // Override với giá trị tính lại nếu có
      };
    });

    return {
      docCode: firstSale.docCode,
      docDate: firstSale.docDate,
      branchCode: firstSale.branchCode,
      docSourceType: firstSale.docSourceType,
      customer: firstSale.customer,
      totalRevenue,
      totalQty,
      totalItems: sales.length,
      sales: enrichedSalesWithPromotion,
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
    const orderData = await this.findByOrderCode(docCode);

    // In hóa đơn
    const printResult = await this.invoicePrintService.printInvoiceFromOrder(orderData);

    // Tạo và lưu invoice vào database
    const invoice = await this.createInvoiceFromOrder(orderData, printResult);

    // Đánh dấu tất cả các sale trong đơn hàng là đã xử lý
    // Đảm bảo luôn được gọi ngay cả khi có lỗi ở trên
    try {
      await this.markOrderAsProcessed(docCode);
    } catch (error) {
      // Log lỗi nhưng không throw để không ảnh hưởng đến response
      console.error(`Lỗi khi đánh dấu đơn hàng ${docCode} là đã xử lý:`, error);
    }

    return {
      success: true,
      message: `In hóa đơn ${docCode} thành công`,
      invoice,
      printResult,
    };
  }

  async printMultipleOrders(docCodes: string[]): Promise<any> {
    const results: Array<{
      docCode: string;
      success: boolean;
      message: string;
      invoice?: Invoice;
      error?: string;
    }> = [];

    for (const docCode of docCodes) {
      try {
        const result = await this.printOrder(docCode);
        results.push({
          docCode,
          success: true,
          message: result.message,
          invoice: result.invoice,
        });
      } catch (error: any) {
        this.logger.error(`Lỗi khi in đơn hàng ${docCode}: ${error?.message || error}`);
        results.push({
          docCode,
          success: false,
          message: `In hóa đơn ${docCode} thất bại`,
          error: error?.response?.data?.message || error?.message || 'Unknown error',
        });
      }
    }

    const successCount = results.filter((r) => r.success).length;
    const failureCount = results.length - successCount;

    return {
      total: results.length,
      successCount,
      failureCount,
      results,
    };
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

  /**
   * Lưu phiếu xuất kho vào bảng warehouse_releases
   */

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

  private async createInvoiceFromOrder(orderData: any, printResult: any): Promise<any> {
    // Kiểm tra xem invoice đã tồn tại chưa (dựa trên key = docCode)
    const existingInvoice = await this.invoiceRepository.findOne({
      where: { key: orderData.docCode },
      relations: ['items'],
    });

    if (existingInvoice) {
      // Cập nhật invoice đã tồn tại
      existingInvoice.isPrinted = true;
      existingInvoice.printResponse = JSON.stringify(printResult);
      await this.invoiceRepository.save(existingInvoice);
      return existingInvoice;
    }

    // Tính toán các giá trị
    const totalAmount = orderData.totalRevenue || 0;
    const taxAmount = Math.round(totalAmount * 0.08); // 8% VAT
    const amountBeforeTax = totalAmount - taxAmount;
    const discountAmount = 0;

    // Format ngày - đảm bảo parse đúng
    let invoiceDate: Date;
    if (orderData.docDate instanceof Date) {
      invoiceDate = orderData.docDate;
    } else if (typeof orderData.docDate === 'string') {
      // Thử parse ISO string trước
      invoiceDate = new Date(orderData.docDate);
      // Kiểm tra nếu date không hợp lệ
      if (isNaN(invoiceDate.getTime())) {
        // Thử parse format khác hoặc fallback
        invoiceDate = new Date(); // Fallback to current date
      }
    } else {
      invoiceDate = new Date(); // Fallback to current date
    }

    // Tạo invoice items từ sales
    const items = orderData.sales.map((sale: any) => {
      const qty = Number(sale.qty);
      const revenue = Number(sale.revenue);
      const price = qty > 0 ? revenue / qty : 0;
      const taxRate = 8.0; // 8% VAT
      const itemTaxAmount = Math.round(revenue * taxRate / 100);
      const itemAmountBeforeTax = revenue - itemTaxAmount;

      return {
        processType: '1',
        itemCode: sale.itemCode || '',
        itemName: sale.itemName || '',
        uom: 'Pcs',
        quantity: qty,
        price: price,
        amount: itemAmountBeforeTax,
        taxRate: taxRate,
        taxAmount: itemTaxAmount,
        discountRate: 0.00,
        discountAmount: 0.00,
      };
    });

    // Format date cho DTO - InvoiceService.parseDate() expect DD/MM/YYYY
    const day = invoiceDate.getDate().toString().padStart(2, '0');
    const month = (invoiceDate.getMonth() + 1).toString().padStart(2, '0');
    const year = invoiceDate.getFullYear();
    const invoiceDateStr = `${day}/${month}/${year}`;

    // Tạo invoice DTO
    const invoiceDto = {
      key: orderData.docCode, // Sử dụng docCode làm key
      invoiceDate: invoiceDateStr,
      customerCode: orderData.customer?.code || '',
      customerName: orderData.customer?.name || '',
      customerTaxCode: '',
      address: orderData.customer?.street || orderData.customer?.address || '',
      phoneNumber: orderData.customer?.phone || orderData.customer?.mobile || '',
      idCardNo: orderData.customer?.idnumber || '',
      voucherBook: '1C25MCD',
      items: items,
    };

    // Tạo invoice
    const invoice = await this.invoiceService.createInvoice(invoiceDto);

    // Cập nhật trạng thái đã in và lưu response
    invoice.isPrinted = true;
    invoice.printResponse = JSON.stringify(printResult);
    await this.invoiceRepository.save(invoice);

    return invoice;
  }

  /**
   * Đồng bộ dữ liệu từ Zappy API và lưu vào database
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 04DEC2025)
   * @returns Kết quả đồng bộ
   */
  async syncFromZappy(date: string): Promise<{
    success: boolean;
    message: string;
    ordersCount: number;
    salesCount: number;
    customersCount: number;
    errors?: string[];
  }> {

    try {
      // Lấy dữ liệu từ Zappy API
      const orders = await this.zappyApiService.getDailySales(date);

      // Lấy dữ liệu cash/voucher từ get_daily_cash để enrich
      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date);
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
            this.logger.log(`[SalesService] Đang check ${orderItemCodes.length} sản phẩm từ Loyalty API cho order ${order.docCode}...`);
            await Promise.all(
              orderItemCodes.map(async (trimmedItemCode) => {
                let isNotFound = false;

                // Check endpoint /products/code/ trước
                try {
                  const response = await this.httpService.axiosRef.get(
                    `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(trimmedItemCode)}`,
                    {
                      headers: { accept: 'application/json' },
                      timeout: 5000,
                    },
                  );
                  // Parse response: endpoint /products/code/ trả về data.item
                  const loyaltyProduct = response?.data?.data?.item || response?.data?.data || response?.data;
                  // Nếu có data, sản phẩm tồn tại → return sớm
                  if (loyaltyProduct) {
                    return;
                  }
                  // Nếu response 200 nhưng không có data → thử fallback
                  this.logger.warn(`[SalesService] Response 200 nhưng không có data tại /products/code/: ${trimmedItemCode}, thử /products/old-code/...`);
                } catch (error: any) {
                  // Nếu 404 → thử fallback
                  if (error?.response?.status === 404) {
                    this.logger.warn(`[SalesService] Sản phẩm không tồn tại tại /products/code/: ${trimmedItemCode} (404), thử /products/old-code/...`);
                  } else {
                    // Lỗi khác 404 - không coi là not found, có thể là network error
                    this.logger.warn(`[SalesService] Lỗi khi fetch product ${trimmedItemCode} từ /products/code/: ${error?.message || error?.response?.status || 'Unknown error'}`);
                    return; // Không phải 404, không check fallback
                  }
                }

                // Nếu đến đây, /products/code/ không tìm thấy → thử fallback /products/old-code/
                try {
                  const fallbackResponse = await this.httpService.axiosRef.get(
                    `https://loyaltyapi.vmt.vn/products/old-code/${encodeURIComponent(trimmedItemCode)}`,
                    {
                      headers: { accept: 'application/json' },
                      timeout: 5000,
                    },
                  );
                  // Parse response: endpoint /products/old-code/ trả về trực tiếp object (không có wrapper data.item)
                  const loyaltyProduct = fallbackResponse?.data || null;
                  if (loyaltyProduct) {
                    // Tìm thấy tại fallback endpoint
                    this.logger.log(`[SalesService] Tìm thấy sản phẩm ${trimmedItemCode} tại /products/old-code/`);
                    return;
                  }
                  // Nếu fallback response 200 nhưng không có data → đánh dấu not found
                  isNotFound = true;
                  this.logger.warn(`[SalesService] Response 200 nhưng không có data tại /products/old-code/: ${trimmedItemCode}`);
                } catch (fallbackError: any) {
                  if (fallbackError?.response?.status === 404) {
                    // Cả 2 endpoint đều 404 → đánh dấu not found
                    isNotFound = true;
                    this.logger.warn(`[SalesService] Sản phẩm không tồn tại tại cả /products/code/ và /products/old-code/: ${trimmedItemCode}`);
                  } else {
                    // Lỗi khác 404 - không coi là not found
                    this.logger.warn(`[SalesService] Lỗi khi fetch product ${trimmedItemCode} từ /products/old-code/: ${fallbackError?.message || fallbackError?.response?.status || 'Unknown error'}`);
                  }
                }

                // Đánh dấu not found nếu cả 2 endpoint đều không tìm thấy
                if (isNotFound) {
                  notFoundItemCodes.add(trimmedItemCode);
                  this.logger.log(`[SalesService] Đã thêm ${trimmedItemCode} vào danh sách bỏ qua (notFoundItemCodes size: ${notFoundItemCodes.size})`);
                }
              }),
            );
            this.logger.log(`[SalesService] Hoàn thành check sản phẩm. Tổng số sản phẩm không tồn tại: ${notFoundItemCodes.size}`);
          }

          // Xử lý từng sale trong order - LƯU TẤT CẢ, đánh dấu statusAsys = false nếu sản phẩm không tồn tại (404)
          if (order.sales && order.sales.length > 0) {
            for (const saleItem of order.sales) {
              try {
                // Kiểm tra xem sản phẩm có tồn tại trong Loyalty API không
                const itemCode = saleItem.itemCode?.trim();
                const isNotFound = itemCode && notFoundItemCodes.has(itemCode);
                // Set statusAsys: false nếu không tồn tại (404), true nếu tồn tại
                const statusAsys = !isNotFound;

                if (isNotFound) {
                  this.logger.warn(`[SalesService] Sale item ${itemCode} (${saleItem.itemName || 'N/A'}) trong order ${order.docCode} - Sản phẩm không tồn tại trong Loyalty API (404), sẽ lưu với statusAsys = false`);
                }

                // Kiểm tra xem sale đã tồn tại chưa (dựa trên docCode, itemCode)
                const existingSale = await this.saleRepository.findOne({
                  where: {
                    docCode: order.docCode,
                    itemCode: saleItem.itemCode,
                    customer: { id: customer.id },
                  },
                });

                // Enrich voucher data từ get_daily_cash
                let voucherRefno: string | undefined;
                let voucherAmount: number | undefined;
                if (voucherData.length > 0) {
                  // Lấy voucher đầu tiên (có thể có nhiều voucher)
                  const firstVoucher = voucherData[0];
                  voucherRefno = firstVoucher.refno;
                  voucherAmount = firstVoucher.total_in || 0;
                }

                if (existingSale) {
                  // Cập nhật sale đã tồn tại
                  existingSale.qty = saleItem.qty || existingSale.qty;
                  existingSale.revenue = saleItem.revenue || existingSale.revenue;
                  existingSale.linetotal = saleItem.linetotal || existingSale.linetotal;
                  existingSale.tienHang = saleItem.tienHang || existingSale.tienHang;
                  existingSale.giaBan = saleItem.giaBan || existingSale.giaBan;
                  existingSale.itemName = saleItem.itemName || existingSale.itemName;
                  existingSale.ordertype = saleItem.ordertype || existingSale.ordertype;
                  existingSale.branchCode = saleItem.branchCode || existingSale.branchCode;
                  existingSale.promCode = saleItem.promCode || existingSale.promCode;
                  existingSale.serial = saleItem.serial !== undefined ? saleItem.serial : existingSale.serial;
                  existingSale.soSerial = saleItem.serial !== undefined ? saleItem.serial : existingSale.soSerial;
                  existingSale.disc_amt = saleItem.disc_amt || existingSale.disc_amt;
                  existingSale.grade_discamt = saleItem.grade_discamt || existingSale.grade_discamt;
                  existingSale.other_discamt = saleItem.other_discamt !== undefined ? saleItem.other_discamt : existingSale.other_discamt;
                  existingSale.chietKhauMuaHangGiamGia = saleItem.chietKhauMuaHangGiamGia !== undefined ? saleItem.chietKhauMuaHangGiamGia : existingSale.chietKhauMuaHangGiamGia;
                  existingSale.paid_by_voucher_ecode_ecoin_bp = saleItem.paid_by_voucher_ecode_ecoin_bp || existingSale.paid_by_voucher_ecode_ecoin_bp;
                  existingSale.maCa = saleItem.shift_code || existingSale.maCa;
                  existingSale.saleperson_id = saleItem.saleperson_id || existingSale.saleperson_id;
                  existingSale.partnerCode = saleItem.partnerCode || existingSale.partnerCode;
                  existingSale.partner_name = saleItem.partner_name || existingSale.partner_name;
                  existingSale.order_source = saleItem.order_source || existingSale.order_source;
                  // Lưu mvc_serial vào maThe
                  existingSale.maThe = saleItem.mvc_serial !== undefined ? saleItem.mvc_serial : existingSale.maThe;
                  // Category fields
                  existingSale.cat1 = saleItem.cat1 !== undefined ? saleItem.cat1 : existingSale.cat1;
                  existingSale.cat2 = saleItem.cat2 !== undefined ? saleItem.cat2 : existingSale.cat2;
                  existingSale.cat3 = saleItem.cat3 !== undefined ? saleItem.cat3 : existingSale.cat3;
                  existingSale.catcode1 = saleItem.catcode1 !== undefined ? saleItem.catcode1 : existingSale.catcode1;
                  existingSale.catcode2 = saleItem.catcode2 !== undefined ? saleItem.catcode2 : existingSale.catcode2;
                  existingSale.catcode3 = saleItem.catcode3 !== undefined ? saleItem.catcode3 : existingSale.catcode3;
                  // Enrich voucher data
                  if (voucherRefno) {
                    existingSale.voucherDp1 = voucherRefno;
                  }
                  if (voucherAmount !== undefined && voucherAmount > 0) {
                    existingSale.thanhToanVoucher = voucherAmount;
                  }
                  existingSale.statusAsys = statusAsys; // Update statusAsys
                  await this.saleRepository.save(existingSale);
                } else {
                  // Tạo sale mới
                  const newSale = this.saleRepository.create({
                    docCode: order.docCode,
                    docDate: new Date(order.docDate),
                    branchCode: order.branchCode,
                    docSourceType: order.docSourceType,
                    ordertype: saleItem.ordertype,
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
                    saleperson_id: saleItem.saleperson_id,
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
                    // Enrich voucher data từ get_daily_cash
                    voucherDp1: voucherRefno,
                    thanhToanVoucher: voucherAmount && voucherAmount > 0 ? voucherAmount : undefined,
                    customer: customer,
                    isProcessed: false,
                    statusAsys: statusAsys, // Set statusAsys: true nếu sản phẩm tồn tại, false nếu 404
                  } as Partial<Sale>);
                  await this.saleRepository.save(newSale);
                  salesCount++;
                }
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
        message: `Đồng bộ thành công ${orders.length} đơn hàng cho ngày ${date}`,
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
            responseMessage = firstItem.message || 'Tạo hóa đơn thành công';
            responseGuid = Array.isArray(firstItem.guid) ? firstItem.guid[0] : firstItem.guid || null;
          } else {
            // status === 0 hoặc khác = lỗi
            isSuccess = false;
            responseStatus = firstItem.status ?? 0;
            responseMessage = firstItem.message || firstItem.error || 'Tạo hóa đơn thất bại';
          }
        }
      } else if (result && typeof result === 'object') {
        // Nếu result không phải mảng
        if (result.status === 1) {
          isSuccess = true;
          responseStatus = 1;
          responseMessage = result.message || 'Tạo hóa đơn thành công';
          responseGuid = result.guid || null;
        } else {
          isSuccess = false;
          responseStatus = result.status ?? 0;
          responseMessage = result.message || result.error || 'Tạo hóa đơn thất bại';
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
      await this.markOrderAsProcessed(docCode);

      // Log màu vàng khi tạo thành công: "TẠI ĐƠN (MÃ ĐƠN)-Ngày(Ngày đocate)"
      const ngayCtDate = invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date();
      const ngayCtFormatted = ngayCtDate.toLocaleDateString('vi-VN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
      });
      this.logger.log(`\x1b[33mTẠI ĐƠN (${docCode})-Ngày(${ngayCtFormatted})\x1b[0m`);

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

      // Xử lý từng sale với index để tính dong
      const detail = await Promise.all(allSales.map(async (sale: any, index: number) => {
        const tienHang = toNumber(sale.tienHang || sale.linetotal || sale.revenue, 0);
        const qty = toNumber(sale.qty, 0);
        let giaBan = toNumber(sale.giaBan, 0);
        if (tienHang > 0 && qty > 0) {
          giaBan = tienHang / qty;
        }

        // Tính toán các chiết khấu
        const ck01_nt = toNumber(sale.other_discamt || sale.chietKhauMuaHangGiamGia, 0);
        const ck02_nt = toNumber(sale.chietKhauCkTheoChinhSach, 0);
        const ck03_nt = toNumber(sale.chietKhauMuaHangCkVip || sale.grade_discamt, 0);

        // Tính VIP type nếu có chiết khấu VIP
        // Lấy brand từ orderData để phân biệt logic VIP
        const brand = orderData.customer?.brand || orderData.brand || '';
        let brandLower = (brand || '').toLowerCase().trim();
        // Normalize: "facialbar" → "f3"
        if (brandLower === 'facialbar') {
          brandLower = 'f3';
        }

        let maCk03 = sale.muaHangCkVip || '';
        if (ck03_nt > 0) {
          // Nếu là f3, luôn tính lại theo logic cũ (override giá trị cũ nếu có)
          if (brandLower === 'f3') {
            // Logic cũ cho f3: DIVU → "FBV CKVIP DV", còn lại → "FBV CKVIP SP"
            const productType = sale.productType || sale.product?.productType || sale.product?.producttype || null;
            if (productType === 'DIVU') {
              maCk03 = 'FBV CKVIP DV';
            } else {
              maCk03 = 'FBV CKVIP SP';
            }
          } else if (!maCk03) {
            // Logic mới cho các brand khác (menard, labhair, yaman) - chỉ tính nếu chưa có
            const productType = sale.productType || sale.product?.productType || sale.product?.producttype || null;
            const materialCode = sale.product?.maVatTu || sale.product?.materialCode || sale.itemCode || null;
            const code = sale.itemCode || null;
            const trackInventory = sale.trackInventory ?? sale.product?.trackInventory ?? null;
            const trackSerial = sale.trackSerial ?? sale.product?.trackSerial ?? null;
            maCk03 = this.calculateVipType(productType, materialCode, code, trackInventory, trackSerial);
          }
        }
        // ma_ck04: Thanh toán coupon
        const ck04_nt = toNumber(sale.chietKhauThanhToanCoupon || sale.chietKhau09, 0);
        // ma_ck15: Voucher DP1 dự phòng - Ưu tiên kiểm tra trước
        let ck15_nt_voucherDp1 = toNumber(sale.chietKhauVoucherDp1, 0);
        const paidByVoucher = toNumber(sale.chietKhauThanhToanVoucher || sale.paid_by_voucher_ecode_ecoin_bp, 0);

        // Kiểm tra các điều kiện để xác định voucher dự phòng
        const pkgCode = (sale as any).pkg_code || (sale as any).pkgCode || null;
        const promCode = sale.promCode || sale.prom_code || null;
        const soSource = sale.order_source || (sale as any).so_source || null;
        const productType = sale.productType || sale.producttype || sale.product?.productType || sale.product?.producttype || null;

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
        if (ck15_nt_voucherDp1 > 0 && !isVoucherDuPhong) {
          voucherAmountToMove = ck15_nt_voucherDp1;
          ck15_nt_voucherDp1 = 0;
        }

        // Fallback: Nếu chietKhauVoucherDp1 = 0 nhưng thỏa điều kiện voucher dự phòng
        // → Đây là dữ liệu cũ chưa sync, coi là voucher dự phòng
        if (ck15_nt_voucherDp1 === 0 && paidByVoucher > 0 && isVoucherDuPhong) {
          ck15_nt_voucherDp1 = paidByVoucher;
        }

        // ma_ck05: Thanh toán voucher chính
        // Chỉ map vào ck05_nt nếu không có voucher dự phòng (ck15_nt_voucherDp1 = 0)
        // Nếu có voucherAmountToMove (chuyển từ DP sang chính), dùng giá trị đó
        // Nếu không, dùng paidByVoucher
        const ck05_nt = ck15_nt_voucherDp1 > 0 ? 0 : (voucherAmountToMove > 0 ? voucherAmountToMove : paidByVoucher);
        // Tính ma_ck05 giống frontend - truyền customer từ orderData nếu sale chưa có
        const saleWithCustomer = {
          ...sale,
          customer: sale.customer || orderData.customer,
          brand: sale.customer?.brand || orderData.customer?.brand || sale?.brand || orderData?.brand,
        };
        const maCk05Value = this.calculateMaCk05(saleWithCustomer);
        const formattedMaCk05 = this.formatVoucherCode(maCk05Value);
        const ck06_nt = 0; // Dự phòng 1 - không sử dụng
        const ck07_nt = toNumber(sale.chietKhauVoucherDp2, 0);
        const ck08_nt = toNumber(sale.chietKhauVoucherDp3, 0);
        // Các chiết khấu từ 09-22 mặc định là 0
        const ck09_nt = toNumber(sale.chietKhau09, 0);
        const ck10_nt = toNumber(sale.chietKhau10, 0);
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
        const ck12_nt = toNumber(sale.chietKhau12, 0);
        const ck13_nt = toNumber(sale.chietKhau13, 0);
        const ck14_nt = toNumber(sale.chietKhau14, 0);
        const ck15_nt = ck15_nt_voucherDp1 > 0 ? ck15_nt_voucherDp1 : toNumber(sale.chietKhau15, 0);
        const ck16_nt = toNumber(sale.chietKhau16, 0);
        const ck17_nt = toNumber(sale.chietKhau17, 0);
        const ck18_nt = toNumber(sale.chietKhau18, 0);
        const ck19_nt = toNumber(sale.chietKhau19, 0);
        const ck20_nt = toNumber(sale.chietKhau20, 0);
        const ck21_nt = toNumber(sale.chietKhau21, 0);
        const ck22_nt = toNumber(sale.chietKhau22, 0);

        // Tính tổng chiết khấu
        const tongChietKhau = ck01_nt + ck02_nt + ck03_nt + ck04_nt + ck05_nt + ck06_nt + ck07_nt + ck08_nt +
          ck09_nt + ck10_nt + ck11_nt + ck12_nt + ck13_nt + ck14_nt + ck15_nt + ck16_nt +
          ck17_nt + ck18_nt + ck19_nt + ck20_nt + ck21_nt + ck22_nt;

        // tien_hang phải là giá gốc (trước chiết khấu)
        // Ưu tiên: mn_linetotal > linetotal > tienHang > (revenue + tongChietKhau)
        let tienHangGoc = toNumber((sale as any).mn_linetotal || sale.linetotal || sale.tienHang, 0);
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

        // Tính mã kho từ ordertype + ma_bp (bộ phận)
        // Nếu không tính được thì fallback về sale.maKho hoặc branchCode
        const maBpForMaKho = sale.department?.ma_bp || sale.branchCode || orderData.branchCode || '';
        const calculatedMaKho = this.calculateMaKho(sale.ordertype, maBpForMaKho);
        let maKho = toString(calculatedMaKho || sale.maKho || sale.branchCode, '');
        
        // FIX: Nếu ma_bp = "MSO1" và có mã kho, fix cứng mã kho = "BMHT2"
        if (maBpForMaKho === 'MSO1' && maKho) {
          maKho = 'BMHT2';
        }

        // Debug: Log maLo value từ sale
        if (index === 0) {
        }

        // Fetch trackSerial và trackBatch từ Loyalty API để xác định dùng ma_lo hay so_serial
        // Nếu ma_vt (materialCode) khác itemCode, cần fetch lại product bằng materialCode
        const materialCode = sale.product?.maVatTu || sale.product?.materialCode || sale.itemCode;
        let trackSerial: boolean | null = null;
        let trackBatch: boolean | null = null;
        let trackInventory: boolean | null = null;
        let productTypeFromLoyalty: string | null = null;

        // Luôn fetch từ Loyalty API để lấy trackSerial, trackBatch, trackInventory và productType
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(materialCode)}`,
            { headers: { accept: 'application/json' } },
          );
          const loyaltyProduct = response?.data?.data?.item || response?.data;
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
        } catch (error) {
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

        // Extract chỉ phần số từ ordertype (ví dụ: "01.Thường" -> "01", "02. Làm dịch vụ" -> "02")
        let loaiGd = '01';
        if (sale.ordertype) {
          const match = String(sale.ordertype).match(/^(\d+)/);
          loaiGd = match ? match[1] : '01';
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
              const tangSpCode = this.convertPromCodeToTangSp(sale.promCode, orderData.docDate);
              maCtkmTangHang = tangSpCode || toString(sale.promotionDisplayCode || sale.promCode, '');
            } else {
              // Các trường hợp khác: dùng promCode nếu có
              maCtkmTangHang = toString(sale.promotionDisplayCode || sale.promCode, '');
            }
          }
        } else {
          // Nếu không phải hàng tặng, dùng giá trị từ sale.maCtkmTangHang (nếu có)
          maCtkmTangHang = toString(sale.maCtkmTangHang, '');
        }

        // Nếu là hàng tặng, không set ma_ck01 (Mã CTKM mua hàng giảm giá)
        // Nếu không phải hàng tặng, set ma_ck01 từ promCode như cũ
        const maCk01 = isTangHang ? '' : (sale.promCode ? sale.promCode : '');

        // Kiểm tra nếu ma_ctkm_th = "TT DAU TU" thì không set km_yn = 1
        const isTTDauTu = maCtkmTangHang && maCtkmTangHang.trim() === 'TT DAU TU';

        return {
          // ma_vt: Mã vật tư (String, max 16 ký tự) - Bắt buộc
          ma_vt: limitString(toString(sale.product?.maVatTu || sale.itemCode || ''), 16),
          // dvt: Đơn vị tính (String, max 32 ký tự) - Bắt buộc
          dvt: limitString(dvt, 32),
          // loai: Loại (String, max 2 ký tự) - 07-phí,lệ phí; 90-giảm thuế (mặc định rỗng)
          loai: limitString(loai, 2),
          // ma_ctkm_th: Mã ctkm tặng hàng (String, max 32 ký tự)
          ma_ctkm_th: limitString(maCtkmTangHang, 32),
          // ma_kho: Mã kho (String, max 16 ký tự) - Bắt buộc
          ma_kho: limitString(maKho, 16),
          // so_luong: Số lượng (Decimal)
          so_luong: Number(qty),
          // gia_ban: Giá bán (Decimal) - giá gốc trước chiết khấu
          gia_ban: Number(giaBan),
          // tien_hang: Tiền hàng (Decimal) - giá gốc trước chiết khấu
          tien_hang: Number(tienHangGoc),
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
          ...(ck05_nt > 0 ? {
            ma_ck05: limitString(formattedMaCk05 || toString(sale.maCk05 || 'VOUCHER', ''), 32),
          } : {}),
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
          // dt_tg_nt: Tiền trợ giá (Decimal)
          dt_tg_nt: Number(toNumber(sale.dtTgNt, 0)),
          // ma_thue: Mã thuế (String, max 8 ký tự) - Bắt buộc
          // Nếu không có mã thuế, set thành "00" thay vì "10"
          ma_thue: limitString(toString(sale.maThue, '00'), 8),
          // thue_suat: Thuế suất (Decimal)
          thue_suat: Number(toNumber(sale.thueSuat, 0)),
          // tien_thue: Tiền thuế (Decimal)
          tien_thue: Number(toNumber(sale.tienThue, 0)),
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
        };
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

      // Extract chỉ phần số từ ordertype (ví dụ: "01.Thường" -> "01")
      let loaiGd = '01';
      if (firstSale?.ordertype) {
        const match = String(firstSale.ordertype).match(/^(\d+)/);
        loaiGd = match ? match[1] : '01';
      }

      // Lấy ma_dvcs từ department API (ưu tiên), nếu không có thì fallback
      const maDvcs = firstSale?.department?.ma_dvcs
        || firstSale?.department?.ma_dvcs_ht
        || orderData.customer?.brand
        || orderData.branchCode
        || '';

      return {
        action: 0,
        ma_dvcs: maDvcs,
        ma_kh: orderData.customer?.code || '',
        ong_ba: orderData.customer?.name || null,
        ma_gd: '2',
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
   * Tạo phiếu xuất kho từ STOCK_TRANSFER data
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

    // Lấy ma_kh từ order
    const maKh = orderData?.customer?.code || '';

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
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(item.item_code)}`,
              { headers: { accept: 'application/json' } },
            );
            const loyaltyProduct = response?.data?.data?.item || response?.data;
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
      ma_gd: '2', // Mã giao dịch: 2 - Xuất nội bộ
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
   * Build warehouse release data cho Fast API
   */

  /**
   * Lấy checkFaceID data theo partner_code và date
   * @param partnerCode - Partner code (customer code)
   * @param date - Date format: YYYY-MM-DD hoặc DDMMMYYYY
   */
  async getCheckFaceIdByPartnerCode(partnerCode: string, date?: string): Promise<CheckFaceId[]> {
    const query = this.checkFaceIdRepository
      .createQueryBuilder('checkFaceId')
      .where('checkFaceId.code = :partnerCode', { partnerCode })
      .orderBy('checkFaceId.startTime', 'DESC');

    if (date) {
      // Parse date nếu là format DDMMMYYYY
      let dateObj: Date;
      if (date.includes('-')) {
        // Format: YYYY-MM-DD
        dateObj = new Date(date);
      } else {
        // Format: DDMMMYYYY
        const day = parseInt(date.substring(0, 2));
        const monthStr = date.substring(2, 5).toUpperCase();
        const year = parseInt(date.substring(5, 9));
        const monthMap: Record<string, number> = {
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
        };
        dateObj = new Date(year, monthMap[monthStr] || 0, day);
      }
      query.andWhere('DATE(checkFaceId.date) = DATE(:date)', { date: dateObj });
    }

    return query.getMany();
  }

  /**
   * Lấy orders với checkFaceID data theo partner_code
   * @param partnerCode - Partner code (customer code)
   * @param date - Date format: YYYY-MM-DD hoặc DDMMMYYYY
   */
  async getOrdersWithCheckFaceId(partnerCode: string, date?: string): Promise<{
    orders: Order[];
    checkFaceIds: CheckFaceId[];
  }> {
    // Lấy orders theo partner_code với date filter và limit để tránh query quá nhiều records
    const queryBuilder = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer')
      .where('sale.partnerCode = :partnerCode', { partnerCode })
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.createdAt', 'DESC')
      .limit(1000); // Giới hạn tối đa 1000 records để tránh query quá chậm

    // Thêm date filter nếu có
    if (date) {
      let dateObj: Date;
      if (date.includes('-')) {
        // Format: YYYY-MM-DD
        dateObj = new Date(date);
      } else {
        // Format: DDMMMYYYY
        const day = parseInt(date.substring(0, 2));
        const monthStr = date.substring(2, 5).toUpperCase();
        const year = parseInt(date.substring(5, 9));
        const monthMap: Record<string, number> = {
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
        };
        dateObj = new Date(year, monthMap[monthStr] || 0, day);
      }
      queryBuilder.andWhere('DATE(sale.docDate) = DATE(:date)', { date: dateObj });
    }

    const sales = await queryBuilder.getMany();

    // Lấy tất cả mobile và partnerCode từ sales để query FaceID
    const allMobiles = new Set<string>();
    const allPartnerCodes = new Set<string>();
    for (const sale of sales) {
      const mobile = sale.mobile ? String(sale.mobile).trim() : (sale.customer?.mobile ? String(sale.customer.mobile).trim() : null);
      if (mobile && mobile.length > 0) {
        allMobiles.add(mobile);
      }
      if (sale.partnerCode) {
        allPartnerCodes.add(String(sale.partnerCode).trim());
      }
    }

    // Query FaceID theo mobile hoặc partnerCode
    const normalizedMobiles = Array.from(allMobiles).map((mobile) => String(mobile).trim()).filter(m => m.length > 0);
    const normalizedPartnerCodes = Array.from(allPartnerCodes).map((code) => String(code).trim()).filter(c => c.length > 0);

    let checkFaceIdQuery = this.checkFaceIdRepository
      .createQueryBuilder('checkFaceId')
      .orderBy('checkFaceId.date', 'DESC')
      .addOrderBy('checkFaceId.startTime', 'DESC');

    if (normalizedMobiles.length > 0 && normalizedPartnerCodes.length > 0) {
      checkFaceIdQuery = checkFaceIdQuery.where(
        '(TRIM(checkFaceId.mobile) IN (:...mobiles) OR TRIM(checkFaceId.partnerCode) IN (:...partnerCodes))',
        { mobiles: normalizedMobiles, partnerCodes: normalizedPartnerCodes }
      );
    } else if (normalizedMobiles.length > 0) {
      checkFaceIdQuery = checkFaceIdQuery.where('TRIM(checkFaceId.mobile) IN (:...mobiles)', { mobiles: normalizedMobiles });
    } else if (normalizedPartnerCodes.length > 0) {
      checkFaceIdQuery = checkFaceIdQuery.where('TRIM(checkFaceId.partnerCode) IN (:...partnerCodes)', { partnerCodes: normalizedPartnerCodes });
    } else {
      checkFaceIdQuery = checkFaceIdQuery.where('1 = 0');
    }

    if (date) {
      let dateObj: Date;
      if (date.includes('-')) {
        dateObj = new Date(date);
      } else {
        const day = parseInt(date.substring(0, 2));
        const monthStr = date.substring(2, 5).toUpperCase();
        const year = parseInt(date.substring(5, 9));
        const monthMap: Record<string, number> = {
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
        };
        dateObj = new Date(year, monthMap[monthStr] || 0, day);
      }
      checkFaceIdQuery = checkFaceIdQuery.andWhere('DATE(checkFaceId.date) = DATE(:date)', { date: dateObj });
    }

    const allFaceRows = await checkFaceIdQuery.getMany();

    // Map FaceID theo mobile và partnerCode
    const checkFaceIdsByMobile = new Map<string, CheckFaceId[]>();
    const checkFaceIdsByPartnerCode = new Map<string, CheckFaceId[]>();
    for (const cf of allFaceRows) {
      if (cf.mobile) {
        const normalizedMobile = String(cf.mobile).trim();
        if (normalizedMobile && normalizedMobile.length > 0) {
          if (!checkFaceIdsByMobile.has(normalizedMobile)) {
            checkFaceIdsByMobile.set(normalizedMobile, []);
          }
          checkFaceIdsByMobile.get(normalizedMobile)!.push(cf);
        }
      }
      if (cf.partnerCode) {
        const normalizedCode = String(cf.partnerCode).trim();
        if (normalizedCode && normalizedCode.length > 0) {
          if (!checkFaceIdsByPartnerCode.has(normalizedCode)) {
            checkFaceIdsByPartnerCode.set(normalizedCode, []);
          }
          checkFaceIdsByPartnerCode.get(normalizedCode)!.push(cf);
        }
      }
    }

    // Group sales by docCode
    const ordersMap = new Map<string, Order>();
    for (const sale of sales) {
      if (!ordersMap.has(sale.docCode)) {
        // Map Customer entity sang OrderCustomer type
        const customer = sale.customer;
        const orderCustomer = customer ? {
          code: customer.code,
          name: customer.name,
          brand: customer.brand || '',
          mobile: sale.mobile || customer.mobile, // Ưu tiên sale.mobile (partner_mobile)
          phone: customer.phone || sale.mobile || customer.mobile,
          sexual: customer.sexual,
          idnumber: customer.idnumber,
          enteredat: customer.enteredat ? customer.enteredat.toISOString() : undefined,
          crm_lead_source: customer.crm_lead_source,
          address: customer.address,
          province_name: customer.province_name,
          birthday: customer.birthday ? customer.birthday.toISOString().split('T')[0] : undefined,
          grade_name: customer.grade_name,
          branch_code: customer.branch_code,
        } : null;

        ordersMap.set(sale.docCode, {
          docCode: sale.docCode,
          docDate: sale.docDate.toISOString(),
          branchCode: sale.branchCode,
          docSourceType: sale.docSourceType || 'sale',
          customer: orderCustomer as any,
          totalRevenue: 0,
          totalQty: 0,
          totalItems: 0,
          isProcessed: sale.isProcessed,
          sales: [],
        });
      }
      const order = ordersMap.get(sale.docCode)!;
      order.sales = order.sales || [];
      order.sales.push(sale as any);
      order.totalRevenue += sale.revenue || 0;
      order.totalQty += sale.qty || 0;
      order.totalItems += 1;
    }

    // Lấy tất cả checkFaceIds để trả về (remove duplicates)
    const allCheckFaceIds = Array.from(checkFaceIdsByMobile.values()).flat().concat(
      Array.from(checkFaceIdsByPartnerCode.values()).flat()
    );
    const uniqueCheckFaceIds = Array.from(
      new Map(allCheckFaceIds.map(cf => [cf.id, cf])).values()
    );

    return {
      orders: Array.from(ordersMap.values()),
      checkFaceIds: uniqueCheckFaceIds,
    };
  }

  /**
   * Lấy tất cả orders với checkFaceID data đã join, có phân trang
   * @param options - Pagination options
   */
  async getAllGiaiTrinhFaceId(options: {
    page?: number;
    limit?: number;
    date?: string;
    dateFrom?: string;
    dateTo?: string;
    orderCode?: string;
    partnerCode?: string;
    faceStatus?: 'yes' | 'no';
    brandCode?: string;
  }): Promise<{
    items: Array<{
      partnerCode: string;
      partnerName: string;
      isCheckFaceId: boolean;
      orders: Order[];
    }>;
    pagination?: {
      page: number;
      limit: number;
      total: number;
      totalLines: number;
      totalPages: number;
      hasNext: boolean;
      hasPrev: boolean;
    };
  }> {
    try {
      const { page, limit, date, dateFrom, dateTo, orderCode, partnerCode, faceStatus, brandCode } = options;
      
      // Kiểm tra xem có phân trang không (cần cả page và limit, và phải là số hợp lệ)
      const hasPagination = page !== undefined && page !== null && limit !== undefined && limit !== null && !isNaN(page) && !isNaN(limit);
      const skip = hasPagination ? (page! - 1) * limit! : 0;

      // Ưu tiên lọc theo khoảng ngày nếu có dateFrom / dateTo, fallback về date (1 ngày)
      const hasDateRange = !!(dateFrom || dateTo);
      const dateParam = date;

      // 1) Base query cho sales theo ngày + filter orderCode / partnerCode / brandCode
      let baseSalesQuery = this.saleRepository
        .createQueryBuilder('sale')
        .leftJoinAndSelect('sale.customer', 'customer');

      if (hasDateRange) {
        if (dateFrom && dateTo) {
          // Nếu dateFrom và dateTo giống nhau → chỉ lấy đúng ngày đó
          if (dateFrom === dateTo) {
            baseSalesQuery = baseSalesQuery.andWhere('DATE(sale.docDate) = DATE(:dateFrom)', { dateFrom });
          } else {
          baseSalesQuery = baseSalesQuery.andWhere('sale.docDate BETWEEN :dateFrom AND :dateTo', {
            dateFrom,
            dateTo,
          });
          }
        } else if (dateFrom) {
          baseSalesQuery = baseSalesQuery.andWhere('sale.docDate >= :dateFrom', { dateFrom });
        } else if (dateTo) {
          baseSalesQuery = baseSalesQuery.andWhere('sale.docDate <= :dateTo', { dateTo });
        }
      } else if (dateParam) {
        // Dùng chuỗi date truyền từ FE trực tiếp cho so sánh DATE(column) = :date
        baseSalesQuery = baseSalesQuery.andWhere('DATE(sale.docDate) = :date', { date: dateParam });
      }
      if (orderCode) {
        baseSalesQuery = baseSalesQuery.andWhere('LOWER(sale.docCode) LIKE LOWER(:orderCode)', {
          orderCode: `%${orderCode}%`,
        });
      }
      if (partnerCode) {
        baseSalesQuery = baseSalesQuery.andWhere('LOWER(sale.partnerCode) LIKE LOWER(:partnerCode)', {
          partnerCode: `%${partnerCode}%`,
        });
      }
      if (brandCode) {
        baseSalesQuery = baseSalesQuery.andWhere('LOWER(sale.branchCode) = LOWER(:brandCode)', {
          brandCode: brandCode.trim(),
        });
      }

      // 2) Lấy tất cả sales với customer (áp dụng pagination nếu có)
      // Tăng limit để lấy nhiều sales hơn trước khi group thành orders và items
      // Ví dụ: limit=10 → lấy 100 sales để có nhiều orders hơn
      let queryToExecute = baseSalesQuery.clone();
      const salesLimit = hasPagination ? (limit! * 20) : undefined; // Lấy nhiều sales hơn để có nhiều orders
      const salesSkip = hasPagination ? skip : undefined;
      
      if (!hasDateRange && !dateParam && hasPagination) {
        queryToExecute = queryToExecute
          .orderBy('sale.docDate', 'DESC')
          .addOrderBy('sale.createdAt', 'DESC')
          .skip(salesSkip || 0)
          .take(salesLimit || limit!);
      } else if (hasPagination) {
        queryToExecute = queryToExecute
          .orderBy('sale.docDate', 'DESC')
          .addOrderBy('sale.createdAt', 'DESC')
          .skip(salesSkip || 0)
          .take(salesLimit || limit!);
      } else {
        queryToExecute = queryToExecute
          .orderBy('sale.docDate', 'DESC')
          .addOrderBy('sale.createdAt', 'DESC');
      }

      const allSalesWithCustomer = await queryToExecute.getMany();

      // Đếm total để pagination
      const totalCount = await baseSalesQuery.clone().getCount();
      const totalLines = totalCount;
      const totalPages = hasPagination ? Math.ceil(totalCount / limit!) : 1;

      // Lấy tất cả mobile numbers từ sales để query FaceID
      // Join bằng: sale.mobile (partner_mobile) = FaceID.mobile
      const allMobiles = new Set<string>();
      for (const sale of allSalesWithCustomer) {
        // Ưu tiên lấy từ sale.mobile (partner_mobile từ Zappy API)
        const mobile = sale.mobile ? String(sale.mobile).trim() : (sale.customer?.mobile ? String(sale.customer.mobile).trim() : null);
        if (mobile && mobile.length > 0) {
          allMobiles.add(mobile);
        }
      }

      const allMobileList = Array.from(allMobiles);

      this.logger.debug(
        `[getAllGiaiTrinhFaceId] Tìm thấy ${allSalesWithCustomer.length} sales, ${allMobileList.length} mobile numbers với filters: dateFrom=${dateFrom}, dateTo=${dateTo}, brandCode=${brandCode}, orderCode=${orderCode}, partnerCode=${partnerCode}`,
      );

      // 3) Lấy tất cả checkFaceIds cho các mobile này (theo ngày nếu có)
      // Join bằng: sale.mobile (partner_mobile) = FaceID.mobile
      const normalizedMobiles = Array.from(allMobiles).map((mobile) => String(mobile).trim()).filter(m => m.length > 0);

      let checkFaceIdQuery = this.checkFaceIdRepository
        .createQueryBuilder('checkFaceId')
        .orderBy('checkFaceId.date', 'DESC')
        .addOrderBy('checkFaceId.startTime', 'DESC');

      // Join chỉ bằng mobile
      if (normalizedMobiles.length > 0) {
        checkFaceIdQuery = checkFaceIdQuery.where('TRIM(checkFaceId.mobile) IN (:...mobiles)', { mobiles: normalizedMobiles });
      } else {
        // Không có mobile nào → không query FaceID
        checkFaceIdQuery = checkFaceIdQuery.where('1 = 0'); // Không trả về kết quả nào
      }

      if (hasDateRange) {
        if (dateFrom && dateTo) {
          // Nếu dateFrom và dateTo giống nhau → chỉ lấy đúng ngày đó
          if (dateFrom === dateTo) {
            checkFaceIdQuery = checkFaceIdQuery.andWhere('DATE(checkFaceId.date) = DATE(:dateFrom)', { dateFrom });
          } else {
          checkFaceIdQuery = checkFaceIdQuery.andWhere(
            'checkFaceId.date BETWEEN :dateFrom AND :dateTo',
            { dateFrom, dateTo },
          );
          }
        } else if (dateFrom) {
          checkFaceIdQuery = checkFaceIdQuery.andWhere('checkFaceId.date >= :dateFrom', { dateFrom });
        } else if (dateTo) {
          checkFaceIdQuery = checkFaceIdQuery.andWhere('checkFaceId.date <= :dateTo', { dateTo });
        }
      } else if (dateParam) {
        checkFaceIdQuery = checkFaceIdQuery.andWhere('DATE(checkFaceId.date) = :date', { date: dateParam });
      }

      const allFaceRows = await checkFaceIdQuery.getMany();
      
      this.logger.debug(
        `[getAllGiaiTrinhFaceId] Tìm thấy ${allFaceRows.length} FaceID records với ${normalizedMobiles.length} mobiles`,
      );
      
      // Map: normalized mobile -> CheckFaceId[]
      const checkFaceIdsByMobile = new Map<string, CheckFaceId[]>();

      for (const cf of allFaceRows) {
        // Map theo mobile nếu có
        if (cf.mobile) {
          const normalizedMobile = String(cf.mobile).trim();
          if (normalizedMobile && normalizedMobile.length > 0) {
            if (!checkFaceIdsByMobile.has(normalizedMobile)) {
              checkFaceIdsByMobile.set(normalizedMobile, []);
            }
            checkFaceIdsByMobile.get(normalizedMobile)!.push(cf);
          }
        }
      }
      
      this.logger.debug(
        `[getAllGiaiTrinhFaceId] Mapped ${checkFaceIdsByMobile.size} unique mobiles trong FaceID data`,
      );

      // 4) Map sales với FaceID để xác định isCheckFaceId cho từng sale
      // Map: sale.id -> isCheckFaceId
      const saleFaceIdMap = new Map<string, boolean>();
      let salesWithFaceIdCount = 0;
      for (const sale of allSalesWithCustomer) {
        const saleMobile = sale.mobile ? String(sale.mobile).trim() : (sale.customer?.mobile ? String(sale.customer.mobile).trim() : null);
        
        // Tìm FaceID chỉ theo mobile
        let checkFaceIds: CheckFaceId[] = [];
        if (saleMobile) {
          const normalizedMobile = String(saleMobile).trim();
          checkFaceIds = checkFaceIdsByMobile.get(normalizedMobile) || [];
        }
        const isCheckFaceId = checkFaceIds.length > 0;
        if (isCheckFaceId) {
          salesWithFaceIdCount++;
        }
        const saleId = (sale as any).id;
        if (saleId) {
          saleFaceIdMap.set(saleId, isCheckFaceId);
        }
      }
      
      this.logger.debug(
        `[getAllGiaiTrinhFaceId] Mapped ${salesWithFaceIdCount}/${allSalesWithCustomer.length} sales có FaceID, saleFaceIdMap size: ${saleFaceIdMap.size}`,
      );

      // 5) Group sales theo docCode để build Order[] (lấy sales làm gốc)
      const ordersMap = new Map<string, Order>();
      for (const sale of allSalesWithCustomer) {
        if (!ordersMap.has(sale.docCode)) {
            const customer = sale.customer;
          const orderCustomer = customer
            ? {
              code: customer.code,
              name: customer.name,
              brand: customer.brand || '',
              mobile: sale.mobile || customer.mobile, // Ưu tiên sale.mobile (partner_mobile)
              phone: customer.phone || sale.mobile || customer.mobile,
              sexual: customer.sexual,
              idnumber: customer.idnumber,
              enteredat: customer.enteredat ? customer.enteredat.toISOString() : undefined,
              crm_lead_source: customer.crm_lead_source,
              address: customer.address,
              province_name: customer.province_name,
              birthday: customer.birthday ? customer.birthday.toISOString().split('T')[0] : undefined,
              grade_name: customer.grade_name,
              branch_code: customer.branch_code,
              }
            : null;

          ordersMap.set(sale.docCode, {
              docCode: sale.docCode,
              docDate: sale.docDate.toISOString(),
              branchCode: sale.branchCode,
              docSourceType: sale.docSourceType || 'sale',
              customer: orderCustomer as any,
              totalRevenue: 0,
              totalQty: 0,
              totalItems: 0,
              isProcessed: sale.isProcessed,
              sales: [],
            });
          }

        const order = ordersMap.get(sale.docCode)!;
          order.sales = order.sales || [];

        const slimSale: SaleItem = {
          id: (sale as any).id,
          itemCode: sale.itemCode,
          itemName: sale.itemName,
          qty: sale.qty,
        };

        order.sales.push(slimSale);
        const revenue = typeof sale.revenue === 'string' ? parseFloat(sale.revenue) || 0 : (sale.revenue || 0);
        const qty = typeof sale.qty === 'string' ? parseFloat(sale.qty) || 0 : (sale.qty || 0);
        order.totalRevenue += revenue;
        order.totalQty += qty;
          order.totalItems += 1;
        }

      // 6) Build items: group orders theo partnerCode và xác định isCheckFaceId
      const items: Array<{
        partnerCode: string;
        partnerName: string;
        isCheckFaceId: boolean;
        orders: Order[];
        checkFaceIds: CheckFaceId[];
      }> = [];

      // Group orders theo partnerCode
      const ordersByPartnerCode = new Map<string, Order[]>();
      for (const order of Array.from(ordersMap.values())) {
        const partnerCode = (order.customer as any)?.code || '';
        if (!partnerCode) continue;
        
        if (!ordersByPartnerCode.has(partnerCode)) {
          ordersByPartnerCode.set(partnerCode, []);
        }
        ordersByPartnerCode.get(partnerCode)!.push(order);
      }

      for (const [partnerCode, orders] of ordersByPartnerCode.entries()) {
        if (orders.length === 0) continue;

        const firstOrder = orders[0];
        
        // Kiểm tra tất cả sales trong orders để xem có FaceID không
        // Lấy tất cả mobile từ tất cả sales trong orders
        const allMobilesInOrders = new Set<string>();
        let hasFaceIdInOrders = false;
        
        for (const order of orders) {
          // Lấy mobile từ order.customer.mobile (có thể là sale.mobile hoặc customer.mobile)
          const orderMobile = (order.customer as any)?.mobile;
          if (orderMobile) {
            const normalizedMobile = String(orderMobile).trim();
            if (normalizedMobile && normalizedMobile.length > 0) {
              allMobilesInOrders.add(normalizedMobile);
            }
          }
          
          // Kiểm tra từng sale trong order để xem có FaceID không (dùng saleFaceIdMap)
          for (const saleItem of (order.sales || [])) {
            const saleId = (saleItem as any).id;
            if (saleId && saleFaceIdMap.has(saleId) && saleFaceIdMap.get(saleId)) {
              hasFaceIdInOrders = true;
              break; // Đã tìm thấy ít nhất 1 sale có FaceID
            }
          }
          if (hasFaceIdInOrders) break; // Đã tìm thấy trong order này, không cần kiểm tra tiếp
        }
        
        // Tìm FaceID theo tất cả mobile từ orders
        let checkFaceIds: CheckFaceId[] = [];
        for (const mobile of allMobilesInOrders) {
          const normalizedMobile = String(mobile).trim();
          if (normalizedMobile && normalizedMobile.length > 0) {
            const faceIds = checkFaceIdsByMobile.get(normalizedMobile) || [];
            if (faceIds.length > 0) {
              checkFaceIds.push(...faceIds);
            }
          }
        }
        
        // isCheckFaceId = true nếu có ít nhất 1 sale trong orders có FaceID (theo saleFaceIdMap) HOẶC có FaceID khớp theo mobile
        const isCheckFaceId = hasFaceIdInOrders || checkFaceIds.length > 0;
        
        this.logger.debug(
          `[getAllGiaiTrinhFaceId] partnerCode=${partnerCode}, hasFaceIdInOrders=${hasFaceIdInOrders}, checkFaceIds.length=${checkFaceIds.length}, isCheckFaceId=${isCheckFaceId}, faceStatus=${faceStatus}`,
        );
        
        // Áp dụng filter faceStatus
        if (faceStatus === 'yes' && !isCheckFaceId) {
          continue;
        }
        if (faceStatus === 'no' && isCheckFaceId) {
          continue;
        }

        const partnerName =
          checkFaceIds[0]?.name ||
          (firstOrder.customer as any)?.name ||
          partnerCode;

        items.push({
          partnerCode,
          partnerName,
          isCheckFaceId,
          orders,
          checkFaceIds: checkFaceIds || [],
        });
      }

      // Tính total dựa trên số lượng unique partnerCode (sau khi filter faceStatus)
      const totalItems = items.length;
      
      // Paginate items sau khi group
      let paginatedItems = items;
      if (hasPagination) {
        const itemsSkip = (page! - 1) * limit!;
        paginatedItems = items.slice(itemsSkip, itemsSkip + limit!);
      }

      if (hasPagination) {
      return {
        items: paginatedItems,
        pagination: {
            page: page!,
            limit: limit!,
          total: totalItems,
            totalLines,
          totalPages: Math.ceil(totalItems / limit!),
            hasNext: page! < Math.ceil(totalItems / limit!),
            hasPrev: page! > 1,
        },
        };
      }
      
      return {
        items: paginatedItems,
      };
    } catch (error: any) {
      this.logger.error(`[SalesService] getAllGiaiTrinhFaceId error: ${error?.message || error}`);
      throw new InternalServerErrorException('getAllGiaiTrinhFaceId error');
    }
  }

  /**
   * Giải trình FaceID cho một đơn hàng
   * @param explainDto - DTO chứa mã đơn, ngày giải trình và thông tin giải trình
   */
  async explainFaceId(explainDto: ExplainFaceIdDto): Promise<{
    success: boolean;
    message: string;
    updatedCount: number;
  }> {
    try {
      const { docCode, explanationDate, explanationMessage } = explainDto;

      // Tìm đơn hàng theo docCode
      const sale = await this.saleRepository.findOne({
        where: { docCode },
        relations: ['customer'],
      });

      if (!sale || !sale.customer?.mobile) {
        throw new NotFoundException(`Không tìm thấy đơn hàng với mã: ${docCode} hoặc đơn hàng không có số điện thoại`);
      }

      // Parse ngày giải trình
      const explanationDateObj = new Date(explanationDate);
      if (isNaN(explanationDateObj.getTime())) {
        throw new BadRequestException('Ngày giải trình không hợp lệ (format: YYYY-MM-DD)');
      }

      // Tìm các CheckFaceId records theo mobile và ngày đơn hàng
      const mobile = String(sale.customer.mobile).trim();
      const checkFaceIds = await this.checkFaceIdRepository
        .createQueryBuilder('checkFaceId')
        .where('TRIM(checkFaceId.mobile) = :mobile', { mobile })
        .andWhere('DATE(checkFaceId.date) = DATE(:orderDate)', { orderDate: sale.docDate })
        .getMany();

      // Update tất cả các CheckFaceId records
      let updatedCount = 0;
      for (const checkFaceId of checkFaceIds) {
        checkFaceId.isExplained = true;
        checkFaceId.explanationMessage = explanationMessage;
        checkFaceId.explanationDate = explanationDateObj;
        await this.checkFaceIdRepository.save(checkFaceId);
        updatedCount++;
      }

      return {
        success: true,
        message: explanationMessage,
        updatedCount,
      };
    } catch (error: any) {
      this.logger.error(`[SalesService] explainFaceId error: ${error?.message || error}`);
      throw new InternalServerErrorException(`Lỗi khi giải trình FaceID: ${error?.message || error}`);
    }
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
      this.logger.debug(`[exportOrders] Starting export with filters: ${JSON.stringify(options)}`);

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

      this.logger.debug(`[exportOrders] Fetched ${allSales.length} sales records for export`);
      
      // Debug: Đếm số đơn lỗi
      const errorCount = allSales.filter(s => s.statusAsys === false).length;
      this.logger.debug(`[exportOrders] Found ${errorCount} error orders (statusAsys = false) out of ${allSales.length} total`);

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
      
      if (validItemCodes.length > 0) {
        const productPromises = validItemCodes.map(async (itemCode) => {
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
              { headers: { accept: 'application/json' } },
            );
            const loyaltyProduct = response?.data?.data?.item || response?.data;
            return { itemCode, loyaltyProduct };
          } catch (error) {
            this.logger.warn(`Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
            return { itemCode, loyaltyProduct: null };
          }
        });
        const productResults = await Promise.all(productPromises);
        productResults.forEach(({ itemCode, loyaltyProduct }) => {
          if (loyaltyProduct) {
            loyaltyProductMap.set(itemCode, loyaltyProduct);
          }
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
        const product = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
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
        
        // Tính maKho
        const maBp = department?.ma_bp || sale.branchCode || null;
        const maKho = this.calculateMaKho(sale.ordertype || '', maBp) || sale.maKho || sale.branchCode || '';
        
        // Tính maLo (đơn giản hóa - cần logic đầy đủ từ frontend)
        let maLo = sale.maLo || '';
        const serial = sale.serial || '';
        if (!maLo && serial) {
          const underscoreIndex = serial.indexOf('_');
          if (underscoreIndex > 0 && underscoreIndex < serial.length - 1) {
            maLo = serial.substring(underscoreIndex + 1);
          } else if (product?.trackBatch === true) {
            if (normalizedBrand === 'f3') {
              maLo = serial;
            } else {
              const productType = product?.productType || product?.producttype || '';
              const productTypeUpper = productType.toUpperCase().trim();
              if (productTypeUpper === 'TPCN') {
                maLo = serial.length >= 8 ? serial.slice(-8) : serial;
              } else {
                maLo = serial.length >= 4 ? serial.slice(-4) : serial;
              }
            }
          }
        }
        
        // Tính soSerial
        let soSerial = '';
        if (product?.trackSerial === true && product?.trackBatch !== true) {
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
            const tangSpCode = this.convertPromCodeToTangSp(sale.promCode, docDate);
            maCtkmTangHang = tangSpCode || this.getPromotionDisplayCode(sale.promCode) || sale.promCode || '';
          } else {
            maCtkmTangHang = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || '';
          }
        }
        
        // Tính muaHangGiamGia
        let muaHangGiamGia = '';
        if (!isTangHang && sale.promCode) {
          muaHangGiamGia = this.getPromotionDisplayCode(sale.promCode) || sale.promCode || '';
        }
        
        return {
          '* Mã khách': sale.partnerCode || sale.customer?.code || '',
          '* Ngày': sale.docDate ? new Date(sale.docDate).toLocaleDateString('vi-VN') : '',
          '* Số hóa đơn': sale.docCode || '',
          '* Ký hiệu': department?.branchcode || sale.branchCode || '',
          'Diễn giải': sale.docCode || '',
          '* Mã hàng': product?.maVatTu || sale.itemCode || '',
          'Đvt': product?.dvt || sale.dvt || '',
          'Loại': sale.loai || (sale.cat1 ? `${sale.cat1}${sale.cat2 ? ` / ${sale.cat2}` : ''}${sale.cat3 ? ` / ${sale.cat3}` : ''}` : '') || '',
          'Khuyến mãi': promCode,
          '* Mã kho': maKho,
          '* Mã lô': maLo,
          'Số lượng': qty,
          'Giá bán': giaBan,
          'Tiền hàng': tienHang,
          'Tỷ giá': sale.tyGia || 1,
          '* Mã thuế': sale.maThue ,
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
          this.logger.debug(`[exportOrders] Row ${row} (saleIndex ${saleIndex}) is ERROR ROW: docCode=${sale.docCode}, statusAsys=${statusAsysValue}, type=${typeof statusAsysValue}, isErrorRow=${isErrorRow}`);
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
              this.logger.debug(`[exportOrders] ERROR ROW STYLE APPLIED to cell ${cellAddress}, fill=${JSON.stringify(cell.s.fill)}`);
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
      this.logger.debug(`[exportOrders] Creating Excel buffer...`);
      const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
      this.logger.debug(`[exportOrders] Excel buffer created, size: ${buffer.length} bytes`);

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
}
