import * as SalesUtils from './sales.utils';
import * as VoucherUtils from './voucher.utils';

/**
 * Interface cho kết quả xử lý hạch toán tài khoản
 */
export interface AccountingAccounts {
  tkChietKhau: string | null;
  tkChiPhi: string | null;
  maPhi: string | null;
}

/**
 * Interface cho các flag phân loại đơn hàng
 */
export interface OrderTypes {
  isDoiDiem: boolean;
  isDoiVo: boolean;
  isDauTu: boolean;
  isSinhNhat: boolean;
  isThuong: boolean;
  isDoiDv: boolean;
  isTachThe: boolean;
  isDichVu: boolean;
  isBanTaiKhoan: boolean;
  isSanTmdt: boolean;
  isTtPhiKeep: boolean;
}

/**
 * Centralized business logic for Invoices to ensure consistency
 * between FastAPI payloads and Frontend display.
 */
export class InvoiceLogicUtils {
  /**
   * Xác định các loại đơn hàng đặc biệt
   */
  static getOrderTypes(ordertypeName: string = ''): OrderTypes {
    const normalized = (ordertypeName || '').trim();

    const isDoiDiem =
      normalized.includes('03. Đổi điểm') || normalized.includes('03.Đổi điểm');
    const isDoiVo =
      normalized.toLowerCase().includes('đổi vỏ') ||
      normalized.toLowerCase().includes('doi vo');
    const isDauTu =
      normalized.includes('06. Đầu tư') ||
      normalized.includes('06.Đầu tư') ||
      normalized.toLowerCase().includes('đầu tư') ||
      normalized.toLowerCase().includes('dau tu');
    const isSinhNhat =
      normalized.includes('05. Tặng sinh nhật') ||
      normalized.includes('05.Tặng sinh nhật') ||
      normalized.toLowerCase().includes('tặng sinh nhật') ||
      normalized.toLowerCase().includes('tang sinh nhat');

    const isTtPhiKeep = normalized.toLowerCase().includes('tt phí keep');

    const isThuong =
      normalized.startsWith('01.') ||
      normalized.startsWith('01 ') ||
      normalized.toLowerCase() === 'thường' ||
      normalized.toLowerCase() === 'thuong' ||
      isTtPhiKeep;
    const isDoiDv =
      normalized.includes('04. Đổi DV') || normalized.includes('04.Đổi DV');
    const isTachThe =
      normalized.includes('08. Tách thẻ') || normalized.includes('08.Tách thẻ');
    const isBanTaiKhoan =
      normalized.includes('07. Bán tài khoản') ||
      normalized.includes('07.Bán tài khoản');
    const isSanTmdt =
      normalized.includes('9. Sàn TMDT') || normalized.includes('9.Sàn TMDT');
    const isDichVu =
      normalized.includes('02. Làm dịch vụ') ||
      isDoiDv ||
      isTachThe ||
      normalized.includes('Đổi thẻ KEEP->Thẻ DV');

    return {
      isDoiDiem,
      isDoiVo,
      isDauTu,
      isSinhNhat,
      isThuong,
      isDoiDv,
      isTachThe,
      isDichVu,
      isBanTaiKhoan,
      isSanTmdt,
      isTtPhiKeep,
    };
  }

  /**
   * Helper detects Wholesale order from Sale
   */
  static isWholesale(sale: any): boolean {
    const typeSale = (sale?.type_sale || '').toUpperCase().trim();
    return typeSale === 'WHOLESALE' || typeSale === 'WS';
  }

  /**
   * Tính toán các tài khoản hạch toán (Source of Truth)
   */
  static async resolveAccountingAccounts(params: {
    sale: any;
    loyaltyProduct: any;
    orderTypes: OrderTypes;
    isTangHang: boolean;
    hasMaCtkm: boolean;
    hasMaCtkmTangHang: boolean;
    loyaltyService?: any; // Allow injecting service
  }): Promise<AccountingAccounts> {
    const {
      sale,
      loyaltyProduct,
      orderTypes,
      isTangHang,
      hasMaCtkm,
      hasMaCtkmTangHang,
      loyaltyService,
    } = params;
    const { isDoiVo, isDoiDiem, isDauTu, isSinhNhat, isThuong } = orderTypes;

    let tkChietKhau: string | null = null;
    let tkChiPhi: string | null = null;
    let maPhi: string | null = null;

    const productType = sale.productType || null;
    const productTypeWholesale = loyaltyProduct?.productType || null;

    const productTypeUpper = productType
      ? String(productType).toUpperCase().trim()
      : null;
    const productTypeWholesaleUpper = productTypeWholesale
      ? String(productTypeWholesale).toUpperCase().trim()
      : null;
    const isKmVip = Number(
      sale.grade_discamt || sale.chietKhauMuaHangCkVip || 0,
    );
    const hasVoucher = Number(
      sale.paid_by_voucher_ecode_ecoin_bp || sale.thanhToanVoucher || 0,
    );
    const hasChietKhauMuaHangGiamGia =
      Number(sale.other_discamt || sale.chietKhauMuaHangGiamGia || 0) > 0;
    const isTangSP =
      loyaltyProduct?.productType === 'GIFT' ||
      loyaltyProduct?.producttype === 'GIFT';

    // Check wholesale alias defined in system
    const typeSale = (sale.type_sale || '').toUpperCase().trim();
    const isWholesale = typeSale === 'WHOLESALE' || typeSale === 'WS';
    const isEcode = loyaltyProduct?.materialType === '94';

    if (isWholesale) {
      // === LOGIC BÁN BUÔN (WHOLESALE) ===
      const wholesaleAccounts = await InvoiceLogicUtils.getWholesaleAccounts(
        isWholesale,
        productTypeWholesaleUpper,
        isEcode,
        loyaltyService,
      );

      if (wholesaleAccounts) {
        tkChietKhau = wholesaleAccounts.tkChietKhau || null;
        maPhi = wholesaleAccounts.maPhi || null;
      } else {
        // Fallback if not matched
        tkChietKhau = sale.tkChietKhau || null;
        tkChiPhi = sale.tkChiPhi || null;
        maPhi = sale.maPhi || null;
      }
    } else {
      // === LOGIC BÁN LẺ (RETAIL / NORMAL) ===

      if (isDoiVo || isDoiDiem || isDauTu) {
        tkChiPhi = '64191';
        maPhi = '161010';
      } else if (isSinhNhat) {
        tkChiPhi = '64192';
        maPhi = '162010';
      } else if (hasMaCtkmTangHang && isTangHang) {
        tkChiPhi = '64191';
        maPhi = '161010';
        tkChietKhau = sale.tkChietKhau || null;
      } else if (isKmVip > 0 && productTypeUpper === 'I') {
        tkChietKhau = '521113';
      } else if (isKmVip > 0 && productTypeUpper === 'S') {
        tkChietKhau = '521132';
      } else if (hasVoucher > 0 && isTangSP) {
        tkChietKhau = '5211631';
      } else if (hasVoucher > 0 && productTypeUpper === 'I') {
        tkChietKhau = '5211611';
      } else if (hasVoucher > 0 && productTypeUpper === 'S') {
        tkChietKhau = '5211621';
      } else if (hasChietKhauMuaHangGiamGia && productTypeUpper === 'S') {
        tkChietKhau = '521131';
      } else if (hasChietKhauMuaHangGiamGia && productTypeUpper === 'I') {
        tkChietKhau = '521111';
      } else if (hasMaCtkm && !(hasMaCtkmTangHang && isTangHang)) {
        tkChietKhau = productTypeUpper === 'S' ? '521131' : '521111';
      } else {
        // Default Retail
        tkChietKhau = sale.tkChietKhau || null;
        tkChiPhi = sale.tkChiPhi || null;
        maPhi = sale.maPhi || null;
      }
    }

    return { tkChietKhau, tkChiPhi, maPhi };
  }

  /**
   * Helper xác định tài khoản cho đơn bán buôn (Wholesale)
   */
  static async getWholesaleAccounts(
    isWholesale: boolean,
    productType: string | null,
    isEcode: boolean,
    loyaltyService?: any,
  ): Promise<Partial<AccountingAccounts> | null> {
    if (!isWholesale) return null;

    const category = InvoiceLogicUtils.getWholesaleCategory(
      String(productType || ''),
    );

    let promotionCode = '';

    if (isEcode) {
      // Ecode Logic
      if (category === 'MP') promotionCode = 'CKCSBH.E.MP';
      else if (category === 'TPCN') promotionCode = 'CKCSBH.E.TPCN';
      else if (category === 'CCDC') promotionCode = 'CKCSBH.E.CCDC';
    } else {
      // Non-Ecode Logic
      if (category === 'MP') promotionCode = 'CKCSBH.MP';
      else if (category === 'TPCN') promotionCode = 'CKCSBH.TPCN';
      else if (category === 'CCDC') promotionCode = 'CKCSBH.CCDC';
    }

    if (promotionCode && loyaltyService) {
      try {
        const config = await loyaltyService.fetchPromotionConfig(promotionCode);
        if (config) {
          return {
            tkChietKhau: config.tk_ck || null,
            maPhi: config.ma_phi || null,
            tkChiPhi: config.tk_cpkm || null,
          };
        }
      } catch (error) {
        console.warn(
          `[InvoiceLogicUtils] Failed to fetch config for ${promotionCode}`,
          error,
        );
      }
    }

    return null;
  }

  /**
   * Tính toán mã CTKM và mã quà tặng
   */
  static resolvePromotionCodes(params: {
    sale: any;
    orderTypes: OrderTypes;
    isTangHang: boolean;
    maDvcs: string;
    productTypeUpper: string | null;
    promCode: string | null;
    maHangGiamGia: any;
    isSanTmdtOverride?: boolean;
    isEmployee?: boolean; // [NEW]
    loyaltyProduct?: any; // [NEW]
  }) {
    const {
      sale,
      orderTypes,
      isTangHang,
      maDvcs,
      productTypeUpper,
      promCode,
      maHangGiamGia,
      isEmployee,
      loyaltyProduct,
    } = params;
    const { isDoiDiem, isDauTu, isThuong, isBanTaiKhoan, isSanTmdt } =
      orderTypes;

    // [NEW] Allow override for platform orders (detected by OrderFee)
    const effectiveIsSanTmdt =
      params.isSanTmdtOverride === true ? true : isSanTmdt;

    // Safety: Re-derive productTypeUpper if missing (Robust Check V9)
    let effectiveProductType = productTypeUpper;
    // Xác định productType an toàn (ưu tiên loyaltyProduct)
    const productType = sale.productType || '';
    if (!effectiveProductType) {
      effectiveProductType = String(productType).toUpperCase().trim();
    }

    const isWholesale =
      sale.type_sale === 'WHOLESALE' || sale.type_sale === 'WS';
    let maCk01;
    if (isWholesale && sale.disc_reasons && sale.disc_ctkm > 0) {
      maCk01 = `${sale.disc_reasons}.${maHangGiamGia}` || '';
    } else {
      // [FIX] Don't rely on display fields that might not exist yet.
      // Calculate from promCode later if not wholesale.
      maCk01 = '';
    }

    let maCtkmTangHang = sale.maCtkmTangHang || '';

    // FIX V8.1: Handle PRMN vs Raw RMN consistent logic
    // 1. Transform PRMN -> RMN (and flag it to skip suffix)
    let code = (promCode || '').trim();
    let isPRMNDerived = false;

    if (code.toUpperCase().startsWith('PRMN')) {
      code = code.replace(/^PRMN/i, 'RMN');
      isPRMNDerived = true;
    }

    if (isDoiDiem) {
      const mapMaDvcsToKmDiem: Record<string, string> = {
        TTM: 'TTM.KMDIEM',
        AMA: 'TTM.KMDIEM',
        TSG: 'TTM.KMDIEM',
        FBV: 'FBV.KMDIEM',
        BTH: 'BTH.KMDIEM',
        CDV: 'CDV.KMDIEM',
        LHV: 'LHV.KMDIEM',
      };
      maCtkmTangHang = mapMaDvcsToKmDiem[maDvcs] || '';
      maCk01 = ''; // Đổi điểm không có ck01
    } else if (isTangHang) {
      if (isDauTu) {
        maCtkmTangHang = 'TT DAU TU';
      } else if (isThuong || isBanTaiKhoan || isSanTmdt) {
        // 2. Gift Case: Assign + Cut Code + Strip Suffixes (V10)
        // User requirements: "Cut" like standard code (RMN.xxx-yyy -> RMN.xxx)
        // BUT: Do NOT add .I / .S / .V suffix.
        const cutCode = SalesUtils.getPromotionDisplayCode(code) || code;
        maCtkmTangHang = cutCode.replace(/\.(I|S|V)$/, '');
      }
    }

    // [NEW] Logic "Xuất hàng KM cho đại lý" (Wholesale Promotion)
    // If revenue/linetotal is 0:
    // - Group 16INAN -> CTKM.INAN
    // - Other Groups -> GT.TANGHANG
    const isXuatHangKmDaiLy =
      sale.ordertypeName === 'Xuất hàng KM cho đại lý' ||
      sale.ordertype === 'Xuất hàng KM cho đại lý';

    if (isXuatHangKmDaiLy) {
      const revenue = Number(sale.revenue || 0);
      if (Math.abs(revenue) < 0.01) {
        // [FIX] Use sale.product.productType to get granular code (e.g. 16INAN)

        const granularProductType = loyaltyProduct?.productType || '';
        if (granularProductType.includes('16INAN')) {
          maCtkmTangHang = 'CTKM.INAN';
        } else {
          maCtkmTangHang = 'GT.TANGHANG';
        }
      }
    }

    // [MOVED] Logic Discount for Retail Purchase (other_discamt > 0) AND Employee Order
    // Lifted out of restriction block to support "All order types"
    const otherDiscamt = Number(
      sale.other_discamt || sale.chietKhauMuaHangGiamGia || 0,
    );
    if (
      isEmployee && // [CHECK] Only for Employee
      otherDiscamt > 0 &&
      !effectiveIsSanTmdt &&
      !maCk01
    ) {
      if (['TTM', 'TSG', 'THP'].includes(maDvcs)) {
        // Format: Fixed as per request
        maCk01 = '2505MN.CK521';
      } else if (maDvcs === 'FBV') {
        if (effectiveProductType === 'I') maCk01 = 'SPQTNV';
        else if (effectiveProductType === 'S') maCk01 = 'DVQTNV';
      } else if (maDvcs === 'LHV') {
        if (effectiveProductType === 'S') maCk01 = 'R504DICHVU';
        else if (effectiveProductType === 'I') maCk01 = 'R504SANPHAM';
      }
    }

    if (!isDoiDiem && !isTangHang && !isWholesale) {
      // [NEW] Platform Order (Đơn sàn) Logic
      if (effectiveIsSanTmdt) {
        const brand = (sale.brand || '').trim().toLowerCase();
        if (brand === 'menard') {
          maCk01 = 'TTM.R601ECOM';
        } else if (brand === 'yaman') {
          maCk01 = 'BTH.R601ECOM';
        }
      }

      // 3. Standard Case: Assign + Add Suffix if missing
      if (!maCk01) {
        maCk01 = SalesUtils.getPromotionDisplayCode(code) || code || '';
      }

      // FIX V8.1: Add suffix if NOT PRMN-derived.
      // E.g: Input 'RMN...' -> isPRMNDerived=false -> Adds Suffix.
      //      Input 'PRMN...' -> isPRMNDerived=true -> Skips Suffix.
      // [NEW] Skip suffix for Platform Orders (Sàn TMĐT) AND Retail Discount Codes (which are hardcoded)
      // Also skip if it is one of our special Employee Discount Codes
      const isRetailDiscountCode =
        isEmployee && // [CHECK] Only for Employee
        otherDiscamt > 0 &&
        (['SPQTNV', 'DVQTNV', 'R504DICHVU', 'R504SANPHAM'].includes(maCk01) ||
          maCk01.endsWith('.CK521'));

      if (
        maCk01 &&
        effectiveProductType &&
        !effectiveIsSanTmdt &&
        !isRetailDiscountCode // Skip suffix for new custom codes
      ) {
        let suffix = '';
        if (effectiveProductType === 'I') suffix = '.I';
        else if (effectiveProductType === 'S') suffix = '.S';
        else if (effectiveProductType === 'V') suffix = '.V';

        if (suffix && !maCk01.endsWith(suffix)) {
          maCk01 += suffix;
        }
      }
    }

    return { maCk01, maCtkmTangHang };
  }

  /**
   * Tính toán mã voucher ck05 cho Ecommerce và các brand
   */
  static resolveVoucherCode(params: {
    sale: any;
    customer: any;
    brand: string;
    maDvcs?: string;
    isEmployee?: boolean; // [NEW] added for employee check
  }): string | null {
    const { sale, customer, brand, maDvcs, isEmployee } = params;
    const listEcomName = ['shopee', 'lazada', 'tiktok'];
    const brandLower = brand?.toLowerCase();

    // [NEW] Logic Voucher Payment Code based on Branch (maDvcs) AND Employee Order
    if (isEmployee && maDvcs && ['TTM', 'TSG', 'THP'].includes(maDvcs)) {
      return '2505MN.CK511';
    }

    if (
      customer &&
      customer.ecomName &&
      listEcomName.includes(customer.ecomName.toLowerCase())
    ) {
      const brandCk05Map: Record<string, string> = {
        menard: 'TTM.R601ECOM',
        yaman: 'BTH.R601ECOM',
        cdv: 'CDV.R601ECOM',
      };
      return brandCk05Map[brandLower] || 'VC CTKM SÀN';
    }

    return VoucherUtils.calculateMaCk05FromSale(sale);
  }

  /**
   * Tính toán đơn giá và tiền hàng (Source of Truth)
   */
  static calculatePrices(params: {
    sale: any;
    orderTypes: OrderTypes;
    allocationRatio: number;
    qtyFromStock?: number;
  }) {
    const { sale, orderTypes, allocationRatio, qtyFromStock } = params;
    const { isDoiDiem, isThuong: isNormalOrder } = orderTypes;

    const linetotal = Number(sale.linetotal || 0);
    const revenue = Number(sale.revenue || 0);
    const saleTienHang = Number(sale.tienHang || 0);
    const saleQty = Number(sale.qty || 0);

    let tienHang = saleTienHang || linetotal || revenue || 0;
    let giaBan = Number(sale.giaBan || 0);

    // FIX V7: Sync Pricing Logic (Source of Truth)
    // Re-calculate giaBan for non-normal orders using Gross Amount if available
    if (!isDoiDiem && !isNormalOrder) {
      const tongChietKhau =
        Number(sale.other_discamt || sale.chietKhauMuaHangGiamGia || 0) +
        Number(sale.chietKhauCkTheoChinhSach || 0) +
        Number(sale.chietKhauMuaHangCkVip || sale.grade_discamt || 0) +
        Number(
          sale.paid_by_voucher_ecode_ecoin_bp ||
          sale.chietKhauThanhToanVoucher ||
          0,
        );

      let calcTienHangGoc = Number(
        sale.mn_linetotal || sale.linetotal || sale.tienHang || 0,
      );
      if (calcTienHangGoc === 0) {
        calcTienHangGoc = tienHang + tongChietKhau;
      }

      if (giaBan === 0 && saleQty > 0 && calcTienHangGoc > 0) {
        giaBan = calcTienHangGoc / saleQty;
      }
    }

    if (isDoiDiem) {
      giaBan = 0;
      tienHang = 0;
    } else if (giaBan === 0 && tienHang > 0 && saleQty !== 0) {
      giaBan = tienHang / Math.abs(saleQty);
    }

    let tienHangGoc = isDoiDiem ? 0 : tienHang;
    if (!isDoiDiem && isNormalOrder && allocationRatio !== 1) {
      // Nếu có qtyFromStock (đã lấy từ ST), dùng nó. Nếu không, tính dựa trên ratio
      const qty =
        qtyFromStock ??
        (saleQty !== 0 ? Math.abs(saleQty * allocationRatio) : 0);
      if (qty > 0 && giaBan > 0) {
        tienHangGoc = qty * giaBan;
      } else {
        tienHangGoc = tienHang * allocationRatio;
      }
    }

    return { giaBan, tienHang, tienHangGoc };
  }

  /**
   * Xác định mã lô hoặc số serial (Source of Truth)
   */
  static resolveBatchSerial(params: {
    batchSerialFromST: string | null;
    trackBatch: boolean;
    trackSerial: boolean;
  }) {
    const { batchSerialFromST, trackBatch, trackSerial } = params;
    const serialValue = batchSerialFromST || '';

    // Re-use logic from SalesUtils.shouldUseBatch for consistency
    // [FIX] Prioritize trackBatch if both are true (matches shouldUseBatch comments)
    const isActuallyBatch = trackBatch === true;

    return {
      maLo: isActuallyBatch ? serialValue : null,
      soSerial: isActuallyBatch ? null : serialValue,
    };
  }

  /**
   * Xác định loại giao dịch (loai_gd) (Source of Truth)
   */
  static resolveLoaiGd(params: {
    sale: any;
    orderTypes: OrderTypes;
    loyaltyProduct: any;
  }): string {
    const { sale, orderTypes, loyaltyProduct } = params;
    const {
      isDoiDv,
      isTachThe,
      isDichVu,
      isBanTaiKhoan,
      isSanTmdt,
      isThuong,
      isDauTu,
      isDoiDiem,
      isDoiVo,
      isSinhNhat,
    } = orderTypes;
    const qty = Number(sale.qty || 0);

    if (isDoiDv || isTachThe) {
      return qty < 0 ? '11' : '12';
    }

    // Check wholesale from sale.type_sale OR orderTypes
    const isWholesale = sale.type_sale === 'WHOLESALE';

    if (isWholesale) {
      if (loyaltyProduct?.materialType === '94') {
        return '04';
      }
    }

    if (isThuong) {
      if (sale.productType === 'I') {
        return '01';
      } else if (sale.productType === 'S' && sale.qty > 0) {
        if (
          loyaltyProduct?.materialCode === 'J00001' ||
          loyaltyProduct?.materialCode === 'J00003'
        ) {
          return '01';
        }
        return '02';
      } else if (sale.productType === 'V') {
        const specialVCodes = [
          'I00398',
          'I00399',
          'I00400',
          'I00401',
          'I00402',
        ];
        if (specialVCodes.includes(loyaltyProduct?.materialCode)) {
          return '01';
        }
        return '03';
      }
    }

    if (isDichVu) {
      if (sale.productType === 'S' && sale.qty > 0) {
        return '01';
      } else if (sale.productType === 'S' && Number(sale.giaBan) === 0) {
        return '06';
      }
    }

    return '01';
  }

  /**
   * Xác định mã kho (Source of Truth)
   */
  static resolveMaKho(params: {
    maKhoFromST: string | null;
    maKhoFromSale: string | null;
    maBp: string;
    orderTypes: OrderTypes;
  }): string {
    const { maKhoFromST, maKhoFromSale, maBp, orderTypes } = params;
    const { isTachThe } = orderTypes;
    // Với đơn Tách thẻ: luôn là B + mã bộ phận
    if (isTachThe) return 'B' + maBp;
    // Ưu tiên kho từ Stock Transfer, sau đó đến kho từ Sale
    return maKhoFromST || maKhoFromSale || '';
  }

  /**
   * Helper xác định Category cho Wholesale (MP, TPCN, CCDC)
   */
  static getWholesaleCategory(productType: string): string {
    if (!productType) return 'MP';

    const groupProductType = productType.toUpperCase();
    const tpcnCategories = ['03TPCN'];
    const ccdcCategories = ['11MMOC'];

    if (tpcnCategories.some((cat) => groupProductType.includes(cat))) {
      return 'TPCN';
    } else if (ccdcCategories.some((cat) => groupProductType.includes(cat))) {
      return 'CCDC';
    } else {
      return 'MP';
    }
  }

  /**
   * Xác định mã CTKM cho đơn hàng bán buôn (WHOLESALE)
   * Áp dụng khi: type_sale = WHOLESALE, ordertypeName = "Bán buôn kênh Đại lý", dist_tm > 0
   */
  static resolveWholesalePromotionCode(params: {
    product: any;
    distTm: number;
  }): string {
    const { product, distTm } = params;

    // Chỉ áp dụng khi dist_tm > 0
    if (!distTm || distTm <= 0) {
      return '';
    }

    // Determine groupProductType from product object
    const groupProductType = product?.productType || '';

    // Xác định loại hàng (0 hoặc 1)
    // Loại hàng = 1 (Ecode) nếu materialType === '94'
    // Loại hàng = 0 cho các trường hợp còn lại
    const isEcode = product?.materialType === '94';

    // Xác định nhóm sản phẩm dựa trên productType
    // Mỹ phẩm: 01SKIN, 02MAKE, 04BODY, 05HAIR, 06FRAG, 07PROF, 10GIFT
    // TPCN: 03TPCN
    // CCDC: 11MMOC
    // Determine category using helper
    const category = InvoiceLogicUtils.getWholesaleCategory(groupProductType);

    // Map mã CTKM theo quy tắc
    let mappedCode = '';

    if (isEcode) {
      // Loại hàng = 1 (Ecode)
      if (category === 'MP') {
        mappedCode = 'CKCSBH.E.MP';
      } else if (category === 'TPCN') {
        mappedCode = 'CKCSBH.E.TPCN';
      } else if (category === 'CCDC') {
        mappedCode = 'CKCSBH.E.CCDC';
      }
    } else {
      // Loại hàng = 0 (Thường)
      if (category === 'MP') {
        mappedCode = 'CKCSBH.MP';
      } else if (category === 'TPCN') {
        mappedCode = 'CKCSBH.TPCN';
      } else if (category === 'CCDC') {
        mappedCode = 'CKCSBH.CCDC';
      }
    }

    return mappedCode;
  }
  /**
   * Resolve "Chiết khấu mua hàng giảm giá" (disc_ctkm)
   * - Nếu là đơn bán buôn (WHOLESALE): trả về "" (rỗng)
   * - Nếu là đơn đổi điểm: trả về 0
   * - Còn lại: trả về disc_ctkm hoặc chietKhauMuaHangGiamGia
   */
  static resolveChietKhauMuaHangGiamGia(
    sale: any,
    isDoiDiem: boolean,
  ): number | string {
    const typeSale = (sale.type_sale || '').toUpperCase().trim();
    // Check wholesale alias defined in system
    const isWholesale = typeSale === 'WHOLESALE' || typeSale === 'WS';

    if (isWholesale && sale.disc_ctkm > 0) {
      return sale.disc_ctkm;
    }

    if (isDoiDiem) {
      return '-';
    }

    let val = sale.disc_ctkm;
    // [FIX] Fallback if disc_ctkm is null, undefined OR 0 (since SalesQueryService might default it to 0)
    if (!val || Number(val) === 0) {
      val = sale.chietKhauMuaHangGiamGia;
    }

    // Explicit 0 check (number or string)
    if (
      val === null ||
      val === undefined ||
      val === 0 ||
      val === '0' ||
      Number(val) === 0
    ) {
      return '-';
    }

    return val;
  }

  /**
   * Resolve "Mua hàng giảm giá" code for Employee orders
   * Returns specific promotion code based on company (maDvcs) and product type
   * ONLY when other_discamt > 0 AND isEmployee
   *
   * Logic:
   * - TTM, TSG, THP: 2505MN.CK521
   * - FBV + type I: SPQTNV, FBV + type S: DVQTNV
   * - LHV + type I: R504SANPHAM, LHV + type S: R504DICHVU
   *
   * @returns code string if applicable, null otherwise
   */
  static resolveMuaHangGiamGiaCode(params: {
    sale: any;
    maDvcs: string;
    productType: string | null;
    isEmployee: boolean;
  }): string | null {
    const { sale, maDvcs, productType, isEmployee } = params;

    // Get other_discamt value
    const otherDiscamt = Number(
      sale.other_discamt || sale.chietKhauMuaHangGiamGia || 0,
    );

    // Only apply for Employee AND when other_discamt > 0
    if (!isEmployee || otherDiscamt <= 0) {
      return null;
    }

    // Normalize product type
    const effectiveProductType = productType
      ? String(productType).toUpperCase().trim()
      : null;

    // Map codes based on company
    if (['TTM', 'TSG', 'THP'].includes(maDvcs)) {
      return '2505MN.CK521';
    } else if (maDvcs === 'FBV') {
      if (effectiveProductType === 'I') return 'SPQTNV';
      else if (effectiveProductType === 'S') return 'DVQTNV';
    } else if (maDvcs === 'LHV') {
      if (effectiveProductType === 'I') return 'R504SANPHAM';
      else if (effectiveProductType === 'S') return 'R504DICHVU';
    }

    return null;
  }

  /**
   * Resolve ma_ck03 based on Brand and Product Type (Retail Rules)
   *
   * Rules:
   * YAMAN:
   *  - SP: BTH CKVIP SP
   *  - DV: BTH CKVIP DV
   *
   * FACIALBAR / f3:
   *  - SP: FBV CKVIP SP
   *  - DV: FBV CKVIP DV
   *
   * LABHAIR:
   *  - SP: LHV CKVIP SP
   *  - DV: LHV CKVIP DV
   *
   * MENARD:
   *  - SP: MN CKVIP SP
   *  - DV: MN CKVIP DV
   *  - VC: MN CKVIP VC
   */
  static resolveMaCk03(params: {
    brand: string;
    productType: string | null;
  }): string | null {
    const { brand, productType } = params;
    const normBrand = (brand || '').trim().toUpperCase();
    const type = (productType || '').trim().toUpperCase();

    if (normBrand === 'YAMAN') {
      if (type === 'I') return 'BTH CKVIP SP';
      if (type === 'S') return 'BTH CKVIP DV';
    }

    if (normBrand === 'FACIALBAR' || normBrand === 'F3') {
      if (type === 'I') return 'FBV CKVIP SP';
      if (type === 'S') return 'FBV CKVIP DV';
    }

    if (normBrand === 'LABHAIR') {
      if (type === 'I') return 'LHV CKVIP SP';
      if (type === 'S') return 'LHV CKVIP DV';
    }

    if (normBrand === 'MENARD') {
      if (type === 'I') return 'MN CKVIP SP';
      if (type === 'S') return 'MN CKVIP DV';
      if (type === 'V') return 'MN CKVIP VC';
    }

    return null;
  }
  /**
   * Resolve ma_ck11 (Thanh toán TK tiền ảo) based on Brand and Product Type
   * [NEW] Centralized logic compatible with SalesQueryService
   */
  static resolveMaCk11(params: {
    brand: string;
    productType: string | null;
  }): string | null {
    const { brand, productType } = params;
    const normBrand = (brand || '').trim().toUpperCase();
    const type = (productType || '').trim().toUpperCase();

    // [UPDATED] 2026-02-10: Simplified Brand-based mapping per user request
    const brandMap: Record<string, string> = {
      'F3': 'FBV.TKECOIN',
      'FACIALBAR': 'FBV.TKECOIN',
      'LABHAIR': 'LHVTT.VCDV',
      'MENARD': 'MN.TKDV',
      'YAMAN': '', // Explicitly empty
    };

    let effectiveBrand = normBrand;
    // Map common aliases
    if (effectiveBrand === 'BTH') effectiveBrand = 'YAMAN';
    if (effectiveBrand === 'LHV') effectiveBrand = 'LABHAIR';

    return brandMap[effectiveBrand] || null;
  }
  /**
   * Xác định xem sale item có phải là hàng tặng (Giá = 0) hay không
   * Source of Truth cho cả Frontend Display và Fast API Payload (km_yn)
   */
  static isTangHang(giaBan: number, tienHang: number): boolean {
    return Math.abs(giaBan) < 0.01 && Math.abs(tienHang) < 0.01;
  }

  /**
   * Xác định giá trị hiển thị cho cột Khuyến mại (promCodeDisplay)
   * Source of Truth cho Frontend Display
   */
  static getPromCodeDisplay(
    isTangHang: boolean,
    isDichVu: boolean,
    maCtkmTangHang: string | null,
    isDoiDv: boolean = false,
    isDoiVo: boolean = false,
  ): string | null {
    if (isDoiDv || isDoiVo) return null;
    if (isTangHang && !isDichVu) {
      const maCtkmTangHangStr = maCtkmTangHang
        ? String(maCtkmTangHang).trim()
        : '';
      if (maCtkmTangHangStr !== 'TT DAU TU') {
        return '1';
      }
    }
    return null;
  }
  /**
   * Helper chuyển đổi về số an toàn (Source of Truth)
   */
  static toNumber(value: any, defaultValue: number = 0): number {
    if (value === null || value === undefined || value === '')
      return defaultValue;
    const num = Number(value);
    return isNaN(num) ? defaultValue : num;
  }

  /**
   * Helper giới hạn độ dài chuỗi (Source of Truth)
   */
  static limitString(
    value: any,
    maxLength: number,
    defaultValue: string = '',
  ): string {
    const val =
      value === null || value === undefined || value === ''
        ? defaultValue
        : String(value);
    return val.length > maxLength ? val.substring(0, maxLength) : val;
  }

  /**
   * Tính toán các giá trị tiền (Discount, Tax, etc.)
   * Source of Truth cho Fast API Payload và Frontend Display
   */
  static calculateInvoiceAmounts(params: {
    sale: any;
    orderData: any; // Header data (sales[0] usually) or explicit header
    allocationRatio: number;
    isPlatformOrder?: boolean;
    cashioData?: any[]; // Optional, for ECoin logic (NEW: Pass cashio data for payment method check)
  }): any {
    const { sale, orderData, allocationRatio, isPlatformOrder, cashioData } = params;

    // Determine header order types safely
    const headerSale = orderData?.sales?.[0] || {};
    const headerOrderTypes = InvoiceLogicUtils.getOrderTypes(
      headerSale.ordertypeName || headerSale.ordertype || '',
    );
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );

    const amounts: any = {
      tienThue: InvoiceLogicUtils.toNumber(sale.tienThue, 0),
      dtTgNt: InvoiceLogicUtils.toNumber(sale.dtTgNt, 0),
      ck01_nt: InvoiceLogicUtils.toNumber(
        InvoiceLogicUtils.resolveChietKhauMuaHangGiamGia(
          sale,
          orderTypes.isDoiDiem || headerOrderTypes.isDoiDiem,
        ),
        0,
      ),
      ck02_nt:
        InvoiceLogicUtils.toNumber(sale.disc_tm, 0) > 0
          ? InvoiceLogicUtils.toNumber(sale.disc_tm, 0)
          : InvoiceLogicUtils.toNumber(sale.chietKhauCkTheoChinhSach, 0),
      ck03_nt: InvoiceLogicUtils.toNumber(
        sale.chietKhauMuaHangCkVip || sale.grade_discamt,
        0,
      ),
      ck04_nt: InvoiceLogicUtils.toNumber(
        sale.chietKhauThanhToanCoupon || sale.chietKhau09,
        0,
      ),
      ck05_nt:
        !InvoiceLogicUtils.isWholesale(sale) && // [NEW] No ck05_nt for Wholesale
          InvoiceLogicUtils.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0
          ? InvoiceLogicUtils.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0)
          : 0,
      ck07_nt: InvoiceLogicUtils.toNumber(sale.chietKhauVoucherDp2, 0),
      ck08_nt: InvoiceLogicUtils.toNumber(sale.chietKhauVoucherDp3, 0),
    };

    // Fill others with default 0 or from sale fields
    for (let i = 9; i <= 22; i++) {
      if (i === 11) continue; // ck11 handled separately
      const key = `ck${i.toString().padStart(2, '0')}_nt`;
      const saleKey = `chietKhau${i.toString().padStart(2, '0')}`;
      amounts[key] = InvoiceLogicUtils.toNumber(sale[saleKey] || sale[key], 0);
    }

    // Map platform voucher (VC CTKM SÀN) to ck06 => REQ: Map to ck15 for Platform Order
    if (isPlatformOrder) {
      amounts.ck15_nt =
        InvoiceLogicUtils.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0
          ? InvoiceLogicUtils.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0)
          : InvoiceLogicUtils.toNumber(sale.chietKhauVoucherDp1, 0); // Fallback
      amounts.ck05_nt = 0; // Clear ck05
      amounts.ck06_nt = 0; // Clear ck06
    } else {
      amounts.ck06_nt = !InvoiceLogicUtils.isWholesale(sale) // [FIX] Zero out for Wholesale
        ? InvoiceLogicUtils.toNumber(sale.chietKhauVoucherDp1, 0)
        : 0;
    }

    // [SIMPLIFIED] ck05 vs ck11 logic - Strictly follow fop_syscode
    // Rule: ECOIN → ck11_nt, VOUCHER → ck05_nt, NO OVERLAP
    let ck11_nt = 0;

    // Check payment method from cashioData
    const ecoinRecord = cashioData?.find((c: any) => c.fop_syscode === 'ECOIN');
    const voucherRecord = cashioData?.find((c: any) => c.fop_syscode === 'VOUCHER');

    // Helper to get amount (ALWAYS use sale distributed amount)
    const getDiscountAmount = (record: any) => {
      // [FIX] Always use the distributed item amount (paid_by_voucher_ecode_ecoin_bp)
      // Do NOT use record.total_in because that is the Global Payment Amount useful only for single-line orders or checks
      return InvoiceLogicUtils.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0);
    };

    if (ecoinRecord) {
      // ECOIN payment → ck11_nt ONLY
      // [FIX] If total_in is 0, fallback to paid_by_voucher
      ck11_nt = getDiscountAmount(ecoinRecord);
      amounts.ck05_nt = 0; // Clear voucher (which was preset above)
    } else if (voucherRecord) {
      // VOUCHER payment → ck05_nt ONLY
      if (!InvoiceLogicUtils.isWholesale(sale) && !isPlatformOrder) {
        amounts.ck05_nt = getDiscountAmount(voucherRecord);
      }
      ck11_nt = 0; // Clear ECOIN
    } else {
      // No ECOIN/VOUCHER in cashioData → fallback to sale fields
      // ck05_nt is already set above based on paid_by_voucher_ecode_ecoin_bp
      ck11_nt = InvoiceLogicUtils.toNumber(
        sale.chietKhauThanhToanTkTienAo || sale.chietKhau11,
        0,
      );
    }

    amounts.ck11_nt = ck11_nt;

    // Allocation
    if (allocationRatio !== 1 && allocationRatio > 0) {
      Object.keys(amounts).forEach((k) => {
        if (k.endsWith('_nt') || k === 'tienThue' || k === 'dtTgNt') {
          amounts[k] *= allocationRatio;
        }
      });
    }

    if (orderTypes.isDoiDiem || headerOrderTypes.isDoiDiem) amounts.ck05_nt = 0;

    // [NEW] Wholesale Promotion (Zero Revenue) - Force all discounts to 0
    if (
      sale.ordertypeName === 'Xuất hàng KM cho đại lý' ||
      sale.ordertype === 'Xuất hàng KM cho đại lý'
    ) {
      Object.keys(amounts).forEach((k) => {
        if (k.startsWith('ck') && k.endsWith('_nt')) {
          amounts[k] = 0;
        }
      });
      // Also clear ck15_nt if it was set
      if (amounts.ck15_nt) amounts.ck15_nt = 0;
    }

    return amounts;
  }

  /**
   * Helper terse wrapper for limitString(toString(value, def), max)
   */
  static val(value: any, maxLength: number, defaultValue: string = ''): string {
    return this.limitString(
      value === null || value === undefined ? defaultValue : String(value),
      maxLength,
    );
  }

  static mapDiscountFields(params: {
    detailItem: any;
    amounts: any;
    sale: any;
    orderData: any;
    loyaltyProduct: any;
    isPlatformOrder?: boolean;
  }) {
    const {
      detailItem,
      amounts,
      sale,
      orderData,
      loyaltyProduct,
      isPlatformOrder,
    } = params;

    for (let i = 1; i <= 22; i++) {
      const idx = i.toString().padStart(2, '0');
      const key = `ck${idx}_nt`;
      const maKey = `ma_ck${idx}`;
      detailItem[key] = Number(amounts[key] || 0);

      // Special ma_ck logic
      if (i === 2) {
        // 02. Chiết khấu theo chính sách (Bán buôn)
        const isWholesale =
          sale.type_sale === 'WHOLESALE' || sale.type_sale === 'WS';
        const distTm = detailItem.ck02_nt;

        // [FIX] Explicitly handle Wholesale case logic structure
        if (isWholesale) {
          if (distTm > 0) {
            detailItem[maKey] = InvoiceLogicUtils.val(
              InvoiceLogicUtils.resolveWholesalePromotionCode({
                product: loyaltyProduct,
                distTm: distTm,
              }),
              32,
            );
          } else {
            // [FIX] If Wholesale and distTm <= 0, force empty ma_ck02
            detailItem[maKey] = '';
          }
        } else {
          // Retail / Normal behavior
          detailItem[maKey] = InvoiceLogicUtils.val(sale.maCk02 || '', 32);
        }
      } else if (i === 3) {
        // Note: calculateMuaHangCkVip might need to be imported or moved.
        // For now, assuming we pass the value or move it.
        // BUT calculateMuaHangCkVip is in SalesCalculationUtils (frontend/backend both use it)
        // Let's assume we copy logic or use simple value for now?
        // Ref: SalesPayloadService calls SalesCalculationUtils.calculateMuaHangCkVip
        // To avoid circular dep, we might need to duplicate specific small logic or refactor calculateMuaHangCkVip to here.
        // Let's keep it simple: if sales-calculation imports THIS, we can't import THAT.
        // Solution: Move calculateMuaHangCkVip to InvoiceLogicUtils as well.

        // [NEW] Resolve ma_ck03 for Retail Orders based on Brand/Type rules
        const isRetail =
          !InvoiceLogicUtils.isWholesale(sale) &&
          InvoiceLogicUtils.getOrderTypes(sale.ordertypeName).isThuong;

        let resolvedMaCk03: string | null = null;
        if (isRetail) {
          const brand =
            orderData.customer?.brand || // Primary source
            orderData.brand || // Fallback
            '';
          resolvedMaCk03 = InvoiceLogicUtils.resolveMaCk03({
            brand,
            productType:
              sale.productType || loyaltyProduct?.productType || null, // Prefer sale.productType
          });
        }

        if (detailItem.ck03_nt > 0) {
          // Use resolved value if available, otherwise fallback to existing logic
          detailItem[maKey] = InvoiceLogicUtils.val(
            resolvedMaCk03 || sale.muaHangCkVip || '',
            32,
          );
        } else {
          detailItem[maKey] = '';
        }
      } else if (i === 4) {
        detailItem[maKey] = InvoiceLogicUtils.val(
          detailItem.ck04_nt > 0 || sale.thanhToanCoupon
            ? sale.maCk04 || 'COUPON'
            : '',
          32,
        );
      } else if (i === 5) {
        const { isDoiDiem } = InvoiceLogicUtils.getOrderTypes(
          sale.ordertype || sale.ordertypeName,
        );
        const { isDoiDiem: isDoiDiemHeader } = InvoiceLogicUtils.getOrderTypes(
          orderData.sales?.[0]?.ordertype ||
          orderData.sales?.[0]?.ordertypeName ||
          '',
        );

        if (isDoiDiem || isDoiDiemHeader) {
          detailItem[maKey] = '';
        } else if (detailItem.ck05_nt > 0) {
          // Note: using logic from buildFastApiInvoiceData
          detailItem[maKey] = InvoiceLogicUtils.val(
            InvoiceLogicUtils.resolveVoucherCode({
              sale: {
                ...sale,
                customer: sale.customer || orderData.customer,
              },
              customer: null, // Resolution happens inside resolveVoucherCode
              brand: orderData.customer?.brand || orderData.brand || '',
            }),
            32,
            sale.maCk05 || 'VOUCHER',
          );
        }
      } else if (i === 7) {
        detailItem[maKey] = InvoiceLogicUtils.val(
          sale.voucherDp2 ? 'VOUCHER_DP2' : '',
          32,
        );
      } else if (i === 6) {
        // [FIX] Explicit check for ma_ck06 (Platform or Normal)
        if (InvoiceLogicUtils.isWholesale(sale)) {
          detailItem[maKey] = '';
        } else if (isPlatformOrder) {
          detailItem[maKey] = 'VC CTKM SÀN'; // [NEW] Platform Order Voucher Name
        } else {
          // [FIX] ma_ck06 only for Platform Orders or explicitly defined cases
          // User Request: "Phải kiểm tra nó là đơn sàn thì mới có giá trị"
          detailItem[maKey] = '';
        }
      } else if (i === 8) {
        detailItem[maKey] = InvoiceLogicUtils.val(
          sale.voucherDp3 ? 'VOUCHER_DP3' : '',
          32,
        );
      } else if (i === 11) {
        // [FIX] ma_ck11 Logic (Centralized)
        if (detailItem.ck11_nt > 0) {
          const brand =
            orderData.brand ||
            '';

          const resolvedCode = InvoiceLogicUtils.resolveMaCk11({
            brand,
            productType: sale.productType,
          });

          detailItem[maKey] = InvoiceLogicUtils.val(resolvedCode || sale.maCk11 || '', 32);
        } else {
          detailItem[maKey] = '';
        }

      } else {
        // Default mapping for other ma_ck fields
        if (i !== 1 && i !== 6) {
          // [FIX] Added i!==6 because handled above
          if (i === 15 && isPlatformOrder) {
            detailItem[maKey] = 'VC CTKM SÀN'; // [NEW] Platform Order Voucher Name
          } else {
            const saleMaKey = `maCk${idx}`;
            detailItem[maKey] = InvoiceLogicUtils.val(
              sale[saleMaKey] || '',
              32,
            );
          }
        }
      }
    }
  }
  /**
   * Xác định Mã Thẻ (maThe/ma_the)
   * Rule:
   * - Nếu là materialType 94 (Ecode/Voucher) và có soSerial -> dùng soSerial
   * - Nếu là đơn hàng thường (isThuong/isNormalOrder) và productType S hoặc V và có soSerial -> dùng soSerial
   * - Fallback: cardSerialMap (cho Fast API) hoặc sale.maThe (cho Frontend)
   */
  static resolveInvoiceMaThe(params: {
    loyaltyProduct: any;
    soSerial: string | null;
    isNormalOrder: boolean; // isThuong
    saleProductType: string | null;
    cardSerialFromMap?: string | null; // For Fast API
    saleMaThe?: string | null; // For Frontend default
  }): string {
    const {
      loyaltyProduct,
      soSerial,
      isNormalOrder,
      saleProductType,
      cardSerialFromMap,
      saleMaThe,
    } = params;

    const materialType = String(loyaltyProduct?.materialType || '');
    const productType = String(saleProductType || '');

    // Rule 1 & 2: Priority to soSerial for Type 94 OR (Normal Order & Type S/V)
    if (
      (materialType === '94' && soSerial) ||
      (isNormalOrder &&
        (productType === 'S' || productType === 'V') &&
        soSerial)
    ) {
      return soSerial || '';
    }

    const res = saleMaThe || cardSerialFromMap || '';
    return res;
  }

  /**
   * Xác định Mã khách gửi (ma_kh_i)
   * Chuẩn hóa bỏ prefix NV nếu có
   */
  static resolveInvoiceIssuePartnerCode(
    issuePartnerCode: string | null,
  ): string {
    if (!issuePartnerCode) return '';
    const trimmed = String(issuePartnerCode).trim();
    if (trimmed.length > 2 && trimmed.substring(0, 2).toUpperCase() === 'NV') {
      return trimmed.substring(2);
    }
    return trimmed;
  }

  /**
   * Xác định Mã thuế (ma_thue)
   * Default '00' nếu không có
   */
  static resolveInvoiceTaxCode(maThue: any): string {
    return maThue ? String(maThue).trim() : '00';
  }

  /**
   * Xác định Vật tư & ĐVT
   * Trả về { maVt, dvt }
   */
  static resolveInvoiceMaterial(
    sale: any,
    loyaltyProduct: any,
  ): { maVt: string; dvt: string; tenVt: string | null } {
    // 1. Ma Vat Tu
    const materialCode =
      loyaltyProduct?.materialCode ||
      sale.product?.maVatTu ||
      sale.itemCode ||
      '';

    // 2. DVT (Default 'Cái')
    const dvt =
      loyaltyProduct?.unit ||
      sale.product?.dvt ||
      sale.product?.unit ||
      sale.dvt ||
      'Cái';

    // 3. Ten VT
    const tenVt =
      loyaltyProduct?.name || sale.product?.tenVatTu || sale.itemName || null;

    return {
      maVt: String(materialCode).trim(),
      dvt: String(dvt).trim(),
      tenVt,
    };
  }
  /**
   * Orchestrate calculation of all derived fields for a sale item
   * Aggregates logic from other helper methods in this class
   */
  static async calculateSaleFields(
    sale: any,
    loyaltyProduct: any,
    department: any,
    branchCode: string,
    loyaltyService?: any,
    isEmployee?: boolean, // [NEW] Accept isEmployee param
    batchSerialFromST?: string | null, // [NEW] Accept batchSerial param
    maKhoFromST?: string | null, // [NEW] Accept maKho param
  ) {
    const orderTypes = this.getOrderTypes(sale.ordertypeName);
    const { isDoiDiem, isDauTu } = orderTypes;

    // 1. Calculate Prices (Source of Truth)
    const { giaBan, tienHang, tienHangGoc } = this.calculatePrices({
      sale,
      orderTypes,
      allocationRatio: 1, // Default to 1 (full allocation) if not split
      qtyFromStock: undefined,
    });

    const isTangHang = this.isTangHang(giaBan, tienHang);
    const maDvcs = department?.ma_dvcs || '';
    const maBp = department?.ma_bp || '';
    const productType = sale.productType || loyaltyProduct?.productType || null;
    const productTypeUpper = productType
      ? String(productType).toUpperCase().trim()
      : null;
    const promCode = sale.promCode || sale.maCk01 || '';

    // maHangGiamGia is needed for resolvePromotionCodes
    const maHangGiamGia =
      sale.maHangGiamGia ||
      this.calcCodeDisCount(
        sale.product?.productType || loyaltyProduct?.productType,
        sale.product?.materialType || loyaltyProduct?.materialType,
      ) ||
      '';

    // 2. Resolve Promotion Codes
    const { maCk01, maCtkmTangHang } = this.resolvePromotionCodes({
      sale,
      orderTypes,
      isTangHang,
      maDvcs,
      productTypeUpper,
      promCode,
      maHangGiamGia,
      isEmployee, // [NEW] Pass isEmployee
      loyaltyProduct, // [NEW] Pass loyaltyProduct
    });

    // 3. Resolve Accounting Accounts
    const { tkChietKhau, tkChiPhi, maPhi } =
      await this.resolveAccountingAccounts({
        sale,
        loyaltyProduct,
        orderTypes,
        isTangHang,
        hasMaCtkm: !!(maCk01 || maCtkmTangHang),
        hasMaCtkmTangHang: !!maCtkmTangHang,
        loyaltyService,
      });

    // 4. Resolve Batch/Serial
    const { maLo, soSerial } = this.resolveBatchSerial({
      batchSerialFromST: batchSerialFromST || null, // Logic này thường từ Stock Transfer wrapper
      trackBatch: !!loyaltyProduct?.trackBatch,
      trackSerial: !!loyaltyProduct?.trackSerial,
    });

    // 5. Resolve Loai GD
    const loaiGd = this.resolveLoaiGd({
      sale,
      orderTypes,
      loyaltyProduct,
    });

    // 6. Resolve Ma Kho
    // Cần inject logic kho sau hoặc pass vào param.
    // Tạm thời để rỗng hoặc tính trong SalesQueryService sau khi join ST
    const maKho = this.resolveMaKho({
      maKhoFromST: maKhoFromST || null,
      maKhoFromSale: sale.maKho || null,
      maBp: maBp, // Fixed: use maBp from department.ma_bp
      orderTypes,
    });

    // 7. Resolve Prom Code Display
    const promCodeDisplay = this.getPromCodeDisplay(
      isTangHang,
      orderTypes.isDichVu,
      maCtkmTangHang,
      orderTypes.isDoiDv,
      orderTypes.isDoiVo,
    );

    return {
      orderTypes,
      isTangHang,
      isDichVu: orderTypes.isDichVu,
      giaBan,
      tienHang,
      tienHangGoc,
      tkChietKhau,
      tkChiPhi,
      maPhi,
      maCk01, // Used as muaHangCkVip in some contexts
      muaHangCkVip: maCk01, // Alias for compatibility
      maCtkmTangHang,
      maLo,
      soSerial,
      loaiGd,
      maKho,
      promCodeDisplay,
      // [NEW] Wholesale fields
      maCk02:
        (sale.type_sale === 'WHOLESALE' || sale.type_sale === 'WS') &&
          (sale.disc_tm > 0 || sale.disc_amt > 0)
          ? this.resolveWholesalePromotionCode({
            product: loyaltyProduct,
            distTm:
              Number(sale.disc_tm || 0) > 0
                ? Number(sale.disc_tm)
                : Number(sale.disc_amt || 0),
          })
          : null,
      ck02Nt:
        (sale.type_sale === 'WHOLESALE' || sale.type_sale === 'WS') &&
          (sale.disc_tm > 0 || sale.disc_amt > 0)
          ? Number(sale.disc_tm || sale.disc_amt || 0)
          : 0,
    };
  }
  /**
   * Mã mua hàng giảm giá
   */
  static calcCodeDisCount(productType: any, loaiVt: any): string {
    let isEcode;
    if (String(loaiVt) === '94') {
      isEcode = true;
    } else {
      isEcode = false;
    }

    const ECG = ['10GIFT'];

    if (!isEcode) {
      return 'SP';
    } else {
      if (ECG.includes(productType)) {
        return 'ECG';
      } else {
        return 'E';
      }
    }
  }
  static resolveKmYn(
    sale: any,
    maCtkmTangHang: string | null,
    giaBan: number,
    tienHang: number,
  ): number {
    // [NEW] Wholesale Promotion Logic: Force km_yn = 1 if revenue is 0
    const isXuatHangKmDaiLy =
      sale.ordertypeName === 'Xuất hàng KM cho đại lý' ||
      sale.ordertype === 'Xuất hàng KM cho đại lý';

    if (isXuatHangKmDaiLy && Math.abs(Number(sale.revenue || 0)) < 0.01) {
      return 1;
    }

    const types = InvoiceLogicUtils.getOrderTypes(sale.ordertypeName);
    if (
      types.isDoiDv ||
      types.isDoiVo ||
      types.isTachThe ||
      types.isSinhNhat ||
      maCtkmTangHang === 'TT DAU TU'
    ) {
      return 0;
    }

    return InvoiceLogicUtils.isTangHang(giaBan, tienHang) ? 1 : 0;
  }
}
