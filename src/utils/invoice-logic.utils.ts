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
    const isThuong =
      normalized.includes('01.Thường') ||
      normalized.includes('01. Thường') ||
      normalized.toLowerCase().includes('thường') ||
      normalized.toLowerCase().includes('thuong');
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
    };
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
  }) {
    const {
      sale,
      orderTypes,
      isTangHang,
      maDvcs,
      productTypeUpper,
      promCode,
      maHangGiamGia,
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
      maCtkmTangHang = mapMaDvcsToKmDiem[maDvcs] || 'TTM.KMDIEM';
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
      // [NEW] Skip suffix for Platform Orders (Sàn TMĐT)
      if (maCk01 && effectiveProductType && !effectiveIsSanTmdt) {
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
  }): string | null {
    const { sale, customer, brand } = params;
    const listEcomName = ['shopee', 'lazada', 'tiktok'];
    const brandLower = brand?.toLowerCase();

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

    // Logic xác định dùng maLo hay soSerial (Priority: Batch > Serial)
    const useBatch = trackBatch && !trackSerial;
    // Nếu cả hai đều true hoặc cả hai đều false, check logic trong SalesUtils.shouldUseBatch
    // (Thường là trackBatch: true, trackSerial: false => ma_lo)

    // Re-use logic from SalesUtils.shouldUseBatch for consistency
    const isActuallyBatch = trackBatch === true && trackSerial !== true;

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
        return '02';
      } else if (sale.productType === 'V') {
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
    const mpCategories = [
      '01SKIN',
      '02MAKE',
      '04BODY',
      '05HAIR',
      '06FRAG',
      '07PROF',
      '10GIFT',
    ];
    const tpcnCategories = ['03TPCN'];
    const ccdcCategories = ['11MMOC'];

    if (mpCategories.some((cat) => groupProductType.includes(cat))) {
      return 'MP';
    } else if (tpcnCategories.some((cat) => groupProductType.includes(cat))) {
      return 'TPCN';
    } else if (ccdcCategories.some((cat) => groupProductType.includes(cat))) {
      return 'CCDC';
    }

    return 'MP'; // Default
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
    if (val === null || val === undefined) {
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
}
