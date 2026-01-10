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
  static resolveAccountingAccounts(params: {
    sale: any;
    loyaltyProduct: any;
    orderTypes: OrderTypes;
    isTangHang: boolean;
    hasMaCtkm: boolean;
    hasMaCtkmTangHang: boolean;
  }): AccountingAccounts {
    const {
      sale,
      loyaltyProduct,
      orderTypes,
      isTangHang,
      hasMaCtkm,
      hasMaCtkmTangHang,
    } = params;
    const { isDoiVo, isDoiDiem, isDauTu, isSinhNhat, isThuong } = orderTypes;

    let tkChietKhau: string | null = null;
    let tkChiPhi: string | null = null;
    let maPhi: string | null = null;

    const productType =
      sale.productType ||
      sale.producttype ||
      loyaltyProduct?.producttype ||
      loyaltyProduct?.productType ||
      null;
    const productTypeUpper = productType
      ? String(productType).toUpperCase().trim()
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
      tkChietKhau = sale.tkChietKhau || null;
      tkChiPhi = sale.tkChiPhi || null;
      maPhi = sale.maPhi || null;
    }

    return { tkChietKhau, tkChiPhi, maPhi };
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
  }) {
    const { sale, orderTypes, isTangHang, maDvcs, productTypeUpper, promCode } =
      params;
    const { isDoiDiem, isDauTu, isThuong, isBanTaiKhoan, isSanTmdt } =
      orderTypes;

    // Safety: Re-derive productTypeUpper if missing (Robust Check V9)
    let effectiveProductType = productTypeUpper;
    if (!effectiveProductType) {
      const pt =
        sale.productType ||
        sale.producttype ||
        sale.product?.productType ||
        sale.product?.producttype ||
        '';
      effectiveProductType = String(pt).toUpperCase().trim();
    }

    let maCk01 = sale.muaHangGiamGiaDisplay || '';
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

    if (!isDoiDiem && !isTangHang) {
      // 3. Standard Case: Assign + Add Suffix if missing
      if (!maCk01) {
        maCk01 = SalesUtils.getPromotionDisplayCode(code) || code || '';
      }

      // FIX V8.1: Add suffix if NOT PRMN-derived.
      // E.g: Input 'RMN...' -> isPRMNDerived=false -> Adds Suffix.
      //      Input 'PRMN...' -> isPRMNDerived=true -> Skips Suffix.
      if (maCk01 && effectiveProductType) {
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
  static resolveLoaiGd(params: { sale: any; orderTypes: OrderTypes }): string {
    const { sale, orderTypes } = params;
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

    if (isThuong) {
      if (sale.productType === 'I') {
        return '01';
      } else if (sale.productType === 'S' && sale.qty > 0) {
        return '02';
      }
    }

    if (isDichVu) {
      if (sale.productType === 'S' && sale.qty > 0) {
        return '01';
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
}
