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
 * Centralized business logic for Invoices to ensure consistency
 * between FastAPI payloads and Frontend display.
 */
export class InvoiceLogicUtils {
  /**
   * Xác định các loại đơn hàng đặc biệt
   */
  static getOrderTypes(ordertypeName: string = '') {
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
    };
  }

  /**
   * Tính toán các tài khoản hạch toán (Source of Truth)
   */
  static resolveAccountingAccounts(params: {
    sale: any;
    loyaltyProduct: any;
    isDoiVo: boolean;
    isDoiDiem: boolean;
    isDauTu: boolean;
    isSinhNhat: boolean;
    isThuong: boolean;
    isTangHang: boolean;
    isGiaBanZero: boolean;
    hasMaCtkm: boolean;
    hasMaCtkmTangHang: boolean;
  }): AccountingAccounts {
    const {
      sale,
      loyaltyProduct,
      isDoiVo,
      isDoiDiem,
      isDauTu,
      isSinhNhat,
      isThuong,
      isTangHang,
      isGiaBanZero,
      hasMaCtkm,
      hasMaCtkmTangHang,
    } = params;

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
    } else if (isThuong && hasMaCtkmTangHang && isGiaBanZero && isTangHang) {
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
    } else if (
      isThuong &&
      hasChietKhauMuaHangGiamGia &&
      productTypeUpper === 'I'
    ) {
      tkChietKhau = '521111';
    } else if (
      isThuong &&
      hasMaCtkm &&
      !(hasMaCtkmTangHang && isGiaBanZero && isTangHang)
    ) {
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
    isDoiDiem: boolean;
    isDauTu: boolean;
    isThuong: boolean;
    isTangHang: boolean;
    maDvcs: string;
    productTypeUpper: string | null;
    promCode: string | null;
  }) {
    const {
      sale,
      isDoiDiem,
      isDauTu,
      isThuong,
      isTangHang,
      maDvcs,
      productTypeUpper,
      promCode,
    } = params;

    let maCk01 = sale.muaHangGiamGiaDisplay || '';
    let maCtkmTangHang = sale.maCtkmTangHang || '';

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
      } else if (
        isThuong ||
        (sale.ordertypeName || '').includes('07. Bán tài khoản') ||
        (sale.ordertypeName || '').includes('9. Sàn TMDT')
      ) {
        maCtkmTangHang = promCode || '';
        if (maCtkmTangHang && productTypeUpper) {
          maCtkmTangHang = `${maCtkmTangHang}.${productTypeUpper}`;
        }
      }
    }

    if (!isDoiDiem && !isTangHang && !maCk01) {
      maCk01 = SalesUtils.getPromotionDisplayCode(promCode) || promCode || '';
      if (maCk01 && productTypeUpper) {
        maCk01 = `${maCk01}.${productTypeUpper}`;
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
}
