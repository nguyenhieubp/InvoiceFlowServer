import * as SalesUtils from './sales.utils';
import { InvoiceLogicUtils } from './invoice-logic.utils';

/**
 * Sales Calculation Utilities
 */

/**
 * Tính muaHangCkVip/maCk03 dựa trên sale, loyaltyProduct và brand
 */
export function calculateMuaHangCkVip(
  sale: any,
  loyaltyProduct: any,
  brand: string | null | undefined,
): string {
  const ck03_nt = Number(sale.chietKhauMuaHangCkVip || sale.grade_discamt || 0);
  if (ck03_nt <= 0) {
    return sale.muaHangCkVip || '';
  }

  const brandLower = SalesUtils.normalizeBrand(brand);
  const productType = SalesUtils.getProductType(sale, loyaltyProduct);
  const materialCode = SalesUtils.getMaterialCode(sale, loyaltyProduct);
  const code = sale.itemCode || null;
  const trackInventory = SalesUtils.getTrackInventory(sale, loyaltyProduct);
  const trackSerial = SalesUtils.getTrackSerial(sale, loyaltyProduct);

  if (brandLower === 'f3') {
    return productType === 'DIVU' ? 'FBV CKVIP DV' : 'FBV CKVIP SP';
  }

  return SalesUtils.calculateVipType(
    productType,
    materialCode,
    code,
    trackInventory,
    trackSerial,
  );
}

/**
 * Tính toán các field phức tạp cho sale: maLo, maCtkmTangHang, muaHangCkVip
 */
export function calculateSaleFields(
  sale: any,
  loyaltyProduct: any,
  department: any,
  branchCode: string | null,
): {
  maLo: string | null;
  maCtkmTangHang: string | null;
  muaHangCkVip: string;
  maKho: string | null;
  isTangHang: boolean;
  isDichVu: boolean;
  promCodeDisplay: string | null;
} {
  const tienHang = Number(sale.tienHang || sale.linetotal || 0);
  const qty = Number(sale.qty || 0);
  let giaBan = Number(sale.giaBan || 0);
  if (giaBan === 0 && tienHang != null && qty > 0) giaBan = tienHang / qty;

  let isTangHang = InvoiceLogicUtils.isTangHang(giaBan, tienHang);

  const orderTypes = InvoiceLogicUtils.getOrderTypes(sale.ordertypeName || '');
  const { isDichVu, isDoiDiem, isDoiDv, isDoiVo } = orderTypes;

  if (isDoiDiem) isTangHang = false;

  let maCtkmTangHang: string | null = sale.maCtkmTangHang
    ? String(sale.maCtkmTangHang).trim()
    : null;
  if (!maCtkmTangHang || maCtkmTangHang === '') maCtkmTangHang = null;

  if (isDoiDiem && !maCtkmTangHang) {
    const maDvcs = department?.ma_dvcs || department?.ma_dvcs_ht || '';
    const mapMaDvcsToKmDiem: Record<string, string> = {
      TTM: 'TTM.KMDIEM',
      AMA: 'TTM.KMDIEM',
      TSG: 'TTM.KMDIEM',
      FBV: 'FBV.KMDIEM',
      BTH: 'BTH.KMDIEM',
      CDV: 'CDV.KMDIEM',
      LHV: 'LHV.KMDIEM',
    };
    maCtkmTangHang = mapMaDvcsToKmDiem[maDvcs] || null;
  }

  if (isTangHang && !maCtkmTangHang) {
    const ordertypeName = sale.ordertypeName || '';
    if (
      ordertypeName.includes('06. Đầu tư') ||
      ordertypeName.includes('06.Đầu tư')
    ) {
      maCtkmTangHang = 'TT DAU TU';
    } else {
      maCtkmTangHang =
        SalesUtils.getPromotionDisplayCode(sale.promCode) ||
        sale.promCode ||
        null;
    }
    if (maCtkmTangHang) maCtkmTangHang = maCtkmTangHang.trim();
  }

  // Use centralized logic for display code
  const promCodeDisplay = InvoiceLogicUtils.getPromCodeDisplay(
    isTangHang,
    isDichVu,
    maCtkmTangHang,
    isDoiDv,
    isDoiVo,
  );

  const customerBrand = sale.customer?.brand || null;
  const muaHangCkVip = calculateMuaHangCkVip(
    sale,
    loyaltyProduct,
    customerBrand,
  );

  const maKho = sale.maKho || null;
  const useBatchForMaLo = SalesUtils.shouldUseBatch(
    loyaltyProduct?.trackBatch,
    loyaltyProduct?.trackSerial,
  );
  const maLo = useBatchForMaLo ? sale.serial || '' : null;

  return {
    maLo,
    maCtkmTangHang,
    muaHangCkVip,
    maKho,
    isTangHang,
    isDichVu,
    promCodeDisplay,
  };
}
