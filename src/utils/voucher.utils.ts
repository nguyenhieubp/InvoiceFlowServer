/**
 * Voucher Utilities
 * Các hàm tiện ích cho voucher/coupon logic
 */

/**
 * Tính mã CK05 (Thanh toán voucher) dựa trên brand và product type
 * @param sale - Sale object
 * @param productType - Product type (I, S, V, etc.)
 * @param isGift - Có phải sản phẩm tặng không
 * @param brand - Brand name (yaman, facialbar, labhair, menard)
 * @returns Mã CK05: "VCDV" | "VCHB" | "VCKM" | "FBV TT VCDV" | "YVC.HB" | etc. | null
 */
export function calculateMaCk05(
  sale: any,
  productType: string | null | undefined,
  isGift: boolean,
  brand: string | null | undefined,
): string | null {
  if (!sale) return null;

  const revenueValue = sale.revenue ?? 0;
  const linetotalValue = sale.linetotal ?? sale.tienHang ?? 0;

  // Nếu revenue = 0 và linetotal = 0 → không gắn nhãn
  if (revenueValue === 0 && linetotalValue === 0) {
    return null;
  }

  const brandLower = brand ? brand.toLowerCase().trim() : '';

  // Logic theo từng brand
  switch (brandLower) {
    case 'yaman':
      return calculateMaCk05Yaman(productType);

    case 'facialbar':
    case 'f3':
      return calculateMaCk05Facialbar(productType);

    case 'labhair':
      return calculateMaCk05Labhair(productType, isGift);

    case 'menard':
      return calculateMaCk05Menard(productType, isGift);

    default:
      return null;
  }
}

/**
 * Wrapper function để tính mã CK05 trực tiếp từ sale object
 * @param sale - Sale object
 * @returns Mã CK05 hoặc null
 */
export function calculateMaCk05FromSale(sale: any): string | null {
  if (!sale) return null;

  // [NEW] Wholesale orders should NOT have ma_ck05 (VC HB, etc.)
  const typeSale = (sale.type_sale || '').toUpperCase().trim();
  if (typeSale === 'WHOLESALE' || typeSale === 'WS') {
    return null;
  }

  // Lấy productType và isGift từ sale bằng helper từ SalesUtils
  // Chú ý: Cần import SalesUtils nếu chưa có, hoặc gọi trực tiếp logic
  const productType =
    sale.productType ||
    sale.producttype ||
    sale.product?.productType ||
    sale.product?.producttype ||
    null;

  const isGift = sale.product?.productType === 'GIFT';
  const brand = sale.brand || sale.customer?.brand || '';

  return calculateMaCk05(sale, productType, isGift, brand);
}

/**
 * Tính mã CK05 cho brand Yaman
 */
function calculateMaCk05Yaman(
  productType: string | null | undefined,
): string | null {
  if (productType === 'I') {
    return 'YVC.HB';
  }
  if (productType === 'S') {
    return 'YVC.DV';
  }
  return null;
}

/**
 * Tính mã CK05 cho brand Facialbar/F3
 */
function calculateMaCk05Facialbar(
  productType: string | null | undefined,
): string | null {
  if (productType === 'I') {
    return 'FBV TT VCDV';
  }
  if (productType === 'S') {
    return 'FBV TT VCHH';
  }
  return null;
}

/**
 * Tính mã CK05 cho brand Labhair
 */
function calculateMaCk05Labhair(
  productType: string | null | undefined,
  isGift: boolean,
): string | null {
  if (productType === 'I') {
    if (isGift) {
      return 'LHVTT.VCKM';
    }
    return 'LHVTT.VCHB';
  }
  if (productType === 'S') {
    return 'LHVTT.VCDV';
  }
  return null;
}

/**
 * Tính mã CK05 cho brand Menard
 */
function calculateMaCk05Menard(
  productType: string | null | undefined,
  isGift: boolean,
): string | null {
  if (productType === 'I') {
    if (isGift) {
      return 'VC KM';
    }
    return 'VC HB';
  }
  if (productType === 'S') {
    return 'VC DV';
  }
  if (productType === 'V') {
    return 'VC KM';
  }
  return null;
}
