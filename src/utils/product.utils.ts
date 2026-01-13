/**
 * Product Utilities
 * Các hàm tiện ích cho products
 */

/**
 * Tính loại VC dựa trên productType và trackInventory
 * @param productType - Loại sản phẩm (DIVU, GIFT, ...)
 * @param trackInventory - Có theo dõi tồn kho hay không
 * @returns Loại VC: "VCDV" | "VCHB" | "VCKM" | null
 */
export function calculateVCType(
  productType: string | null | undefined,
  trackInventory: boolean | null | undefined,
): 'VCDV' | 'VCHB' | 'VCKM' | null {
  // Normalize productType
  const normalizedProductType = productType
    ? String(productType).trim().toUpperCase()
    : null;

  // VCDV: productType = "DIVU"
  if (normalizedProductType === 'DIVU') {
    return 'VCDV';
  }

  // VCKM: productType = "GIFT"
  if (normalizedProductType === 'GIFT') {
    return 'VCKM';
  }

  // VCHB: productType != "DIVU" && productType != "GIFT" && trackInventory = true
  if (
    normalizedProductType &&
    normalizedProductType !== 'DIVU' &&
    normalizedProductType !== 'GIFT'
  ) {
    if (trackInventory === true) {
      return 'VCHB';
    }
  }

  return null;
}
