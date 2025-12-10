/**
 * Product Utilities
 * Các hàm tiện ích cho products
 */

/**
 * Tính loại VC dựa trên productType và trackInventory
 * @param productType - Loại sản phẩm (DIVU, GIFT, ...)
 * @param trackInventory - Có theo dõi tồn kho hay không
 * @returns Loại VC: "VCDV" | "VCBH" | "VCKM" | null
 */
export function calculateVCType(
  productType: string | null | undefined,
  trackInventory: boolean | null | undefined,
): 'VCDV' | 'VCBH' | 'VCKM' | null {
  // Normalize productType
  const normalizedProductType = productType ? String(productType).trim().toUpperCase() : null;

  // VCDV: productType = "DIVU"
  if (normalizedProductType === 'DIVU') {
    return 'VCDV';
  }

  // VCKM: productType = "GIFT"
  if (normalizedProductType === 'GIFT') {
    return 'VCKM';
  }

  // VCBH: productType != "DIVU" && productType != "GIFT" && trackInventory = true
  if (normalizedProductType && normalizedProductType !== 'DIVU' && normalizedProductType !== 'GIFT') {
    if (trackInventory === true) {
      return 'VCBH';
    }
  }

  return null;
}

