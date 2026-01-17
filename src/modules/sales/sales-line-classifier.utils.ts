/**
 * Sales Line Classifier Utilities
 * Các hàm tiện ích để phân loại sales lines theo productType
 */

/**
 * Kiểm tra xem sale line có phải là dịch vụ (Service) không
 * Logic: productType = 'S' (dịch vụ thuần túy)
 *
 * @param sale - Sale item object
 * @returns true nếu là dòng dịch vụ
 */
export function isServiceLine(sale: any): boolean {
  if (!sale) return false;

  const productType = sale.productType?.toUpperCase()?.trim();
  return productType === 'S';
}

/**
 * Kiểm tra xem sale line có phải là vật tư xuất (Export/Item) không
 * Logic: productType = 'I' (vật tư, hàng hóa)
 *
 * @param sale - Sale item object
 * @returns true nếu là dòng vật tư xuất
 */
export function isExportLine(sale: any): boolean {
  if (!sale) return false;

  const productType = sale.productType?.toUpperCase()?.trim();
  return productType === 'I';
}

/**
 * Kiểm tra xem sale line có phải là sản phẩm (Product) không
 * Logic: productType = 'V' (sản phẩm)
 *
 * @param sale - Sale item object
 * @returns true nếu là dòng sản phẩm
 */
export function isProductLine(sale: any): boolean {
  if (!sale) return false;

  const productType = sale.productType?.toUpperCase()?.trim();
  return productType === 'V';
}

/**
 * Filter danh sách sales để lấy các dòng dịch vụ
 *
 * @param sales - Danh sách sale items
 * @returns Danh sách các dòng dịch vụ (productType = 'S')
 */
export function filterServiceLines(sales: any[]): any[] {
  if (!Array.isArray(sales)) return [];
  return sales.filter(isServiceLine);
}

/**
 * Filter danh sách sales để lấy các dòng vật tư xuất
 *
 * @param sales - Danh sách sale items
 * @returns Danh sách các dòng vật tư xuất (productType = 'I')
 */
export function filterExportLines(sales: any[]): any[] {
  if (!Array.isArray(sales)) return [];
  return sales.filter(isExportLine);
}

/**
 * Filter danh sách sales để lấy các dòng sản phẩm
 *
 * @param sales - Danh sách sale items
 * @returns Danh sách các dòng sản phẩm (productType = 'V')
 */
export function filterProductLines(sales: any[]): any[] {
  if (!Array.isArray(sales)) return [];
  return sales.filter(isProductLine);
}

/**
 * Phân loại sales thành các nhóm theo productType
 *
 * @param sales - Danh sách sale items
 * @returns Object chứa các nhóm sales đã phân loại
 */
export function classifySalesLines(sales: any[]): {
  serviceLines: any[];
  exportLines: any[];
  productLines: any[];
  otherLines: any[];
} {
  if (!Array.isArray(sales)) {
    return {
      serviceLines: [],
      exportLines: [],
      productLines: [],
      otherLines: [],
    };
  }

  const serviceLines: any[] = [];
  const exportLines: any[] = [];
  const productLines: any[] = [];
  const otherLines: any[] = [];

  sales.forEach((sale) => {
    if (isServiceLine(sale)) {
      serviceLines.push(sale);
    } else if (isExportLine(sale)) {
      exportLines.push(sale);
    } else if (isProductLine(sale)) {
      productLines.push(sale);
    } else {
      otherLines.push(sale);
    }
  });

  return {
    serviceLines,
    exportLines,
    productLines,
    otherLines,
  };
}
