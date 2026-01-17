/**
 * Sales Utilities
 * Các hàm tiện ích cho sales module
 */

/**
 * Validate integer value để tránh NaN
 */
export function validateInteger(value: any): number | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  const num = Number(value);
  if (isNaN(num) || !isFinite(num)) {
    return undefined;
  }
  return Math.floor(num);
}

/**
 * Normalize mã khách hàng: Bỏ prefix "NV" nếu có
 * VD: "NV8480" => "8480", "KH123" => "KH123"
 */
export function normalizeMaKh(maKh: string | null | undefined): string {
  if (!maKh) return '';
  const trimmed = String(maKh).trim();
  // Bỏ prefix "NV" nếu có (case insensitive)
  if (trimmed.length > 2 && trimmed.substring(0, 2).toUpperCase() === 'NV') {
    return trimmed.substring(2);
  }
  return trimmed;
}

/**
 * Xử lý promotion code: cắt phần sau dấu "-" để lấy code hiển thị
 */
export function getPromotionDisplayCode(
  promCode: string | null | undefined,
): string | null {
  if (!promCode) return null;
  const parts = promCode.split('-');
  return parts[0] || promCode;
}

/**
 * Map brand name sang brand code
 * menard → MN, f3 → FBV, chando → CDV, labhair → LHV, yaman → BTH
 */
export function mapBrandToCode(brand: string | null | undefined): string {
  if (!brand) return 'MN'; // Default

  const brandLower = brand.toLowerCase().trim();
  const brandMap: Record<string, string> = {
    menard: 'MN',
    f3: 'FBV',
    facialbar: 'FBV',
    chando: 'CDV',
    labhair: 'LHV',
    yaman: 'BTH',
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
export function generateTkTienAoLabel(
  docDate: any,
  brand: string | null | undefined,
): string {
  // Lấy ngày từ docDate
  let date: Date;
  if (docDate instanceof Date) {
    date = docDate;
  } else if (typeof docDate === 'string') {
    date = new Date(docDate);
    if (isNaN(date.getTime())) {
      // Nếu không parse được, dùng ngày hiện tại
      date = new Date();
    }
  } else {
    // Fallback: dùng ngày hiện tại
    date = new Date();
  }

  const year = date.getFullYear();
  const month = date.getMonth() + 1; // getMonth() trả về 0-11

  // Lấy 2 số cuối của năm
  const yy = String(year).slice(-2);
  // Format tháng thành 2 số (01, 02, ..., 12)
  const mm = String(month).padStart(2, '0');

  // Map brand name sang brand code
  const brandCode = mapBrandToCode(brand);

  return `${yy}${mm}${brandCode}.TKDV`;
}

/**
 * Lấy prefix từ ordertype để tính mã kho
 * - "L" cho: "02. Làm dịch vụ", "04. Đổi DV", "08. Tách thẻ", "Đổi thẻ KEEP->Thẻ DV"
 * - "B" cho: "01.Thường", "03. Đổi điểm", "05. Tặng sinh nhật", "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT", "Đổi vỏ"
 */
export function getOrderTypePrefix(
  ordertypeName: string | null | undefined,
): string | null {
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
 * Helper: Kiểm tra xem đơn hàng có phải "03. Đổi điểm" không
 */
export function isDoiDiemOrder(
  ordertype: string | null | undefined,
  ordertypeName: string | null | undefined,
): boolean {
  const ordertypeValue = ordertype || ordertypeName || '';
  return (
    ordertypeValue.includes('03. Đổi điểm') ||
    ordertypeValue.includes('03.Đổi điểm') ||
    ordertypeValue.includes('03.  Đổi điểm')
  );
}

/**
 * Helper: Kiểm tra xem đơn hàng có phải "04. Đổi DV" không
 */
export function isDoiDvOrder(
  ordertype: string | null | undefined,
  ordertypeName: string | null | undefined,
): boolean {
  const ordertypeValue = ordertype || ordertypeName || '';
  return (
    ordertypeValue.includes('04. Đổi DV') ||
    ordertypeValue.includes('04.Đổi DV') ||
    ordertypeValue.includes('04.  Đổi DV')
  );
}

/**
 * Helper: Kiểm tra xem đơn hàng có phải "05. Tặng sinh nhật" không
 */
export function isTangSinhNhatOrder(
  ordertype: string | null | undefined,
  ordertypeName: string | null | undefined,
): boolean {
  const ordertypeValue = ordertype || ordertypeName || '';
  return (
    ordertypeValue.includes('05. Tặng sinh nhật') ||
    ordertypeValue.includes('05.Tặng sinh nhật') ||
    ordertypeValue.includes('05.  Tặng sinh nhật')
  );
}

/**
 * Helper: Kiểm tra xem đơn hàng có phải "06. Đầu tư" không
 */
export function isDauTuOrder(
  ordertype: string | null | undefined,
  ordertypeName: string | null | undefined,
): boolean {
  const ordertypeValue = ordertype || ordertypeName || '';
  return (
    ordertypeValue.includes('06. Đầu tư') ||
    ordertypeValue.includes('06.Đầu tư') ||
    ordertypeValue.includes('06.  Đầu tư')
  );
}

/**
 * Helper: Kiểm tra xem đơn hàng có phải "08. Tách thẻ" không
 */
export function isTachTheOrder(
  ordertype: string | null | undefined,
  ordertypeName: string | null | undefined,
): boolean {
  const ordertypeValue = ordertype || ordertypeName || '';
  return (
    ordertypeValue.includes('08. Tách thẻ') ||
    ordertypeValue.includes('08.Tách thẻ') ||
    ordertypeValue.includes('08.  Tách thẻ')
  );
}

/**
 * Helper: Kiểm tra xem đơn hàng có phải "Đổi vỏ" không
 */
export function isDoiVoOrder(
  ordertype: string | null | undefined,
  ordertypeName: string | null | undefined,
): boolean {
  const ordertypeValue = ordertype || ordertypeName || '';
  return ordertypeValue.includes('Đổi vỏ');
}

/**
 * Helper: Kiểm tra xem item có phải "TRUTONKEEP" không
 * Items này sẽ bị bỏ qua khi map kho và tạo hóa đơn
 */
export function isTrutonkeepItem(itemCode: string | null | undefined): boolean {
  if (!itemCode) return false;
  return itemCode.trim().toUpperCase() === 'TRUTONKEEP';
}

/**
 * Normalize brand name: "facialbar" → "f3", giữ nguyên các brand khác
 */
export function normalizeBrand(brand: string | null | undefined): string {
  if (!brand) return '';
  let brandLower = brand.toLowerCase().trim();
  if (brandLower === 'facialbar') {
    brandLower = 'f3';
  }
  return brandLower;
}

/**
 * Lấy productType từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
 */
export function getProductType(sale: any, loyaltyProduct?: any): string | null {
  return (
    loyaltyProduct?.productType ||
    loyaltyProduct?.producttype ||
    sale.productType ||
    sale.producttype ||
    sale.product?.productType ||
    sale.product?.producttype ||
    null
  );
}

/**
 * Lấy materialCode từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
 */
export function getMaterialCode(
  sale: any,
  loyaltyProduct?: any,
): string | null {
  return (
    loyaltyProduct?.materialCode ||
    sale.product?.maVatTu ||
    sale.product?.materialCode ||
    sale.itemCode ||
    null
  );
}

/**
 * Lấy trackInventory từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
 */
export function getTrackInventory(
  sale: any,
  loyaltyProduct?: any,
): boolean | null {
  return (
    loyaltyProduct?.trackInventory ??
    sale.trackInventory ??
    sale.product?.trackInventory ??
    null
  );
}

/**
 * Lấy trackSerial từ nhiều nguồn (ưu tiên loyaltyProduct, sau đó sale)
 */
export function getTrackSerial(
  sale: any,
  loyaltyProduct?: any,
): boolean | null {
  return (
    loyaltyProduct?.trackSerial ??
    sale.trackSerial ??
    sale.product?.trackSerial ??
    null
  );
}

/**
 * Tính VIP type dựa trên quy tắc (theo thứ tự ưu tiên):
 *
 * 1. VIP DV MAT: Nếu productType == "DIVU"
 *    - Ví dụ: SPAMDV511 (productType: "DIVU", catName: "DVBDY", unit: "Lần")
 *
 * 2. VIP VC MP: Nếu productType == "VOUC"
 *    - Ví dụ: E_VCM10.5TR_MDVK04 (productType: "VOUC", code có "E_" và "VC")
 *
 * 3. VIP VC MP: Nếu materialCode bắt đầu bằng "E." HOẶC
 *               "VC" có trong materialCode/code (không phân biệt hoa thường) HOẶC
 *               (trackInventory == false VÀ trackSerial == true)
 *
 * 4. VIP MP: Tất cả các trường hợp còn lại
 *    - Ví dụ: AUTO02 (productType: "MAKE", materialCode: "M00151", không có "VC")
 */
export function calculateVipType(
  productType: string | null | undefined,
  materialCode: string | null | undefined,
  code: string | null | undefined,
  trackInventory: boolean | null | undefined,
  trackSerial: boolean | null | undefined,
): string {
  // Rule 1: VIP DV MAT - Nếu productType == "DIVU"
  if (productType === 'DIVU') {
    return 'VIP DV MAT';
  }

  // Rule 2: VIP VC MP - Nếu productType == "VOUC"
  if (productType === 'VOUC') {
    return 'VIP VC MP';
  }

  // Rule 3: VIP VC MP - Kiểm tra các điều kiện khác
  const materialCodeStr = materialCode || '';
  const codeStr = code || '';
  // Kiểm tra "VC" trong materialCode hoặc code (không phân biệt hoa thường)
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

  // Rule 4: VIP MP - Tất cả các trường hợp còn lại
  return 'VIP MP';
}

/**
 * Xác định nên dùng ma_lo hay so_serial dựa trên trackSerial và trackBatch từ Loyalty API
 * trackSerial: true → dùng so_serial
 * trackBatch: true → dùng ma_lo
 * Nếu cả hai đều true → ưu tiên trackBatch (dùng ma_lo)
 */
export function shouldUseBatch(
  trackBatch: boolean | null | undefined,
  trackSerial: boolean | null | undefined,
): boolean {
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
 * Tạo Map để lookup nhanh products theo maERP
 * @param products - Danh sách products từ database
 * @returns Map với key là maERP, value là ProductItem
 */
export function createProductMap(products: any[]): Map<string, any> {
  const productMap = new Map<string, any>();
  products.forEach((product) => {
    if (product.maERP) {
      productMap.set(product.maERP, product);
    }
  });
  return productMap;
}

/**
 * Enrich sale với product từ database
 * @param sale - Sale object
 * @param productMap - Map products theo maERP
 * @returns Sale object đã được enrich với product
 */
export function enrichSaleWithProduct(
  sale: any,
  productMap: Map<string, any>,
): any {
  return {
    ...sale,
    product: sale.itemCode ? productMap.get(sale.itemCode) || null : null,
  };
}

/**
 * Enrich sale với product từ Loyalty API
 * @param sale - Sale object (đã có product từ database)
 * @param loyaltyProductMap - Map products từ Loyalty API theo itemCode
 * @returns Sale object đã được enrich với thông tin từ Loyalty API
 */
export function enrichSaleWithLoyaltyProduct(
  sale: any,
  loyaltyProductMap: Map<string, any>,
): any {
  const loyaltyProduct = sale.itemCode
    ? loyaltyProductMap.get(sale.itemCode)
    : null;
  const existingProduct = sale.product;

  // Nếu có product từ Loyalty API, merge thông tin (ưu tiên dvt từ Loyalty API)
  if (loyaltyProduct) {
    return {
      ...sale,
      // Lấy producttype từ Loyalty API (không còn trong database)
      producttype:
        loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
      product: {
        ...existingProduct,
        ...loyaltyProduct,
        // Map unit từ Loyalty API thành dvt
        dvt: loyaltyProduct.unit || existingProduct?.dvt || null,
        // Giữ lại các field từ database nếu có, chỉ dùng materialCode từ Loyalty API
        maVatTu:
          existingProduct?.maVatTu ||
          loyaltyProduct.materialCode ||
          sale.itemCode,
        maERP:
          existingProduct?.maERP ||
          loyaltyProduct.materialCode ||
          sale.itemCode,
        // Đảm bảo productType từ Loyalty API được giữ lại (ưu tiên productType, sau đó producttype)
        productType:
          loyaltyProduct.productType ||
          loyaltyProduct.producttype ||
          existingProduct?.productType ||
          null,
        // Lấy producttype từ Loyalty API
        producttype:
          loyaltyProduct.producttype ||
          loyaltyProduct.productType ||
          existingProduct?.producttype ||
          null,
      },
    };
  }

  return sale;
}

/**
 * Tạo Map để lookup card codes theo service item name
 * @param cardData - Data từ card API
 * @returns Map với key là service_item_name, value là serial
 */
export function createCardCodeMap(cardData: any): Map<string, string> {
  const cardCodeMap = new Map<string, string>();

  if (!cardData || !Array.isArray(cardData.data)) {
    return cardCodeMap;
  }

  for (const card of cardData.data) {
    if (!card?.service_item_name || !card?.serial) {
      continue; // bỏ qua record lỗi / rỗng
    }
    cardCodeMap.set(card.service_item_name, card.serial);
  }

  return cardCodeMap;
}

/**
 * Enrich sale với card code (maThe)
 * @param sale - Sale object
 * @param cardCodeMap - Map card codes theo service_item_name
 * @returns Sale object đã được enrich với maThe
 */
export function enrichSaleWithCardCode(
  sale: any,
  cardCodeMap: Map<string, string>,
): any {
  return {
    ...sale,
    maThe: cardCodeMap.get(sale.itemCode) || '',
  };
}

/**
 * Extract unique item codes từ danh sách sales
 * @param sales - Danh sách sales
 * @returns Array các itemCode unique (đã filter null/empty)
 */
export function extractUniqueItemCodes(sales: any[]): string[] {
  return Array.from(
    new Set(
      sales
        .map((sale) => sale.itemCode)
        .filter((code): code is string => !!code && code.trim() !== ''),
    ),
  );
}

/**
 * Extract unique branch codes từ danh sách sales
 * @param sales - Danh sách sales
 * @returns Array các branchCode unique (đã filter null/empty)
 */
export function extractUniqueBranchCodes(sales: any[]): string[] {
  return Array.from(
    new Set(
      sales
        .map((sale) => sale.branchCode)
        .filter((code): code is string => !!code && code.trim() !== ''),
    ),
  );
}

/**
 * Filter item codes cho sales hợp lệ (statusAsys !== false)
 * @param itemCodes - Danh sách item codes
 * @param sales - Danh sách sales
 * @returns Array các itemCode cho sales hợp lệ
 */
export function filterValidItemCodes(
  itemCodes: string[],
  sales: any[],
): string[] {
  return itemCodes.filter((itemCode) => {
    const sale = sales.find((s) => s.itemCode === itemCode);
    return sale && sale.statusAsys !== false;
  });
}

/**
 * Parse date từ format DDMMMYYYY (ví dụ: 01JAN2025)
 */
export function parseDateFromDDMMMYYYY(dateStr: string): Date {
  const day = parseInt(dateStr.substring(0, 2));
  const monthStr = dateStr.substring(2, 5).toUpperCase();
  const year = parseInt(dateStr.substring(5, 9));

  const monthMap: Record<string, number> = {
    JAN: 0,
    FEB: 1,
    MAR: 2,
    APR: 3,
    MAY: 4,
    JUN: 5,
    JUL: 6,
    AUG: 7,
    SEP: 8,
    OCT: 9,
    NOV: 10,
    DEC: 11,
  };

  const month = monthMap[monthStr] || 0;
  return new Date(year, month, day);
}

/**
 * Map company name sang brand code
 */
export function mapCompanyToBrand(company: string | null | undefined): string {
  if (!company) return '';

  const companyUpper = company.toUpperCase();
  const brandMap: Record<string, string> = {
    F3: 'f3',
    FACIALBAR: 'f3',
    MENARD: 'menard',
    LABHAIR: 'labhair',
    YAMAN: 'yaman',
    CHANDO: 'chando',
  };

  return brandMap[companyUpper] || company.toLowerCase();
}

/**
 * Build customer object từ sale data
 */
export function buildCustomerObject(
  customer: any,
  partnerCode?: string | null,
) {
  if (customer) {
    return {
      code: customer.code || partnerCode || null,
      brand: customer.brand || null,
      name: customer.name || null,
      mobile: customer.mobile || null,
    };
  }

  if (partnerCode) {
    return {
      code: partnerCode,
      brand: null,
      name: null,
      mobile: null,
    };
  }

  return null;
}

/**
 * Tạo date range mặc định (30 ngày gần nhất)
 */
export function getDefaultDateRange(): { startDate: Date; endDate: Date } {
  const endDate = new Date();
  endDate.setHours(23, 59, 59, 999);

  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 30);
  startDate.setHours(0, 0, 0, 0);

  return { startDate, endDate };
}

/**
 * Set thời gian bắt đầu ngày (00:00:00.000)
 */
export function setStartOfDay(date: Date): Date {
  const newDate = new Date(date);
  newDate.setHours(0, 0, 0, 0);
  return newDate;
}

/**
 * Set thời gian kết thúc ngày (23:59:59.999)
 */
export function setEndOfDay(date: Date): Date {
  const newDate = new Date(date);
  newDate.setHours(23, 59, 59, 999);
  return newDate;
}

/**
 * Parse date string và set start of day
 */
export function parseDateStart(dateStr: string): Date {
  return setStartOfDay(new Date(dateStr));
}

/**
 * Parse date string và set end of day
 */
export function parseDateEnd(dateStr: string): Date {
  return setEndOfDay(new Date(dateStr));
}
