/**
 * Sales Invoice Constants
 * Centralized constants for order types, document source types, product types, etc.
 */

/**
 * Order Types (Loại đơn hàng)
 */
export const ORDER_TYPES = {
  NORMAL: '01.Thường',
  NORMAL_WITH_SPACE: '01. Thường', // Alternative with space
  SERVICE: '02. làm dịch vụ',
  SERVICE_NO_SPACE: '02.làm dịch vụ', // Alternative without space
  LOYALTY_EXCHANGE: '03. Đổi điểm',
  NORMAL_EXCHANGE: '04. Đổi DV',
  BIRTHDAY_GIFT: '05. Tặng sinh nhật',
  INVESTMENT: '06. Đầu tư',
  ACCOUNT_SALE: '07. Bán tài khoản',
  CARD_SEPARATION: '08. Tách thẻ',
  BOTTLE_EXCHANGE: 'Đổi vỏ',
} as const;

/**
 * Document Source Types
 */
export const DOC_SOURCE_TYPES = {
  SALE_RETURN: 'SALE_RETURN',
  ORDER_RETURN: 'ORDER_RETURN',
} as const;

/**
 * Product Types
 */
export const PRODUCT_TYPES = {
  SERVICE: 'S', // Dịch vụ
  ITEM_EXPORT: 'I', // Xuất kho
  PRODUCT: 'V', // Sản phẩm
} as const;

/**
 * Status Codes
 */
export const STATUS = {
  FAILED: 0,
  INACTIVE: 0,
  SUCCESS: 1,
  ACTIVE: 1,
} as const;

/**
 * Action Codes
 */
export const ACTION = {
  NORMAL: 0,
  CANCEL_UPDATE: 1,
} as const;

/**
 * Helper function: Normalize order type name for comparison
 */
export function normalizeOrderType(
  orderTypeName: string | null | undefined,
): string {
  if (!orderTypeName) return '';
  return String(orderTypeName).trim().toLowerCase();
}

/**
 * Helper function: Check if order is a service order
 */
export function isServiceOrder(
  orderTypeName: string | null | undefined,
): boolean {
  const normalized = normalizeOrderType(orderTypeName);
  return (
    normalized === ORDER_TYPES.SERVICE.toLowerCase() ||
    normalized === ORDER_TYPES.SERVICE_NO_SPACE.toLowerCase()
  );
}

/**
 * Helper function: Check if order is a normal order
 */
export function isNormalOrder(
  orderTypeName: string | null | undefined,
): boolean {
  const normalized = normalizeOrderType(orderTypeName);
  return (
    normalized === ORDER_TYPES.NORMAL.toLowerCase() ||
    normalized === ORDER_TYPES.NORMAL_WITH_SPACE.toLowerCase()
  );
}

/**
 * Type exports for better type safety
 */
export type OrderType = (typeof ORDER_TYPES)[keyof typeof ORDER_TYPES];
export type DocSourceType =
  (typeof DOC_SOURCE_TYPES)[keyof typeof DOC_SOURCE_TYPES];
export type ProductType = (typeof PRODUCT_TYPES)[keyof typeof PRODUCT_TYPES];
export type StatusCode = (typeof STATUS)[keyof typeof STATUS];
export type ActionCode = (typeof ACTION)[keyof typeof ACTION];
