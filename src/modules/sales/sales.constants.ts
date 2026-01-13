/**
 * Sales Module Constants
 * Tập trung tất cả các magic values để dễ bảo trì
 */

// Batch Processing Constants
export const BATCH_CONFIG = {
  DB_BATCH_SIZE: 500,
  PROCESS_BATCH_SIZE: 100,
  CONCURRENT_LIMIT: 10,
} as const;

// Date Format Patterns
export const DATE_PATTERNS = {
  DDMMMYYYY: /^(\d{2})([A-Z]{3})(\d{4})$/i,
} as const;

// Month Mapping
export const MONTH_MAP: Record<string, number> = {
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

// Company to Brand Mapping
export const COMPANY_BRAND_MAP: Record<string, string> = {
  F3: 'f3',
  FACIALBAR: 'f3',
  MENARD: 'menard',
  LABHAIR: 'labhair',
  YAMAN: 'yaman',
  CHANDO: 'chando',
};

// Payment Method Codes
export const PAYMENT_METHODS = {
  ECOIN: 'ECOIN',
  VOUCHER: 'VOUCHER',
} as const;

// Stock Transfer Types
export const STOCK_TRANSFER_TYPES = {
  SALE_STOCKOUT: 'SALE_STOCKOUT',
  STOCK_TRANSFER: 'STOCK_TRANSFER',
  RETURN: 'RETURN',
} as const;

// IO Types
export const IO_TYPES = {
  TRANSFER: 'T',
  IN: 'I',
  OUT: 'O',
} as const;

// Sale Types
export const SALE_TYPES = {
  RETAIL: 'RETAIL',
  WHOLESALE: 'WHOLESALE',
  ALL: 'ALL',
} as const;

// Special Item Codes to Skip
export const SKIP_ITEM_CODES = ['TRUTONKEEP'];

// Default Pagination
export const DEFAULT_PAGINATION = {
  PAGE: 1,
  LIMIT: 50,
  STATUS_ASYS_LIMIT: 10,
} as const;

// Date Range Defaults
export const DATE_RANGE_DEFAULTS = {
  DAYS_BACK: 30,
} as const;
