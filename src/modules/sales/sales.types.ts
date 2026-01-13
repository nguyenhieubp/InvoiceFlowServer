/**
 * Sales Module Type Definitions
 * Thay thế các 'any' types bằng strong types
 */

export interface SaleFilters {
  brand?: string;
  isProcessed?: boolean;
  page?: number;
  limit?: number;
  date?: string;
  dateFrom?: string;
  dateTo?: string;
  search?: string;
  statusAsys?: boolean;
  export?: boolean;
  typeSale?: string;
}

export interface OrderSummary {
  docCode: string;
  docDate: Date;
  branchCode: string;
  docSourceType: string;
  customer: CustomerInfo | null;
  totalRevenue: number;
  totalQty: number;
  totalItems: number;
  isProcessed: boolean;
  sales: EnrichedSale[];
  stockTransfers?: StockTransferFormatted[];
  cashio?: CashioData;
  stockTransferInfo?: StockTransferSummary;
}

export interface CustomerInfo {
  code: string | null;
  brand: string | null;
  name: string | null;
  mobile: string | null;
}

export interface StockTransferSummary {
  totalItems: number;
  totalQty: number;
  uniqueItems?: number;
  stockCodes?: string[];
  hasStockTransfer?: boolean;
}

export interface CashioData {
  docCode: string;
  docDate: Date;
  branchCode: string;
  partnerCode: string;
  partnerName: string;
  totalAmount: number;
  paidAmount: number;
  reservedAmount: number;
  voucherAmount: number;
  paymentMethods: string;
}

export interface SyncResult {
  success: boolean;
  message: string;
  ordersCount: number;
  salesCount: number;
  customersCount: number;
  errors?: string[];
}

export interface WarehouseProcessResult {
  success: boolean;
  message: string;
  totalProcessed: number;
  successCount: number;
  failedCount: number;
  errors: string[];
}

export interface EnrichedSale {
  id: string;
  docCode: string;
  docDate: Date;
  itemCode: string;
  itemName: string;
  qty: number;
  revenue: number;
  materialCode?: string;
  productName?: string;
  maKho?: string;
  stockTransfers?: StockTransferFormatted[];
  [key: string]: any; // Cho các fields động khác
}

export interface StockTransferFormatted {
  id: string;
  docCode: string;
  soCode: string;
  itemCode: string;
  materialCode?: string;
  stockCode: string;
  qty: number;
  doctype: string;
  ioType: string;
  [key: string]: any;
}

export interface SyncErrorResult {
  total: number;
  success: number;
  failed: number;
  updated: Array<{
    id: string;
    docCode: string;
    itemCode: string;
    oldItemCode: string;
    newItemCode: string;
  }>;
}

export interface DateRange {
  startDate: Date;
  endDate: Date;
}

export interface PaginationResult<T> {
  data: T[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}
