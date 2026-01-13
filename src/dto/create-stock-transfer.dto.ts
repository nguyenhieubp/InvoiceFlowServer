export interface StockTransferItem {
  doctype: string;
  doccode: string;
  transdate: string;
  doc_desc: string;
  branch_code: string;
  brand_code: string;
  item_code: string;
  item_name: string;
  stock_code: string;
  related_stock_code: string;
  iotype: string; // 'O' = xuất, 'I' = nhập
  qty: number;
  batchserial: string | null;
  line_info1: string | null;
  line_info2: string | null;
  so_code: string | null; // Mã đơn hàng để join với orders
}

export interface CreateStockTransferDto {
  data: StockTransferItem[];
}
