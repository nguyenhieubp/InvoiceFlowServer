/**
 * Response DTOs for Sales API
 * Chỉ trả về fields mà frontend thực sự cần
 * Giảm response size 60-70%
 */

/**
 * Product Summary DTO
 * Chỉ 4 fields thay vì 15 fields
 */
export class ProductSummaryDto {
  maVatTu: string;
  tenVatTu: string;
  dvt: string;
  productType: string;
}

/**
 * Department Summary DTO
 * Chỉ 3 fields thay vì 5 fields
 */
export class DepartmentSummaryDto {
  ma_bp: string;
  ma_dvcs: string;
  type: string;
}

/**
 * Stock Transfer Summary DTO
 * Chỉ 5 fields thay vì 20 fields
 */
export class StockTransferSummaryDto {
  docCode: string;
  transDate: Date;
  stockCode: string;
  qty: string;
  batchSerial: string;
}

/**
 * Customer Summary DTO
 */
export class CustomerSummaryDto {
  code: string;
  name: string;
  mobile?: string;
  brand?: string;
  grade_name?: string;
  province_name?: string;
}

/**
 * Sale Item Response DTO
 * Chỉ ~50 fields thay vì ~150 fields
 */
export class SaleItemResponseDto {
  // ========== CORE FIELDS ==========
  id: string;
  docCode: string;
  docDate: Date;
  branchCode: string;
  docSourceType: string;

  // ========== ITEM INFO ==========
  itemCode: string;
  itemName: string;
  dvt?: string;
  productType?: string;

  // ========== QUANTITIES & AMOUNTS ==========
  qty: number;
  giaBan?: number;
  tienHang?: number;
  revenue: number;

  // ========== ORDER INFO ==========
  ordertypeName?: string;
  ordertype?: string;
  description?: string;
  partnerCode?: string;
  brand?: string; // Nhãn hàng
  type_sale?: string; // Loại bán (WHOLESALE, RETAIL)

  // ========== ACCOUNTING ==========
  maKho?: string;
  maLo?: string;
  tkChietKhau?: string;
  tkChiPhi?: string;
  maPhi?: string;

  // ========== DISPLAY FIELDS (Backend calculated) ==========
  promCodeDisplay?: string | null;
  promotionDisplayCode?: string;
  cucThueDisplay?: string;
  tkDoanhThuDisplay?: string;
  tkGiaVonDisplay?: string;

  // Voucher/Payment Display Fields
  thanhToanCouponDisplay?: string | null;
  chietKhauThanhToanCouponDisplay?: number | null;
  thanhToanVoucherDisplay?: string | null;
  thanhToanTkTienAoDisplay?: string | null;
  chietKhauThanhToanTkTienAoDisplay?: number | null;
  soSerialDisplay?: string | null;

  // ========== PROMOTION/DISCOUNT ==========
  maCtkmTangHang?: string;
  voucherDp1?: string | null;
  chietKhauVoucherDp1?: number;
  maThe?: string;
  maCkTheoChinhSach?: string; // Mã CK theo chính sách (bán buôn)
  muaHangCkVip?: string; // [FIX] Add VIP classification
  chietKhauMuaHangCkVip?: number; // [FIX] Add VIP discount amount

  // ========== FLAGS ==========
  isProcessed: boolean;
  statusAsys: boolean;
  isTangHang?: boolean;
  isDichVu?: boolean;
  isStockTransferLine?: boolean;

  // ========== RELATIONS (Simplified) ==========
  product?: ProductSummaryDto;
  department?: DepartmentSummaryDto;
  stockTransfer?: StockTransferSummaryDto;

  // ========== ADDITIONAL (if needed) ==========
  saleperson_id?: number;
  issuePartnerCode?: string | null;
  loaiGd?: string;
  svcCode?: string | null;
  maCa?: string | null;

  // ========== WHOLESALE SPECIFIC ==========
  disc_tm?: number | string;
  disc_ctkm?: string;

  // ========== AMOUNTS (for calculations) ==========
  disc_amt?: number;
  grade_discamt?: number;
  other_discamt?: number | null;
  paid_by_voucher_ecode_ecoin_bp?: number;
}

/**
 * Order Response DTO
 */
export class OrderResponseDto {
  docCode: string;
  docDate: Date;
  branchCode: string;
  docSourceType: string;
  customer: CustomerSummaryDto;
  totalRevenue: number;
  totalQty: number;
  totalItems: number;
  isProcessed: boolean;
  sales: SaleItemResponseDto[];
}

/**
 * Sales List Response DTO
 */
export class SalesListResponseDto {
  data: OrderResponseDto[];
  total: number;
  page?: number;
  limit?: number;
  totalPages?: number;
}
