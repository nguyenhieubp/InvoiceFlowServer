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
  trackBatch?: boolean;
  trackSerial?: boolean;
  trackStocktake?: boolean;
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
  maSerial: string;

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

  // ========== STANDARDIZED DISCOUNT FIELDS (01-11) ==========
  ma_ck01?: string | null; // Mua hàng giảm giá
  ck01_nt?: number;

  ma_ck02?: string | null; // CK theo chính sách
  ck02_nt?: number;

  ma_ck03?: string | null; // Mua hàng CK VIP
  ck03_nt?: number;

  ma_ck04?: string | null; // Thanh toán coupon
  ck04_nt?: number;

  ma_ck05?: string | null; // Thanh toán Voucher
  ck05_nt?: number;

  ma_ck06?: string | null; // Dự phòng 1
  ck06_nt?: number;

  ma_ck07?: string | null; // Dự phòng 2
  ck07_nt?: number;

  ma_ck08?: string | null; // Dự phòng 3
  ck08_nt?: number;

  ma_ck09?: string | null; // Chiết khấu hãng
  ck09_nt?: number;

  ma_ck10?: string | null; // Thưởng bằng hàng
  ck10_nt?: number;

  ma_ck11?: string | null; // Thanh toán TK tiền ảo
  ck11_nt?: number;

  // ========== DISPLAY FIELDS ==========
  km_yn?: number; // [RENAME] Was promCodeDisplay. 0=No, 1=Yes
  cucThueDisplay?: string;
  tkDoanhThuDisplay?: string;
  tkGiaVonDisplay?: string;

  // ========== PROMOTION/DISCOUNT (Others) ==========
  maCtkmTangHang?: string;
  maThe?: string;

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
  // ma_ck02/ck02_nt moved to standardized block above

  // ========== AMOUNTS (for calculations) ==========
  disc_amt?: number;
  grade_discamt?: number;
  other_discamt?: number | null;
  paid_by_voucher_ecode_ecoin_bp?: number;
  ma_vt_ref?: string;
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
