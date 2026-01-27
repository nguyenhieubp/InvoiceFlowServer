/**
 * Mapper functions to convert Sale entities to Response DTOs
 * Chỉ map fields cần thiết, loại bỏ fields thừa
 */

import { Sale } from '../../../entities/sale.entity';
import {
  SaleItemResponseDto,
  ProductSummaryDto,
  DepartmentSummaryDto,
  StockTransferSummaryDto,
  OrderResponseDto,
  CustomerSummaryDto,
} from '../dto/sale-response.dto';

/**
 * Map Product to ProductSummaryDto
 * Chỉ lấy 4 fields thay vì 15 fields
 */
export function mapProductSummary(product: any): ProductSummaryDto | undefined {
  if (!product) return undefined;

  return {
    maVatTu: product.maVatTu,
    tenVatTu: product.tenVatTu,
    dvt: product.dvt,
    productType: product.productType,
  };
}

/**
 * Map Department to DepartmentSummaryDto
 * Chỉ lấy 3 fields thay vì 5 fields
 */
export function mapDepartmentSummary(
  department: any,
): DepartmentSummaryDto | undefined {
  if (!department) return undefined;

  return {
    ma_bp: department.ma_bp,
    ma_dvcs: department.ma_dvcs,
    type: department.type,
  };
}

/**
 * Map StockTransfer to StockTransferSummaryDto
 * Chỉ lấy 5 fields thay vì 20 fields
 */
export function mapStockTransferSummary(
  stockTransfer: any,
): StockTransferSummaryDto | undefined {
  if (!stockTransfer) return undefined;

  return {
    docCode: stockTransfer.docCode,
    transDate: stockTransfer.transDate,
    stockCode: stockTransfer.stockCode,
    qty: stockTransfer.qty,
    batchSerial: stockTransfer.batchSerial,
  };
}

/**
 * Map Customer to CustomerSummaryDto
 */
export function mapCustomerSummary(
  customer: any,
): CustomerSummaryDto | undefined {
  if (!customer) return undefined;

  return {
    code: customer.code,
    name: customer.name,
    mobile: customer.mobile,
    brand: customer.brand,
    grade_name: customer.grade_name,
    province_name: customer.province_name,
  };
}

/**
 * Map Sale to SaleItemResponseDto
 * Chỉ lấy ~50 fields thay vì ~150 fields
 */
export function mapToSaleItemResponse(sale: any): SaleItemResponseDto {
  const dto: SaleItemResponseDto = {
    // Core fields
    id: sale.id,
    docCode: sale.docCode,
    docDate: sale.docDate,
    branchCode: sale.branchCode,
    docSourceType: sale.docSourceType,
    maSerial: sale.stockTransfer?.batchSerial || sale.maSerial,

    // Item info
    itemCode: sale.itemCode,
    itemName: sale.itemName,
    dvt: sale.dvt,
    productType: sale.productType,

    // Quantities & amounts
    qty: sale.qty,
    giaBan: sale.giaBan,
    tienHang: sale.tienHang,
    revenue: sale.revenue,

    // Order info
    ordertypeName: sale.ordertypeName,
    ordertype: sale.ordertype,
    description: sale.description,
    partnerCode: sale.partnerCode,
    brand: sale.brand,
    type_sale: sale.type_sale,

    // Accounting
    maKho: sale.maKho,
    maLo: sale.maLo,
    tkChietKhau: sale.tkChietKhau,
    tkChiPhi: sale.tkChiPhi,
    maPhi: sale.maPhi,

    // Display fields (backend calculated)
    promCodeDisplay: sale.promCodeDisplay,
    promotionDisplayCode: sale.promotionDisplayCode,
    muaHangGiamGiaDisplay: sale.muaHangGiamGiaDisplay, // [NEW] Mã CTKM cho chiết khấu mua hàng NV
    cucThueDisplay: sale.cucThueDisplay,
    tkDoanhThuDisplay: sale.tkDoanhThuDisplay,
    tkGiaVonDisplay: sale.tkGiaVonDisplay,

    // Voucher/Payment display fields
    thanhToanCouponDisplay: sale.thanhToanCouponDisplay,
    chietKhauThanhToanCouponDisplay: sale.chietKhauThanhToanCouponDisplay,
    thanhToanVoucherDisplay: sale.thanhToanVoucherDisplay,
    thanhToanTkTienAoDisplay: sale.thanhToanTkTienAoDisplay,
    chietKhauThanhToanTkTienAoDisplay: sale.chietKhauThanhToanTkTienAoDisplay,

    // Promotion/Discount
    maCtkmTangHang: sale.maCtkmTangHang,
    voucherDp1: sale.voucherDp1,
    chietKhauVoucherDp1: sale.chietKhauVoucherDp1,
    maThe: sale.maThe,
    maCkTheoChinhSach: sale.maCkTheoChinhSach,
    // [FIX] Map VIP fields
    muaHangCkVip: sale.muaHangCkVip,
    chietKhauMuaHangCkVip: sale.chietKhauMuaHangCkVip,

    // Flags
    isProcessed: sale.isProcessed,
    statusAsys: sale.statusAsys,
    isTangHang: sale.isTangHang,
    isDichVu: sale.isDichVu,
    isStockTransferLine: sale.isStockTransferLine,

    // Additional
    saleperson_id: sale.saleperson_id,
    issuePartnerCode: sale.issuePartnerCode,
    loaiGd: sale.loaiGd,
    svcCode: sale.svcCode,
    maCa: sale.maCa,

    // Wholesale specific
    disc_tm: sale.disc_tm,
    disc_ctkm: sale.disc_ctkm,

    // Amounts (for calculations)
    disc_amt: sale.disc_amt,
    grade_discamt: sale.grade_discamt,
    other_discamt: sale.other_discamt,
    paid_by_voucher_ecode_ecoin_bp: sale.paid_by_voucher_ecode_ecoin_bp,

    // Relations (simplified)
    product: mapProductSummary(sale.product),
    department: mapDepartmentSummary(sale.department),
    stockTransfer: mapStockTransferSummary(sale.stockTransfer),
  };

  return dto;
}

/**
 * Map Order with Sales to OrderResponseDto
 */
export function mapToOrderResponse(order: any): OrderResponseDto {
  return {
    docCode: order.docCode,
    docDate: order.docDate,
    branchCode: order.branchCode,
    docSourceType: order.docSourceType,
    customer: mapCustomerSummary(order.customer) || {
      code: '',
      name: '',
    },
    totalRevenue: order.totalRevenue,
    totalQty: order.totalQty,
    totalItems: order.totalItems,
    isProcessed: order.isProcessed,
    sales: order.sales.map(mapToSaleItemResponse),
  };
}
