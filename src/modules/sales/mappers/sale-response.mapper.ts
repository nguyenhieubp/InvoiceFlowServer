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
import { InvoiceLogicUtils } from '../../../utils/invoice-logic.utils';

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
    trackBatch: product.trackBatch,
    trackSerial: product.trackSerial,
    trackStocktake: product.trackStocktake,
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
    maSerial: sale.product?.trackBatch
      ? ''
      : sale.maSerial || sale.stockTransfer?.batchSerial || '',

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
    km_yn: sale.km_yn,
    cucThueDisplay: sale.cucThueDisplay,
    tkDoanhThuDisplay: sale.tkDoanhThuDisplay,
    tkGiaVonDisplay: sale.tkGiaVonDisplay,
    // muaHangGiamGiaDisplay - REMOVED
    // thanhToanCouponDisplay - REMOVED
    // ...

    // Promotion/Discount
    maCtkmTangHang: sale.maCtkmTangHang,
    maThe: sale.maThe,

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

    // Standardized Discount Fields (01-11)
    ma_ck01: sale.maCk01,
    ck01_nt: sale.ck01Nt,
    ma_ck02: sale.maCk02,
    ck02_nt: sale.ck02Nt,
    ma_ck03:
      sale.ck03Nt > 0
        ? sale.maCk03 ||
          InvoiceLogicUtils.resolveMaCk03({
            brand: sale.brand,
            productType: sale.productType,
          }) ||
          sale.muaHangCkVip
        : null,
    ck03_nt: sale.ck03Nt,
    ma_ck04: sale.maCk04,
    ck04_nt: sale.ck04Nt,
    ma_ck05: sale.maCk05,
    ck05_nt: sale.ck05Nt,
    ma_ck06: sale.maCk06,
    ck06_nt: sale.ck06Nt,
    ma_ck07: sale.maCk07,
    ck07_nt: sale.ck07Nt,
    ma_ck08: sale.maCk08,
    ck08_nt: sale.ck08Nt,
    ma_ck09: sale.maCk09,
    ck09_nt: sale.ck09Nt,
    ma_ck10: sale.maCk10,
    ck10_nt: sale.ck10Nt,
    ma_ck11: sale.maCk11,
    ck11_nt: sale.ck11Nt,

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
