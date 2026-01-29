import { Injectable, Logger } from '@nestjs/common';
import { Sale } from '../../../entities/sale.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { OrderFee } from '../../../entities/order-fee.entity';
import { CategoriesService } from '../../categories/categories.service';
import { LoyaltyService } from 'src/services/loyalty.service';
import { InvoiceLogicUtils } from '../../../utils/invoice-logic.utils';

/**
 * FormattingContext - All data needed for formatting sales
 */
export interface FormattingContext {
  loyaltyProductMap: Map<string, any>;
  departmentMap: Map<string, any>;
  stockTransferMap?: Map<string, StockTransfer[]>;
  orderFeeMap?: Map<string, OrderFee>;
  warehouseCodeMap?: Map<string, string>;
  svcCodeMap?: Map<string, string>;
  getMaTheMap?: Map<string, string>;
  orderMap?: Map<string, any>;
  isEmployeeMap?: Map<string, boolean>; // [NEW] Pre-fetched employee status
  includeStockTransfers?: boolean;
}

/**
 * SalesFormattingService
 * Centralized service for formatting sales data
 * Eliminates code duplication across multiple endpoints
 */
@Injectable()
export class SalesFormattingService {
  private readonly logger = new Logger(SalesFormattingService.name);

  constructor(
    private categoriesService: CategoriesService,
    private loyaltyService: LoyaltyService,
  ) {}

  /**
   * Format multiple sales in parallel
   * @param sales - Array of sales to format
   * @param context - Formatting context with all required data
   * @returns Formatted sales array
   */
  async formatSales(sales: Sale[], context: FormattingContext): Promise<any[]> {
    const formatPromises = sales.map((sale) =>
      this.formatSingleSale(sale, context),
    );

    return Promise.all(formatPromises);
  }

  /**
   * Format a single sale with all enrichment data
   * @param sale - Sale to format
   * @param context - Formatting context
   * @returns Formatted sale object
   */
  async formatSingleSale(sale: Sale, context: FormattingContext): Promise<any> {
    const {
      loyaltyProductMap,
      departmentMap,
      stockTransferMap,
      orderFeeMap,
      warehouseCodeMap,
      svcCodeMap,
      getMaTheMap,
      orderMap,
      isEmployeeMap,
      includeStockTransfers = false,
    } = context;

    // Get loyalty product
    const loyaltyProduct = sale.itemCode
      ? loyaltyProductMap.get(sale.itemCode)
      : null;

    // Get department
    const department = sale.branchCode
      ? departmentMap.get(sale.branchCode) || null
      : null;

    // Debug: Log when department is missing
    if (!department && sale.branchCode) {
      this.logger.warn(
        `[formatSingleSale] Department not found for branchCode: ${sale.branchCode}, docCode: ${sale.docCode}`,
      );
    }

    // Get maThe if available
    const maThe = getMaTheMap?.get(loyaltyProduct?.materialCode || '') || '';
    if (maThe) {
      sale.maThe = maThe;
    }

    // Get stock transfers if needed
    let saleStockTransfers: StockTransfer[] = [];
    if (includeStockTransfers && stockTransferMap) {
      if ((sale as any).stockTransfers) {
        saleStockTransfers = (sale as any).stockTransfers;
      } else {
        const key = `${sale.docCode}_${sale.itemCode}`;
        saleStockTransfers = stockTransferMap.get(key) || [];
      }
    }

    // Get employee status from pre-fetched map (moved up)
    const isEmployee =
      isEmployeeMap?.get(sale.partnerCode) ||
      isEmployeeMap?.get((sale as any).issuePartnerCode) ||
      false;

    // [NEW] Resolve Batch/Serial from various sources (matches SalesPayloadService)
    let batchSerial: string | null = null;
    if (saleStockTransfers.length > 0) {
      batchSerial = saleStockTransfers[0].batchSerial || null;
    }
    if (!batchSerial) {
      batchSerial =
        (sale as any).ma_vt_ref || sale.serial || sale.soSerial || null;
    }

    // Calculate fields using InvoiceLogicUtils (Unified Logic)
    const calculatedFields = await InvoiceLogicUtils.calculateSaleFields(
      sale,
      loyaltyProduct,
      department,
      sale.branchCode,
      this.loyaltyService, // Pass loyaltyService for Wholesale accounts lookup
      isEmployee,
      batchSerial, // [NEW] Pass resolved batchSerial
    );

    // [FIX] Restore variables for mapping
    const isPlatformOrder = orderFeeMap?.has(sale.docCode) || false;
    const platformBrand = orderFeeMap?.get(sale.docCode)?.brand;

    // Resolve Ma Kho for display (if needed) using Category Service map
    // Priority: Assigned ST > Calculated > Sale.maKho
    let maKhoDisplay = calculatedFields.maKho;

    // If we have an assigned ST, use its stockCode logic
    if (saleStockTransfers.length > 0) {
      // Usually the first one or the one with negative qty determines the warehouse source
      const st = saleStockTransfers[0];
      if (st.stockCode) {
        maKhoDisplay = st.stockCode;
      }
    }

    if (warehouseCodeMap && maKhoDisplay) {
      maKhoDisplay = warehouseCodeMap.get(maKhoDisplay) || maKhoDisplay;
    }

    // Override values in sale object for display consistency if needed
    // But better to return a clean object.
    const enrichedSale = {
      ...(sale as any),
      // Overwrite/Add fields from Logic Utils
      giaBan: calculatedFields.giaBan,
      tienHang: calculatedFields.tienHang,
      tienHangGoc: calculatedFields.tienHangGoc, // Add this if FE uses it
      tkChietKhau: calculatedFields.tkChietKhau,
      tkChiPhi: calculatedFields.tkChiPhi,
      maPhi: calculatedFields.maPhi,
      maLo: calculatedFields.maLo,
      soSerial: calculatedFields.soSerial,
      maKho: maKhoDisplay,
      maCtkmTangHang: calculatedFields.maCtkmTangHang,

      // [RESTORE] Cuc Thue Display
      cucThueDisplay:
        sale.cucThue || department?.ma_dvcs || department?.ma_dvcs_ht || null,

      // [RESTORE] Account Displays
      tkDoanhThuDisplay:
        loyaltyProduct?.tkDoanhThuBanLe ||
        loyaltyProduct?.tkDoanhThuBanBuon ||
        '-',
      tkGiaVonDisplay:
        loyaltyProduct?.tkGiaVonBanLe || loyaltyProduct?.tkGiaVonBanBuon || '-',

      // [STANDARDIZED] Discount Fields Enriched
      maCk01: calculatedFields.maCk01 || null,
      ck01Nt: calculatedFields.maCk01
        ? Number(sale.other_discamt || 0) > 0
          ? Number(sale.other_discamt)
          : Number(sale.chietKhauMuaHangGiamGia || 0)
        : 0,

      maCk02: calculatedFields.maCk02 || null,
      ck02Nt: calculatedFields.ck02Nt || 0,

      maCk03: sale.muaHangCkVip || null,
      ck03Nt: Number(sale.grade_discamt || sale.chietKhauMuaHangCkVip || 0),

      maCk04: (sale as any).maCk04 || null,
      ck04Nt: Number((sale as any).chietKhauThanhToanCoupon || 0),

      maCk05:
        Number(
          sale.paid_by_voucher_ecode_ecoin_bp ||
            (sale as any).chietKhauThanhToanVoucher ||
            0,
        ) > 0
          ? InvoiceLogicUtils.resolveVoucherCode({
              sale: {
                ...sale,
                customer:
                  sale.customer || orderMap?.get(sale.docCode)?.customer,
              },
              customer: sale.customer || orderMap?.get(sale.docCode)?.customer,
              brand: platformBrand || sale.brand || '',
            }) ||
            (sale as any).maCk05 ||
            null
          : null,
      ck05Nt: Number(
        sale.paid_by_voucher_ecode_ecoin_bp ||
          (sale as any).chietKhauThanhToanVoucher ||
          0,
      ),

      maCk06: sale.voucherDp1 || null,
      ck06Nt: Number(sale.chietKhauVoucherDp1 || 0),

      maCk07: (sale as any).voucherDp2 || null,
      ck07Nt: Number((sale as any).chietKhauVoucherDp2 || 0),

      maCk08: (sale as any).voucherDp3 || null,
      ck08Nt: Number((sale as any).chietKhauVoucherDp3 || 0),

      maCk09: (sale as any).hang || null,
      ck09Nt: Number((sale as any).chietKhauHang || 0),

      maCk10: (sale as any).thuongBangHang || null,
      ck10Nt: Number((sale as any).chietKhauThuongMuaBangHang || 0),

      maCk11: (sale as any).maCk11 || null,
      ck11Nt: Number((sale as any).chietKhauThanhToanTkTienAo || 0),

      // [RESTORE] Mua Hang Giam Gia Display (Corresponds to ma_ck01 in Fast API) -- Keeping for backward compat if needed, but redundant
      muaHangGiamGiaDisplay: calculatedFields.maCk01 || null,

      // [UPDATE] User Request: promCodeDisplay renamed to km_yn
      km_yn: (sale as any).km_yn ?? 0,

      // [UPDATE] Remove redundant display fields as requested
      // promotionDisplayCode: ...,
      // muaHangGiamGiaDisplay: ...,

      // Product Info
      tenVatTu:
        loyaltyProduct?.name ||
        (sale as any).product?.tenVatTu ||
        sale.itemName,
      dvt: loyaltyProduct?.unit || (sale as any).product?.dvt || sale.dvt,

      // [FIX] Explicitly Map maVatTu (Source of Truth)
      maVatTu: InvoiceLogicUtils.resolveInvoiceMaterial(sale, loyaltyProduct)
        .maVt,
      itemName: InvoiceLogicUtils.resolveInvoiceMaterial(sale, loyaltyProduct)
        .tenVt,

      // [FIX] Explicitly Map Department Object
      department: department
        ? {
            ma_bp: department.ma_bp || null,
            branchcode: department.branchcode || null,
            ma_dvcs: department.ma_dvcs || null,
            ma_dvcs_ht: department.ma_dvcs_ht || null,
            type: department.type || null,
          }
        : null,

      // [FIX] Enriched Product Object
      product: loyaltyProduct
        ? {
            productType:
              loyaltyProduct.productType || loyaltyProduct.producttype,
            dvt: loyaltyProduct.unit || null,
            maVatTu: loyaltyProduct.materialCode || sale.itemCode,
            tenVatTu: loyaltyProduct.name || null,
            trackInventory: loyaltyProduct.trackInventory ?? null,
            trackSerial: !!loyaltyProduct.trackSerial,
            trackBatch: !!loyaltyProduct.trackBatch,
            trackStocktake: !!loyaltyProduct.trackStocktake,
            tkChietKhau: loyaltyProduct.tkChietKhau || null,
            tkDoanhThuBanLe: loyaltyProduct.tkDoanhThuBanLe || null,
            tkDoanhThuBanBuon: loyaltyProduct.tkDoanhThuBanBuon || null,
            tkGiaVonBanLe: loyaltyProduct.tkGiaVonBanLe || null,
            tkGiaVonBanBuon: loyaltyProduct.tkGiaVonBanBuon || null,
          }
        : (sale as any).product,

      // Platform Info
      isPlatformOrder,
      platformBrand,
      isEmployee,

      // Stock Transfers Attached
      stockTransfers: saleStockTransfers,
    };

    // Override svcCode with materialCode if available
    if (sale.svc_code && svcCodeMap) {
      const materialCode = svcCodeMap.get(sale.svc_code);
      if (materialCode) {
        enrichedSale.svc_code = materialCode;
      }
    }

    // [Compatibility] Re-add properties expected by SalesFormattingUtils if we removed it
    // Or just ensure the FE consumes these new fields correctly.
    // It seems formatSaleForFrontend did a lot of field renaming.
    // To be perfectly safe, we should inline the logic of formatSaleForFrontend but adapted to new Utils.

    return enrichedSale;
  }

  /**
   * Build formatting context from raw data
   * Helper method to prepare all required maps and lookups
   */
  buildContext(options: {
    loyaltyProductMap: Map<string, any>;
    departmentMap: Map<string, any>;
    stockTransferMap?: Map<string, StockTransfer[]>;
    orderFeeMap?: Map<string, OrderFee>;
    warehouseCodeMap?: Map<string, string>;
    svcCodeMap?: Map<string, string>;
    getMaTheMap?: Map<string, string>;
    orderMap?: Map<string, any>;
    isEmployeeMap?: Map<string, boolean>;
    includeStockTransfers?: boolean;
  }): FormattingContext {
    return {
      loyaltyProductMap: options.loyaltyProductMap,
      departmentMap: options.departmentMap,
      stockTransferMap: options.stockTransferMap,
      orderFeeMap: options.orderFeeMap,
      warehouseCodeMap: options.warehouseCodeMap,
      svcCodeMap: options.svcCodeMap,
      getMaTheMap: options.getMaTheMap,
      orderMap: options.orderMap,
      isEmployeeMap: options.isEmployeeMap,
      includeStockTransfers: options.includeStockTransfers ?? false,
    };
  }
}
