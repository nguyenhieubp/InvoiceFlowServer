import { Injectable, Logger } from '@nestjs/common';
import { Sale } from '../../../entities/sale.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { OrderFee } from '../../../entities/order-fee.entity';
import { CategoriesService } from '../../categories/categories.service';
import { LoyaltyService } from 'src/services/loyalty.service';
import * as SalesFormattingUtils from '../../../utils/sales-formatting.utils';
import * as SalesCalculationUtils from '../../../utils/sales-calculation.utils';

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
      const saleMaterialCode = loyaltyProduct?.materialCode;
      if (saleMaterialCode) {
        const stockTransferKey = `${sale.docCode}_${saleMaterialCode}`;
        saleStockTransfers = stockTransferMap.get(stockTransferKey) || [];
      }

      // Fallback to itemCode lookup
      if (saleStockTransfers.length === 0 && sale.itemCode) {
        const key = `${sale.docCode}_${sale.itemCode}`;
        saleStockTransfers = stockTransferMap.get(key) || [];
      }
    }

    // Calculate fields
    const calculatedFields = SalesCalculationUtils.calculateSaleFields(
      sale,
      loyaltyProduct,
      department,
      sale.branchCode,
    );

    // Get maKho from stock transfer if available
    if (includeStockTransfers && warehouseCodeMap && stockTransferMap) {
      const saleMaterialCode = loyaltyProduct?.materialCode;
      // This would need the getMaKhoFromStockTransfer method
      // For now, we'll keep the existing maKho from calculatedFields
    }

    // Get order context
    const order = orderMap?.get(sale.docCode);

    // Check if platform order
    const isPlatformOrder = orderFeeMap?.has(sale.docCode) || false;
    const platformBrand = orderFeeMap?.get(sale.docCode)?.brand;

    // Get employee status from pre-fetched map
    const isEmployee =
      isEmployeeMap?.get(sale.partnerCode) ||
      isEmployeeMap?.get((sale as any).issuePartnerCode) ||
      false;

    // Format using existing utility
    const enrichedSale = await SalesFormattingUtils.formatSaleForFrontend(
      sale,
      loyaltyProduct,
      department,
      calculatedFields,
      order,
      this.categoriesService,
      this.loyaltyService,
      saleStockTransfers,
      isPlatformOrder,
      platformBrand,
      isEmployee, // [API] Pre-fetched employee status
    );

    // Override svcCode with materialCode if available
    if (sale.svc_code && svcCodeMap) {
      const materialCode = svcCodeMap.get(sale.svc_code);
      if (materialCode) {
        enrichedSale.svcCode = materialCode;
      }
    }

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
