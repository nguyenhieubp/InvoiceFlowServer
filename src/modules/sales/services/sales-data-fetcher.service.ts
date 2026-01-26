import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { Sale } from '../../../entities/sale.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { OrderFee } from '../../../entities/order-fee.entity';
import { LoyaltyService } from 'src/services/loyalty.service';
import { CategoriesService } from '../../categories/categories.service';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';

/**
 * RelatedData - All related data fetched for sales
 */
export interface RelatedData {
  loyaltyProductMap: Map<string, any>;
  departmentMap: Map<string, any>;
  warehouseCodeMap: Map<string, string>;
  stockTransfers: StockTransfer[];
  stockTransferMap: Map<string, StockTransfer[]>;
  stockTransferByDocCodeMap: Map<string, StockTransfer[]>;
  orderFeeMap: Map<string, OrderFee>;
  svcCodeMap: Map<string, string>;
}

/**
 * SalesDataFetcherService
 * Centralized service for fetching all related data for sales
 * Optimizes data fetching with parallel requests and proper batching
 */
@Injectable()
export class SalesDataFetcherService {
  private readonly logger = new Logger(SalesDataFetcherService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,
    private loyaltyService: LoyaltyService,
    private categoriesService: CategoriesService,
  ) {}

  /**
   * Fetch all related data for sales in parallel
   * @param sales - Array of sales
   * @param options - Fetch options
   * @returns All related data
   */
  async fetchRelatedData(
    sales: Sale[],
    options: {
      includeStockTransfers?: boolean;
      includeOrderFees?: boolean;
      includeSvcCodeMapping?: boolean;
    } = {},
  ): Promise<RelatedData> {
    const {
      includeStockTransfers = true,
      includeOrderFees = true,
      includeSvcCodeMapping = true,
    } = options;

    // Extract unique codes
    const itemCodes = this.extractItemCodes(sales);
    const branchCodes = this.extractBranchCodes(sales);
    const docCodes = this.extractDocCodes(sales);
    const svcCodes = includeSvcCodeMapping ? this.extractSvcCodes(sales) : [];

    // Prepare stock transfer doc codes
    const docCodesForStockTransfer = includeStockTransfers
      ? StockTransferUtils.getDocCodesForStockTransfer(docCodes)
      : [];

    // Fetch all data in parallel
    const startTime = Date.now();

    const [
      loyaltyProductMap,
      departmentMap,
      warehouseCodeMap,
      stockTransfers,
      orderFees,
      svcCodeMap,
    ] = await Promise.all([
      // Always fetch products and departments
      this.loyaltyService.fetchProducts(itemCodes),
      this.loyaltyService.fetchLoyaltyDepartments(branchCodes),
      this.categoriesService.getWarehouseCodeMap(),

      // Conditionally fetch stock transfers
      includeStockTransfers && docCodesForStockTransfer.length > 0
        ? this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          })
        : Promise.resolve([]),

      // Conditionally fetch order fees
      includeOrderFees && docCodes.length > 0
        ? this.orderFeeRepository.find({
            where: { erpOrderCode: In(docCodes) },
          })
        : Promise.resolve([]),

      // Conditionally fetch svc code mappings
      includeSvcCodeMapping && svcCodes.length > 0
        ? this.loyaltyService.fetchMaterialCodesBySvcCodes(svcCodes)
        : Promise.resolve(new Map<string, string>()),
    ]);

    this.logger.log(
      `[fetchRelatedData] Fetched data for ${sales.length} sales in ${Date.now() - startTime}ms`,
    );

    // Build stock transfer maps
    const { stockTransferMap, stockTransferByDocCodeMap } =
      includeStockTransfers
        ? StockTransferUtils.buildStockTransferMaps(
            stockTransfers,
            loyaltyProductMap,
            docCodes,
          )
        : {
            stockTransferMap: new Map<string, StockTransfer[]>(),
            stockTransferByDocCodeMap: new Map<string, StockTransfer[]>(),
          };

    // Build order fee map
    const orderFeeMap = new Map<string, OrderFee>();
    orderFees.forEach((fee) => {
      orderFeeMap.set(fee.erpOrderCode, fee);
    });

    // Add stock transfer item codes to loyalty product fetch
    if (includeStockTransfers && stockTransfers.length > 0) {
      const stockTransferItemCodes =
        this.extractStockTransferItemCodes(stockTransfers);
      const additionalItemCodes = stockTransferItemCodes.filter(
        (code) => !loyaltyProductMap.has(code),
      );

      if (additionalItemCodes.length > 0) {
        const additionalProducts =
          await this.loyaltyService.fetchProducts(additionalItemCodes);
        additionalProducts.forEach((product, code) => {
          loyaltyProductMap.set(code, product);
        });
      }
    }

    return {
      loyaltyProductMap,
      departmentMap,
      warehouseCodeMap,
      stockTransfers,
      stockTransferMap,
      stockTransferByDocCodeMap,
      orderFeeMap,
      svcCodeMap,
    };
  }

  /**
   * Extract unique item codes from sales
   */
  private extractItemCodes(sales: Sale[]): string[] {
    return Array.from(
      new Set(
        sales
          .filter((sale) => sale.statusAsys !== false)
          .map((sale) => sale.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
  }

  /**
   * Extract unique branch codes from sales
   */
  private extractBranchCodes(sales: Sale[]): string[] {
    return Array.from(
      new Set(
        sales
          .map((sale) => sale.branchCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
  }

  /**
   * Extract unique doc codes from sales
   */
  private extractDocCodes(sales: Sale[]): string[] {
    return Array.from(
      new Set(
        sales
          .map((sale) => sale.docCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
  }

  /**
   * Extract unique svc codes from sales
   */
  private extractSvcCodes(sales: Sale[]): string[] {
    return Array.from(
      new Set(
        sales
          .map((sale) => sale.svc_code)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
  }

  /**
   * Extract unique item codes from stock transfers
   */
  private extractStockTransferItemCodes(
    stockTransfers: StockTransfer[],
  ): string[] {
    return Array.from(
      new Set(
        stockTransfers
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
  }

  /**
   * Enrich sales with platform voucher data
   * Adds VC CTKM SÀN voucher information from order fees
   */
  enrichSalesWithPlatformVouchers(
    sales: Sale[],
    orderFeeMap: Map<string, OrderFee>,
  ): void {
    sales.forEach((sale) => {
      const orderFee = orderFeeMap.get(sale.docCode);
      if (orderFee?.rawData?.raw_data?.voucher_from_seller) {
        const voucherAmount = Number(
          orderFee.rawData.raw_data.voucher_from_seller || 0,
        );
        if (voucherAmount > 0) {
          sale.voucherDp1 = 'VC CTKM SÀN';
          sale.chietKhauVoucherDp1 = voucherAmount;
        }
      }
    });
  }
}
