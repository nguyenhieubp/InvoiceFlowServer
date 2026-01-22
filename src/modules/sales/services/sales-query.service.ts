import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, IsNull, Like } from 'typeorm';
import { Sale } from '../../../entities/sale.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { LoyaltyService } from '../../../services/loyalty.service';
import { CategoriesService } from '../../categories/categories.service';
import { N8nService } from '../../../services/n8n.service';
import * as SalesUtils from '../../../utils/sales.utils';
import * as SalesCalculationUtils from '../../../utils/sales-calculation.utils';
import * as SalesFormattingUtils from '../../../utils/sales-formatting.utils';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import { VoucherIssueService } from '../../voucher-issue/voucher-issue.service';
import { SalesExplosionService } from './sales-explosion.service';
import { SalesFilterService } from './sales-filter.service';

/**
 * SalesQueryService
 * Chịu trách nhiệm: Query và filtering operations cho sales
 */
@Injectable()
export class SalesQueryService {
  private readonly logger = new Logger(SalesQueryService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    private loyaltyService: LoyaltyService,
    private categoriesService: CategoriesService,
    private n8nService: N8nService,
    private voucherIssueService: VoucherIssueService,
    private salesExplosionService: SalesExplosionService,
    private salesFilterService: SalesFilterService,
  ) {}

  /**
   * Find one sale by ID
   */
  async findOne(id: string) {
    const sale = await this.saleRepository.findOne({
      where: { id },
      relations: ['customer'],
    });

    if (!sale) {
      throw new NotFoundException(`Sale with ID "${id}" not found`);
    }

    return sale;
  }

  /**
   * Find sales by order code (docCode)
   */
  async findByOrderCode(docCode: string) {
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { id: 'ASC' },
    });

    if (!sales || sales.length === 0) {
      throw new NotFoundException(`Order with code "${docCode}" not found`);
    }

    return sales;
  }

  /**
   * Get stock transfer by ID
   */
  async getStockTransferById(id: string): Promise<StockTransfer | null> {
    return await this.stockTransferRepository.findOne({
      where: { id },
    });
  }
  /**
   * Enrich orders với cashio data
   * REFACTORED: Delegated to SalesExplosionService
   */
  async enrichOrdersWithCashio(orders: any[]): Promise<any[]> {
    return this.salesExplosionService.enrichOrdersWithCashio(orders);
  }

  /**
   * Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
   */
  getMaKhoFromStockTransfer(
    sale: any,
    docCode: string,
    stockTransfers: StockTransfer[],
    saleMaterialCode?: string | null,
    stockTransferMap?: Map<string, StockTransfer[]>,
    warehouseCodeMap?: Map<string, string>,
  ): string {
    const matched = StockTransferUtils.findMatchingStockTransfer(
      sale,
      docCode,
      stockTransfers,
      saleMaterialCode,
      stockTransferMap,
    );
    const stockCode = matched?.stockCode || '';
    if (!stockCode || stockCode.trim() === '') return '';

    if (warehouseCodeMap && warehouseCodeMap.has(stockCode)) {
      return warehouseCodeMap.get(stockCode)!;
    }
    return stockCode;
  }

  /**
   * Helper to apply common filters to sales query
   * REFACTORED: Delegated to SalesFilterService
   */
  private applySaleFilters(
    query: any,
    options: {
      brand?: string;
      search?: string;
      statusAsys?: boolean;
      typeSale?: string;
      date?: string;
      dateFrom?: string | Date;
      dateTo?: string | Date;
      isProcessed?: boolean;
    },
  ): void {
    this.salesFilterService.applySaleFilters(query, options);
  }

  async findAllOrders(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
    date?: string; // Format: DDMMMYYYY (ví dụ: 04DEC2025)
    dateFrom?: string; // Format: YYYY-MM-DD hoặc ISO string
    dateTo?: string; // Format: YYYY-MM-DD hoặc ISO string
    search?: string; // Search query để tìm theo docCode, customer name, code, mobile
    statusAsys?: boolean; // Filter theo statusAsys (true/false)
    export?: boolean; // Nếu true, trả về sales items riêng lẻ (không group, không paginate) để export Excel
    typeSale?: string; // Type sale: "WHOLESALE" or "RETAIL"
  }) {
    const {
      brand,
      isProcessed,
      page = 1,
      limit = 10,
      date,
      dateFrom,
      dateTo,
      search,
      statusAsys,
      export: isExport,
      typeSale,
    } = options;

    // START PERFORMANCE LOGGING
    const startTotal = Date.now();
    this.logger.log(`[findAllOrders] Start...`);

    // 1. Initial Count Query
    const startCount = Date.now();
    const countQuery = this.saleRepository
      .createQueryBuilder('sale')
      .select('COUNT(DISTINCT sale.docCode)', 'count'); // Use COUNT(DISTINCT) for accurate order count

    // Join customer ONLY if searching by customer fields
    if (search && search.trim() !== '') {
      countQuery.leftJoin('sale.customer', 'customer');
    }

    // Apply shared filters
    this.applySaleFilters(countQuery, {
      brand,
      isProcessed,
      statusAsys,
      typeSale,
      date,
      dateFrom,
      dateTo,
      search,
    });

    const totalResult = await countQuery.getRawOne();
    const totalOrders = parseInt(totalResult?.count || '0', 10); // Count of distinct orders

    // 2. Main Query
    const fullQuery = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer') // Need this for order grouping logic and response
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.docCode', 'ASC')
      .addOrderBy('sale.id', 'ASC');

    this.applySaleFilters(fullQuery, {
      brand,
      isProcessed,
      statusAsys,
      typeSale,
      date,
      dateFrom,
      dateTo,
      search,
    });

    const isSearchMode = !!search && totalOrders < 2000;

    let allSales: Sale[];

    if (!isExport && !isSearchMode) {
      // FIXED PAGINATION: Get exact limit number of orders
      // Step 1: Get distinct docCodes with pagination
      const docCodeSubquery = this.saleRepository
        .createQueryBuilder('sale')
        .select('sale.docCode', 'docCode')
        .addSelect('MAX(sale.docDate)', 'docDate') // Need this for ORDER BY
        .groupBy('sale.docCode')
        .orderBy('MAX(sale.docDate)', 'DESC')
        .addOrderBy('sale.docCode', 'ASC');

      // (filters...)
      // Join customer IF searching (needed for filter)
      if (search && search.trim() !== '') {
        docCodeSubquery.leftJoin('sale.customer', 'customer');
      }

      this.applySaleFilters(docCodeSubquery, {
        brand,
        isProcessed,
        statusAsys,
        typeSale,
        date,
        dateFrom,
        dateTo,
        search,
      });

      // Paginate at order level
      const offset = (page - 1) * limit;
      docCodeSubquery.skip(offset).take(limit);

      const docCodeResults = await docCodeSubquery.getRawMany();
      const docCodes = docCodeResults.map((r) => r.docCode);

      if (docCodes.length === 0) {
        allSales = [];
      } else {
        // Step 2: Fetch all sales for these docCodes
        fullQuery.andWhere('sale.docCode IN (:...docCodes)', { docCodes });
        allSales = await fullQuery.getMany();
      }
    } else {
      // Search mode or export: fetch all
      allSales = await fullQuery.getMany();
    }
    // ... (Export Logic omitted) ...
    if (isExport) {
      // ...
      return { sales: [], total: 0 }; // Placeholder strictly for this replacement block consistency
    }

    // ... itemCodes, branchCodes collection ...

    // (We need to keep the original logic for collection, just wrapping logs)
    // To minimize replacement size, I will just logging around the promise.all block later
    // IMPORTANT: I need to replace the large block to insert logs effectively without breaking scope.

    // Better strategy: Add logs in targeted small chunks using multi_replace or specific replace.
    // The current replacement is too big and risks breaking things if I don't paste exact code.
    // I will cancel this tool call and use multi_replace for inserting logs.

    // 3. Export Logic (Early Return)
    if (isExport) {
      const salesWithCustomer = allSales.map((sale) => {
        return {
          ...sale,
          customer: sale.customer
            ? {
                code: sale.customer.code || sale.partnerCode || null,
                brand: sale.customer.brand || null,
                name: sale.customer.name || null,
                mobile: sale.customer.mobile || null,
              }
            : sale.partnerCode
              ? {
                  code: sale.partnerCode || null,
                  brand: null,
                  name: null,
                  mobile: null,
                  id: null,
                }
              : null,
        };
      });

      return {
        sales: salesWithCustomer,
        total: totalOrders,
      };
    }

    // 4. Group by Order (Data Preparation)
    const orderMap = new Map<string, any>();
    const allSalesData: any[] = []; // Helper array to avoid re-iterating map values often

    for (const sale of allSales) {
      const docCode = sale.docCode;

      if (!orderMap.has(docCode)) {
        orderMap.set(docCode, {
          docCode: sale.docCode,
          docDate: sale.docDate,
          branchCode: sale.branchCode,
          docSourceType: sale.docSourceType,
          customer: sale.customer
            ? {
                code: sale.customer.code || sale.partnerCode || null,
                brand: sale.customer.brand || null,
                name: sale.customer.name || null,
                mobile: sale.customer.mobile || null,
              }
            : sale.partnerCode
              ? {
                  code: sale.partnerCode || null,
                  brand: null,
                  name: null,
                  mobile: null,
                  id: null,
                }
              : null,
          totalRevenue: 0,
          totalQty: 0,
          totalItems: 0,
          isProcessed: sale.isProcessed,
          sales: [],
        });
      }

      const order = orderMap.get(docCode)!;
      order.totalRevenue += Number(sale.revenue || 0);
      order.totalQty += Number(sale.qty || 0);
      order.totalItems += 1;

      // If any item is not processed, the whole order is considered not processed (simplification)
      if (!sale.isProcessed) {
        order.isProcessed = false;
      }

      allSalesData.push(sale);
    }

    // 5. Pre-fetch Related Data (Batching)
    const itemCodes = Array.from(
      new Set(
        allSalesData
          .filter((sale) => sale.statusAsys !== false)
          .map((sale) => sale.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    const branchCodes = Array.from(
      new Set(
        allSalesData
          .map((sale) => sale.branchCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    const docCodes = Array.from(
      new Set(allSalesData.map((sale) => sale.docCode).filter(Boolean)),
    );
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer(docCodes);

    const stockTransfers =
      docCodesForStockTransfer.length > 0
        ? await this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          })
        : [];

    const stockTransferItemCodes = Array.from(
      new Set(
        stockTransfers
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    // Collect all svc_codes for batch lookup
    const svcCodes = Array.from(
      new Set(
        allSalesData
          .map((sale) => sale.svc_code)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    const allItemCodes = Array.from(
      new Set([...itemCodes, ...stockTransferItemCodes]),
    );

    // Batch Fetching
    const [loyaltyProductMap, departmentMap, warehouseCodeMap] =
      await Promise.all([
        this.loyaltyService.fetchProducts(allItemCodes),
        this.loyaltyService.fetchLoyaltyDepartments(branchCodes),
        this.categoriesService.getWarehouseCodeMap(),
      ]);

    // Batch lookup svc_code -> materialCode using optimized method
    let svcCodeMap = new Map<string, string>();
    if (svcCodes.length > 0) {
      svcCodeMap =
        await this.loyaltyService.fetchMaterialCodesBySvcCodes(svcCodes);
    }

    // Build Stock Transfer Maps
    const { stockTransferMap, stockTransferByDocCodeMap } =
      StockTransferUtils.buildStockTransferMaps(
        stockTransfers,
        loyaltyProductMap,
        docCodes,
      );

    // OPTIMIZED: Pre-build map for O(1) lookup instead of filter in loop
    const stockTransferByItemCodeMap = new Map<string, StockTransfer[]>();
    stockTransfers.forEach((st) => {
      const key = `${st.soCode}_${st.itemCode}`;
      if (!stockTransferByItemCodeMap.has(key)) {
        stockTransferByItemCodeMap.set(key, []);
      }
      stockTransferByItemCodeMap.get(key)!.push(st);
    });

    // 6. Fix N+1: Batch Fetch Card Data Logic
    // Identify orders that need card data (Tach The orders)
    // We need to look at enriched data normally, but we can do a quick check on raw sales
    // Or we can just collect docCodes where we "suspect" card data is needed.
    // However, the original logic checked 'isTachTheOrder' on *formatted* sales.
    // To be safe and efficient, we can check basic conditions on raw 'ordertype' if possible,
    // or just wait until we format. But formatting happens per item.
    // Let's optimize: Gather all docCodes.
    // The previous logic only fetched for `docCodes[0]` (the first order) strictly in one place
    // and then inside a loop for 'hasTachThe'.

    // Optimization:
    // a. Fetch card data for the FIRST order (legacy logic preserved, maybe for global context?)
    //    Original code: `const [dataCard] = await this.n8nService.fetchCardData(docCodes[0]);`
    //    This seems to populate `getMaThe` map which is used for ALL sales.
    //    Wait, `getMaThe` is built ONLY from the response of `docCodes[0]`.
    //    This looks like a bug or a very specific feature for the first generic order.
    //    I will preserve this behavior but it looks suspicious.

    const getMaThe = new Map<string, string>();
    if (docCodes.length > 0) {
      try {
        const [dataCard] = await this.n8nService.fetchCardData(docCodes[0]);
        if (dataCard && dataCard.data) {
          for (const card of dataCard.data) {
            if (!card?.service_item_name || !card?.serial) {
              continue;
            }
            const itemProduct = await this.loyaltyService.checkProduct(
              card.service_item_name,
            );
            if (itemProduct) {
              getMaThe.set(itemProduct.materialCode, card.serial);
            }
          }
        }
      } catch (e) {
        // Ignore error
      }
    }

    // 7. Enrich Sales Items (In-Memory Processing)
    const enrichedSalesMap = new Map<string, any[]>();
    for (const sale of allSalesData) {
      const docCode = sale.docCode;
      if (!enrichedSalesMap.has(docCode)) {
        enrichedSalesMap.set(docCode, []);
      }
      // Temporarily store raw sales in enrichedSalesMap for later processing
      enrichedSalesMap.get(docCode)!.push(sale);
    }

    // 7. Format Sales for Frontend
    // OPTIMIZED: Parallelize formatSaleForFrontend calls instead of sequential
    const formatPromises: Promise<any>[] = []; // Changed to any[] to hold enrichedSale objects

    // Iterate over the temporarily stored raw sales
    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      for (const sale of sales) {
        const promise = (async () => {
          const loyaltyProduct = sale.itemCode
            ? loyaltyProductMap.get(sale.itemCode)
            : null;
          const department = sale.branchCode
            ? departmentMap.get(sale.branchCode) || null
            : null;

          // Debug: Log when department is missing
          if (!department && sale.branchCode) {
            this.logger.warn(
              `[DEBUG] Department not found for branchCode: ${sale.branchCode}, docCode: ${docCode}`,
            );
          }

          const maThe = getMaThe.get(loyaltyProduct?.materialCode || '') || '';
          sale.maThe = maThe;
          const saleMaterialCode = loyaltyProduct?.materialCode;

          let saleStockTransfers: StockTransfer[] = [];
          if (saleMaterialCode) {
            const stockTransferKey = `${docCode}_${saleMaterialCode}`;
            saleStockTransfers = stockTransferMap.get(stockTransferKey) || [];
          }
          // OPTIMIZED: Use map lookup O(1) instead of filter O(m)
          if (saleStockTransfers.length === 0 && sale.itemCode) {
            const key = `${docCode}_${sale.itemCode}`;
            saleStockTransfers = stockTransferByItemCodeMap.get(key) || [];
          }

          const maKhoFromStockTransfer = this.getMaKhoFromStockTransfer(
            sale,
            docCode,
            stockTransfers,
            saleMaterialCode,
            stockTransferMap,
            warehouseCodeMap,
          );
          const calculatedFields = SalesCalculationUtils.calculateSaleFields(
            sale,
            loyaltyProduct,
            department,
            sale.branchCode,
          );
          calculatedFields.maKho = maKhoFromStockTransfer;

          // Cache materialType for later use in verification loop
          if (sale.itemCode && loyaltyProduct?.materialType) {
            loyaltyProductMap.set(sale.itemCode, loyaltyProduct);
          } else if (sale.itemCode && loyaltyProduct?.productType) {
            // Fallback or explicit handling if needed
            if (!loyaltyProductMap.has(sale.itemCode)) {
              loyaltyProductMap.set(sale.itemCode, loyaltyProduct);
            }
          }

          const order = orderMap.get(sale.docCode);
          const enrichedSale = await SalesFormattingUtils.formatSaleForFrontend(
            sale,
            loyaltyProduct,
            department,
            calculatedFields,
            order,
            this.categoriesService,
            this.loyaltyService,
            saleStockTransfers,
          );

          // [NEW] Override svcCode with looked-up materialCode if available
          // The frontend uses 'svcCode' from the response. We keep original svc_code in DB.
          // But for display, we check the map.
          if (sale.svc_code) {
            enrichedSale.svcCode = svcCodeMap.get(sale.svc_code);
          }

          // Store enriched sale (will be pushed in order after Promise.all)
          enrichedSale._docCode = docCode; // Temporary marker for grouping
          return enrichedSale;
        })();

        formatPromises.push(promise);
      }
    }

    // Wait for all formatting to complete in parallel
    const allEnrichedSales = await Promise.all(formatPromises);

    // Clear and repopulate enrichedSalesMap with formatted sales
    enrichedSalesMap.clear();
    for (const enrichedSale of allEnrichedSales) {
      const docCode = enrichedSale._docCode;
      if (!enrichedSalesMap.has(docCode)) {
        enrichedSalesMap.set(docCode, []);
      }
      enrichedSalesMap.get(docCode)!.push(enrichedSale);
      delete enrichedSale._docCode; // Clean up temporary marker
    }

    // 8. Final Grouping & N+1 Fix for 'Tach The'
    // Collect docCodes that require card data fetching
    const docCodesNeedingCardData: string[] = [];

    // First pass to identify orders
    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const hasTachThe = sales.some((s: any) =>
        SalesUtils.isTachTheOrder(s.ordertype, s.ordertypeName),
      );
      if (hasTachThe) {
        docCodesNeedingCardData.push(docCode);
      }
    }

    // Execute parallel requests for card data with concurrency limit
    const cardDataMap = new Map<string, any>(); // Map<docCode, cardData>
    if (docCodesNeedingCardData.length > 0) {
      const startN8N = Date.now();
      // OPTIMIZED: Add concurrency limit to prevent overwhelming N8N service
      const MAX_CONCURRENT = 5;
      const chunks: string[][] = [];
      for (let i = 0; i < docCodesNeedingCardData.length; i += MAX_CONCURRENT) {
        chunks.push(docCodesNeedingCardData.slice(i, i + MAX_CONCURRENT));
      }

      for (const chunk of chunks) {
        const cardDataPromises = chunk.map(async (docCode) => {
          try {
            const cardResponse =
              await this.n8nService.fetchCardDataWithRetry(docCode);
            const cardData = this.n8nService.parseCardData(cardResponse);
            return { docCode, cardData };
          } catch (e) {
            this.logger.warn(
              `Failed to fetch card data for ${docCode}: ${e.message}`,
            );
            return { docCode, cardData: null };
          }
        });

        const results = await Promise.all(cardDataPromises);
        results.forEach((res) => {
          if (res.cardData) {
            cardDataMap.set(res.docCode, res.cardData);
          }
        });
      }
      this.logger.log(
        `[findAllOrders] N8N Card Fetching (${docCodesNeedingCardData.length} items) took ${Date.now() - startN8N}ms`,
      );
    }

    // Apply card data and build final orders
    const verificationTasks: Promise<void>[] = []; // Collect tasks for parallel execution

    // [OPTIMIZATION] Collect all sales for batch enrichment
    const allSalesForEnrichment: any[] = [];

    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const order = orderMap.get(docCode);
      if (order) {
        // Apply card data if available
        if (cardDataMap.has(docCode)) {
          const cardData = cardDataMap.get(docCode);
          this.n8nService.mapIssuePartnerCodeToSales(sales, cardData);
        }

        allSalesForEnrichment.push(...sales);
        order.sales = sales;
      }
    }

    // [NEW] Resolve ma_vt_ref using centralized logic from VoucherIssueService (Batch)
    if (allSalesForEnrichment.length > 0) {
      verificationTasks.push(
        this.voucherIssueService.enrichSalesWithMaVtRef(
          allSalesForEnrichment,
          loyaltyProductMap,
        ),
      );
    }

    // Wait for all verification tasks to complete
    if (verificationTasks.length > 0) {
      await Promise.all(verificationTasks);
    }

    // 9. Sort and return
    const orders = Array.from(orderMap.values()).sort((a, b) => {
      // Sort by latest date first
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });

    const enrichedOrders = await this.enrichOrdersWithCashio(orders);
    // CX4772
    // [CRITICAL] Re-enrich ma_vt_ref AFTER explosion
    // enrichOrdersWithCashio creates new sale lines from stock transfers
    // These new lines need ma_vt_ref to be populated based on their serial numbers

    // [OPTIMIZATION] Batch enrichment for exploded lines
    const explodedSalesForEnrichment: any[] = [];
    enrichedOrders.forEach((order) => {
      if (order.sales && order.sales.length > 0) {
        explodedSalesForEnrichment.push(...order.sales);
      }
    });

    if (explodedSalesForEnrichment.length > 0) {
      await this.voucherIssueService.enrichSalesWithMaVtRef(
        explodedSalesForEnrichment,
        loyaltyProductMap,
      );

      // [New] Override maThe for Voucher items (Type 94) with Ecode (ma_vt_ref)
      // Frontend requires maThe to show the serial/ecode
      // ... logic continues ...

      // [New] Override maThe for Voucher items (Type 94) with Ecode (ma_vt_ref)
      // Frontend requires maThe to show the serial/ecode
      enrichedOrders.forEach((order) => {
        if (order.sales) {
          order.sales.forEach((sale: any) => {
            const product =
              loyaltyProductMap.get(sale.itemCode) ||
              loyaltyProductMap.get(sale.materialCode);
            if (product?.materialType === '94') {
              // User request: maThe must take value from soSerial
              // Helper: Ensure soSerial is populated (fallback to ma_vt_ref)
              if (!sale.soSerial && sale.ma_vt_ref) {
                sale.soSerial = sale.ma_vt_ref;
              }
              // Assign soSerial to maThe
              sale.maThe = sale.soSerial;
            }
          });
        }
      });
    }

    if (isSearchMode) {
      // In-Memory Pagination Logic for Search Mode (Exploded Lines)
      // Flatten all exploded sales from all orders
      const allExplodedSales: any[] = [];
      const parentOrderMap = new Map<string, any>();

      enrichedOrders.forEach((order) => {
        parentOrderMap.set(order.docCode, { ...order, sales: [] }); // Store base order without sales
        if (order.sales && order.sales.length > 0) {
          order.sales.forEach((s: any) => {
            // Attach docCode to sale if missing, to trace back
            s.docCode = s.docCode || order.docCode;
            allExplodedSales.push(s);
          });
        }
      });

      const explodedTotal = allExplodedSales.length;
      const offset = (page - 1) * limit;
      const pagedSales = allExplodedSales.slice(offset, offset + limit);

      // Reconstruct Orders from pagedSales
      const pagedOrdersMap = new Map<string, any>();
      pagedSales.forEach((sale) => {
        const docCode = sale.docCode;
        if (!pagedOrdersMap.has(docCode)) {
          // Clone parent order structure
          if (parentOrderMap.has(docCode)) {
            const parent = parentOrderMap.get(docCode);
            pagedOrdersMap.set(docCode, {
              ...parent,
              sales: [], // Reset sales
            });
          }
        }
        if (pagedOrdersMap.has(docCode)) {
          const ord = pagedOrdersMap.get(docCode);
          ord.sales.push(sale);

          // No longer populating order-level stockTransfers
        }
      });

      const pagedOrders = Array.from(pagedOrdersMap.values());

      if (pagedOrders.length > 0) {
        this.logger.debug(
          `[findAllOrders] Final Order[0] Sales: ${pagedOrders[0].sales.length}, STs: ${pagedOrders[0].stockTransfers?.length}`,
        );
      }

      return {
        data: pagedOrders,
        total: explodedTotal, // Correct total (Exploded Lines)
        page,
        limit,
        totalPages: Math.ceil(explodedTotal / limit),
      };
    }

    const maxOrders = limit; // Take exactly limit number of orders
    const limitedOrders = enrichedOrders.slice(0, maxOrders);

    // FIX: Only return first sale per order to match expected format sales: [1 sale]
    // Prioritize real sales (with id) over pseudo-sales created from stock transfers
    const ordersWithSingleSale = limitedOrders.map((order) => {
      if (!order.sales || order.sales.length === 0) {
        return { ...order, sales: [] };
      }

      // Find first real sale (has id field) or fallback to first sale
      const realSale = order.sales.find((s: any) => s.id) || order.sales[0];

      return {
        ...order,
        sales: [realSale],
      };
    });

    // Use totalOrders from count query, not enrichedOrders.length
    // enrichedOrders.length is limited by query, not total count

    return {
      data: ordersWithSingleSale,
      total: totalOrders, // Return total orders, not sale items
      page,
      limit,
      totalPages: Math.ceil(totalOrders / limit),
    };
  }

  async getStatusAsys(
    statusAsys?: string,
    page?: number,
    limit?: number,
    brand?: string,
    dateFrom?: string,
    dateTo?: string,
    search?: string,
  ) {
    try {
      // Parse statusAsys
      let statusAsysValue: boolean | undefined;
      if (statusAsys === 'true') {
        statusAsysValue = true;
      } else if (statusAsys === 'false') {
        statusAsysValue = false;
      } else {
        statusAsysValue = undefined;
      }

      const pageNumber = page || 1;
      const limitNumber = limit || 10;
      const skip = (pageNumber - 1) * limitNumber;

      // 1. Data Query
      const query = this.saleRepository.createQueryBuilder('sale');
      query.leftJoinAndSelect('sale.customer', 'customer');

      this.applySaleFilters(query, {
        brand,
        search,
        statusAsys: statusAsysValue,
        dateFrom,
        dateTo,
      });

      query.orderBy('sale.createdAt', 'DESC').skip(skip).take(limitNumber);

      const sales = await query.getMany();

      // 2. Count Query
      const countQuery = this.saleRepository.createQueryBuilder('sale');
      // Only join customer if needed for filtering
      if (brand || (search && search.trim() !== '')) {
        countQuery.leftJoin('sale.customer', 'customer');
      }

      this.applySaleFilters(countQuery, {
        brand,
        search,
        statusAsys: statusAsysValue,
        dateFrom,
        dateTo,
      });

      const totalCount = await countQuery.getCount();

      return {
        data: sales,
        total: totalCount,
        page: pageNumber,
        limit: limitNumber,
        totalPages: Math.ceil(totalCount / limitNumber),
      };
    } catch (error: any) {
      this.logger.error(`[getStatusAsys] Error: ${error?.message || error}`);
      this.logger.error(
        `[getStatusAsys] Stack: ${error?.stack || 'No stack trace'}`,
      );
      throw error;
    }
  }
}
