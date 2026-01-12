import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, Between } from 'typeorm';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { LoyaltyService } from '../../services/loyalty.service';
import { CategoriesService } from '../categories/categories.service';
import { N8nService } from '../../services/n8n.service';
import * as SalesUtils from '../../utils/sales.utils';
import * as SalesCalculationUtils from '../../utils/sales-calculation.utils';
import * as SalesFormattingUtils from '../../utils/sales-formatting.utils';
import * as StockTransferUtils from '../../utils/stock-transfer.utils';

/**
 * SalesQueryService
 * Handle all query & filtering operations for sales
 * Optimized with QueryBuilder and eager loading to eliminate N+1 queries
 */
@Injectable()
export class SalesQueryService {
  private readonly logger = new Logger(SalesQueryService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    private loyaltyService: LoyaltyService,
    private categoriesService: CategoriesService,
    private n8nService: N8nService,
  ) {}

  /**
   * Find all orders with optimized queries
   * OPTIMIZATION: Use QueryBuilder with eager loading to eliminate N+1 queries
   */
  async findAllOrders(options: {
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
  }) {
    const {
      brand,
      isProcessed,
      page = 1,
      limit = 50,
      date,
      dateFrom,
      dateTo,
      search,
      statusAsys,
      export: isExport,
      typeSale,
    } = options;

    // OPTIMIZATION: Count query with minimal joins
    const countQuery = this.saleRepository
      .createQueryBuilder('sale')
      .select('COUNT(sale.id)', 'count');

    // Only join customer if needed for filters
    if (search || brand) {
      countQuery.leftJoin('sale.customer', 'customer');
    }

    this.applyFilters(countQuery, {
      isProcessed,
      statusAsys,
      typeSale,
      brand,
      search,
      date,
      dateFrom,
      dateTo,
    });

    const totalResult = await countQuery.getRawOne();
    const totalSaleItems = parseInt(totalResult?.count || '0', 10);

    // OPTIMIZATION: Main query with eager loading
    const fullQuery = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer') // Eager load customer
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.docCode', 'ASC')
      .addOrderBy('sale.id', 'ASC');

    this.applyFilters(fullQuery, {
      isProcessed,
      statusAsys,
      typeSale,
      brand,
      search,
      date,
      dateFrom,
      dateTo,
    });

    if (!isExport) {
      const offset = (page - 1) * limit;
      fullQuery.skip(offset).take(limit);
    }

    const allSales = await fullQuery.getMany();

    // Export mode: return sales directly
    if (isExport) {
      return {
        sales: this.formatSalesWithCustomer(allSales),
        total: totalSaleItems,
      };
    }

    // Group sales by order
    const { orderMap, allSalesData } = this.groupSalesByOrder(allSales);

    // OPTIMIZATION: Batch fetch related data
    const {
      loyaltyProductMap,
      departmentMap,
      stockTransferMap,
      stockTransferByDocCodeMap,
      getMaThe,
    } = await this.batchFetchRelatedData(allSalesData);

    // Enrich sales with related data
    const enrichedSalesMap = await this.enrichSalesWithRelatedData(
      allSalesData,
      orderMap,
      loyaltyProductMap,
      departmentMap,
      stockTransferMap,
      getMaThe,
    );

    // Build final orders
    const orders = await this.buildFinalOrders(
      orderMap,
      enrichedSalesMap,
      stockTransferByDocCodeMap,
    );

    // OPTIMIZATION: Batch enrich with cashio
    const enrichedOrders = await this.enrichOrdersWithCashioBatch(orders);

    const maxOrders = limit * 2;
    const limitedOrders = enrichedOrders.slice(0, maxOrders);

    return {
      data: limitedOrders,
      total: totalSaleItems,
      page,
      limit,
      totalPages: Math.ceil(totalSaleItems / limit),
    };
  }

  /**
   * Apply filters to query
   */
  private applyFilters(
    query: any,
    filters: {
      isProcessed?: boolean;
      statusAsys?: boolean;
      typeSale?: string;
      brand?: string;
      search?: string;
      date?: string;
      dateFrom?: string;
      dateTo?: string;
    },
  ) {
    const {
      isProcessed,
      statusAsys,
      typeSale,
      brand,
      search,
      date,
      dateFrom,
      dateTo,
    } = filters;

    if (isProcessed !== undefined) {
      query.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }

    if (statusAsys !== undefined) {
      query.andWhere('sale.statusAsys = :statusAsys', { statusAsys });
    }

    if (typeSale !== undefined && typeSale !== 'ALL') {
      query.andWhere('sale.type_sale = :type_sale', {
        type_sale: typeSale.toUpperCase(),
      });
    }

    if (brand) {
      query.andWhere('sale.brand = :brand', { brand });
    }

    if (search && search.trim() !== '') {
      const searchPattern = `%${search.trim().toLowerCase()}%`;
      query.andWhere(
        "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
        { search: searchPattern },
      );
    }

    // Date filters
    let hasFiltersWithDate = false;
    if (dateFrom || dateTo) {
      hasFiltersWithDate = true;
      if (dateFrom && dateTo) {
        const startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        query.andWhere(
          'sale.docDate >= :dateFrom AND sale.docDate <= :dateTo',
          {
            dateFrom: startDate,
            dateTo: endDate,
          },
        );
      } else if (dateFrom) {
        const startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        query.andWhere('sale.docDate >= :dateFrom', { dateFrom: startDate });
      } else if (dateTo) {
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        query.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }
    } else if (date) {
      hasFiltersWithDate = true;
      const dateMatch = date.match(/^(\d{2})([A-Z]{3})(\d{4})$/i);
      if (dateMatch) {
        const [, day, monthStr, year] = dateMatch;
        const monthMap: { [key: string]: number } = {
          JAN: 0,
          FEB: 1,
          MAR: 2,
          APR: 3,
          MAY: 4,
          JUN: 5,
          JUL: 6,
          AUG: 7,
          SEP: 8,
          OCT: 9,
          NOV: 10,
          DEC: 11,
        };
        const month = monthMap[monthStr.toUpperCase()];
        if (month !== undefined) {
          const dateObj = new Date(parseInt(year), month, parseInt(day));
          const startOfDay = new Date(dateObj);
          startOfDay.setHours(0, 0, 0, 0);
          const endOfDay = new Date(dateObj);
          endOfDay.setHours(23, 59, 59, 999);
          query.andWhere(
            'sale.docDate >= :dateFrom AND sale.docDate <= :dateTo',
            {
              dateFrom: startOfDay,
              dateTo: endOfDay,
            },
          );
        }
      }
    } else if (brand && !hasFiltersWithDate) {
      // Default: last 30 days if brand is selected but no date filter
      const endDate = new Date();
      endDate.setHours(23, 59, 59, 999);
      const startDate = new Date();
      startDate.setDate(startDate.getDate() - 30);
      startDate.setHours(0, 0, 0, 0);
      query.andWhere('sale.docDate >= :dateFrom AND sale.docDate <= :dateTo', {
        dateFrom: startDate,
        dateTo: endDate,
      });
    }
  }

  /**
   * Format sales with customer for export
   */
  private formatSalesWithCustomer(sales: Sale[]) {
    return sales.map((sale) => ({
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
            }
          : null,
    }));
  }

  /**
   * Group sales by order (docCode)
   */
  private groupSalesByOrder(sales: Sale[]) {
    const orderMap = new Map<string, any>();
    const allSalesData: any[] = [];

    for (const sale of sales) {
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

      if (!sale.isProcessed) {
        order.isProcessed = false;
      }

      allSalesData.push(sale);
    }

    return { orderMap, allSalesData };
  }

  /**
   * OPTIMIZATION: Batch fetch all related data in parallel
   */
  private async batchFetchRelatedData(allSalesData: any[]) {
    // Extract unique codes
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

    // OPTIMIZATION: Batch fetch stock transfers
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

    const allItemCodes = Array.from(
      new Set([...itemCodes, ...stockTransferItemCodes]),
    );

    // OPTIMIZATION: Parallel fetch loyalty data
    const [loyaltyProductMap, departmentMap] = await Promise.all([
      this.loyaltyService.fetchProducts(allItemCodes),
      this.loyaltyService.fetchLoyaltyDepartments(branchCodes),
    ]);

    const { stockTransferMap, stockTransferByDocCodeMap } =
      StockTransferUtils.buildStockTransferMaps(
        stockTransfers,
        loyaltyProductMap,
        docCodes,
      );

    // Fetch card data
    const getMaThe = new Map<string, string>();
    if (docCodes.length > 0) {
      try {
        const [dataCard] = await this.n8nService.fetchCardData(docCodes[0]);
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
      } catch (error) {
        this.logger.warn('Failed to fetch card data', error);
      }
    }

    return {
      loyaltyProductMap,
      departmentMap,
      stockTransferMap,
      stockTransferByDocCodeMap,
      getMaThe,
      stockTransfers,
    };
  }

  /**
   * Enrich sales with related data
   */
  private async enrichSalesWithRelatedData(
    allSalesData: any[],
    orderMap: Map<string, any>,
    loyaltyProductMap: Map<string, any>,
    departmentMap: Map<string, any>,
    stockTransferMap: Map<string, StockTransfer[]>,
    getMaThe: Map<string, string>,
  ) {
    const enrichedSalesMap = new Map<string, any[]>();

    for (const sale of allSalesData) {
      const docCode = sale.docCode;
      if (!enrichedSalesMap.has(docCode)) {
        enrichedSalesMap.set(docCode, []);
      }

      const loyaltyProduct = sale.itemCode
        ? loyaltyProductMap.get(sale.itemCode)
        : null;
      const department = sale.branchCode
        ? departmentMap.get(sale.branchCode) || null
        : null;
      const maThe = getMaThe.get(loyaltyProduct?.materialCode || '') || '';
      sale.maThe = maThe;

      const saleMaterialCode = loyaltyProduct?.materialCode;
      let saleStockTransfers: StockTransfer[] = [];
      if (saleMaterialCode) {
        const stockTransferKey = `${docCode}_${saleMaterialCode}`;
        saleStockTransfers = stockTransferMap.get(stockTransferKey) || [];
      }

      const maKhoFromStockTransfer = await this.getMaKhoFromStockTransfer(
        sale,
        docCode,
        saleMaterialCode,
        stockTransferMap,
      );

      const calculatedFields = SalesCalculationUtils.calculateSaleFields(
        sale,
        loyaltyProduct,
        department,
        sale.branchCode,
      );
      calculatedFields.maKho = maKhoFromStockTransfer;

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
      enrichedSale.stockTransfers = saleStockTransfers.map((st) =>
        StockTransferUtils.formatStockTransferForFrontend(st),
      );

      enrichedSalesMap.get(docCode)!.push(enrichedSale);
    }

    return enrichedSalesMap;
  }

  /**
   * Get maKho from stock transfer
   */
  private async getMaKhoFromStockTransfer(
    sale: any,
    docCode: string,
    saleMaterialCode?: string | null,
    stockTransferMap?: Map<string, StockTransfer[]>,
  ): Promise<string> {
    if (!saleMaterialCode || !stockTransferMap) {
      return '';
    }

    const stockTransferKey = `${docCode}_${saleMaterialCode}`;
    const stockTransfers = stockTransferMap.get(stockTransferKey) || [];

    if (stockTransfers.length === 0) {
      return '';
    }

    // Lấy stockCode từ stock transfer đầu tiên
    return stockTransfers[0].stockCode || '';
  }

  /**
   * Build final orders with stock transfers
   */
  private async buildFinalOrders(
    orderMap: Map<string, any>,
    enrichedSalesMap: Map<string, any[]>,
    stockTransferByDocCodeMap: Map<string, StockTransfer[]>,
  ) {
    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const order = orderMap.get(docCode);
      if (order) {
        // Check for tach the orders
        const hasTachThe = sales.some((s: any) =>
          SalesUtils.isTachTheOrder(s.ordertype, s.ordertypeName),
        );
        if (hasTachThe) {
          try {
            const cardResponse =
              await this.n8nService.fetchCardDataWithRetry(docCode);
            const cardData = this.n8nService.parseCardData(cardResponse);
            this.n8nService.mapIssuePartnerCodeToSales(sales, cardData);
          } catch (e) {
            this.logger.warn(`Failed to fetch card data for ${docCode}`, e);
          }
        }
        order.sales = sales;
        order.stockTransfers = (
          stockTransferByDocCodeMap.get(docCode) || []
        ).map((st) => StockTransferUtils.formatStockTransferForFrontend(st));
      }
    }

    return Array.from(orderMap.values()).sort((a, b) => {
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });
  }

  /**
   * OPTIMIZATION: Batch enrich orders with cashio data
   * Eliminates N+1 query problem by fetching all cashio data in one query
   */
  private async enrichOrdersWithCashioBatch(orders: any[]): Promise<any[]> {
    if (orders.length === 0) {
      return orders;
    }

    // Extract all docCodes
    const docCodes = orders.map((order) => order.docCode);

    // OPTIMIZATION: Single batch query instead of N queries
    const cashios = await this.dailyCashioRepository.find({
      where: { so_code: In(docCodes) },
    });

    // Create map for O(1) lookup
    const cashioMap = new Map(cashios.map((c) => [c.so_code, c]));

    // Enrich orders with cashio data
    return orders.map((order) => {
      const cashio = cashioMap.get(order.docCode);
      if (!cashio) {
        return order;
      }

      // Calculate stock transfer summary
      const uniqueStockTransfers = Array.from(
        new Map(
          (order.stockTransfers || []).map((st: any) => [st.id, st]),
        ).values(),
      );

      const stockTransferSummary = {
        totalItems: uniqueStockTransfers.length,
        totalQty: uniqueStockTransfers.reduce(
          (sum: number, st: any) => sum + (Number(st.qty) || 0),
          0,
        ),
      };

      return {
        ...order,
        cashio: {
          docCode: cashio.so_code,
          docDate: cashio.docdate,
          branchCode: cashio.branch_code,
          partnerCode: cashio.partner_code,
          partnerName: cashio.partner_name,
          totalAmount: cashio.total_out,
          paidAmount: cashio.total_in,
          reservedAmount: 0, // Not available in entity
          voucherAmount: cashio.fop_syscode === 'VOUCHER' ? cashio.total_in : 0,
          paymentMethods: cashio.fop_description || '',
        },
        stockTransferInfo: stockTransferSummary,
        stockTransfers:
          uniqueStockTransfers.length > 0
            ? uniqueStockTransfers.map((st: any) =>
                StockTransferUtils.formatStockTransferForFrontend(st),
              )
            : (order.stockTransfers || []).map((st: any) =>
                StockTransferUtils.formatStockTransferForFrontend(st),
              ),
      };
    });
  }

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
   * OPTIMIZATION: Eager load customer relation
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
   * Get status asys with filters
   */
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
      }

      const pageNumber = page || 1;
      const limitNumber = limit || 10;
      const skip = (pageNumber - 1) * limitNumber;

      // OPTIMIZATION: Use QueryBuilder with eager loading
      let query = this.saleRepository
        .createQueryBuilder('sale')
        .leftJoinAndSelect('sale.customer', 'customer');

      if (statusAsysValue !== undefined) {
        query = query.andWhere('sale.statusAsys = :statusAsys', {
          statusAsys: statusAsysValue,
        });
      }

      if (brand) {
        query = query.andWhere('customer.brand = :brand', { brand });
      }

      if (search && search.trim() !== '') {
        const searchPattern = `%${search.trim().toLowerCase()}%`;
        query = query.andWhere(
          "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
          { search: searchPattern },
        );
      }

      if (dateFrom) {
        const startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        query = query.andWhere('sale.docDate >= :dateFrom', {
          dateFrom: startDate,
        });
      }
      if (dateTo) {
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        query = query.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }

      // Count query
      const total = await query.getCount();

      // Data query with pagination
      const sales = await query
        .orderBy('sale.docDate', 'DESC')
        .skip(skip)
        .take(limitNumber)
        .getMany();

      return {
        data: sales,
        total,
        page: pageNumber,
        limit: limitNumber,
        totalPages: Math.ceil(total / limitNumber),
      };
    } catch (error: any) {
      this.logger.error('Error in getStatusAsys', error);
      throw error;
    }
  }

  /**
   * Get stock transfer by ID
   */
  async getStockTransferById(id: string): Promise<StockTransfer | null> {
    return await this.stockTransferRepository.findOne({
      where: { id },
    });
  }
}
