import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
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
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    private loyaltyService: LoyaltyService,
    private categoriesService: CategoriesService,
    private n8nService: N8nService,
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
   */
  async enrichOrdersWithCashio(orders: any[]): Promise<any[]> {
    const docCodes = orders.map((o) => o.docCode);
    if (docCodes.length === 0) return orders;

    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code IN (:...docCodes)', { docCodes })
      .orWhere('cashio.master_code IN (:...docCodes)', { docCodes })
      .getMany();

    const cashioMap = new Map<string, DailyCashio[]>();
    docCodes.forEach((docCode) => {
      const matchingCashios = cashioRecords.filter(
        (c) => c.so_code === docCode || c.master_code === docCode,
      );
      if (matchingCashios.length > 0) {
        cashioMap.set(docCode, matchingCashios);
      }
    });

    // Fetch stock transfers để thêm thông tin stock transfer
    // Join theo soCode (của stock transfer) = docCode (của order)
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodes) },
    });

    const stockTransferMap = new Map<string, StockTransfer[]>();
    docCodes.forEach((docCode) => {
      // Join theo soCode (của stock transfer) = docCode (của order)
      const matchingTransfers = stockTransfers.filter(
        (st) => st.soCode === docCode,
      );
      if (matchingTransfers.length > 0) {
        stockTransferMap.set(docCode, matchingTransfers);
      }
    });

    return orders.map((order) => {
      const cashioRecords = cashioMap.get(order.docCode) || [];
      const ecoinCashio = cashioRecords.find((c) => c.fop_syscode === 'ECOIN');
      const voucherCashio = cashioRecords.find(
        (c) => c.fop_syscode === 'VOUCHER',
      );
      const selectedCashio =
        ecoinCashio || voucherCashio || cashioRecords[0] || null;

      // Lấy thông tin stock transfer cho đơn hàng này
      const orderStockTransfers = stockTransferMap.get(order.docCode) || [];

      // Lọc chỉ lấy các stock transfer XUẤT KHO (SALE_STOCKOUT) với qty < 0
      // Bỏ qua các stock transfer nhập lại (RETURN) với qty > 0
      const stockOutTransfers = orderStockTransfers.filter((st) => {
        // Chỉ lấy các record có doctype = 'SALE_STOCKOUT' hoặc qty < 0 (xuất kho)
        const isStockOut =
          st.doctype === 'SALE_STOCKOUT' || Number(st.qty || 0) < 0;
        return isStockOut;
      });

      // Deduplicate stock transfers để tránh tính trùng
      // Group theo docCode + itemCode + stockCode + qty để đảm bảo chỉ tính một lần cho mỗi combination
      // (có thể có duplicate records trong database với id khác nhau nhưng cùng docCode, itemCode, stockCode, qty)
      const uniqueStockTransfersMap = new Map<string, StockTransfer>();
      stockOutTransfers.forEach((st) => {
        // Tạo key từ docCode + itemCode + stockCode + qty (giữ nguyên dấu âm để phân biệt ST và RT)
        // KHÔNG dùng Math.abs vì ST (qty=-11) và RT (qty=11) là 2 chứng từ khác nhau
        const qty = Number(st.qty || 0);
        const key = `${st.docCode || ''}_${st.itemCode || ''}_${st.stockCode || ''}_${qty}`;

        // Chỉ lưu nếu chưa có key này, hoặc nếu có thì giữ record có id (ưu tiên record có id)
        if (!uniqueStockTransfersMap.has(key)) {
          uniqueStockTransfersMap.set(key, st);
        } else {
          // Nếu đã có, chỉ thay thế nếu record hiện tại có id và record cũ không có id
          const existing = uniqueStockTransfersMap.get(key)!;
          if (st.id && !existing.id) {
            uniqueStockTransfersMap.set(key, st);
          }
        }
      });
      const uniqueStockTransfers = Array.from(uniqueStockTransfersMap.values());

      // Debug log nếu có duplicate hoặc có RT records bị loại bỏ
      if (orderStockTransfers.length > stockOutTransfers.length) {
        const returnCount =
          orderStockTransfers.length - stockOutTransfers.length;
        this.logger.debug(
          `[StockTransfer] Đơn hàng ${order.docCode}: Loại bỏ ${returnCount} records nhập lại (RETURN), chỉ tính ${stockOutTransfers.length} records xuất kho (ST)`,
        );
      }
      if (stockOutTransfers.length > uniqueStockTransfers.length) {
        this.logger.warn(
          `[StockTransfer] Đơn hàng ${order.docCode}: ${stockOutTransfers.length} records xuất kho → ${uniqueStockTransfers.length} unique (đã loại bỏ ${stockOutTransfers.length - uniqueStockTransfers.length} duplicates)`,
        );
      }

      // Tính tổng hợp thông tin stock transfer (chỉ tính từ unique records XUẤT KHO)
      // Lấy giá trị tuyệt đối của qty (vì qty xuất kho là số âm, nhưng số lượng xuất là số dương)
      const totalQty = uniqueStockTransfers.reduce((sum, st) => {
        const qty = Math.abs(Number(st.qty || 0));
        return sum + qty;
      }, 0);

      const stockTransferSummary = {
        totalItems: uniqueStockTransfers.length, // Số dòng stock transfer xuất kho (sau khi deduplicate)
        totalQty: totalQty, // Tổng số lượng xuất kho (lấy giá trị tuyệt đối vì qty xuất kho là số âm)
        uniqueItems: new Set(uniqueStockTransfers.map((st) => st.itemCode))
          .size, // Số sản phẩm khác nhau
        stockCodes: Array.from(
          new Set(
            uniqueStockTransfers.map((st) => st.stockCode).filter(Boolean),
          ),
        ), // Danh sách mã kho
        hasStockTransfer: uniqueStockTransfers.length > 0, // Có stock transfer xuất kho hay không
      };

      return {
        ...order,
        cashioData: cashioRecords.length > 0 ? cashioRecords : null,
        cashioFopSyscode: selectedCashio?.fop_syscode || null,
        cashioFopDescription: selectedCashio?.fop_description || null,
        cashioCode: selectedCashio?.code || null,
        cashioMasterCode: selectedCashio?.master_code || null,
        cashioTotalIn: selectedCashio?.total_in || null,
        cashioTotalOut: selectedCashio?.total_out || null,
        // Thông tin stock transfer
        stockTransferInfo: stockTransferSummary,
        stockTransfers:
          uniqueStockTransfers.length > 0
            ? uniqueStockTransfers.map((st) =>
                StockTransferUtils.formatStockTransferForFrontend(st),
              )
            : (order.stockTransfers || []).map((st: any) =>
                StockTransferUtils.formatStockTransferForFrontend(st),
              ), // Format để trả về materialCode
      };
    });
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
   */
  private applySaleFilters(
    query: any,
    options: {
      brand?: string;
      search?: string;
      statusAsys?: boolean;
      typeSale?: string;
      date?: string;
      dateFrom?: string | Date; // Allow Date object
      dateTo?: string | Date; // Allow Date object
      isProcessed?: boolean;
    },
  ) {
    const {
      brand,
      search,
      statusAsys,
      typeSale,
      date,
      dateFrom,
      dateTo,
      isProcessed,
    } = options;

    if (isProcessed !== undefined) {
      query.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }
    if (statusAsys !== undefined) {
      query.andWhere('sale.statusAsys = :statusAsys', { statusAsys });
    }
    if (typeSale && typeSale !== 'ALL') {
      query.andWhere('sale.type_sale = :type_sale', {
        type_sale: typeSale.toUpperCase(),
      });
    }

    if (brand) {
      // Check if we need to join customer (if not already joined in calling code)
      // Assuming 'customer' alias is used for sale.customer join
      query.andWhere('customer.brand = :brand', { brand });
    }

    if (search && search.trim() !== '') {
      const searchPattern = `%${search.trim().toLowerCase()}%`;
      query.andWhere(
        "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
        { search: searchPattern },
      );
    }

    // Date logic
    let startDate: Date | undefined;
    let endDate: Date | undefined;

    // Handle string inputs for dateFrom/dateTo (from API query params) or Date objects
    if (dateFrom) {
      startDate = new Date(dateFrom);
      startDate.setHours(0, 0, 0, 0);
    }
    if (dateTo) {
      endDate = new Date(dateTo);
      endDate.setHours(23, 59, 59, 999);
    }

    if (startDate && endDate) {
      query.andWhere(
        'sale.docDate >= :startDate AND sale.docDate <= :endDate',
        {
          startDate,
          endDate,
        },
      );
    } else if (startDate) {
      query.andWhere('sale.docDate >= :startDate', { startDate });
    } else if (endDate) {
      query.andWhere('sale.docDate <= :endDate', { endDate });
    } else if (date) {
      // Special format DDMMMYYYY
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
            'sale.docDate >= :startDate AND sale.docDate <= :endDate',
            {
              startDate: startOfDay,
              endDate: endOfDay,
            },
          );
        }
      }
    } else if (brand && !startDate && !endDate && !date) {
      // Default: Last 30 days if only brand is specified
      const end = new Date();
      end.setHours(23, 59, 59, 999);
      const start = new Date();
      start.setDate(start.getDate() - 30);
      start.setHours(0, 0, 0, 0);
      query.andWhere(
        'sale.docDate >= :startDate AND sale.docDate <= :endDate',
        {
          startDate: start,
          endDate: end,
        },
      );
    }
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
      limit = 50,
      date,
      dateFrom,
      dateTo,
      search,
      statusAsys,
      export: isExport,
      typeSale,
    } = options;

    // 1. Initial Count Query
    const countQuery = this.saleRepository
      .createQueryBuilder('sale')
      .select('COUNT(sale.id)', 'count');

    // Join customer for search/brand filtering
    countQuery.leftJoin('sale.customer', 'customer');

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
    const totalSaleItems = parseInt(totalResult?.count || '0', 10);

    // 2. Main Query
    const fullQuery = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer')
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

    if (!isExport) {
      const offset = (page - 1) * limit;
      fullQuery.skip(offset).take(limit);
    }

    const allSales = await fullQuery.getMany();

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
        total: totalSaleItems,
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

    // Build Stock Transfer Maps
    const { stockTransferMap, stockTransferByDocCodeMap } =
      StockTransferUtils.buildStockTransferMaps(
        stockTransfers,
        loyaltyProductMap,
        docCodes,
      );

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
      if (saleStockTransfers.length === 0 && sale.itemCode) {
        saleStockTransfers = stockTransfers.filter(
          (st) => st.soCode === docCode && st.itemCode === sale.itemCode,
        );
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

      // Map stock transfers to simple Frontend format
      enrichedSale.stockTransfers = saleStockTransfers.map((st) =>
        StockTransferUtils.formatStockTransferForFrontend(st),
      );

      enrichedSalesMap.get(docCode)!.push(enrichedSale);
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

    // Execute parallel requests for card data
    const cardDataMap = new Map<string, any>(); // Map<docCode, cardData>
    if (docCodesNeedingCardData.length > 0) {
      // Limit concurrency if needed, but for now Promise.all is fine for reasonable page sizes (e.g. 50)
      // If limit is large (e.g. 200), we might want to use a concurrency limiter (like p-limit)
      // but adding a library might be overkill. Let's stick to Promise.all for pagination limits.
      const cardDataPromises = docCodesNeedingCardData.map(async (docCode) => {
        try {
          const cardResponse =
            await this.n8nService.fetchCardDataWithRetry(docCode);
          const cardData = this.n8nService.parseCardData(cardResponse);
          return { docCode, cardData };
        } catch (e) {
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

    // Apply card data and build final orders
    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const order = orderMap.get(docCode);
      if (order) {
        // Apply card data if available
        if (cardDataMap.has(docCode)) {
          const cardData = cardDataMap.get(docCode);
          this.n8nService.mapIssuePartnerCodeToSales(sales, cardData);
        }

        order.sales = sales;
        order.stockTransfers = (
          stockTransferByDocCodeMap.get(docCode) || []
        ).map((st) => StockTransferUtils.formatStockTransferForFrontend(st));
      }
    }

    // 9. Sort and return
    const orders = Array.from(orderMap.values()).sort((a, b) => {
      // Sort by latest date first
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });

    const enrichedOrders = await this.enrichOrdersWithCashio(orders);

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
