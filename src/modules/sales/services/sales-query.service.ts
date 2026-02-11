import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, IsNull, Like, SelectQueryBuilder } from 'typeorm';
import { Sale } from '../../../entities/sale.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { OrderFee } from '../../../entities/order-fee.entity';
import { LoyaltyService } from '../../../services/loyalty.service';
import { CategoriesService } from '../../categories/categories.service';
import { N8nService } from '../../../services/n8n.service';
import * as SalesUtils from '../../../utils/sales.utils';
// import * as SalesFormattingUtils from '../../../utils/sales-formatting.utils'; // REMOVED
import { SalesFormattingService } from './sales-formatting.service';
import { VoucherIssueService } from '../../voucher-issue/voucher-issue.service';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import { InvoiceLogicUtils } from '../../../utils/invoice-logic.utils';
import { HttpService } from '@nestjs/axios';
import { Customer } from '../../../entities/customer.entity';
import { DailyCashio } from '../../../entities/daily-cashio.entity';
import { ZappyApiService } from '../../../services/zappy-api.service';
import { FastApiInvoice } from '../../../entities/fast-api-invoice.entity';
import { Invoice } from '../../../entities/invoice.entity';

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
    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,
    private loyaltyService: LoyaltyService,
    private categoriesService: CategoriesService,
    private n8nService: N8nService,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private voucherIssueService: VoucherIssueService,
    private salesFormattingService: SalesFormattingService,
    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
  ) { }

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
    // 1. Fetch Sales
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { id: 'ASC' },
    });

    if (!sales || sales.length === 0) {
      throw new NotFoundException(`Order with code "${docCode}" not found`);
    }

    // 2. Prepare Data for Enrichment
    const itemCodes = Array.from(
      new Set(
        sales
          .map((s) => s.itemCode)
          .filter((c): c is string => !!c && c.trim() !== ''),
      ),
    );
    const branchCodes = Array.from(
      new Set(
        sales
          .map((s) => s.branchCode)
          .filter((c): c is string => !!c && c.trim() !== ''),
      ),
    );

    // 3. Batch Fetch Supporting Data
    const [loyaltyProductMap, departmentMap, warehouseCodeMap] =
      await Promise.all([
        this.loyaltyService.fetchProducts(itemCodes),
        this.loyaltyService.fetchLoyaltyDepartments(branchCodes),
        this.categoriesService.getWarehouseCodeMap(),
      ]);

    // 4. Stock Transfers
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: [
        { soCode: In(docCodesForStockTransfer) },
        { docCode: In(docCodesForStockTransfer) },
      ],
      order: { createdAt: 'ASC' }, // [FIX] Ensure FIFO for sequential matching
    });

    // 4.1. Order Fee (for Platform Order detection)
    const orderFee = await this.orderFeeRepository.findOne({
      where: { erpOrderCode: docCode },
    });
    const isPlatformOrder = !!orderFee;
    const platformBrand = orderFee?.brand;

    // [FIX] Group by ItemCode and Pre-assign to Sales (1-1 Sequential)
    // REWRITE STRATEGY: Index Sales, iterate STs for matching
    const saleIdToStockTransferMap = new Map<
      string,
      { st: StockTransfer | null; rt: StockTransfer | null }
    >();

    const salesByItemCode = new Map<string, Sale[]>();
    sales.forEach((s) => {
      const k = s.itemCode;
      if (!salesByItemCode.has(k)) salesByItemCode.set(k, []);
      salesByItemCode.get(k)!.push(s);
    });

    stockTransfers.forEach((st) => {
      // 1. Try ItemCode
      let candidateSales = salesByItemCode.get(st.itemCode);
      // 2. Try MaterialCode (fallback)
      if ((!candidateSales || candidateSales.length === 0) && st.materialCode) {
        candidateSales = salesByItemCode.get(st.materialCode);
      }

      if (candidateSales && candidateSales.length > 0) {
        const targetSale = candidateSales.shift(); // Consume Sale!
        if (targetSale) {
          const split = saleIdToStockTransferMap.get(targetSale.id) || {
            st: null,
            rt: null,
          };
          if (st.docCode.startsWith('RT')) split.rt = st;
          else if (st.docCode.startsWith('ST')) split.st = st;
          saleIdToStockTransferMap.set(targetSale.id, split);
        }
      }
    });

    // 5. Card Data (for Tach The orders)
    const hasTachThe = sales.some((s) =>
      SalesUtils.isTachTheOrder(s.ordertypeName),
    );
    let cardData: any = null;
    if (hasTachThe) {
      try {
        const cardResponse =
          await this.n8nService.fetchCardDataWithRetry(docCode);
        cardData = this.n8nService.parseCardData(cardResponse);
      } catch (e) {
        this.logger.warn(
          `Failed to fetch card data for ${docCode}: ${e.message}`,
        );
      }
    }

    // 5.1. Pre-fetch Employee Status via API
    // Collect unique partnerCodes and issuePartnerCodes
    const firstSale = sales[0];
    const brand = firstSale?.customer?.brand || platformBrand || 'menard';
    const partnerCodesToCheck = Array.from(
      new Set(
        sales
          .flatMap((s) => [s.partnerCode, (s as any).issuePartnerCode])
          .filter((c): c is string => !!c && c.trim() !== ''),
      ),
    ).map((partnerCode) => ({ partnerCode, sourceCompany: brand }));

    const isEmployeeMap =
      await this.n8nService.checkCustomersIsEmployee(partnerCodesToCheck);

    // 6. Format Sales
    const enrichedSales = await Promise.all(
      sales.map(async (sale) => {
        const loyaltyProduct = sale.itemCode
          ? loyaltyProductMap.get(sale.itemCode)
          : null;
        const department = sale.branchCode
          ? departmentMap.get(sale.branchCode)
          : null;

        // [FIX] Use Pre-assigned Stock Transfers
        const { st: assignedSt, rt: assignedRt } = saleIdToStockTransferMap.get(
          sale.id,
        ) || { st: null, rt: null };
        const saleStockTransfers: StockTransfer[] = [];
        if (assignedSt) saleStockTransfers.push(assignedSt);
        if (assignedRt) saleStockTransfers.push(assignedRt);

        // Resolve Ma Kho directly
        let maKhoFromStockTransfer = '';
        const targetSt = assignedSt || assignedRt;
        if (targetSt?.stockCode) {
          maKhoFromStockTransfer =
            warehouseCodeMap?.get(targetSt.stockCode) || targetSt.stockCode;
        }

        const calculatedFields = await InvoiceLogicUtils.calculateSaleFields(
          sale,
          loyaltyProduct,
          department,
          sale.branchCode,
        );
        calculatedFields.maKho = maKhoFromStockTransfer;

        // Mock Order Object (partial) for formatting utils if needed
        const mockOrder = {
          docCode,
          docDate: sale.docDate,
          customer: sale.customer,
          // Add other fields if FormatUtils needs them from order
        };

        // Get employee status from pre-fetched map
        const isEmployee =
          isEmployeeMap.get(sale.partnerCode) ||
          isEmployeeMap.get((sale as any).issuePartnerCode) ||
          false;

        // [NEW] Use SalesFormattingService instead of Utils

        // Temporarily attach assigned transfers for the service to pick up
        // This aligns with our update in SalesFormattingService
        (sale as any).stockTransfers = saleStockTransfers;

        // Build minimal context for single sale format (Optimization: reuse context object if possible, but here we build per loop for safety with local vars)
        // Actually, we should build a GLOBAL context for the batch, but 'saleStockTransfers' is specific to this sale's resolution logic.
        // So we pass a partial context or just what's needed.
        // The service expects a full context. Let's construct it efficiently.
        const context = this.salesFormattingService.buildContext({
          loyaltyProductMap,
          departmentMap,
          stockTransferMap: undefined, // We pass explicit STs via sale property
          orderFeeMap: undefined, // Handled via override params in service? No, service reads map.
          // Wait, service reads orderFeeMap. We need to pass it if we want platform logic to work.
          // But findAllOrders didn't load orderFeeMap fully? It loaded 'orderFee' for single docCode.
          // Let's create a mini map for this single order context if needed.
          // In findByOrderCode, we have 'isPlatformOrder' and 'platformBrand' variables.
          // SalesFormattingService checks `orderFeeMap?.has(sale.docCode)`.
          // So we should construct a map if we want to use the service fully.

          // To maintain compatibility with existing variables:
          warehouseCodeMap,
          svcCodeMap: undefined, // Not used in findByOrderCode scope
          getMaTheMap: undefined, // Not used, passed via sale.maThe? Service checks map.
          // The service checks `getMaTheMap`. We should pass it if we have it?
          // `findByOrderCode` doesn't seem to fetch `getMaTheMap`?
          // It fetches `cardData` LATER.
          // So for `findByOrderCode`, `getMaTheMap` is empty at this stage.
          orderMap: new Map([[docCode, mockOrder]]),
          isEmployeeMap,
          includeStockTransfers: true,
        });

        // Manual override for Platform Order since we have local vars
        // We can fake the map entry
        const orderFeeMap = new Map();
        if (isPlatformOrder) {
          orderFeeMap.set(docCode, { brand: platformBrand } as any);
        }
        context.orderFeeMap = orderFeeMap;

        const enriched = await this.salesFormattingService.formatSingleSale(
          sale,
          context,
        );

        // Clean up temp property
        delete (sale as any).stockTransfers;

        // [EXPAND] Return keys for Fast API Payload reuse
        return {
          ...enriched,
          stockTransfer: assignedSt || assignedRt || undefined,
          ma_nx_st: assignedSt?.docCode || null,
          ma_nx_rt: assignedRt?.docCode || null,
        };
      }),
    );

    // 7. Apply Card Data (Group Level logic applied to list)
    if (cardData) {
      this.n8nService.mapIssuePartnerCodeToSales(enrichedSales, cardData);
    }

    // 8. Enrich ma_vt_ref (Voucher logic)
    await this.voucherIssueService.enrichSalesWithMaVtRef(
      enrichedSales,
      loyaltyProductMap,
    );

    return enrichedSales;
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
  /**
   * Enrich orders với cashio data
   * Logic: 1 sale item with N stock transfers → N exploded sale lines
   */
  async enrichOrdersWithCashio(
    orders: any[],
    preFilteredStockTransfers?: StockTransfer[],
    prefetchedData?: {
      cashioRecords?: DailyCashio[];
      loyaltyProductMap?: Map<string, any>;
      warehouseCodeMap?: Map<string, string>;
      skipMaVtRef?: boolean;
    },
  ): Promise<any[]> {
    const docCodes = orders.map((o) => o.docCode);
    if (docCodes.length === 0) return orders;

    // [OPTIMIZATION] Use pre-fetched cashio records if available
    const cashioRecords =
      prefetchedData?.cashioRecords ??
      (await this.dailyCashioRepository
        .createQueryBuilder('cashio')
        .where('cashio.so_code IN (:...docCodes)', { docCodes })
        .orWhere('cashio.master_code IN (:...docCodes)', { docCodes })
        .getMany());

    // OPTIMIZED: Build map in single pass O(m) instead of nested loop O(n×m)
    const cashioMap = new Map<string, DailyCashio[]>();
    cashioRecords.forEach((cashio) => {
      const docCode = cashio.so_code || cashio.master_code;
      if (!docCode) return;

      if (!cashioMap.has(docCode)) {
        cashioMap.set(docCode, []);
      }
      cashioMap.get(docCode)!.push(cashio);
    });

    // Use pre-filtered stock transfers if provided, otherwise fetch all
    let stockTransfers: StockTransfer[];
    if (preFilteredStockTransfers) {
      stockTransfers = preFilteredStockTransfers;
    } else {
      // Fetch stock transfers để thêm thông tin stock transfer
      // [FIX] Sử dụng helper để lấy cả mã đơn gốc cho đơn RT
      const docCodesForST =
        StockTransferUtils.getDocCodesForStockTransfer(docCodes);
      stockTransfers = await this.stockTransferRepository.find({
        where: [
          { soCode: In(docCodesForST) },
          { docCode: In(docCodesForST) },
        ],
      });
    }

    const stockTransferMap = new Map<string, StockTransfer[]>();
    docCodes.forEach((docCode) => {
      // [FIX] Logic tìm ST cho đơn hàng (hỗ trợ đơn RT)
      let originalOrderCode = null;
      if (docCode.startsWith('RT')) {
        originalOrderCode = docCode.replace(/^RT/, 'SO').replace(/_\d+$/, '');
      }

      const matchingTransfers = stockTransfers.filter(
        (st) =>
          st.soCode === docCode ||
          st.docCode === docCode ||
          (originalOrderCode && st.soCode === originalOrderCode),
      );
      if (matchingTransfers.length > 0) {
        stockTransferMap.set(docCode, matchingTransfers);
      }
    });

    // 3. Pre-fetch product info for ALL items (Sales + Stock Transfers)
    // to determine Batch vs Serial AND for ma_vt_ref enrichment
    const allItemCodes = new Set<string>();
    orders.forEach((order) => {
      order.sales?.forEach((sale: any) => {
        if (sale.itemCode) allItemCodes.add(sale.itemCode);
      });
    });
    stockTransfers.forEach((st) => {
      if (st.itemCode) allItemCodes.add(st.itemCode);
    });

    // [OPTIMIZATION] Use pre-fetched product map if available
    const loyaltyProductMap =
      prefetchedData?.loyaltyProductMap ??
      (await this.loyaltyService.fetchProducts(Array.from(allItemCodes)));

    // [OPTIMIZATION] Use pre-fetched warehouse code map if available
    let warehouseCodeMap: Map<string, string>;
    if (prefetchedData?.warehouseCodeMap) {
      warehouseCodeMap = prefetchedData.warehouseCodeMap;
    } else {
      const allStockCodes = Array.from(
        new Set(
          stockTransfers
            .map((st) => st.stockCode)
            .filter((code): code is string => !!code),
        ),
      );
      warehouseCodeMap = new Map<string, string>();
      await Promise.all(
        allStockCodes.map(async (code) => {
          const mapped = await this.categoriesService.mapWarehouseCode(code);
          if (mapped) {
            warehouseCodeMap.set(code, mapped);
          }
        }),
      );
    }

    const enrichedOrders = orders.map((order) => {
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
        if (SalesUtils.isTrutonkeepItem(st.itemCode)) {
          return false;
        }

        // Chỉ lấy các record có ioType = 'O' (Output - Xuất kho)
        // Bỏ qua các record ioType = 'I' (Input - Nhập/Trả lại)
        // User Request: "Các đơn hàng khi join với bên stocktranfer chỉ khớp với các đơn xuất kho ioType: O"
        const isStockOut = st.ioType === 'O';
        return isStockOut;
      });

      const uniqueStockTransfers = stockOutTransfers;

      // Explode sales based on stock transfers (Stock Transfer is Root)
      const explodedSales: any[] = [];
      const usedSalesIds = new Set<string>();

      if (uniqueStockTransfers.length > 0) {
        // OPTIMIZATION: Pre-build Map for O(1) lookup instead of O(n) find()
        // Build map: itemCode (lowercase) -> Sale[]
        const saleByItemCodeMap = new Map<string, any[]>();
        (order.sales || []).forEach((sale: any) => {
          if (!sale.itemCode) return;
          const key = sale.itemCode.toLowerCase().trim();
          if (!saleByItemCodeMap.has(key)) {
            saleByItemCodeMap.set(key, []);
          }
          saleByItemCodeMap.get(key)!.push(sale);
        });

        // 1. Map Stock Transfers to Sales Lines
        uniqueStockTransfers.forEach((st) => {
          // Find matching product info
          const product = loyaltyProductMap.get(st.itemCode);
          const isSerial = !!product?.trackSerial;
          const isBatch = !!product?.trackBatch;

          // OPTIMIZED: O(1) lookup instead of O(n) find()
          // Try matching by itemCode first
          const itemKey = st.itemCode?.toLowerCase().trim();
          let matchingSales = itemKey ? saleByItemCodeMap.get(itemKey) : null;

          // If no match and materialCode exists, try matching by materialCode
          // This handles vouchers where Sale.itemCode = materialCode (e.g., E.M00033A)
          // but StockTransfer.itemCode = original code (e.g., E_JUPTD011A)
          if (!matchingSales && st.materialCode) {
            const materialKey = st.materialCode.toLowerCase().trim();
            matchingSales = saleByItemCodeMap.get(materialKey);
          }

          // Find first unused sale, or fallback to any sale
          let sale: any = null;
          if (matchingSales && matchingSales.length > 0) {
            const stQty = Math.abs(Number(st.qty || 0));

            // [IMPROVED] Best Fit Strategy: Prioritize exact quantity match
            // This handles cases with duplicate item codes (e.g. 1 gift line (Qty 1) + 1 paid line (Qty 5))
            sale = matchingSales.find(
              (s: any) =>
                !usedSalesIds.has(s.id) &&
                Math.abs(Number(s.qty || 0)) === stQty,
            );

            // If no exact match, fallback to first unused
            if (!sale) {
              sale = matchingSales.find((s: any) => !usedSalesIds.has(s.id));
            }

            // If all used, take first one (legacy behavior for 1 sale -> N splits)
            if (!sale) {
              sale = matchingSales[0];
            }
          }

          if (sale) {
            usedSalesIds.add(sale.id);
            const oldQty = Number(sale.qty || 1) || 1; // Avoid divide by zero
            const newQty = Math.abs(Number(st.qty || 0));
            const ratio = newQty / oldQty;

            explodedSales.push({
              ...sale,
              id: st.id || sale.id, // Use ST id if available
              qty: newQty, // Update Qty
              // Recalculate financial fields based on ratio
              revenue: Number(sale.revenue || 0) * ratio,
              tienHang: Number(sale.tienHang || 0) * ratio,
              linetotal: Number(sale.linetotal || 0) * ratio,
              // Update discount fields if proportional
              discount: Number(sale.discount || 0) * ratio,
              chietKhauMuaHangGiamGia:
                Number(sale.chietKhauMuaHangGiamGia || 0) * ratio,
              other_discamt: Number(sale.other_discamt || 0) * ratio,
              disc_amt: Number(sale.disc_amt || 0) * ratio,
              disc_tm: Number(sale.disc_tm || 0) * ratio,
              grade_discamt: Number(sale.grade_discamt || 0) * ratio,
              revenue_wsale: Number(sale.revenue_wsale || 0) * ratio,
              revenue_retail: Number(sale.revenue_retail || 0) * ratio,

              // [NEW] Proportional Discount Allocation for FAST API fields
              ck01_nt: Number(sale.ck01_nt || 0) * ratio,
              ck02_nt: Number(sale.ck02_nt || 0) * ratio,
              ck03_nt: Number(sale.ck03_nt || 0) * ratio,
              ck04_nt: Number(sale.ck04_nt || 0) * ratio,
              ck05_nt: Number(sale.ck05_nt || 0) * ratio,
              ck06_nt: Number(sale.ck06_nt || 0) * ratio,
              ck07_nt: Number(sale.ck07_nt || 0) * ratio,
              ck08_nt: Number(sale.ck08_nt || 0) * ratio,
              ck09_nt: Number(sale.ck09_nt || 0) * ratio,
              ck10_nt: Number(sale.ck10_nt || 0) * ratio,
              ck11_nt: Number(sale.ck11_nt || 0) * ratio,

              // [NEW] Proportional Discount Allocation for UI Display
              ck01Nt: Number(sale.ck01Nt || 0) * ratio,
              ck02Nt: Number(sale.ck02Nt || 0) * ratio,
              ck03Nt: Number(sale.ck03Nt || 0) * ratio,
              ck04Nt: Number(sale.ck04Nt || 0) * ratio,
              ck05Nt: Number(sale.ck05Nt || 0) * ratio,
              ck06Nt: Number(sale.ck06Nt || 0) * ratio,
              ck07Nt: Number(sale.ck07Nt || 0) * ratio,
              ck08Nt: Number(sale.ck08Nt || 0) * ratio,
              ck09Nt: Number(sale.ck09Nt || 0) * ratio,
              ck10Nt: Number(sale.ck10Nt || 0) * ratio,
              ck11Nt: Number(sale.ck11Nt || 0) * ratio,

              disc_ctkm: Number(sale.disc_ctkm || 0) * ratio,

              itemcost: Number(sale.itemcost || 0) * ratio,
              totalcost: Number(sale.totalcost || 0) * ratio,
              ck_tm: Number(sale.ck_tm || 0) * ratio,
              ck_dly: Number(sale.ck_dly || 0) * ratio,
              paid_by_voucher_ecode_ecoin_bp:
                Number(sale.paid_by_voucher_ecode_ecoin_bp || 0) * ratio,
              chietKhauCkTheoChinhSach:
                Number(sale.chietKhauCkTheoChinhSach || 0) * ratio,
              chietKhauMuaHangCkVip:
                Number(sale.chietKhauMuaHangCkVip || 0) * ratio,
              chietKhauThanhToanCoupon:
                Number(sale.chietKhauThanhToanCoupon || 0) * ratio,
              chietKhauThanhToanVoucher:
                Number(sale.chietKhauThanhToanVoucher || 0) * ratio,
              chietKhauDuPhong1: Number(sale.chietKhauDuPhong1 || 0) * ratio,
              chietKhauDuPhong2: Number(sale.chietKhauDuPhong2 || 0) * ratio,
              chietKhauDuPhong3: Number(sale.chietKhauDuPhong3 || 0) * ratio,
              chietKhauHang: Number(sale.chietKhauHang || 0) * ratio,
              chietKhauThuongMuaBangHang:
                Number(sale.chietKhauThuongMuaBangHang || 0) * ratio,
              chietKhauThanhToanTkTienAo:
                Number(sale.chietKhauThanhToanTkTienAo || 0) * ratio,
              chietKhauThem1: Number(sale.chietKhauThem1 || 0) * ratio,
              chietKhauThem2: Number(sale.chietKhauThem2 || 0) * ratio,
              chietKhauThem3: Number(sale.chietKhauThem3 || 0) * ratio,
              chietKhauVoucherDp1:
                Number(sale.chietKhauVoucherDp1 || 0) * ratio,
              chietKhauVoucherDp2:
                Number(sale.chietKhauVoucherDp2 || 0) * ratio,
              chietKhauVoucherDp3:
                Number(sale.chietKhauVoucherDp3 || 0) * ratio,
              chietKhauVoucherDp4:
                Number(sale.chietKhauVoucherDp4 || 0) * ratio,
              chietKhauVoucherDp5:
                Number(sale.chietKhauVoucherDp5 || 0) * ratio,
              chietKhauVoucherDp6:
                Number(sale.chietKhauVoucherDp6 || 0) * ratio,
              chietKhauVoucherDp7:
                Number(sale.chietKhauVoucherDp7 || 0) * ratio,
              chietKhauVoucherDp8:
                Number(sale.chietKhauVoucherDp8 || 0) * ratio,
              troGia: Number(sale.troGia || 0) * ratio,
              tienThue: Number(sale.tienThue || 0) * ratio,
              dtTgNt: Number(sale.dtTgNt || 0) * ratio,

              maKho: warehouseCodeMap.get(st.stockCode) || st.stockCode,
              // Logic check trackBatch/trackSerial
              maLo: isBatch ? st.batchSerial : undefined,
              soSerial: isSerial ? st.batchSerial : undefined,
              // Store original itemCode from StockTransfer for voucher lookup
              originalItemCode: st.itemCode,

              isStockTransferLine: true,
              stockTransferId: st.id,
              stockTransfer:
                StockTransferUtils.formatStockTransferForFrontend(st),
              stockTransfers: undefined,
            });
          } else {
            // If no sale found, create a pseudo-sale line from ST
            explodedSales.push({
              // Minimal Sale structure
              docCode: order.docCode,
              itemCode: st.itemCode,
              itemName: st.itemName,
              qty: Math.abs(Number(st.qty || 0)),
              maKho: warehouseCodeMap.get(st.stockCode) || st.stockCode,

              // Logic check trackBatch/trackSerial
              maLo: isBatch ? st.batchSerial : undefined,
              soSerial: isSerial ? st.batchSerial : undefined,

              isStockTransferLine: true,
              stockTransferId: st.id,
              stockTransfer:
                StockTransferUtils.formatStockTransferForFrontend(st),
              stockTransfers: undefined,
              price: 0, // Unknown
              revenue: 0,
            });
          }
        });

        // 2. Add remaining Sales lines (e.g. Services) that were not matched by any ST
        (order.sales || []).forEach((sale: any) => {
          if (!usedSalesIds.has(sale.id)) {
            explodedSales.push(sale);
          }
        });
      } else {
        explodedSales.push(...(order.sales || []));
      }

      return {
        ...order,
        sales: explodedSales,
        cashioData: cashioRecords.length > 0 ? cashioRecords : null,
        cashioFopSyscode: selectedCashio?.fop_syscode || null,
        cashioFopDescription: selectedCashio?.fop_description || null,
        cashioCode: selectedCashio?.code || null,
        cashioMasterCode: selectedCashio?.master_code || null,
        cashioTotalIn: selectedCashio?.total_in || null,
        cashioTotalOut: selectedCashio?.total_out || null,
      };
    });

    // 4. Enrich ma_vt_ref (Voucher logic) for all exploded lines
    const allExplodedSales: any[] = [];
    enrichedOrders.forEach((order) => {
      if (order.sales) {
        allExplodedSales.push(...order.sales);
      }
    });

    // [OPTIMIZATION] Skip if caller already handles enrichWithMaVtRef post-explosion
    if (allExplodedSales.length > 0 && !prefetchedData?.skipMaVtRef) {
      await this.voucherIssueService.enrichSalesWithMaVtRef(
        allExplodedSales,
        loyaltyProductMap,
      );
    }

    return enrichedOrders;
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
   * Persist FastApiInvoice record
   */
  async saveFastApiInvoice(data: {
    docCode: string;
    maDvcs?: string;
    maKh?: string;
    tenKh?: string;
    ngayCt?: Date;
    status: number;
    guid?: string | null;
    fastApiResponse?: string;
    payload?: string;
    lastErrorMessage?: string;
    xemNhanh?: string; // [New]
  }): Promise<FastApiInvoice> {
    try {
      let invoice = await this.fastApiInvoiceRepository.findOne({
        where: { docCode: data.docCode },
      });

      if (invoice) {
        // Update existing
        invoice.status = data.status;
        if (data.maDvcs !== undefined) invoice.maDvcs = data.maDvcs;
        if (data.maKh !== undefined) invoice.maKh = data.maKh;
        if (data.tenKh !== undefined) invoice.tenKh = data.tenKh;
        if (data.ngayCt !== undefined) invoice.ngayCt = data.ngayCt;
        if (data.guid !== undefined) invoice.guid = data.guid || '';
        if (data.fastApiResponse !== undefined)
          invoice.fastApiResponse = data.fastApiResponse;
        if (data.payload !== undefined) invoice.payload = data.payload;
        if (data.lastErrorMessage !== undefined)
          invoice.lastErrorMessage = data.lastErrorMessage;
        if (data.xemNhanh !== undefined) invoice.xemNhanh = data.xemNhanh; // [New]
      } else {
        // Create new
        invoice = this.fastApiInvoiceRepository.create({
          docCode: data.docCode,
          maDvcs: data.maDvcs,
          maKh: data.maKh,
          tenKh: data.tenKh,
          ngayCt: data.ngayCt || new Date(),
          status: data.status,
          guid: data.guid || '',
          fastApiResponse: data.fastApiResponse,
          payload: data.payload,
          lastErrorMessage: data.lastErrorMessage,
          xemNhanh: data.xemNhanh, // [New]
        });
      }

      const saved = await this.fastApiInvoiceRepository.save(invoice);
      return Array.isArray(saved) ? saved[0] : saved;
    } catch (error: any) {
      this.logger.error(
        `Error saving FastApiInvoice for ${data.docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Update isProcessed status for sales
   */
  async markOrderAsProcessed(docCode: string): Promise<void> {
    const sales = await this.saleRepository.find({
      where: { docCode },
    });
    if (sales.length > 0) {
      await this.saleRepository.update({ docCode }, { isProcessed: true });
    }
  }

  /**
   * Retroactive fix: Mark processed orders based on existing invoices
   * OPTIMIZED: Batch fetching to eliminate N+1 queries (was 3000+ queries, now 2 queries)
   */
  async markProcessedOrdersFromInvoices(): Promise<{
    updated: number;
    message: string;
  }> {
    // 1. Fetch all invoices (Query 1)
    const invoices = await this.invoiceRepository.find({
      where: { isPrinted: true },
    });

    // 2. Extract all potential docCodes from invoices (in-memory processing)
    const potentialDocCodes = new Set<string>();

    for (const invoice of invoices) {
      // Add invoice.key as potential docCode
      if (invoice.key) {
        potentialDocCodes.add(invoice.key);
      }

      // Parse printResponse to extract additional potential docCodes
      try {
        if (invoice.printResponse) {
          const printResponse = JSON.parse(invoice.printResponse);

          // Extract from Message field
          if (printResponse.Message) {
            try {
              const messageData = JSON.parse(printResponse.Message);
              if (Array.isArray(messageData) && messageData.length > 0) {
                const data = messageData[0];
                if (data.key) {
                  const keyParts = data.key.split('_');
                  if (keyParts.length > 0) {
                    potentialDocCodes.add(keyParts[0]);
                  }
                }
              }
            } catch (msgError) {
              // Ignore parse errors
            }
          }

          // Extract from Data field
          if (
            printResponse.Data &&
            Array.isArray(printResponse.Data) &&
            printResponse.Data.length > 0
          ) {
            const data = printResponse.Data[0];
            if (data.key) {
              const keyParts = data.key.split('_');
              if (keyParts.length > 0) {
                potentialDocCodes.add(keyParts[0]);
              }
            }
          }
        }
      } catch (error) {
        // Ignore parse errors
      }
    }

    // 3. Batch fetch all sales with these docCodes (Query 2)
    const allDocCodes = Array.from(potentialDocCodes);
    const existingSales = await this.saleRepository.find({
      where: { docCode: In(allDocCodes) },
      select: ['docCode'], // Only need docCode for validation
    });

    // 4. Build a Set of valid docCodes for O(1) lookup
    const validDocCodesSet = new Set(existingSales.map((sale) => sale.docCode));

    // 5. Process invoices with pre-fetched data (same logic as before, but in-memory)
    const processedDocCodes = new Set<string>();

    for (const invoice of invoices) {
      let docCode: string | null = null;

      // Check invoice.key first (same priority as before)
      if (invoice.key && validDocCodesSet.has(invoice.key)) {
        docCode = invoice.key;
      } else {
        // Parse printResponse to find alternative docCode (same logic as before)
        try {
          if (invoice.printResponse) {
            const printResponse = JSON.parse(invoice.printResponse);

            // Try Message field first
            if (printResponse.Message) {
              try {
                const messageData = JSON.parse(printResponse.Message);
                if (Array.isArray(messageData) && messageData.length > 0) {
                  const data = messageData[0];
                  if (data.key) {
                    const keyParts = data.key.split('_');
                    if (keyParts.length > 0) {
                      const potentialDocCode = keyParts[0];
                      if (validDocCodesSet.has(potentialDocCode)) {
                        docCode = potentialDocCode;
                      }
                    }
                  }
                }
              } catch (msgError) {
                // Ignore
              }
            }

            // Try Data field if Message didn't work (same logic as before)
            if (
              !docCode &&
              printResponse.Data &&
              Array.isArray(printResponse.Data) &&
              printResponse.Data.length > 0
            ) {
              const data = printResponse.Data[0];
              if (data.key) {
                const keyParts = data.key.split('_');
                if (keyParts.length > 0) {
                  const potentialDocCode = keyParts[0];
                  if (validDocCodesSet.has(potentialDocCode)) {
                    docCode = potentialDocCode;
                  }
                }
              }
            }
          }
        } catch (error) {
          // Ignore
        }
      }

      // Collect valid docCodes (same logic as before)
      if (docCode && !processedDocCodes.has(docCode)) {
        processedDocCodes.add(docCode);
      }
    }

    // 6. Batch update all processed docCodes in a single query
    let updatedCount = 0;
    if (processedDocCodes.size > 0) {
      const updateResult = await this.saleRepository.update(
        { docCode: In(Array.from(processedDocCodes)) },
        { isProcessed: true },
      );
      updatedCount = updateResult.affected || 0;
    }

    return {
      updated: updatedCount,
      message: `Đã đánh dấu ${processedDocCodes.size} đơn hàng là đã xử lý (${updatedCount} sale records)`,
    };
  }

  /**
   * Helper to apply common filters to sales query
   * REFACTORED: Delegated to SalesFilterService
   */
  private applySaleFilters(
    query: SelectQueryBuilder<Sale>,
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
  ): void {
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
      // Use sale.brand directly instead of joining customer
      query.andWhere('sale.brand = :brand', { brand });
    }

    if (search && search.trim() !== '') {
      const searchPattern = `%${search.trim().toLowerCase()}%`;
      // Searching by customer fields requires customer join
      query.andWhere(
        "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
        { search: searchPattern },
      );
    }

    // Date logic - Filter by Export Date (StockTransfer.transDate)
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

    // Join StockTransfer to filter by export date (transDate)
    // OPTIMIZATION: Skip date filter if searching by docCode (search param)
    // When searching for specific order, we want ALL items regardless of stock transfer date
    if ((startDate || endDate || date) && !search) {
      query.innerJoin(
        StockTransfer,
        'st_filter',
        '(st_filter.soCode = sale.docCode OR st_filter.docCode = sale.docCode) AND st_filter.itemCode = sale.itemCode',
      );

      if (startDate && endDate) {
        query.andWhere(
          'st_filter.transDate >= :startDate AND st_filter.transDate <= :endDate',
          {
            startDate,
            endDate,
          },
        );
      } else if (startDate) {
        query.andWhere('st_filter.transDate >= :startDate', { startDate });
      } else if (endDate) {
        query.andWhere('st_filter.transDate <= :endDate', { endDate });
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
              'st_filter.transDate >= :startDate AND st_filter.transDate <= :endDate',
              {
                startDate: startOfDay,
                endDate: endOfDay,
              },
            );
          }
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
      limit = 10,
      date,
      dateFrom,
      dateTo,
      search,
      statusAsys,
      export: isExport,
      typeSale,
    } = options;

    // [OPTIMIZATION] Skip expensive COUNT query to avoid timeout
    // Instead, we'll fetch limit+1 and check if there are more results
    // This eliminates 10-15s COUNT overhead on large date ranges
    let totalOrders = -1; // Unknown total (will be calculated after fetch)

    // 2. Main Query
    const fullQuery = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer') // Need this for order grouping logic and response
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.docCode', 'ASC')
      .addOrderBy('sale.id', 'ASC');

    // [OPTIMIZATION] In pagination mode, skip date params to avoid INNER JOIN on fullQuery
    // DocCodes are already date-filtered by the two-step ST approach
    // For search/export modes, date params are still needed
    const isSearchMode = !!search;
    const needsDateJoinOnFullQuery = isExport || isSearchMode;
    this.applySaleFilters(fullQuery, {
      brand,
      isProcessed,
      statusAsys,
      typeSale,
      ...(needsDateJoinOnFullQuery ? { date, dateFrom, dateTo } : {}),
      search,
    });

    let allSales: Sale[];

    if (!isExport && !isSearchMode) {
      // === [OPTIMIZATION] Direct sale.docDate filtering ===
      // Previously used stock_transfers.transDate pre-filter (~15s full table scan, no index).
      // Now filter by sale.docDate directly (fast, on sales table).
      // Explosion step still uses actual stock_transfer data for accuracy.

      // Build docCode pagination query
      const docCodeSubquery = this.saleRepository
        .createQueryBuilder('sale')
        .select('sale.docCode', 'docCode')
        .addSelect('MAX(sale.docDate)', 'docDate')
        .groupBy('sale.docCode')
        .orderBy('MAX(sale.docDate)', 'DESC')
        .addOrderBy('sale.docCode', 'ASC');

      if (search && search.trim() !== '') {
        docCodeSubquery.leftJoin('sale.customer', 'customer');
      }

      // Apply non-date filters (brand, search, status, etc.)
      this.applySaleFilters(docCodeSubquery, {
        brand,
        isProcessed,
        statusAsys,
        typeSale,
        search,
      });

      // [OPTIMIZATION] Filter by sale.docDate directly (no stock_transfer JOIN)
      if ((dateFrom || dateTo || date) && !search) {
        let startDate: Date | undefined;
        let endDate: Date | undefined;

        if (dateFrom) {
          startDate = new Date(dateFrom);
          startDate.setHours(0, 0, 0, 0);
        }
        if (dateTo) {
          endDate = new Date(dateTo);
          endDate.setHours(23, 59, 59, 999);
        }

        if (date && !startDate && !endDate) {
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
              startDate = new Date(dateObj);
              startDate.setHours(0, 0, 0, 0);
              endDate = new Date(dateObj);
              endDate.setHours(23, 59, 59, 999);
            }
          }
        }

        if (startDate && endDate) {
          docCodeSubquery.andWhere(
            'sale.docDate >= :docDateStart AND sale.docDate <= :docDateEnd',
            {
              docDateStart: startDate,
              docDateEnd: endDate,
            },
          );
        } else if (startDate) {
          docCodeSubquery.andWhere('sale.docDate >= :docDateStart', {
            docDateStart: startDate,
          });
        } else if (endDate) {
          docCodeSubquery.andWhere('sale.docDate <= :docDateEnd', {
            docDateEnd: endDate,
          });
        }
      }

      const offset = (page - 1) * limit;
      docCodeSubquery.skip(offset).take(limit + 1);

      // Build parallel COUNT query (same filters, no pagination)
      const countQuery = this.saleRepository
        .createQueryBuilder('sale')
        .select('COUNT(DISTINCT sale.docCode)', 'total');
      this.applySaleFilters(countQuery, {
        brand,
        isProcessed,
        statusAsys,
        typeSale,
        search,
      });

      // Apply same date filter to count query
      if ((dateFrom || dateTo || date) && !search) {
        let startDate: Date | undefined;
        let endDate: Date | undefined;
        if (dateFrom) {
          startDate = new Date(dateFrom);
          startDate.setHours(0, 0, 0, 0);
        }
        if (dateTo) {
          endDate = new Date(dateTo);
          endDate.setHours(23, 59, 59, 999);
        }

        if (date && !startDate && !endDate) {
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
              startDate = new Date(dateObj);
              startDate.setHours(0, 0, 0, 0);
              endDate = new Date(dateObj);
              endDate.setHours(23, 59, 59, 999);
            }
          }
        }

        if (startDate && endDate) {
          countQuery.andWhere(
            'sale.docDate >= :docDateStart AND sale.docDate <= :docDateEnd',
            {
              docDateStart: startDate,
              docDateEnd: endDate,
            },
          );
        } else if (startDate) {
          countQuery.andWhere('sale.docDate >= :docDateStart', {
            docDateStart: startDate,
          });
        } else if (endDate) {
          countQuery.andWhere('sale.docDate <= :docDateEnd', {
            docDateEnd: endDate,
          });
        }
      }

      // Run pagination + COUNT in parallel
      const [docCodeResults, countResult] = await Promise.all([
        docCodeSubquery.getRawMany(),
        countQuery.getRawOne(),
      ]);

      totalOrders = parseInt(countResult?.total || '0', 10);

      const trimmedResults =
        docCodeResults.length > limit
          ? docCodeResults.slice(0, limit)
          : docCodeResults;
      const docCodes = trimmedResults.map((r) => r.docCode);

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

    // Create a set of (soCode, itemCode) pairs from filtered sales
    const saleItemKeys = new Set(
      allSalesData.map((sale) => `${sale.docCode}_${sale.itemCode}`),
    );

    // === [OPTIMIZATION] Collect identifiers for parallel fetching ===
    const svcCodes = Array.from(
      new Set(
        allSalesData
          .map((sale) => sale.svc_code)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    // Prepare employee check params early (only depends on allSalesData)
    const firstSaleForBrand = allSalesData[0];
    const brandForEmployeeCheck =
      firstSaleForBrand?.customer?.brand ||
      firstSaleForBrand?.brand ||
      'menard';
    const partnerCodesToCheckForAll = Array.from(
      new Set(
        allSalesData
          .flatMap((s) => [s.partnerCode, (s as any).issuePartnerCode])
          .filter((c): c is string => !!c && c.trim() !== ''),
      ),
    ).map((partnerCode) => ({
      partnerCode,
      sourceCompany: brandForEmployeeCheck,
    }));

    // === [OPTIMIZATION] Run ALL independent fetches in parallel ===
    // Previously these were 7+ sequential calls (~5-8s). Now runs in parallel (~1-2s).
    const [
      allStockTransfers,
      departmentMap,
      warehouseCodeMap,
      svcCodeMap,
      orderFeesRaw,
      isEmployeeMapForAll,
      cardDataForFirst,
      cashioRecords,
    ] = await Promise.all([
      // 1. Stock transfers
      docCodesForStockTransfer.length > 0
        ? this.stockTransferRepository.find({
          where: { soCode: In(docCodesForStockTransfer) },
        })
        : Promise.resolve([] as StockTransfer[]),
      // 2. Departments
      this.loyaltyService.fetchLoyaltyDepartments(branchCodes),
      // 3. Warehouse codes
      this.categoriesService.getWarehouseCodeMap(),
      // 4. SVC code -> materialCode mapping
      svcCodes.length > 0
        ? this.loyaltyService.fetchMaterialCodesBySvcCodes(svcCodes)
        : Promise.resolve(new Map<string, string>()),
      // 5. Order fees
      docCodes.length > 0
        ? this.orderFeeRepository.find({
          where: { erpOrderCode: In(docCodes) },
        })
        : Promise.resolve([] as OrderFee[]),
      // 6. Employee status check
      this.n8nService.checkCustomersIsEmployee(partnerCodesToCheckForAll),
      // 7. N8n card data for first order
      docCodes.length > 0
        ? this.n8nService.fetchCardData(docCodes[0]).catch(() => [null])
        : Promise.resolve([null] as any[]),
      // 8. [NEW] DailyCashio records — replaces redundant enrichOrdersWithCashio call #1
      // Previously called enrichOrdersWithCashio which re-fetched ST + products from DB/API
      docCodes.length > 0
        ? this.dailyCashioRepository
          .createQueryBuilder('cashio')
          .where('cashio.so_code IN (:...docCodes)', { docCodes })
          .orWhere('cashio.master_code IN (:...docCodes)', { docCodes })
          .getMany()
        : Promise.resolve([] as DailyCashio[]),
    ]);

    // Process stock transfers (depends on allStockTransfers result)
    // Support both SO and RT orders by checking both soCode and docCode
    const stockTransfers = allStockTransfers.filter(
      (st) =>
        saleItemKeys.has(`${st.soCode}_${st.itemCode}`) ||
        saleItemKeys.has(`${st.docCode}_${st.itemCode}`),
    );
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

    // Fetch products AFTER stock transfers (needs allItemCodes including ST item codes)

    const loyaltyProductMap =
      await this.loyaltyService.fetchProducts(allItemCodes);

    // Build OrderFee map
    const orderFeeMap = new Map<string, OrderFee>();
    orderFeesRaw.forEach((fee) => {
      orderFeeMap.set(fee.erpOrderCode, fee);
    });

    // Enrich sales with platform voucher data (VC CTKM SÀN)
    allSalesData.forEach((sale) => {
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

    // Build Stock Transfer Maps

    // Pre-build map for O(1) lookup instead of filter in loop

    // 6. Card Data Logic (preserved legacy behavior)
    const getMaThe = new Map<string, string>();
    const [dataCard] = cardDataForFirst;
    if (dataCard && dataCard.data) {
      const serviceItemNames = dataCard.data
        .map((card) => card?.service_item_name)
        .filter((name): name is string => !!name && name.trim() !== '');
      const productMap =
        await this.loyaltyService.checkProductsBatch(serviceItemNames);
      for (const card of dataCard.data) {
        if (!card?.service_item_name || !card?.serial) {
          continue;
        }
        const itemProduct = productMap.get(card.service_item_name);
        if (itemProduct) {
          getMaThe.set(itemProduct.materialCode, card.serial);
        }
      }
    }

    // 7. Enrich Sales Items (In-Memory Processing)
    const enrichedSalesMap = new Map<string, any[]>();
    for (const sale of allSalesData) {
      const docCode = sale.docCode;
      if (!enrichedSalesMap.has(docCode)) {
        enrichedSalesMap.set(docCode, []);
      }
      enrichedSalesMap.get(docCode)!.push(sale);
    }

    // [OPTIMIZATION] Build orderCashioMap directly from queried records
    // Replaces enrichOrdersWithCashio call #1 which redundantly re-fetched ST and products
    const orderCashioMap = new Map<string, any[]>();
    cashioRecords.forEach((cashio) => {
      const docCode = cashio.so_code || cashio.master_code;
      if (!docCode) return;
      if (!orderCashioMap.has(docCode)) {
        orderCashioMap.set(docCode, []);
      }
      orderCashioMap.get(docCode)!.push(cashio);
    });

    // [FIX] Robust 1-1 Stock Transfer Matching Logic (Batch for all orders)
    // Refactored to private method for clarity and debugging
    const saleIdToStockTransferMap = this.matchStockTransfersForOrders(
      enrichedSalesMap,
      stockTransfers,
    );

    // 8. Format Sales for Frontend
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

          const maThe = getMaThe.get(loyaltyProduct?.materialCode || '') || '';
          sale.maThe = maThe;
          const saleMaterialCode = loyaltyProduct?.materialCode;

          // [FIX] Use Pre-assigned Stock Transfers
          const { st: assignedSt, rt: assignedRt } =
            saleIdToStockTransferMap.get(sale.id) || { st: null, rt: null };

          let saleStockTransfers: StockTransfer[] = [];
          if (assignedSt) saleStockTransfers.push(assignedSt);
          if (assignedRt) saleStockTransfers.push(assignedRt);

          // [FIX] Resolve MaKho from assigned ST directly (check both ST and RT)
          const stockTransfer = assignedSt || assignedRt;
          let maKhoFromStockTransfer = '';
          if (stockTransfer?.stockCode) {
            maKhoFromStockTransfer =
              warehouseCodeMap.get(stockTransfer.stockCode) ||
              stockTransfer.stockCode;
          }

          // [NEW] Inject Cashio Data for Discount Calculation
          const cashioData = orderCashioMap.get(docCode);

          const calculatedFields = await InvoiceLogicUtils.calculateSaleFields(
            sale,
            loyaltyProduct,
            department,
            sale.branchCode,
          );
          calculatedFields.maKho = maKhoFromStockTransfer;

          // [NEW] Recalculate Discount Fields based on Cashio Logic (ECOIN vs VOUCHER)
          // Use calculateInvoiceAmounts logic re-applied here
          // This is critical to sync GET API with Payload
          const overrideDiscount: any = {};

          if (cashioData && cashioData.length > 0) {
            const ecoinRecord = cashioData.find(
              (c: any) =>
                String(c.fop_syscode).trim().toUpperCase() === 'ECOIN',
            );
            const voucherRecord = cashioData.find(
              (c: any) =>
                String(c.fop_syscode).trim().toUpperCase() === 'VOUCHER',
            );

            if (ecoinRecord) {
              // ECOIN -> ck11
              if (
                InvoiceLogicUtils.toNumber(
                  sale.paid_by_voucher_ecode_ecoin_bp,
                  0,
                ) > 0
              ) {
                const amount = InvoiceLogicUtils.toNumber(
                  sale.paid_by_voucher_ecode_ecoin_bp,
                  0,
                );

                // [FIX] Determine ma_ck11 based on Brand & Product Type
                const currentBrand = (sale.brand || '').trim().toUpperCase();
                const pType = (sale.productType || '').toUpperCase();

                const maCk11 = InvoiceLogicUtils.resolveMaCk11({
                  brand: currentBrand,
                  productType: pType,
                });

                overrideDiscount.ck11_nt = amount;
                overrideDiscount.ck05_nt = 0;
                overrideDiscount.ma_ck11 = maCk11;
                overrideDiscount.ma_ck05 = null;

                // Also override display fields for UI consistency
                overrideDiscount.ck11Nt = amount;
                overrideDiscount.ck05Nt = 0;
                overrideDiscount.maCk11 = maCk11;
                overrideDiscount.maCk05 = null;
              }
            } else if (voucherRecord) {
              // VOUCHER -> ck05
              if (
                InvoiceLogicUtils.toNumber(
                  sale.paid_by_voucher_ecode_ecoin_bp,
                  0,
                ) > 0
              ) {
                const amount = InvoiceLogicUtils.toNumber(
                  sale.paid_by_voucher_ecode_ecoin_bp,
                  0,
                );
                overrideDiscount.ck05_nt = amount;
                overrideDiscount.ck11_nt = 0;
                overrideDiscount.ma_ck05 = 'VOUCHER';
                overrideDiscount.ma_ck11 = null;

                // Also override display fields for UI consistency
                overrideDiscount.ck05Nt = amount;
                overrideDiscount.ck11Nt = 0;
                overrideDiscount.maCk05 = 'VOUCHER';
                overrideDiscount.maCk11 = null;
              }
            }
          }

          // [FIX] Final clearing for Point Exchange orders
          // Even if override set ck05_nt, we must clear it for Point Exchange
          const orderTypes = InvoiceLogicUtils.getOrderTypes(
            sale.ordertypeName,
          );
          if (orderTypes.isDoiDiem) {
            overrideDiscount.ck05_nt = 0;
            overrideDiscount.ck05Nt = 0;
            overrideDiscount.ma_ck05 = null;
            overrideDiscount.maCk05 = null;
          }

          // Cache materialType for later use in verification loop
          if (sale.itemCode && loyaltyProduct?.materialType) {
            loyaltyProductMap.set(sale.itemCode, loyaltyProduct);
          } else if (sale.itemCode && loyaltyProduct?.productType) {
            if (!loyaltyProductMap.has(sale.itemCode)) {
              loyaltyProductMap.set(sale.itemCode, loyaltyProduct);
            }
          }

          // [FIX] Apply card data from legacy fetch (mimic Backend Payload logic)
          // We must use loyaltyProduct.materialCode as the key
          const matCode = loyaltyProduct?.materialCode;
          if (matCode && getMaThe.has(matCode) && !sale.maThe) {
            const serialFromCard = getMaThe.get(matCode);
            sale.maThe = serialFromCard;
            // Also fill soSerial as fallback
            if (!sale.soSerial) sale.soSerial = serialFromCard;
          }

          const order = orderMap.get(sale.docCode);

          // Get employee status from pre-fetched map
          const isEmployeeInAll =
            isEmployeeMapForAll.get(sale.partnerCode) ||
            isEmployeeMapForAll.get((sale as any).issuePartnerCode) ||
            false;

          // [NEW] Use SalesFormattingService for findAllOrders (Explosion)

          (sale as any).stockTransfers = saleStockTransfers; // Pass assigned STs

          const context = this.salesFormattingService.buildContext({
            loyaltyProductMap,
            departmentMap,
            stockTransferMap: undefined, // Passed via sale property
            orderFeeMap,
            warehouseCodeMap,
            svcCodeMap,
            getMaTheMap: getMaThe,
            orderMap,
            isEmployeeMap: isEmployeeMapForAll,
            includeStockTransfers: true,
          });

          const enrichedSale =
            await this.salesFormattingService.formatSingleSale(sale, context);

          // [FIX] Explicitly add single stockTransfer field for backward compatibility/FE usage
          // Priority: assignedSt > assignedRt
          enrichedSale.stockTransfer = assignedSt || assignedRt || null;

          // [NEW] Apply Override Discount Logic (ECOIN/VOUCHER logic calculated above)
          // Essential for syncing GET API with Fast API Payload logic
          if (Object.keys(overrideDiscount).length > 0) {
            Object.assign(enrichedSale, overrideDiscount);
          }

          // [NEW] Override svcCode with looked-up materialCode if available
          // (Service does this too, but let's keep consistent with existing flow if explicit override needed)
          if (sale.svc_code) {
            enrichedSale.svcCode = svcCodeMap.get(sale.svc_code);
          }

          // Store enriched sale (will be pushed in order after Promise.all)
          enrichedSale._docCode = docCode; // Temporary marker for grouping

          delete (sale as any).stockTransfers;
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

    // 8. Update orders with formatted sales (Critical step restored)
    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const order = orderMap.get(docCode);
      if (order) {
        order.sales = sales; // Assign FORMATTED sales to order
      }
    }
    // [OPTIMIZATION] Removed pre-explosion enrichWithMaVtRef call — redundant
    // The post-explosion call (after enrichOrdersWithCashio #2) covers ALL lines

    // 9. Sort and return
    const orders = Array.from(orderMap.values()).sort((a, b) => {
      // Sort by latest date first
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });

    // 10. Enrich with Cashio (Explosion)
    // This explodes sales based on Stock Transfers. Fields like Qty are reset here.
    // Pass pre-filtered stock transfers to prevent phantom items
    // Pass pre-filtered stock transfers to prevent phantom items
    const enrichedOrders = await this.enrichOrdersWithCashio(
      orders,
      stockTransfers,
      {
        cashioRecords,
        loyaltyProductMap,
        warehouseCodeMap,
        skipMaVtRef: true, // Caller handles enrichWithMaVtRef post-explosion (L1904)
      },
    );

    // [FIX] N8n Integration for Card Data (Enrichment source of truth)
    // Applied AFTER explosion to ensure we overwrite any Stock Transfer duplicates or Qty resets
    // Collect docCodes that require card data fetching
    const docCodesNeedingCardData: string[] = [];
    enrichedOrders.forEach((order) => {
      if (
        order.sales?.some((s: any) =>
          SalesUtils.isTachTheOrder(s.ordertypeName),
        )
      ) {
        docCodesNeedingCardData.push(order.docCode);
      }
    });

    // Execute parallel requests for card data with concurrency limit
    const cardDataMap = new Map<string, any>(); // Map<docCode, cardData>
    if (docCodesNeedingCardData.length > 0) {
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
    }

    // Apply card data to ENRICHED orders
    enrichedOrders.forEach((order) => {
      if (cardDataMap.has(order.docCode)) {
        const cardData = cardDataMap.get(order.docCode);
        // Use the CONSUMPTION logic to map data to enriched sales
        this.n8nService.mapIssuePartnerCodeToSales(order.sales || [], cardData);
      }
    });
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
      // AND for Normal Orders (01. Thường) with product type S or V
      // Frontend requires maThe to show the serial/ecode
      enrichedOrders.forEach((order) => {
        if (order.sales) {
          order.sales.forEach((sale: any) => {
            const product =
              loyaltyProductMap.get(sale.itemCode) ||
              loyaltyProductMap.get(sale.materialCode);
            const orderTypes = InvoiceLogicUtils.getOrderTypes(
              sale.ordertypeName || sale.ordertype || '',
            );
            const isNormalOrder = orderTypes.isThuong;
            const isTypeV =
              sale.productType === 'V' || product?.productType === 'V';

            if (product?.materialType === '94' || (isNormalOrder && isTypeV)) {
              // User request: maThe must take value from soSerial
              // Helper: Ensure soSerial is populated (fallback to ma_vt_ref or stockTransfer.batchSerial)
              if (!sale.soSerial) {
                // [FIX] Try ma_vt_ref first, then stockTransfer.batchSerial
                sale.soSerial =
                  sale.ma_vt_ref || sale.stockTransfer?.batchSerial;
              }
              // Assign soSerial to maThe
              if (sale.soSerial) {
                sale.maThe = sale.soSerial;
              }
            }
          });
        }
      });
    }

    if (isExport) {
      return {
        data: enrichedOrders,
        total: totalOrders,
        page,
        limit,
        totalPages: Math.ceil(totalOrders / limit),
      };
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

    return {
      data: ordersWithSingleSale,
      total: totalOrders, // Return total orders, not sale items
      page,
      limit,
      totalPages: Math.ceil(totalOrders / limit),
    };
  }

  /**
   * Find all orders with aggregated sales (no stock transfer explosion)
   * This method query sales and enrich them, but skip the step of exploding sales based on stock transfers
   */
  async findAllAggregatedOrders(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
    date?: string;
    dateFrom?: string;
    dateTo?: string;
    search?: string;
    statusAsys?: boolean;
    typeSale?: string;
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
      typeSale,
    } = options;

    // --- Query Logic (Same as findAllOrders) ---
    const countQuery = this.saleRepository
      .createQueryBuilder('sale')
      .select('COUNT(DISTINCT sale.docCode)', 'count');

    if (search && search.trim() !== '') {
      countQuery.leftJoin('sale.customer', 'customer');
    }

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
    const totalOrders = parseInt(totalResult?.count || '0', 10);

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

    const isSearchMode = !!search && totalOrders < 2000;
    let allSales: Sale[];

    if (!isSearchMode) {
      // Pagination Logic (Same as findAllOrders)
      const docCodeSubquery = this.saleRepository
        .createQueryBuilder('sale')
        .select('sale.docCode', 'docCode')
        .addSelect('MAX(sale.docDate)', 'docDate')
        .groupBy('sale.docCode')
        .orderBy('MAX(sale.docDate)', 'DESC')
        .addOrderBy('sale.docCode', 'ASC');

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

      const offset = (page - 1) * limit;
      docCodeSubquery.skip(offset).take(limit);

      const docCodeResults = await docCodeSubquery.getRawMany();
      const docCodes = docCodeResults.map((r) => r.docCode);

      if (docCodes.length === 0) {
        allSales = [];
      } else {
        fullQuery.andWhere('sale.docCode IN (:...docCodes)', { docCodes });
        allSales = await fullQuery.getMany();
      }
    } else {
      allSales = await fullQuery.getMany();
    }

    const orderMap = new Map<string, any>();
    const allSalesData: any[] = [];

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

      if (!sale.isProcessed) {
        order.isProcessed = false;
      }

      allSalesData.push(sale);
    }

    // --- Enrichment Logic ---
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
    const svcCodes = Array.from(
      new Set(
        allSalesData
          .map((sale) => sale.svc_code)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
    const allItemCodes = Array.from(new Set([...itemCodes]));

    const [loyaltyProductMap, departmentMap, warehouseCodeMap] =
      await Promise.all([
        this.loyaltyService.fetchProducts(allItemCodes),
        this.loyaltyService.fetchLoyaltyDepartments(branchCodes),
        this.categoriesService.getWarehouseCodeMap(),
      ]);

    // [NEW] Batch fetch OrderFees for platform voucher enrichment
    let orderFeeMap = new Map<string, OrderFee>();
    if (docCodes.length > 0) {
      const orderFees = await this.orderFeeRepository.find({
        where: { erpOrderCode: In(docCodes) },
      });
      orderFees.forEach((fee) => {
        orderFeeMap.set(fee.erpOrderCode, fee);
      });
    }
    let svcCodeMap = new Map<string, string>();
    if (svcCodes.length > 0) {
      svcCodeMap =
        await this.loyaltyService.fetchMaterialCodesBySvcCodes(svcCodes);
    }

    // Fetch Cards (Tach The)
    const getMaThe = new Map<string, string>();
    if (docCodes.length > 0) {
      try {
        const [dataCard] = await this.n8nService.fetchCardData(docCodes[0]);
        if (dataCard && dataCard.data) {
          // FIX N+1: Extract all service_item_names first
          const serviceItemNames = dataCard.data
            .map((card) => card?.service_item_name)
            .filter((name): name is string => !!name && name.trim() !== '');

          // Batch fetch all products at once
          const productMap =
            await this.loyaltyService.checkProductsBatch(serviceItemNames);

          // Map products to serials
          for (const card of dataCard.data) {
            if (!card?.service_item_name || !card?.serial) continue;
            const itemProduct = productMap.get(card.service_item_name);
            if (itemProduct) {
              getMaThe.set(itemProduct.materialCode, card.serial);
            }
          }
        }
      } catch (e) {
        // Ignore error
      }
    }

    // Pre-fetch Employee Status via API (similar to getAll)
    const firstSaleAggregated = allSalesData[0];
    const brandForAggregated =
      firstSaleAggregated?.customer?.brand ||
      firstSaleAggregated?.brand ||
      'menard';
    const partnerCodesToCheckAggregated = Array.from(
      new Set(
        allSalesData
          .flatMap((s) => [s.partnerCode, (s as any).issuePartnerCode])
          .filter((c): c is string => !!c && c.trim() !== ''),
      ),
    ).map((partnerCode) => ({
      partnerCode,
      sourceCompany: brandForAggregated,
    }));

    const isEmployeeMapAggregated =
      await this.n8nService.checkCustomersIsEmployee(
        partnerCodesToCheckAggregated,
      );

    // --- Format Sales (Parallel) ---
    const formatPromises: Promise<any>[] = [];

    for (const sale of allSalesData) {
      const promise = (async () => {
        const loyaltyProduct = sale.itemCode
          ? loyaltyProductMap.get(sale.itemCode)
          : null;
        const department = sale.branchCode
          ? departmentMap.get(sale.branchCode) || null
          : null;

        const maThe = getMaThe.get(loyaltyProduct?.materialCode || '') || '';
        sale.maThe = maThe;

        const calculatedFields = await InvoiceLogicUtils.calculateSaleFields(
          sale,
          loyaltyProduct,
          department,
          sale.branchCode,
        );

        const order = orderMap.get(sale.docCode);

        // Get employee status from pre-fetched map
        const isEmployeeAggregated =
          isEmployeeMapAggregated.get(sale.partnerCode) ||
          isEmployeeMapAggregated.get((sale as any).issuePartnerCode) ||
          false;

        // Use empty stock transfers for this aggregation view
        // [NEW] Use SalesFormattingService

        // Prepare context for this sale (or batch it outside loop for performance)
        // For performance, we should ideally reuse the context.
        // But `isEmployeeAggregated` is specific? No, map is global.
        // `order` is specific? No, map is global.
        // `stockTransfer` is specific? We used empty array.

        // Let's make a context outside loop?
        // We are inside `promise` closure.
        // We can build context inside.

        const context = this.salesFormattingService.buildContext({
          loyaltyProductMap,
          departmentMap,
          stockTransferMap: undefined, // Empty for aggregation view
          orderFeeMap: orderFeeMap,
          warehouseCodeMap,
          svcCodeMap,
          getMaTheMap: getMaThe,
          orderMap: orderMap,
          isEmployeeMap: isEmployeeMapAggregated,
          includeStockTransfers: false,
        });

        const enrichedSale = await this.salesFormattingService.formatSingleSale(
          sale,
          context,
        );

        if (sale.svc_code) {
          enrichedSale.svcCode = svcCodeMap.get(sale.svc_code);
        }

        enrichedSale._itemCode = sale.itemCode; // Pass for batch

        return enrichedSale;
      })();
      formatPromises.push(promise);
    }

    const enrichedSales = await Promise.all(formatPromises);

    // Group back to orders
    for (const enrichedSale of enrichedSales) {
      const docCode = enrichedSale.docCode;
      if (orderMap.has(docCode)) {
        orderMap.get(docCode).sales.push(enrichedSale);
      }
    }

    // --- SKIP EXPLOSION ---
    if (enrichedSales.length > 0) {
      await this.voucherIssueService.enrichSalesWithMaVtRef(
        enrichedSales,
        loyaltyProductMap,
      );
    }

    // Sort orders
    const orders = Array.from(orderMap.values()).sort((a, b) => {
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });

    // Manual maThe overrides (Voucher Item Type 94 AND Normal Orders with type S/V)
    orders.forEach((order) => {
      if (order.sales) {
        order.sales.forEach((sale: any) => {
          const product =
            loyaltyProductMap.get(sale.itemCode) ||
            loyaltyProductMap.get(sale.materialCode);
          const orderTypes = InvoiceLogicUtils.getOrderTypes(
            sale.ordertypeName || sale.ordertype || '',
          );
          const isNormalOrder = orderTypes.isThuong;
          const isTypeS_or_V =
            sale.productType === 'S' || sale.productType === 'V';

          if (
            product?.materialType === '94' ||
            (isNormalOrder && isTypeS_or_V)
          ) {
            if (!sale.soSerial && sale.ma_vt_ref) {
              sale.soSerial = sale.ma_vt_ref;
            }
            sale.maThe = sale.soSerial;
          }
        });
      }
    });

    return {
      data: orders,
      total: totalOrders,
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

  /**
   * Tính tổng số đơn (distinct docCode) theo các bộ lọc
   */
  async countOrders(options: {
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    search?: string;
    typeSale?: string;
    isProcessed?: boolean;
    statusAsys?: boolean;
  }): Promise<number> {
    const query = this.saleRepository
      .createQueryBuilder('sale')
      .select('COUNT(DISTINCT sale.docCode)', 'count');

    // Join customer ONLY if searching by customer fields
    if (options.search && options.search.trim() !== '') {
      query.leftJoin('sale.customer', 'customer');
    }

    this.applySaleFilters(query, options);

    const result = await query.getRawOne();
    return parseInt(result?.count || '0', 10);
  }

  /**
   * Refactored Helper: Match Stock Transfers to Sales Lines
   * Handles complex logic for SO (Output) and RT (Input) matching
   */
  private matchStockTransfersForOrders(
    enrichedSalesMap: Map<string, any[]>,
    stockTransfers: StockTransfer[],
  ): Map<string, { st: StockTransfer | null; rt: StockTransfer | null }> {
    const saleIdToStockTransferMap = new Map<
      string,
      { st: StockTransfer | null; rt: StockTransfer | null }
    >();

    // Group Stock Transfers by SO Code for efficiency
    const stockTransfersBySoCode = new Map<string, StockTransfer[]>();
    stockTransfers.forEach((st) => {
      // Use docCode for RT stock transfers (SALE_RETURN), soCode for others
      const key =
        st.doctype === 'SALE_RETURN' ? st.docCode : st.soCode || st.docCode;
      if (!stockTransfersBySoCode.has(key)) {
        stockTransfersBySoCode.set(key, []);
      }
      stockTransfersBySoCode.get(key)!.push(st);
    });

    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      // [FIX] Support lookup for RT orders using original order code
      let orderStockTransfers = stockTransfersBySoCode.get(docCode) || [];
      if (orderStockTransfers.length === 0 && docCode.startsWith('RT')) {
        const originalOrderCode = docCode
          .replace(/^RT/, 'SO')
          .replace(/_\d+$/, '');
        orderStockTransfers =
          stockTransfersBySoCode.get(originalOrderCode) || [];
      }

      // Group STs by ItemCode
      const stByItem = new Map<
        string,
        { st: StockTransfer[]; rt: StockTransfer[] }
      >();
      orderStockTransfers.forEach((st) => {
        // [FIX] User Request: Only join with ioType: O (Output)
        // This ensures Returns match with Original Output ST, avoiding duplication
        // EXCEPTION: For SALE_RETURN (RT), we need the Input transfer (ioType: I)
        if (st.ioType !== 'O' && st.doctype !== 'SALE_RETURN') return;

        // Logic to find match key: itemCode
        const key = st.itemCode;
        if (!key) return; // limit capability if no itemCode

        if (!stByItem.has(key)) stByItem.set(key, { st: [], rt: [] });
        const m = stByItem.get(key)!;

        // Distribute to correct bucket
        if (st.doctype === 'SALE_RETURN') {
          m.rt.push(st);
        } else {
          m.st.push(st);
        }
      });

      // Loop sales and assign
      sales.forEach((sale) => {
        if (!sale.id) return;
        const key = sale.itemCode; // Primary match key

        if (key && stByItem.has(key)) {
          const m = stByItem.get(key)!;
          const assignedSt = m.st.shift() || null;
          const assignedRt = m.rt.shift() || null;
          saleIdToStockTransferMap.set(sale.id, {
            st: assignedSt,
            rt: assignedRt,
          });
        } else {
          saleIdToStockTransferMap.set(sale.id, { st: null, rt: null });
        }
      });
    }

    return saleIdToStockTransferMap;
  }
}
