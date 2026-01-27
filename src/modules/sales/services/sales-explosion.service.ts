import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { DailyCashio } from '../../../entities/daily-cashio.entity';
import { LoyaltyService } from '../../../services/loyalty.service';
import * as SalesUtils from '../../../utils/sales.utils';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';

/**
 * SalesExplosionService
 * Chịu trách nhiệm: Explosion logic - explode sales by stock transfers
 *
 * Logic: 1 sale item with N stock transfers → N exploded sale lines
 */
import { CategoriesService } from 'src/modules/categories/categories.service';

@Injectable()
export class SalesExplosionService {
  private readonly logger = new Logger(SalesExplosionService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    private loyaltyService: LoyaltyService,
    private categoriesService: CategoriesService,
  ) {}

  /**
   * Enrich orders với cashio data và explode by stock transfers
   * NOTE: Method name giữ nguyên để tương thích với code cũ
   */
  async enrichOrdersWithCashio(orders: any[]): Promise<any[]> {
    const docCodes = orders.map((o) => o.docCode);
    if (docCodes.length === 0) return orders;

    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code IN (:...docCodes)', { docCodes })
      .orWhere('cashio.master_code IN (:...docCodes)', { docCodes })
      .getMany();

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

    // Pre-fetch product info for ALL Stock Transfer items to determine Batch vs Serial
    const allStItemCodes = Array.from(
      new Set(
        stockTransfers
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code),
      ),
    );
    const stLoyaltyProductMap =
      await this.loyaltyService.fetchProducts(allStItemCodes);

    // [FIX] Pre-fetch Warehouse Code Mappings
    const allStockCodes = Array.from(
      new Set(
        stockTransfers
          .map((st) => st.stockCode)
          .filter((code): code is string => !!code),
      ),
    );
    const warehouseCodeMap = new Map<string, string>();
    // Note: mapWarehouseCode needs to be awaited sequentially or parallelized.
    // Assuming we can parallelize:
    await Promise.all(
      allStockCodes.map(async (code) => {
        const mapped = await this.categoriesService.mapWarehouseCode(code);
        if (mapped) {
          warehouseCodeMap.set(code, mapped);
        }
      }),
    );

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
        if (SalesUtils.isTrutonkeepItem(st.itemCode)) {
          return false;
        }

        // Chỉ lấy các record có doctype = 'SALE_STOCKOUT' hoặc qty < 0 (xuất kho)
        const isStockOut =
          st.doctype === 'SALE_STOCKOUT' || Number(st.qty || 0) < 0;
        return isStockOut;
      });

      const uniqueStockTransfers = stockOutTransfers;

      // Tính tổng hợp thông tin stock transfer (chỉ tính từ unique records XUẤT KHO)
      // Lấy giá trị tuyệt đối của qty (vì qty xuất kho là số âm, nhưng số lượng xuất là số dương)
      const totalQty = uniqueStockTransfers.reduce((sum, st) => {
        const qty = Math.abs(Number(st.qty || 0));
        return sum + qty;
      }, 0);

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
          const product = stLoyaltyProductMap.get(st.itemCode);
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
            // Try to find unused sale first
            sale = matchingSales.find((s: any) => !usedSalesIds.has(s.id));
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

              maKho: warehouseCodeMap.get(st.stockCode) || st.stockCode, // [FIX] Use Mapped Code
              // Logic check trackBatch/trackSerial
              maLo: isBatch ? st.batchSerial : undefined,
              soSerial: isSerial ? st.batchSerial : undefined,
              // Store original itemCode from StockTransfer for voucher lookup
              originalItemCode: st.itemCode,

              isStockTransferLine: true,
              stockTransferId: st.id,
              stockTransfer:
                StockTransferUtils.formatStockTransferForFrontend(st), // Singular ST
              stockTransfers: undefined,
            });
          } else {
            // If no sale found (e.g. extra item in ST?), create a pseudo-sale line from ST
            // Or skip? User wants "StockTransfer as root", so we should show it.
            explodedSales.push({
              // Minimal Sale structure
              docCode: order.docCode,
              itemCode: st.itemCode,
              itemName: st.itemName,
              qty: Math.abs(Number(st.qty || 0)),
              maKho: warehouseCodeMap.get(st.stockCode) || st.stockCode, // [FIX] Use Mapped Code

              // Logic check trackBatch/trackSerial
              maLo: isBatch ? st.batchSerial : undefined,
              soSerial: isSerial ? st.batchSerial : undefined,

              isStockTransferLine: true,
              stockTransferId: st.id,
              stockTransfer:
                StockTransferUtils.formatStockTransferForFrontend(st), // Singular ST
              stockTransfers: undefined,
              price: 0, // Unknown
              revenue: 0,
            });
          }
        });

        // 2. Add remaining Sales lines (e.g. Services) that were not matched by any ST
        (order.sales || []).forEach((sale: any) => {
          // Check if this sale was used.
          // Note: A sale might be matched by MULTIPLE STs. `usedSalesIds` tracks if it was matched at least once.
          // If it was matched, we assume it's fully covered by the STs (or at least replaced by them).
          // If NOT matched, it's likely a non-stock item.
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
  }
}
