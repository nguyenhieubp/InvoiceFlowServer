import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { Sale } from '../../entities/sale.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { LoyaltyService } from '../../services/loyalty.service';
import { N8nService } from '../../services/n8n.service';
import { SalesQueryService } from './sales-query.service';
import { VoucherIssueService } from '../voucher-issue/voucher-issue.service';
import * as SalesUtils from '../../utils/sales.utils';
import * as StockTransferUtils from '../../utils/stock-transfer.utils';
import * as SalesFormattingUtils from '../../utils/sales-formatting.utils';

/**
 * InvoiceDataEnrichmentService
 * Chịu trách nhiệm: Enrichment và transformation của order data
 */
@Injectable()
export class InvoiceDataEnrichmentService {
  private readonly logger = new Logger(InvoiceDataEnrichmentService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    private httpService: HttpService,
    private loyaltyService: LoyaltyService,
    private n8nService: N8nService,
    private salesQueryService: SalesQueryService,
    private voucherIssueService: VoucherIssueService,
  ) {}

  /**
   * Enrich order data với tất cả thông tin cần thiết
   * - Product information (database + Loyalty API)
   * - Department information
   * - Stock transfer information
   * - Cashio data
   * - Card codes
   */
  async findByOrderCode(docCode: string) {
    // Lấy tất cả sales có cùng docCode (cùng đơn hàng)
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    if (sales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // Join với daily_cashio để lấy cashio data
    // Join dựa trên: cashio.so_code = docCode HOẶC cashio.master_code = docCode
    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code = :docCode', { docCode })
      .orWhere('cashio.master_code = :docCode', { docCode })
      .getMany();

    // Ưu tiên ECOIN, sau đó VOUCHER, sau đó các loại khác
    const ecoinCashio = cashioRecords.find((c) => c.fop_syscode === 'ECOIN');
    const voucherCashio = cashioRecords.find(
      (c) => c.fop_syscode === 'VOUCHER',
    );
    const selectedCashio =
      ecoinCashio || voucherCashio || cashioRecords[0] || null;

    // Lấy tất cả itemCode unique từ sales
    const itemCodes = SalesUtils.extractUniqueItemCodes(sales);

    // Load tất cả products một lần
    const products =
      itemCodes.length > 0
        ? await this.productItemRepository.find({
            where: { maERP: In(itemCodes) },
          })
        : [];

    // Tạo map để lookup nhanh
    const productMap = SalesUtils.createProductMap(products);

    // Fetch card data và tạo card code map
    const [dataCard] = await this.n8nService.fetchCardData(docCode);
    const cardCodeMap = SalesUtils.createCardCodeMap(dataCard);

    // Enrich sales với product information từ database và card code
    const enrichedSales = sales.map((sale) => {
      const saleWithProduct = SalesUtils.enrichSaleWithProduct(
        sale,
        productMap,
      );
      return SalesUtils.enrichSaleWithCardCode(saleWithProduct, cardCodeMap);
    });

    // Fetch products từ Loyalty API cho các itemCode không có trong database hoặc không có dvt
    // BỎ QUA các sale có statusAsys = false (đơn lỗi) - không fetch từ Loyalty API
    const loyaltyProductMap = new Map<string, any>();
    // Filter itemCodes: chỉ fetch cho các sale không phải đơn lỗi
    const validItemCodes = SalesUtils.filterValidItemCodes(itemCodes, sales);

    // Fetch products từ Loyalty API sử dụng LoyaltyService
    if (validItemCodes.length > 0) {
      const fetchedProducts =
        await this.loyaltyService.fetchProducts(validItemCodes);
      fetchedProducts.forEach((product, itemCode) => {
        loyaltyProductMap.set(itemCode, product);
      });
    }

    // Enrich sales với product từ Loyalty API (thêm dvt từ unit)
    const enrichedSalesWithLoyalty = enrichedSales.map((sale) =>
      SalesUtils.enrichSaleWithLoyaltyProduct(sale, loyaltyProductMap),
    );

    // Fetch departments để lấy ma_dvcs
    const branchCodes = SalesUtils.extractUniqueBranchCodes(sales);

    const departmentMap = new Map<string, any>();
    // Fetch departments parallel để tối ưu performance
    if (branchCodes.length > 0) {
      const departmentPromises = branchCodes.map(async (branchCode) => {
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
            { headers: { accept: 'application/json' } },
          );
          const department = response?.data?.data?.items?.[0];
          return { branchCode, department };
        } catch (error) {
          this.logger.warn(
            `Failed to fetch department for branchCode ${branchCode}: ${error}`,
          );
          return { branchCode, department: null };
        }
      });

      const departmentResults = await Promise.all(departmentPromises);
      departmentResults.forEach(({ branchCode, department }) => {
        if (department) {
          departmentMap.set(branchCode, department);
        }
      });
    }

    // [New] Batch lookup svc_code -> materialCode for FAST API
    const svcCodes = Array.from(
      new Set(
        sales
          .map((sale) => sale.svc_code)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
    const svcCodeMap = new Map<string, string>();
    if (svcCodes.length > 0) {
      await Promise.all(
        svcCodes.map(async (code) => {
          try {
            const materialCode =
              await this.loyaltyService.getMaterialCodeBySvcCode(code);
            if (materialCode) {
              svcCodeMap.set(code, materialCode);
            }
          } catch (error) {
            // Ignore
          }
        }),
      );
    }

    // Fetch stock transfers để lấy ma_nx (ST* và RT* từ stock transfer)
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    // Sử dụng materialCode đã được lưu trong database (đã được đồng bộ từ Loyalty API khi sync)
    // Nếu chưa có materialCode trong database, mới fetch từ Loyalty API
    const stockTransferItemCodesWithoutMaterialCode = Array.from(
      new Set(
        stockTransfers
          .filter((st) => st.itemCode && !st.materialCode)
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    // Chỉ fetch materialCode cho các itemCode chưa có materialCode trong database
    const stockTransferLoyaltyMap = new Map<string, any>();
    if (stockTransferItemCodesWithoutMaterialCode.length > 0) {
      const fetchedStockTransferProducts =
        await this.loyaltyService.fetchProducts(
          stockTransferItemCodesWithoutMaterialCode,
        );
      fetchedStockTransferProducts.forEach((product, itemCode) => {
        stockTransferLoyaltyMap.set(itemCode, product);
      });
    }

    // Tạo map: soCode_materialCode -> stock transfer (phân biệt ST và RT)
    const stockTransferMapBySoCodeAndMaterialCode = new Map<
      string,
      { st?: StockTransfer[]; rt?: StockTransfer[] }
    >();
    stockTransfers.forEach((st) => {
      const materialCode =
        st.materialCode ||
        stockTransferLoyaltyMap.get(st.itemCode)?.materialCode;
      if (!materialCode) {
        return;
      }

      const soCode = st.soCode || st.docCode || docCode;
      const key = `${soCode}_${materialCode}`;

      if (!stockTransferMapBySoCodeAndMaterialCode.has(key)) {
        stockTransferMapBySoCodeAndMaterialCode.set(key, {});
      }
      const itemMap = stockTransferMapBySoCodeAndMaterialCode.get(key)!;
      if (st.docCode.startsWith('ST')) {
        if (!itemMap.st) {
          itemMap.st = [];
        }
        itemMap.st.push(st);
      }
      if (st.docCode.startsWith('RT')) {
        if (!itemMap.rt) {
          itemMap.rt = [];
        }
        itemMap.rt.push(st);
      }
    });

    // Enrich sales với department information và lấy maKho từ stock transfer
    const enrichedSalesWithDepartment = await Promise.all(
      enrichedSalesWithLoyalty.map(async (sale) => {
        const department = sale.branchCode
          ? departmentMap.get(sale.branchCode) || null
          : null;

        const saleLoyaltyProduct = sale.itemCode
          ? loyaltyProductMap.get(sale.itemCode)
          : null;
        const saleMaterialCode = saleLoyaltyProduct?.materialCode;
        const finalMaKho =
          await this.salesQueryService.getMaKhoFromStockTransfer(
            sale,
            docCode,
            stockTransfers,
            saleMaterialCode,
          );

        const matchedStockTransfer = stockTransfers.find(
          (st) => st.soCode === docCode && st.itemCode === sale.itemCode,
        );
        const firstSt =
          matchedStockTransfer && matchedStockTransfer.docCode.startsWith('ST')
            ? matchedStockTransfer
            : null;
        const firstRt =
          matchedStockTransfer && matchedStockTransfer.docCode.startsWith('RT')
            ? matchedStockTransfer
            : null;

        const maNxSt = firstSt?.docCode || null;
        const maNxRt = firstRt?.docCode || null;
        const maLo = firstSt?.batchSerial || null;
        const maSerial = firstSt?.batchSerial || null;

        return {
          ...sale,
          department,
          maKho: finalMaKho,
          ma_nx_st: maNxSt,
          ma_nx_rt: maNxRt,
          maLo,
          maSerial,
        };
      }),
    );

    // Format sales
    const formattedSales = enrichedSalesWithDepartment.map((sale) => {
      // Simple formatting - just return the sale with loyalty product info
      const loyaltyProduct = sale.itemCode
        ? loyaltyProductMap.get(sale.itemCode)
        : null;

      // Determine materialCode: Prefer lookup from svc_code, then loyaltyProduct, then existing
      let materialCode = loyaltyProduct?.materialCode || null;
      if (sale.svc_code && svcCodeMap.has(sale.svc_code)) {
        materialCode = svcCodeMap.get(sale.svc_code) || materialCode;
      }

      return {
        ...sale,
        materialCode: materialCode,
        dvt: sale.dvt || loyaltyProduct?.unit || null,
      };
    });

    // Validating ma_vt_ref
    const verificationTasks: Promise<void>[] = [];
    for (const sale of formattedSales) {
      const saleSerial = sale.maSerial;
      const itemCode = sale.itemCode; // Use itemCode from sale

      if (itemCode && saleSerial) {
        verificationTasks.push(
          (async () => {
            try {
              const ecode =
                await this.voucherIssueService.findEcodeBySerialAndItemCode(
                  itemCode,
                  saleSerial,
                );
              if (ecode) {
                // @ts-ignore
                sale.ma_vt_ref = ecode;
              }
            } catch (e) {
              /* ignore */
            }
          })(),
        );
      }
    }

    if (verificationTasks.length > 0) {
      await Promise.all(verificationTasks);
    }

    // Tạo order data object
    const orderData = {
      docCode,
      docDate: sales[0]?.docDate || new Date(),
      docSourceType: sales[0]?.docSourceType || null,
      ordertype: sales[0]?.ordertype || null,
      ordertypeName: sales[0]?.ordertypeName || null,
      branchCode: sales[0]?.branchCode || null,
      customer: sales[0]?.customer || null,
      sales: formattedSales,
      cashio: selectedCashio,
      stockTransfers,
    };

    return orderData;
  }
}
