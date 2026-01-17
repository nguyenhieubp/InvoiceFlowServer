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
    // 1. Get Base Sales Data
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    if (sales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // 2. Prepare mock order object for SalesQueryService
    // We construct minimal object needed for enrichOrdersWithCashio
    const baseOrder = {
      docCode: sales[0].docCode,
      docDate: sales[0].docDate,
      docSourceType: sales[0].docSourceType,
      branchCode: sales[0].branchCode,
      customer: sales[0].customer
        ? {
            code: sales[0].customer.code,
            name: sales[0].customer.name,
            brand: sales[0].customer.brand,
            mobile: sales[0].customer.mobile,
            address: sales[0].customer.address,
            idnumber: sales[0].customer.idnumber,
            birthday: sales[0].customer.birthday,
            sexual: sales[0].customer.sexual,
            phone: sales[0].customer.phone,
          }
        : null,
      sales: sales,
    };

    // 3. Call SalesQueryService to explode/enrich items (Batch/Serial logic)
    const enrichedOrders = await this.salesQueryService.enrichOrdersWithCashio([
      baseOrder,
    ]);
    const enrichedOrder = enrichedOrders[0];

    // 4. Fetch Stock Transfers (Required for SalesPayloadService - e.g. Sales Return)
    // Note: enrichOrdersWithCashio fetches them internally but doesn't return the raw list on root
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    // 5. Enrich Sales with Department (ma_dvcs) - using fixed Loyalty endpoint
    const branchCodes = SalesUtils.extractUniqueBranchCodes(baseOrder.sales);
    const departmentMap =
      await this.loyaltyService.fetchLoyaltyDepartments(branchCodes);

    enrichedOrder.sales = enrichedOrder.sales.map((sale: any) => {
      const department = sale.branchCode
        ? departmentMap.get(sale.branchCode) || null
        : null;
      return {
        ...sale,
        department,
        ma_dvcs: department?.ma_dvcs || '', // Explicitly attach ma_dvcs if needed by consumers
      };
    });

    // 6. Return combined result
    return {
      ...enrichedOrder,
      stockTransfers, // Attach specifically for Payload Service usage
    };
  }
}
