import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { Sale } from '../../../entities/sale.entity';
import { ProductItem } from '../../../entities/product-item.entity';
import { DailyCashio } from '../../../entities/daily-cashio.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { LoyaltyService } from '../../../services/loyalty.service';
import { N8nService } from '../../../services/n8n.service';
import { SalesQueryService } from '../services/sales-query.service';
import { VoucherIssueService } from '../../voucher-issue/voucher-issue.service';
import * as SalesUtils from '../../../utils/sales.utils';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import * as SalesFormattingUtils from '../../../utils/sales-formatting.utils';

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
    // 1. [REFACTORED] Reuse Frontend Logic (SalesQueryService)
    // This fetches Sales, performs robust 1-1 Stick Transfer matching, and enriches data.
    // Avoids "Processing Again" as per user request.
    const formattedSales =
      await this.salesQueryService.findByOrderCode(docCode);

    if (!formattedSales || formattedSales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // 2. Fetch Cashio (Required for Payment Payload)
    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code = :docCode', { docCode })
      .orWhere('cashio.master_code = :docCode', { docCode })
      .getMany();

    const ecoinCashio = cashioRecords.find((c) => c.fop_syscode === 'ECOIN');
    const voucherCashio = cashioRecords.find(
      (c) => c.fop_syscode === 'VOUCHER',
    );
    const selectedCashio =
      ecoinCashio || voucherCashio || cashioRecords[0] || null;

    // 3. Fetch Stick Transfers (Raw) for Root Payload
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { createdAt: 'ASC' },
    });

    // 4. Construct Order Data
    const firstSale = formattedSales[0];

    const orderData = {
      docCode,
      docDate: firstSale.docDate || new Date(),
      docSourceType: firstSale.docSourceType || null,
      ordertype: firstSale.ordertype || null,
      ordertypeName: firstSale.ordertypeName || null,
      branchCode: firstSale.branchCode || null,
      customer: firstSale.customer || null,
      sales: formattedSales,
      cashio: selectedCashio,
      stockTransfers,
    };

    return orderData;
  }
}
