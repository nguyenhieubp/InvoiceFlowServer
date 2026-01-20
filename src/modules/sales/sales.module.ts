import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { SalesController } from './sales.controller';
import { SalesService } from './sales.service';
import { SalesQueryService } from './sales-query.service';
import { SalesSyncService } from './sales-sync.service';
import { SalesInvoiceService } from './sales-invoice.service';
import { SalesPayloadService } from './sales-payload.service';
import { SalesWarehouseService } from './sales-warehouse.service';
import { InvoicePersistenceService } from './invoice-persistence.service';
import { InvoiceDataEnrichmentService } from './invoice-data-enrichment.service';
import { SpecialOrderHandlerService } from './special-order-handler.service';
import { NormalOrderHandlerService } from './normal-order-handler.service';
import { SaleReturnHandlerService } from './sale-return-handler.service';
import { InvoiceFlowOrchestratorService } from './invoice-flow-orchestrator.service';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { ZappyApiService } from '../../services/zappy-api.service';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { InvoiceValidationService } from '../../services/invoice-validation.service';
import { N8nService } from '../../services/n8n.service';
import { Invoice } from '../../entities/invoice.entity';
import { InvoiceItem } from '../../entities/invoice-item.entity';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';
import { InvoicesModule } from '../invoices/invoices.module';
import { CategoriesModule } from '../categories/categories.module';
import { SyncModule } from '../sync/sync.module';
import { PaymentModule } from '../payment/payment.module';
import { VoucherIssueModule } from '../voucher-issue/voucher-issue.module';

import { PaymentSyncLog } from '../../entities/payment-sync-log.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      Sale,
      Customer,
      ProductItem,
      Invoice,
      InvoiceItem,
      FastApiInvoice,
      DailyCashio,
      StockTransfer,
      WarehouseProcessed,
      PaymentSyncLog,
    ]),
    HttpModule,
    forwardRef(() => InvoicesModule),
    CategoriesModule,
    forwardRef(() => SyncModule),
    forwardRef(() => PaymentModule),
    VoucherIssueModule,
  ],
  controllers: [SalesController],
  providers: [
    // Main service (orchestrator)
    SalesService,
    // New specialized services
    SalesQueryService,
    SalesSyncService,
    SalesInvoiceService,
    SalesPayloadService,
    SalesWarehouseService,
    InvoicePersistenceService,
    InvoiceDataEnrichmentService,
    // New handler services
    SpecialOrderHandlerService,
    NormalOrderHandlerService,
    SaleReturnHandlerService,
    InvoiceFlowOrchestratorService,
    // Existing services
    ZappyApiService,
    FastApiClientService,
    FastApiInvoiceFlowService,
    LoyaltyService,
    InvoiceValidationService,
    N8nService,
  ],
  exports: [
    SalesService,
    FastApiInvoiceFlowService,
    // Export specialized services để có thể sử dụng ở module khác
    SalesQueryService,
    SalesSyncService,
    SalesInvoiceService,
    SalesPayloadService,
    SalesWarehouseService,
    InvoicePersistenceService,
    InvoiceDataEnrichmentService,
    // Export new handler services
    SpecialOrderHandlerService,
    NormalOrderHandlerService,
    SaleReturnHandlerService,
    InvoiceFlowOrchestratorService,
  ],
})
export class SalesModule {}
