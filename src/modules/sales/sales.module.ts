import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { SalesController } from './sales.controller';
import { SalesService } from './sales.service';
import { SalesQueryService } from './sales-query.service';
import { SalesSyncService } from './sales-sync.service';
import { SalesInvoiceService } from './sales-invoice.service';
import { SalesWarehouseService } from './sales-warehouse.service';
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
    ]),
    HttpModule,
    forwardRef(() => InvoicesModule),
    CategoriesModule,
    forwardRef(() => SyncModule),
    forwardRef(() => PaymentModule),
  ],
  controllers: [SalesController],
  providers: [
    // Main service (orchestrator)
    SalesService,
    // New specialized services
    SalesQueryService,
    SalesSyncService,
    SalesInvoiceService,
    SalesWarehouseService,
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
    SalesWarehouseService,
  ],
})
export class SalesModule {}
