import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { SalesController } from './sales.controller';
import { SalesService } from './sales.service';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { InvoicePrintService } from '../../services/invoice-print.service';
import { InvoiceService } from '../../services/invoice.service';
import { ZappyApiService } from '../../services/zappy-api.service';
import { FastApiService } from '../../services/fast-api.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { Invoice } from '../../entities/invoice.entity';
import { InvoiceItem } from '../../entities/invoice-item.entity';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { CheckFaceId } from '../../entities/check-face-id.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { InvoicesModule } from '../invoices/invoices.module';
import { CategoriesModule } from '../categories/categories.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([Sale, Customer, ProductItem, Invoice, InvoiceItem, FastApiInvoice, DailyCashio, CheckFaceId, StockTransfer]),
    HttpModule,
    forwardRef(() => InvoicesModule),
    CategoriesModule,
  ],
  controllers: [SalesController],
  providers: [SalesService, InvoicePrintService, ZappyApiService, FastApiService, FastApiInvoiceFlowService, LoyaltyService],
  exports: [SalesService],
})
export class SalesModule {}

