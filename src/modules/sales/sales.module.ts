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
import { Invoice } from '../../entities/invoice.entity';
import { InvoiceItem } from '../../entities/invoice-item.entity';
import { InvoicesModule } from '../invoices/invoices.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([Sale, Customer, ProductItem, Invoice, InvoiceItem]),
    HttpModule,
    forwardRef(() => InvoicesModule),
  ],
  controllers: [SalesController],
  providers: [SalesService, InvoicePrintService, ZappyApiService],
  exports: [SalesService],
})
export class SalesModule {}

