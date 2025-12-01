import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { InvoicesController } from './invoices.controller';
import { InvoiceService } from '../../services/invoice.service';
import { InvoicePrintService } from '../../services/invoice-print.service';
import { Invoice } from '../../entities/invoice.entity';
import { InvoiceItem } from '../../entities/invoice-item.entity';
import { Sale } from '../../entities/sale.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([Invoice, InvoiceItem, Sale]),
    HttpModule,
  ],
  controllers: [InvoicesController],
  providers: [InvoiceService, InvoicePrintService],
  exports: [InvoiceService],
})
export class InvoicesModule {}

