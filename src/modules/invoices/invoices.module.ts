import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { InvoicesController } from './invoices.controller';
import { InvoiceService } from './invoice.service';
import { Invoice } from '../../entities/invoice.entity';
import { InvoiceItem } from '../../entities/invoice-item.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([Invoice, InvoiceItem]),
    HttpModule,
  ],
  controllers: [InvoicesController],
  providers: [InvoiceService],
  exports: [InvoiceService],
})
export class InvoicesModule {}

