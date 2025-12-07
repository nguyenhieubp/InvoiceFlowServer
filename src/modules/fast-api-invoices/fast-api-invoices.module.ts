import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { FastApiInvoicesController } from './fast-api-invoices.controller';
import { FastApiInvoiceService } from '../../services/fast-api-invoice.service';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';

@Module({
  imports: [TypeOrmModule.forFeature([FastApiInvoice])],
  controllers: [FastApiInvoicesController],
  providers: [FastApiInvoiceService],
  exports: [FastApiInvoiceService],
})
export class FastApiInvoicesModule {}

