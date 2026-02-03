import { Module, forwardRef } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { TypeOrmModule } from '@nestjs/typeorm';
import { FastApiInvoicesController } from './fast-api-invoices.controller';
import { WebhookController } from './controllers/webhook.controller';
import { FastApiInvoiceService } from './fast-api-invoice.service';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { SalesModule } from '../sales/sales.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([FastApiInvoice]),
    forwardRef(() => SalesModule),
    HttpModule,
  ],
  controllers: [FastApiInvoicesController, WebhookController],
  providers: [FastApiInvoiceService],
  exports: [FastApiInvoiceService],
})
export class FastApiInvoicesModule {}
