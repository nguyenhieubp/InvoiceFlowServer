import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { Sale } from '../../entities/sale.entity';
import { PaymentMethod } from '../../entities/payment-method.entity';
import { PaymentController } from './payment.controller';
import { PaymentService } from './payment.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { CategoriesModule } from '../categories/categories.module';
import { SalesModule } from '../sales/sales.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([DailyCashio, Sale, PaymentMethod]),
    HttpModule,
    CategoriesModule,
    forwardRef(() => SalesModule),
  ],
  controllers: [PaymentController],
  providers: [PaymentService, LoyaltyService],
  exports: [PaymentService],
})
export class PaymentModule {}
