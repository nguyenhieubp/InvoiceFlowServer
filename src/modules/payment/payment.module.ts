import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { Sale } from '../../entities/sale.entity';
import { PaymentController } from './payment.controller';
import { PaymentService } from './payment.service';
import { LoyaltyService } from 'src/services/loyalty.service';
import { HttpModule } from '@nestjs/axios';
import { CategoriesModule } from '../categories/categories.module';
import { CategoriesService } from '../categories/categories.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([DailyCashio, Sale]),
    HttpModule,
    CategoriesModule,
  ],
  controllers: [PaymentController],
  providers: [PaymentService, LoyaltyService],
  exports: [PaymentService],
})
export class PaymentModule {}
