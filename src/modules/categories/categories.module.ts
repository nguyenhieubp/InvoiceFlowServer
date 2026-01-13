import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { CategoriesController } from './categories.controller';
import { CategoriesService } from './categories.service';
import { ProductItem } from '../../entities/product-item.entity';
import { PromotionItem } from '../../entities/promotion-item.entity';
import { WarehouseItem } from '../../entities/warehouse-item.entity';
import { WarehouseCodeMapping } from '../../entities/warehouse-code-mapping.entity';
import { PaymentMethod } from '../../entities/payment-method.entity';
import { Customer } from '../../entities/customer.entity';
import { Sale } from '../../entities/sale.entity';
import { EcommerceCustomer } from '../../entities/ecommerce-customer.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      ProductItem,
      PromotionItem,
      WarehouseItem,
      WarehouseCodeMapping,
      PaymentMethod,
      Customer,
      Sale,
      EcommerceCustomer,
    ]),
    HttpModule,
  ],
  controllers: [CategoriesController],
  providers: [CategoriesService],
  exports: [CategoriesService],
})
export class CategoriesModule {}
