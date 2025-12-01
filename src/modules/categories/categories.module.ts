import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { CategoriesController } from './categories.controller';
import { CategoriesService } from './categories.service';
import { ProductItem } from '../../entities/product-item.entity';
import { PromotionItem } from '../../entities/promotion-item.entity';
import { WarehouseItem } from '../../entities/warehouse-item.entity';

@Module({
  imports: [TypeOrmModule.forFeature([ProductItem, PromotionItem, WarehouseItem])],
  controllers: [CategoriesController],
  providers: [CategoriesService],
  exports: [CategoriesService],
})
export class CategoriesModule {}

