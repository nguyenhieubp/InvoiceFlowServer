import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { PurchaseOrder } from '../../entities/purchase-order.entity';
import { PurchaseOrderService } from './purchase-order.service';
import { PurchaseOrderController } from './purchase-order.controller';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';

@Module({
  imports: [TypeOrmModule.forFeature([PurchaseOrder, WarehouseProcessed]), HttpModule],
  providers: [
    PurchaseOrderService,
    ZappyApiService,
    LoyaltyService,
    FastApiClientService,
  ],
  controllers: [PurchaseOrderController],
  exports: [PurchaseOrderService],
})
export class PurchaseOrderModule { }
