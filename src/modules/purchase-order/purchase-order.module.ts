import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { PurchaseOrder } from '../../entities/purchase-order.entity';
import { PurchaseOrderService } from './purchase-order.service';
import { PurchaseOrderController } from './purchase-order.controller';
import { ZappyApiService } from '../../services/zappy-api.service';

@Module({
  imports: [TypeOrmModule.forFeature([PurchaseOrder]), HttpModule],
  providers: [PurchaseOrderService, ZappyApiService],
  controllers: [PurchaseOrderController],
  exports: [PurchaseOrderService],
})
export class PurchaseOrderModule {}
