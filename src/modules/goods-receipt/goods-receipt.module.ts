import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { GoodsReceipt } from '../../entities/goods-receipt.entity';
import { GoodsReceiptService } from './goods-receipt.service';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { GoodsReceiptController } from './goods-receipt.controller';

@Module({
  imports: [TypeOrmModule.forFeature([GoodsReceipt]), HttpModule],
  providers: [GoodsReceiptService, ZappyApiService, LoyaltyService],
  controllers: [GoodsReceiptController],
  exports: [GoodsReceiptService],
})
export class GoodsReceiptModule { }
