import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OrderFee } from '../../entities/order-fee.entity';
import { ShopeeFee } from '../../entities/shopee-fee.entity';
import { TikTokFee } from '../../entities/tiktok-fee.entity';
import { OrderFeeController } from './order-fee.controller';

import { PlatformFeeImportShopee } from '../../entities/platform-fee-import-shopee.entity';
import { PlatformFeeImportTiktok } from '../../entities/platform-fee-import-tiktok.entity';
import { Sale } from '../../entities/sale.entity';

@Module({
  imports: [TypeOrmModule.forFeature([OrderFee, ShopeeFee, TikTokFee, PlatformFeeImportShopee, PlatformFeeImportTiktok, Sale])],
  controllers: [OrderFeeController],
  providers: [],
  exports: [],
})
export class OrderFeeModule { }
