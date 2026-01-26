import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ShopeeFeesService } from './shopee-fees.service';
import { ShopeeFeesController } from './shopee-fees.controller';
import { OrderFee } from '../../entities/order-fee.entity';

@Module({
  imports: [TypeOrmModule.forFeature([OrderFee])],
  controllers: [ShopeeFeesController],
  providers: [ShopeeFeesService],
  exports: [ShopeeFeesService],
})
export class ShopeeFeesModule {}
