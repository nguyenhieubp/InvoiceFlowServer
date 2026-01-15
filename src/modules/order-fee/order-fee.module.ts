import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OrderFee } from '../../entities/order-fee.entity';
import { OrderFeeController } from './order-fee.controller';

@Module({
  imports: [TypeOrmModule.forFeature([OrderFee])],
  controllers: [OrderFeeController],
  providers: [],
  exports: [],
})
export class OrderFeeModule {}
