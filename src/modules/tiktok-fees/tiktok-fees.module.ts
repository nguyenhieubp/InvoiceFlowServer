import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { TikTokFeesService } from './tiktok-fees.service';
import { TikTokFeesController } from './tiktok-fees.controller';
import { OrderFee } from '../../entities/order-fee.entity';

@Module({
  imports: [TypeOrmModule.forFeature([OrderFee])],
  controllers: [TikTokFeesController],
  providers: [TikTokFeesService],
  exports: [TikTokFeesService],
})
export class TikTokFeesModule {}
