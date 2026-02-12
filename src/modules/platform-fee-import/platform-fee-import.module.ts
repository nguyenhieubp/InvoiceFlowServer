import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { PlatformFeeImportService } from './platform-fee-import.service';
import { PlatformFeeImportController } from './platform-fee-import.controller';
import { PlatformFeeImportShopee } from '../../entities/platform-fee-import-shopee.entity';
import { PlatformFeeImportTiktok } from '../../entities/platform-fee-import-tiktok.entity';
import { PlatformFeeImportLazada } from '../../entities/platform-fee-import-lazada.entity';
import { PlatformFeeMap } from '../../entities/platform-fee-map.entity';
import { ShopeeFee } from '../../entities/shopee-fee.entity';
import { TikTokFee } from '../../entities/tiktok-fee.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      PlatformFeeImportShopee,
      PlatformFeeImportTiktok,
      PlatformFeeImportLazada,
      PlatformFeeMap,
      ShopeeFee,
      TikTokFee,
    ]),
  ],
  controllers: [PlatformFeeImportController],
  providers: [PlatformFeeImportService],
  exports: [PlatformFeeImportService],
})
export class PlatformFeeImportModule { }
