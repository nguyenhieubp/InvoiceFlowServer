import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { WarehouseReleasesController } from './warehouse-releases.controller';
import { WarehouseReleaseService } from '../../services/warehouse-release.service';
import { WarehouseRelease } from '../../entities/warehouse-release.entity';

@Module({
  imports: [TypeOrmModule.forFeature([WarehouseRelease])],
  controllers: [WarehouseReleasesController],
  providers: [WarehouseReleaseService],
  exports: [WarehouseReleaseService],
})
export class WarehouseReleasesModule {}

