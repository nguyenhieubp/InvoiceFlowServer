import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ScheduleModule } from '@nestjs/schedule';
import { MultiDbService } from './multi-db.service';
import { MultiDbController } from './multi-db.controller';
import { MultiDbSyncService } from './multi-db-sync.service';
import { OrderFee } from '../../entities/order-fee.entity';

@Module({
  imports: [
    // Primary database entities
    TypeOrmModule.forFeature([OrderFee]),

    // Secondary database entities
    TypeOrmModule.forFeature([], 'secondary'),
    TypeOrmModule.forFeature([], 'third'),

    // Schedule module for cronjobs
    ScheduleModule.forRoot(),
  ],
  controllers: [MultiDbController],
  providers: [MultiDbService, MultiDbSyncService],
  exports: [MultiDbService],
})
export class MultiDbModule {}
