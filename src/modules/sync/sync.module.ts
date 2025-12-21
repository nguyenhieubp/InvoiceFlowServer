import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { SyncService } from '../../services/sync.service';
import { SyncController } from './sync.controller';
import { SyncTask } from '../../tasks/sync.task';
import { ZappyApiService } from '../../services/zappy-api.service';
import { SalesModule } from '../sales/sales.module';
import { Customer } from '../../entities/customer.entity';
import { Sale } from '../../entities/sale.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { CheckFaceId } from '../../entities/check-face-id.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([Customer, Sale, DailyCashio, CheckFaceId, StockTransfer]),
    HttpModule,
    forwardRef(() => SalesModule),
  ],
  controllers: [SyncController],
  providers: [SyncService, SyncTask, ZappyApiService],
  exports: [SyncService],
})
export class SyncModule {}

