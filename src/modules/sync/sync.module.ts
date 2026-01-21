import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { SyncService } from './sync.service';
import { StockTransferSyncService } from './stock-transfer-sync.service';
import { SyncController } from './sync.controller';
import { SyncTask } from '../../tasks/sync.task';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { SalesModule } from '../sales/sales.module';
import { Customer } from '../../entities/customer.entity';
import { Sale } from '../../entities/sale.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';
import { ShiftEndCash } from '../../entities/shift-end-cash.entity';
import { ShiftEndCashLine } from '../../entities/shift-end-cash-line.entity';
import { RepackFormula } from '../../entities/repack-formula.entity';
import { RepackFormulaItem } from '../../entities/repack-formula-item.entity';
import { Promotion } from '../../entities/promotion.entity';
import { PromotionLine } from '../../entities/promotion-line.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      Customer,
      Sale,
      DailyCashio,
      StockTransfer,
      WarehouseProcessed,
      ShiftEndCash,
      ShiftEndCashLine,
      RepackFormula,
      RepackFormulaItem,
      Promotion,
      PromotionLine,
    ]),
    HttpModule,
    forwardRef(() => SalesModule),
  ],
  controllers: [SyncController],
  providers: [
    SyncService,
    StockTransferSyncService,
    SyncTask,
    ZappyApiService,
    LoyaltyService,
    FastApiClientService,
  ],
  exports: [SyncService, StockTransferSyncService],
  // Import SalesModule để có thể inject FastApiInvoiceFlowService (đã export từ SalesModule)
})
export class SyncModule {}
