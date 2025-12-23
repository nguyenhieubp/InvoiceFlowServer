import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { SyncService } from '../../services/sync.service';
import { SyncController } from './sync.controller';
import { SyncTask } from '../../tasks/sync.task';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { SalesModule } from '../sales/sales.module';
import { Customer } from '../../entities/customer.entity';
import { Sale } from '../../entities/sale.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { CheckFaceId } from '../../entities/check-face-id.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { ShiftEndCash } from '../../entities/shift-end-cash.entity';
import { ShiftEndCashLine } from '../../entities/shift-end-cash-line.entity';
import { RepackFormula } from '../../entities/repack-formula.entity';
import { RepackFormulaItem } from '../../entities/repack-formula-item.entity';
import { Promotion } from '../../entities/promotion.entity';
import { PromotionLine } from '../../entities/promotion-line.entity';
import { VoucherIssue } from '../../entities/voucher-issue.entity';
import { VoucherIssueDetail } from '../../entities/voucher-issue-detail.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([Customer, Sale, DailyCashio, CheckFaceId, StockTransfer, ShiftEndCash, ShiftEndCashLine, RepackFormula, RepackFormulaItem, Promotion, PromotionLine, VoucherIssue, VoucherIssueDetail]),
    HttpModule,
    forwardRef(() => SalesModule),
  ],
  controllers: [SyncController],
  providers: [SyncService, SyncTask, ZappyApiService, LoyaltyService],
  exports: [SyncService],
})
export class SyncModule {}

