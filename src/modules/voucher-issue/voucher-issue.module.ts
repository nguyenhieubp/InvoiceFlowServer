import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { VoucherIssueController } from './voucher-issue.controller';
import { VoucherIssueService } from './voucher-issue.service';
import { VoucherIssue } from '../../entities/voucher-issue.entity';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { SyncModule } from '../sync/sync.module';

@Module({
  imports: [TypeOrmModule.forFeature([VoucherIssue]), HttpModule],
  controllers: [VoucherIssueController],
  providers: [VoucherIssueService, ZappyApiService, LoyaltyService],
  exports: [VoucherIssueService],
})
export class VoucherIssueModule {}
