import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { HttpModule } from '@nestjs/axios';
import { SyncService } from '../../services/sync.service';
import { SyncController } from './sync.controller';
import { SyncTask } from '../../tasks/sync.task';
import { Customer } from '../../entities/customer.entity';
import { Sale } from '../../entities/sale.entity';

@Module({
  imports: [TypeOrmModule.forFeature([Customer, Sale]), HttpModule],
  controllers: [SyncController],
  providers: [SyncService, SyncTask],
  exports: [SyncService],
})
export class SyncModule {}

