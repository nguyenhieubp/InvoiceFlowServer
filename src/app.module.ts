import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import {
  getDatabaseConfig,
  getSecondaryDatabaseConfig,
  getThirdDatabaseConfig,
} from './config/database.config';
import { SyncModule } from './modules/sync/sync.module';
import { InvoicesModule } from './modules/invoices/invoices.module';
import { SalesModule } from './modules/sales/sales.module';
import { CategoriesModule } from './modules/categories/categories.module';
import { FastApiInvoicesModule } from './modules/fast-api-invoices/fast-api-invoices.module';
import { MultiDbModule } from './modules/multi-db/multi-db.module';
import { PlatformFeeModule } from './modules/platform-fee/platform-fee.module';
import { OrderFeeModule } from './modules/order-fee/order-fee.module';
import { PaymentModule } from './modules/payment/payment.module';
import { SyncTask } from './tasks/sync.task';
import { Sale } from './entities/sale.entity';
import { VoucherIssueModule } from './modules/voucher-issue/voucher-issue.module';
import { StockTransferModule } from './modules/stock-transfer/stock-transfer.module';
import { ShopeeFeesModule } from './modules/shopee-fees/shopee-fees.module';
import { TikTokFeesModule } from './modules/tiktok-fees/tiktok-fees.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    ScheduleModule.forRoot(),
    // Primary Database Connection (103.145.79.165)
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: getDatabaseConfig,
      inject: [ConfigService],
    }),
    // Secondary Database Connection (103.145.79.165)
    TypeOrmModule.forRootAsync({
      name: 'secondary',
      imports: [ConfigModule],
      useFactory: getSecondaryDatabaseConfig,
      inject: [ConfigService],
    }),
    // Third Database Connection (103.145.79.37)
    TypeOrmModule.forRootAsync({
      name: 'third',
      imports: [ConfigModule],
      useFactory: getThirdDatabaseConfig,
      inject: [ConfigService],
    }),
    SyncModule,
    InvoicesModule,
    SalesModule,
    CategoriesModule,
    FastApiInvoicesModule,
    MultiDbModule,
    PlatformFeeModule,
    OrderFeeModule,
    PaymentModule,
    VoucherIssueModule,
    StockTransferModule,
    ShopeeFeesModule,
    TikTokFeesModule,
    TypeOrmModule.forFeature([Sale]),
  ],
  controllers: [AppController],
  providers: [AppService, SyncTask],
})
export class AppModule {}
