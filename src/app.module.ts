import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { getDatabaseConfig } from './config/database.config';
import { SyncModule } from './modules/sync/sync.module';
import { InvoicesModule } from './modules/invoices/invoices.module';
import { SalesModule } from './modules/sales/sales.module';
import { CategoriesModule } from './modules/categories/categories.module';
import { FastApiInvoicesModule } from './modules/fast-api-invoices/fast-api-invoices.module';
import { SyncTask } from './tasks/sync.task';
import { AutoInvoiceTask } from './tasks/auto-invoice.task';
import { Sale } from './entities/sale.entity';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    ScheduleModule.forRoot(),
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: getDatabaseConfig,
      inject: [ConfigService],
    }),
    SyncModule,
    InvoicesModule,
    SalesModule,
    CategoriesModule,
    FastApiInvoicesModule,
    TypeOrmModule.forFeature([Sale]),
  ],
  controllers: [AppController],
  providers: [AppService, SyncTask, AutoInvoiceTask],
})
export class AppModule {}
