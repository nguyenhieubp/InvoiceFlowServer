import { Module, forwardRef } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { FastIntegrationService } from './fast-integration.service';
import { SalesModule } from '../sales/sales.module';
import { FastIntegrationController } from './fast-integration.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { POChargeHistory } from './entities/po-charge-history.entity';
import { AuditPo } from './entities/audit-po.entity';

@Module({
    imports: [
        HttpModule,
        TypeOrmModule.forFeature([POChargeHistory, AuditPo]),
        forwardRef(() => SalesModule),
    ],
    controllers: [FastIntegrationController],
    providers: [FastIntegrationService],
    exports: [FastIntegrationService],
})
export class FastIntegrationModule { }
