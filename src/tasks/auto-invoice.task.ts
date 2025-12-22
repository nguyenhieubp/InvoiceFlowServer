import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Sale } from '../entities/sale.entity';
import { SalesService } from '../modules/sales/sales.service';

@Injectable()
export class AutoInvoiceTask {
  private readonly logger = new Logger(AutoInvoiceTask.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    private salesService: SalesService,
  ) {}

}

