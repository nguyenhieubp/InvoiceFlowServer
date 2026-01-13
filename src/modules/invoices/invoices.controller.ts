import {
  Controller,
  Get,
  Post,
  Put,
  Body,
  Param,
  Delete,
} from '@nestjs/common';
import { InvoiceService } from './invoice.service';
import { CreateInvoiceDto } from '../../dto/create-invoice.dto';

@Controller('invoices')
export class InvoicesController {
  constructor(private readonly invoiceService: InvoiceService) {}

  @Post()
  async create(@Body() createInvoiceDto: CreateInvoiceDto) {
    return this.invoiceService.createInvoice(createInvoiceDto);
  }

  @Get()
  async findAll() {
    return this.invoiceService.getAllInvoices();
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    return this.invoiceService.getInvoice(id);
  }

  @Put(':id')
  async update(
    @Param('id') id: string,
    @Body() updateInvoiceDto: Partial<CreateInvoiceDto>,
  ) {
    return this.invoiceService.updateInvoice(id, updateInvoiceDto);
  }
}
