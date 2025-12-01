import {
  Controller,
  Get,
  Post,
  Put,
  Body,
  Param,
  Delete,
  Res,
} from '@nestjs/common';
import { InvoiceService } from '../../services/invoice.service';
import { CreateInvoiceDto } from '../../dto/create-invoice.dto';
import type { Response } from 'express';

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

  @Get(':id/pdf')
  async downloadPdf(@Param('id') id: string, @Res() res: Response) {
    const { buffer, fileName } = await this.invoiceService.downloadInvoicePdf(id);
    res.set({
      'Content-Type': 'application/pdf',
      'Content-Disposition': `attachment; filename="${fileName}"`,
    });
    res.send(buffer);
  }

  @Put(':id')
  async update(
    @Param('id') id: string,
    @Body() updateInvoiceDto: Partial<CreateInvoiceDto>,
  ) {
    return this.invoiceService.updateInvoice(id, updateInvoiceDto);
  }

  @Post(':id/print')
  async print(@Param('id') id: string) {
    return this.invoiceService.printInvoice(id);
  }
}

