import {
  Controller,
  Get,
  Query,
  Param,
  ParseIntPipe,
  DefaultValuePipe,
} from '@nestjs/common';
import { FastApiInvoiceService } from '../../services/fast-api-invoice.service';

@Controller('fast-api-invoices')
export class FastApiInvoicesController {
  constructor(private readonly fastApiInvoiceService: FastApiInvoiceService) {}

  @Get('test')
  async test() {
    return { message: 'FastApiInvoicesController is working', timestamp: new Date().toISOString() };
  }

  @Get('statistics')
  async getStatistics(
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Query('maDvcs') maDvcs?: string,
  ) {
    const options: any = {};

    if (startDate) {
      options.startDate = new Date(startDate);
    }

    if (endDate) {
      options.endDate = new Date(endDate);
    }

    if (maDvcs) {
      options.maDvcs = maDvcs;
    }

    return this.fastApiInvoiceService.getStatistics(options);
  }

  @Get('doc-code/:docCode')
  async findByDocCode(@Param('docCode') docCode: string) {
    return this.fastApiInvoiceService.findByDocCode(docCode);
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    return this.fastApiInvoiceService.findOne(id);
  }

  @Get()
  async findAll(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('status') status?: string,
    @Query('docCode') docCode?: string,
    @Query('maKh') maKh?: string,
    @Query('tenKh') tenKh?: string,
    @Query('maDvcs') maDvcs?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    const options: any = {
      page: page ? parseInt(page) || 1 : 1,
      limit: limit ? parseInt(limit) || 50 : 50,
    };

    if (status !== undefined && status !== '') {
      options.status = parseInt(status);
    }

    if (docCode) {
      options.docCode = docCode;
    }

    if (maKh) {
      options.maKh = maKh;
    }

    if (tenKh) {
      options.tenKh = tenKh;
    }

    if (maDvcs) {
      options.maDvcs = maDvcs;
    }

    if (startDate) {
      options.startDate = new Date(startDate);
    }

    if (endDate) {
      options.endDate = new Date(endDate);
    }

    return this.fastApiInvoiceService.findAll(options);
  }
}

