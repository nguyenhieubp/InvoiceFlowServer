import { Controller, Get, Query, Post, Body, Param, Res } from '@nestjs/common';
import express from 'express';
import { PaymentService } from './payment.service';

@Controller('payments')
export class PaymentController {
  constructor(private readonly paymentService: PaymentService) {}

  @Get('audit')
  async getAuditLogs(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('docCode') docCode?: string,
    @Query('status') status?: string,
  ) {
    return this.paymentService.getAuditLogs({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 20,
      docCode,
      status,
    });
  }

  @Post('audit/:id/retry')
  async retryPaymentSync(@Param('id') id: string) {
    return this.paymentService.retryPaymentSync(id);
  }

  @Get()
  async findAll(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('brand') brand?: string,
    @Query('fopSyscode') fopSyscode?: string,
  ) {
    return this.paymentService.findAll({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 10,
      search,
      dateFrom,
      dateTo,
      brand,
      fopSyscode,
    });
  }

  @Get('export')
  async exportPaymentsExcel(
    @Query('search') search?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('brand') brand?: string,
    @Query('fopSyscode') fopSyscode?: string,
    @Res() res?: express.Response,
  ) {
    const buffer = await this.paymentService.exportPaymentsToExcel({
      search,
      dateFrom,
      dateTo,
      brand,
      fopSyscode,
    });

    const fileName = `PaymentDocuments_${new Date().toISOString().split('T')[0]}.xlsx`;

    if (res) {
      res.set({
        'Content-Type':
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="${fileName}"`,
        'Content-Length': buffer.length,
      });

      res.send(buffer);
    }

    return buffer;
  }

  @Post('fast')
  async fastPaymentLog(@Body() body: any) {
    return this.paymentService.processFastPayment(body);
  }

  @Get('statistics')
  async getStatistics(
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('brand') brand?: string,
  ) {
    return this.paymentService.getStatistics({
      dateFrom,
      dateTo,
      brand,
    });
  }

  @Get('payment-methods')
  async getPaymentMethods(
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('brand') brand?: string,
  ) {
    return this.paymentService.getPaymentMethods({
      dateFrom,
      dateTo,
      brand,
    });
  }
}
