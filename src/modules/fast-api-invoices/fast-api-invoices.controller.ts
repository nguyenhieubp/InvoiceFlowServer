import {
  Controller,
  Get,
  Query,
  Param,
  ParseIntPipe,
  DefaultValuePipe,
  Post,
  Body,
  BadRequestException,
} from '@nestjs/common';
import { FastApiInvoiceService } from '../../services/fast-api-invoice.service';
import { SalesService } from '../sales/sales.service';

@Controller('fast-api-invoices')
export class FastApiInvoicesController {
  constructor(
    private readonly fastApiInvoiceService: FastApiInvoiceService,
    private readonly salesService: SalesService,
  ) {}

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

  @Post('sync-by-date-range')
  async syncByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('maDvcs') maDvcs?: string,
  ) {
    if (!startDate || !endDate) {
      throw new BadRequestException('startDate và endDate là bắt buộc (format: YYYY-MM-DD)');
    }

    const startDateObj = new Date(startDate);
    const endDateObj = new Date(endDate);

    if (isNaN(startDateObj.getTime()) || isNaN(endDateObj.getTime())) {
      throw new BadRequestException('startDate và endDate phải là định dạng ngày hợp lệ');
    }

    if (startDateObj > endDateObj) {
      throw new BadRequestException('startDate phải nhỏ hơn hoặc bằng endDate');
    }

    // Lấy danh sách invoice thất bại trong khoảng thời gian
    const failedInvoices = await this.fastApiInvoiceService.getFailedInvoicesByDateRange({
      startDate: startDateObj,
      endDate: endDateObj,
      maDvcs,
    });

    if (failedInvoices.length === 0) {
      return {
        success: true,
        message: `Không có invoice thất bại nào trong khoảng thời gian ${startDate} đến ${endDate}${maDvcs ? ` (Mã ĐVCS: ${maDvcs})` : ''}`,
        total: 0,
        successCount: 0,
        failCount: 0,
        alreadyExistsCount: 0,
        results: [],
      };
    }

    // Đồng bộ từng invoice
    let successCount = 0;
    let failCount = 0;
    let alreadyExistsCount = 0;
    const results: Array<{
      docCode: string;
      success: boolean;
      message?: string;
      alreadyExists?: boolean;
      error?: string;
    }> = [];

    for (const invoice of failedInvoices) {
      try {
        const result = await this.salesService.createInvoiceViaFastApi(invoice.docCode, true);
        
        if (result.alreadyExists) {
          alreadyExistsCount++;
          results.push({
            docCode: invoice.docCode,
            success: true,
            message: result.message,
            alreadyExists: true,
          });
        } else if (result.success) {
          successCount++;
          results.push({
            docCode: invoice.docCode,
            success: true,
            message: result.message,
          });
        } else {
          failCount++;
          results.push({
            docCode: invoice.docCode,
            success: false,
            message: result.message,
            error: result.message,
          });
        }
      } catch (error: any) {
        failCount++;
        results.push({
          docCode: invoice.docCode,
          success: false,
          error: error?.message || 'Lỗi không xác định',
        });
      }
    }

    return {
      success: true,
      message: `Đồng bộ hoàn tất: ${successCount} thành công, ${failCount} thất bại, ${alreadyExistsCount} đã tồn tại`,
      total: failedInvoices.length,
      successCount,
      failCount,
      alreadyExistsCount,
      results,
    };
  }
}

