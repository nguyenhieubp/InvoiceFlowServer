import { Controller, Get, Query, Param, Post, Body, BadRequestException, Res } from '@nestjs/common';
import { SalesService } from './sales.service';
import type { CreateStockTransferDto } from '../../dto/create-stock-transfer.dto';
import type { Response } from 'express';

@Controller('sales')
export class SalesController {
  constructor(private readonly salesService: SalesService) {}

  @Get()
  async findAll(
    @Query('brand') brand?: string,
    @Query('processed') processed?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('date') date?: string, // Format: DDMMMYYYY (ví dụ: 04DEC2025)
    @Query('dateFrom') dateFrom?: string, // Format: YYYY-MM-DD hoặc ISO string
    @Query('dateTo') dateTo?: string, // Format: YYYY-MM-DD hoặc ISO string
    @Query('search') search?: string, // Search query để tìm theo docCode, customer name, code, mobile
  ) {
    // Luôn trả về danh sách đơn hàng (gộp theo docCode) với dữ liệu cơ bản
    return this.salesService.findAllOrders({
      brand,
      isProcessed: processed === 'true' ? true : processed === 'false' ? false : undefined,
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      date, // Pass date parameter
      dateFrom, // Pass dateFrom parameter
      dateTo, // Pass dateTo parameter
      search, // Pass search parameter
    });
  }

  @Get('export-orders')
  async exportOrders(
    @Res() res: Response,
    @Query('brand') brand?: string,
    @Query('processed') processed?: string,
    @Query('date') date?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
    @Query('statusAsys') statusAsys?: string,
  ) {
    const buffer = await this.salesService.exportOrders({
      brand,
      isProcessed: processed === 'true' ? true : processed === 'false' ? false : undefined,
      date,
      dateFrom,
      dateTo,
      search,
      statusAsys: statusAsys === 'true' ? true : statusAsys === 'false' ? false : undefined,
    });

    // Generate filename
    const now = new Date();
    const dateStr = now.toISOString().split('T')[0].replace(/-/g, '');
    const brandSuffix = brand ? `_${brand.toUpperCase()}` : '';
    const fileName = `DonHang_${dateStr}${brandSuffix}.xlsx`;

    // Set headers
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(fileName)}"`);
    res.setHeader('Content-Length', buffer.length);

    // Send buffer
    res.send(buffer);
  }

  @Get('giai-trinh-faceid')
  async getAllGiaiTrinhFaceId(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('date') date?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('orderCode') orderCode?: string,
    @Query('partnerCode') partnerCode?: string,
    @Query('faceStatus') faceStatus?: 'yes' | 'no',
    @Query('brandCode') brandCode?: string,
  ) {
    return this.salesService.getAllGiaiTrinhFaceId({
      page: page ? parseInt(page) : undefined,
      limit: limit ? parseInt(limit) : undefined,
      date,
      dateFrom,
      dateTo,
      orderCode,
      partnerCode,
      faceStatus,
      brandCode,
    });
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    return this.salesService.findOne(id);
  }

  @Get('order/:docCode')
  async findByOrderCode(@Param('docCode') docCode: string) {
    return this.salesService.findByOrderCode(docCode);
  }

  @Post('order/:docCode/print')
  async printOrder(@Param('docCode') docCode: string) {
    return this.salesService.printOrder(docCode);
  }

  @Post('orders/print')
  async printOrders(@Body('docCodes') docCodes: string[]) {
    if (!Array.isArray(docCodes) || docCodes.length === 0) {
      throw new BadRequestException('Danh sách đơn hàng không hợp lệ');
    }
    return this.salesService.printMultipleOrders(docCodes);
  }

  @Post('mark-processed-from-invoices')
  async markProcessedOrdersFromInvoices() {
    return this.salesService.markProcessedOrdersFromInvoices();
  }

  @Post('sync-from-zappy')
  async syncFromZappy(@Body('date') date: string) {
    if (!date) {
      throw new BadRequestException('Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 04DEC2025)');
    }
    return this.salesService.syncFromZappy(date);
  }

  @Post('order/:docCode/create-invoice-fast')
  async createInvoiceViaFastApi(
    @Param('docCode') docCode: string,
    @Body('forceRetry') forceRetry?: boolean,
  ) {
    return this.salesService.createInvoiceViaFastApi(docCode, forceRetry || false);
  }

  @Post('orders/create-invoice-fast')
  async createMultipleInvoicesViaFastApi(@Body('docCodes') docCodes: string[]) {
    if (!Array.isArray(docCodes) || docCodes.length === 0) {
      throw new BadRequestException('Danh sách đơn hàng không hợp lệ');
    }
    
    const results: Array<{
      docCode: string;
      success: boolean;
      message?: string;
      result?: any;
      error?: string;
    }> = [];
    for (const docCode of docCodes) {
      try {
        const result = await this.salesService.createInvoiceViaFastApi(docCode);
        results.push({
          docCode,
          success: true,
          ...result,
        });
      } catch (error: any) {
        results.push({
          docCode,
          success: false,
          error: error?.message || 'Unknown error',
        });
      }
    }

    const successCount = results.filter((r) => r.success).length;
    const failureCount = results.length - successCount;

    return {
      total: results.length,
      successCount,
      failureCount,
      results,
    };
  }

  @Post('stock-transfer')
  async createStockTransfer(@Body() createDto: CreateStockTransferDto) {
    if (!createDto.data || !Array.isArray(createDto.data) || createDto.data.length === 0) {
      throw new BadRequestException('Dữ liệu stock transfer không hợp lệ');
    }
    return this.salesService.createStockTransfer(createDto);
  }

  @Get('check-face-id/:partnerCode')
  async getCheckFaceIdByPartnerCode(
    @Param('partnerCode') partnerCode: string,
    @Query('date') date?: string,
  ) {
    return this.salesService.getCheckFaceIdByPartnerCode(partnerCode, date);
  }

  @Get('orders-with-check-face-id/:partnerCode')
  async getOrdersWithCheckFaceId(
    @Param('partnerCode') partnerCode: string,
    @Query('date') date?: string,
  ) {
    return this.salesService.getOrdersWithCheckFaceId(partnerCode, date);
  }
}

