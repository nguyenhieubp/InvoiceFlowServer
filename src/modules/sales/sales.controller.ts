import { Controller, Get, Query, Param, Post, Body, BadRequestException } from '@nestjs/common';
import { SalesService } from './sales.service';
import type { CreateStockTransferDto } from '../../dto/create-stock-transfer.dto';

@Controller('sales')
export class SalesController {
  constructor(private readonly salesService: SalesService) {}

  @Get()
  async findAll(
    @Query('brand') brand?: string,
    @Query('processed') processed?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('groupBy') groupBy?: string,
    @Query('date') date?: string, // Format: DDMMMYYYY (ví dụ: 04DEC2025)
  ) {
    // Nếu groupBy=order thì trả về danh sách đơn hàng (gộp theo docCode)
    if (groupBy === 'order') {
      return this.salesService.findAllOrders({
        brand,
        isProcessed: processed === 'true' ? true : processed === 'false' ? false : undefined,
        page: page ? parseInt(page) : 1,
        limit: limit ? parseInt(limit) : 50,
        date, // Pass date parameter
      });
    }
    
    // Mặc định trả về danh sách sales
    return this.salesService.findAll({
      brand,
      isProcessed: processed === 'true' ? true : processed === 'false' ? false : undefined,
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
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
}

