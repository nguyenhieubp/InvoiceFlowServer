import {
  Controller,
  Get,
  Query,
  Param,
  Post,
  Body,
  BadRequestException,
  NotFoundException,
  Res,
} from '@nestjs/common';
import { SalesService } from '../services/sales.service';
import type { CreateStockTransferDto } from '../../../dto/create-stock-transfer.dto';
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
    @Query('typeSale') typeSale?: string, // Type sale: "WHOLESALE" or "RETAIL"
  ) {
    // Luôn trả về danh sách đơn hàng (gộp theo docCode) với dữ liệu cơ bản
    return this.salesService.findAllOrders({
      brand,
      isProcessed:
        processed === 'true' ? true : processed === 'false' ? false : undefined,
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      date, // Pass date parameter
      dateFrom, // Pass dateFrom parameter
      dateTo, // Pass dateTo parameter
      search, // Pass search parameter
      typeSale, // Pass typeSale parameter
    });
  }

  @Get('status-asys')
  async getStatusAsys(
    @Query('statusAsys') statusAsys?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
  ) {
    try {
      const pageNumber = page ? parseInt(page) : undefined;
      const limitNumber = limit ? parseInt(limit) : undefined;

      return await this.salesService.getStatusAsys(
        statusAsys,
        pageNumber,
        limitNumber,
        brand,
        dateFrom,
        dateTo,
        search,
      );
    } catch (error: any) {
      throw new BadRequestException(
        `Lỗi khi lấy danh sách đơn hàng: ${error?.message || error}`,
      );
    }
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    return this.salesService.findOne(id);
  }

  @Get('order/:docCode')
  async findByOrderCode(@Param('docCode') docCode: string) {
    return this.salesService.findByOrderCode(docCode);
  }

  @Post('mark-processed-from-invoices')
  async markProcessedOrdersFromInvoices() {
    return this.salesService.markProcessedOrdersFromInvoices();
  }

  @Post('sync-from-zappy')
  async syncFromZappy(
    @Body('date') date: string,
    @Body('brand') brand?: string,
  ) {
    if (!date) {
      throw new BadRequestException(
        'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 04DEC2025)',
      );
    }
    return this.salesService.syncFromZappy(date, brand);
  }

  @Post('sync-sales-by-date-range')
  async syncSalesByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
  ) {
    if (!startDate || !endDate) {
      throw new BadRequestException(
        'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
      );
    }
    return this.salesService.syncSalesByDateRange(startDate, endDate);
  }

  @Post('sync-sales-oct-dec-2025')
  async syncSalesOctDec2025() {
    // Đồng bộ sale từ 01/10/2025 đến 01/12/2025 cho tất cả các nhãn
    return this.salesService.syncSalesByDateRange('01OCT2025', '01DEC2025');
  }

  @Post('order/:docCode/create-invoice-fast')
  async createInvoiceViaFastApi(
    @Param('docCode') docCode: string,
    @Body() body: { forceRetry?: boolean },
  ) {
    return this.salesService.createInvoiceViaFastApi(
      docCode,
      body?.forceRetry || false,
    );
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
    if (
      !createDto.data ||
      !Array.isArray(createDto.data) ||
      createDto.data.length === 0
    ) {
      throw new BadRequestException('Dữ liệu stock transfer không hợp lệ');
    }
    return this.salesService.createStockTransfer(createDto);
  }

  @Post('sync-error-orders')
  async syncErrorOrders() {
    return this.salesService.syncErrorOrders();
  }

  @Post('sync-error-order/:docCode')
  async syncErrorOrderByDocCode(@Param('docCode') docCode: string) {
    return this.salesService.syncErrorOrderByDocCode(docCode);
  }

  @Post('stock-transfer/:id/warehouse')
  async processWarehouseFromStockTransfer(@Param('id') id: string) {
    try {
      // Lấy stock transfer từ database
      const stockTransfer = await this.salesService.getStockTransferById(id);
      if (!stockTransfer) {
        throw new NotFoundException(
          `Stock transfer với id ${id} không tồn tại`,
        );
      }

      // Xử lý warehouse receipt/release
      return await this.salesService.processWarehouseFromStockTransfer(
        stockTransfer,
      );
    } catch (error: any) {
      throw new BadRequestException(error.message || 'Lỗi khi xử lý warehouse');
    }
  }

  @Post('stock-transfer/doc-code/:docCode/warehouse-retry')
  async retryWarehouseFromStockTransferByDocCode(
    @Param('docCode') docCode: string,
  ) {
    try {
      return await this.salesService.processWarehouseFromStockTransferByDocCode(
        docCode,
      );
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi xử lý lại warehouse',
      );
    }
  }

  @Post('stock-transfer/warehouse-retry-failed-by-date-range')
  async retryWarehouseFailedByDateRange(
    @Body('dateFrom') dateFrom: string,
    @Body('dateTo') dateTo: string,
  ) {
    if (!dateFrom || !dateTo) {
      throw new BadRequestException(
        'dateFrom và dateTo là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
      );
    }

    try {
      const result = await this.salesService.retryWarehouseFailedByDateRange(
        dateFrom,
        dateTo,
      );
      return result;
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi xử lý lại warehouse batch',
      );
    }
  }
}
