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
import { mapToOrderResponse } from '../mappers/sale-response.mapper';
import { SalesListResponseDto } from '../dto/sale-response.dto';
import * as SalesUtils from '../../../utils/sales.utils';

@Controller('sales')
export class SalesController {
  constructor(private readonly salesService: SalesService) {}

  /**
   * V2 Endpoint - Optimized Response (60-70% smaller)
   * Chỉ trả về fields frontend thực sự cần
   */
  @Get('v2')
  async findAllV2(
    @Query('brand') brand?: string,
    @Query('processed') processed?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('date') date?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
    @Query('typeSale') typeSale?: string,
  ): Promise<SalesListResponseDto> {
    // Get data từ service (full data)
    const result = await this.salesService.findAllOrders({
      brand,
      isProcessed:
        processed === 'true' ? true : processed === 'false' ? false : undefined,
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      date,
      dateFrom,
      dateTo,
      search,
      typeSale,
    });

    // Map to optimized DTOs
    return {
      data: result.data ? result.data.map(mapToOrderResponse) : [],
      total: result.total,
      page: result.page,
      limit: result.limit,
      totalPages: result.totalPages,
    };
  }

  /**
   * V2 Aggregated Endpoint - Optimized Response
   */
  @Get('v2/aggregated')
  async findAllAggregatedV2(
    @Query('brand') brand?: string,
    @Query('processed') processed?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('date') date?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
    @Query('typeSale') typeSale?: string,
  ): Promise<SalesListResponseDto> {
    const result = await this.salesService.findAllAggregatedOrders({
      brand,
      isProcessed:
        processed === 'true' ? true : processed === 'false' ? false : undefined,
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      date,
      dateFrom,
      dateTo,
      search,
      typeSale,
    });

    // Map to optimized DTOs
    return {
      data: result.data ? result.data.map(mapToOrderResponse) : [],
      total: result.total,
      page: result.page,
      limit: result.limit,
      totalPages: result.totalPages,
    };
  }

  @Get('aggregated')
  async findAllAggregated(
    @Query('brand') brand?: string,
    @Query('processed') processed?: string,
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('date') date?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
    @Query('typeSale') typeSale?: string,
  ) {
    return this.salesService.findAllAggregatedOrders({
      brand,
      isProcessed:
        processed === 'true' ? true : processed === 'false' ? false : undefined,
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      date,
      dateFrom,
      dateTo,
      search,
      typeSale,
    });
  }

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

  @Get('export-orders')
  async exportOrders(
    @Res() res: Response,
    @Query('brand') brand?: string,
    @Query('typeSale') typeSale?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
  ) {
    try {
      const XLSX = await import('xlsx');

      // Get all filtered orders without pagination, using standard mode with export=true for full enrichment (including Stock Transfers)
      const result = await this.salesService.findAllOrders({
        brand,
        typeSale,
        dateFrom,
        dateTo,
        search,
        page: 1,
        limit: 100000, // Get all results
        export: true,
      });

      // Flatten data for Excel export
      const flatData: any[] = [];
      const formatDate = (date: any) => {
        if (!date) return '';
        const d = new Date(date);
        if (isNaN(d.getTime())) return date;
        const day = String(d.getDate()).padStart(2, '0');
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const year = d.getFullYear();
        return `${day}/${month}/${year}`;
      };

      for (const order of result.data || []) {
        if (order.sales && order.sales.length > 0) {
          for (const sale of order.sales) {
            const ordertypeName = sale.ordertypeName || sale.ordertype || '';
            const isTachThe = SalesUtils.isTachTheOrder(ordertypeName);

            const partnerCode =
              isTachThe && sale.issuePartnerCode
                ? sale.issuePartnerCode
                : order.customer?.code || order.partnerCode || '';

            flatData.push({
              '* Mã khách': partnerCode,
              '* Ngày': formatDate(order.docDate || order.date),
              '* Số hóa đơn': order.docCode,
              '* Ký hiệu':
                sale.department?.ma_dvcs ||
                sale.branchCode ||
                order.branchCode ||
                '',
              'Nhãn Hàng': order.brand || sale.brand || '',
              'Loại Bán': order.typeSale || sale.type_sale || '',
              'Diễn giải': order.docCode || '',
              '* Mã hàng': sale.product?.maVatTu || sale.itemCode || '',
              Đvt: sale.product?.dvt || sale.dvt || '',
              'Loại đơn hàng': sale.ordertypeName || '',
              'Loại sản phẩm':
                sale.productType || sale.product?.productType || '',
              'Khuyến mãi': sale.km_yn || '',
              '* Mã kho':
                sale.maKho ||
                sale.stockTransfer?.stockCode ||
                (isTachThe ? 'B' + (sale.branchCode || order.branchCode) : ''),
              '* Mã lô': sale.maLo || '',
              'Số lượng': sale.qty || 0,
              'Giá bán': sale.giaBan || sale.price || 0,
              'Tiền hàng': sale.tienHang || sale.amount || 0,
              'Tỷ giá': sale.tyGia || 1,
              '* Mã thuế': sale.maThue || 'VPT',
              '* Tk nợ': sale.tkNo || '1311',
              '* Tk doanh thu': sale.tkDoanhThuDisplay || '',
              '* Tk giá vốn': sale.tkGiaVonDisplay || '',
              'TK Chiết khấu': sale.tkChietKhau || '',
              'TK Chi phí': sale.tkChiPhi || '',
              'Mã phí': sale.maPhi || '',
              '* Cục thuế': sale.cucThueDisplay || '',
              'Mã thanh toán': sale.maThanhToan || '',
              'Vụ việc': sale.vuViec || '',
              'Bộ phận': sale.department?.ma_bp || sale.branchCode || '',
              'Mã dịch vụ': sale.svcCode || '',
              'Trạng thai': order.isProcessed ? 'Đã xử lý' : 'Chưa xử lý',
              Barcode: sale.barcode || sale.product?.barcode || '',
              'Mua hàng giảm giá': sale.ma_ck01 || sale.maCk01 || '',
              'Chiết khấu mua hàng giảm giá':
                sale.ck01_nt ||
                sale.ck01Nt ||
                sale.chietKhauMuaHangGiamGia ||
                0,
              'Mã CK theo chính sách': sale.ma_ck02 || sale.maCk02 || '',
              'CK theo chính sách': sale.ck02_nt || sale.ck02Nt || 0,
              'Mua hàng CK VIP': sale.ma_ck03 || sale.maCk03 || '',
              'Chiết khấu mua hàng CK VIP':
                sale.ck03_nt || sale.ck03Nt || sale.chietKhauMuaHangCkVip || 0,
              'Thanh toán coupon': sale.ma_ck04 || sale.maCk04 || '',
              'Chiết khấu thanh toán coupon':
                sale.ck04_nt ||
                sale.ck04Nt ||
                sale.chietKhauThanhToanCoupon ||
                0,
              'Thanh toán voucher': sale.ma_ck05 || sale.maCk05 || '',
              'Chiết khấu thanh toán voucher':
                sale.ck05_nt ||
                sale.ck05Nt ||
                sale.chietKhauThanhToanVoucher ||
                0,
              'Thanh toán TK tiền ảo': sale.ma_ck11 || sale.maCk11 || '',
              'Chiết khấu thanh toán TK tiền ảo':
                sale.ck11_nt ||
                sale.ck11Nt ||
                sale.chietKhauThanhToanTkTienAo ||
                0,
              'Mã CTKM tặng hàng': sale.maCtkmTangHang || '',
              'Mã thẻ': sale.maThe || '',
              'Số serial': sale.maSerial || sale.soSerial || sale.serial || '',
              'Mã VT tham chiếu': sale.ma_vt_ref || '',
              'Mã kho xuất': sale.stockTransfer?.stockCode || '',
              'Số lượng xuất kho': sale.stockTransfer?.qty
                ? Math.abs(Number(sale.stockTransfer.qty))
                : '',
              'Ngày xuất kho': sale.stockTransfer?.transDate
                ? formatDate(sale.stockTransfer.transDate)
                : '',
              'Mã CT': sale.stockTransfer?.docCode || '',
            });
          }
        } else {
          flatData.push({
            '* Mã khách': order.customer?.code || order.partnerCode || '',
            '* Ngày': formatDate(order.docDate || order.date),
            '* Số hóa đơn': order.docCode,
            'Nhãn Hàng': order.brand || '',
            'Loại Bán': order.typeSale || '',
            'Trạng thai': order.isProcessed ? 'Đã xử lý' : 'Chưa xử lý',
          });
        }
      }

      // Create worksheet
      const worksheet = XLSX.utils.json_to_sheet(flatData);
      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Orders');

      // Generate buffer
      const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });

      // Set headers for Excel download
      res.setHeader(
        'Content-Type',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      );
      res.setHeader(
        'Content-Disposition',
        `attachment; filename=orders_${new Date().toISOString().split('T')[0]}.xlsx`,
      );
      res.setHeader('Content-Length', buffer.length);

      return res.send(buffer);
    } catch (error: any) {
      throw new BadRequestException(
        `Lỗi khi xuất dữ liệu: ${error?.message || error}`,
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

  @Post('order/:docCode/create-invoice-fast')
  async createInvoiceViaFastApi(
    @Param('docCode') docCode: string,
    @Body() body: { forceRetry?: boolean; onlySalesOrder?: boolean },
  ) {
    const result = await this.salesService.createInvoiceViaFastApi(
      docCode,
      body?.forceRetry || false,
      { onlySalesOrder: body?.onlySalesOrder },
    );

    if (result && result.success === false) {
      throw new BadRequestException(result.message || 'Tạo hóa đơn thất bại');
    }

    return result;
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

  @Post('invoice/retry-failed')
  async retryFailedInvoices() {
    return this.salesService.retryFailedInvoices();
  }

  @Post('invoice/batch-process')
  async batchProcessInvoices(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
  ) {
    if (!startDate || !endDate) {
      throw new BadRequestException(
        'startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
      );
    }
    return this.salesService.processInvoicesByDateRange(startDate, endDate);
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

  @Post('error-order/:id')
  async updateErrorOrder(
    @Param('id') id: string,
    @Body() body: { materialCode?: string; branchCode?: string },
  ) {
    return this.salesService.updateErrorOrder(id, body);
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

  @Get('statistics/order-count')
  countOrders(
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('search') search?: string,
    @Query('typeSale') typeSale?: string,
    @Query('isProcessed') isProcessed?: string,
    @Query('statusAsys') statusAsys?: string,
  ) {
    return this.salesService.countOrders({
      brand,
      dateFrom,
      dateTo,
      search,
      typeSale,
      isProcessed:
        isProcessed === 'true'
          ? true
          : isProcessed === 'false'
            ? false
            : undefined,
      statusAsys:
        statusAsys === 'true'
          ? true
          : statusAsys === 'false'
            ? false
            : undefined,
    });
  }

  @Post('stock-transfer/warehouse-retry-failed-by-date-range')
  async retryWarehouseFailedByDateRange(
    @Body('dateFrom') dateFrom: string,
    @Body('dateTo') dateTo: string,
    @Body('doctype') doctype?: string,
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
        doctype,
      );
      return result;
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi xử lý lại warehouse batch',
      );
    }
  }

  @Post('stock-transfer/warehouse-sync-by-date-range')
  async processWarehouseByDateRange(
    @Body('dateFrom') dateFrom: string,
    @Body('dateTo') dateTo: string,
    @Body('doctype') doctype?: string,
  ) {
    if (!dateFrom || !dateTo) {
      throw new BadRequestException(
        'dateFrom và dateTo là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
      );
    }

    try {
      const result =
        await this.salesService.processWarehouseByDateRangeAndDoctype(
          dateFrom,
          dateTo,
          doctype,
        );
      return result;
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi đồng bộ warehouse batch',
      );
    }
  }
}
