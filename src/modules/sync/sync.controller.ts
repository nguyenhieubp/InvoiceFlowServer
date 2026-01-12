import {
  Controller,
  Post,
  Param,
  Get,
  Body,
  Query,
  HttpException,
  HttpStatus,
  Res,
} from '@nestjs/common';
import { SyncService } from './sync.service';
import type { Response } from 'express';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Post('brand/:brandName')
  async syncBrand(
    @Param('brandName') brandName: string,
    @Body('date') date: string,
  ) {
    if (!date) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 04DEC2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncBrand(brandName, date);
      return {
        ...result,
        brand: brandName,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      throw new HttpException(
        {
          success: false,
          message: `Lỗi khi đồng bộ ${brandName}`,
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get('brands')
  getAvailableBrands() {
    return {
      brands: [
        { name: 'chando', displayName: 'Chando' },
        { name: 'f3', displayName: 'F3' },
        { name: 'labhair', displayName: 'LabHair' },
        { name: 'yaman', displayName: 'Yaman' },
        { name: 'menard', displayName: 'Menard' },
      ],
    };
  }

  /**
   * Đồng bộ stock transfer từ ngày đến ngày
   * Phải đặt trước route /:brandName để tránh conflict
   * @param dateFrom - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param dateTo - Date format: DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('stock-transfer/range')
  async syncStockTransferRange(@Body() body: any) {
    const dateFrom = body?.dateFrom || body?.DateFrom;
    const dateTo = body?.dateTo || body?.DateTo;
    const brand = body?.brand || body?.Brand;

    if (
      !dateFrom ||
      !dateTo ||
      (typeof dateFrom === 'string' && dateFrom.trim() === '') ||
      (typeof dateTo === 'string' && dateTo.trim() === '')
    ) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số dateFrom và dateTo là bắt buộc (format: DDMMMYYYY, ví dụ: 01NOV2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncStockTransferRange(
        dateFrom,
        dateTo,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ stock transfer range',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ stock transfer cho một brand và một ngày
   * @param brandName - Brand name (f3, labhair, yaman, menard)
   * @param date - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   */
  @Post('stock-transfer/:brandName')
  async syncStockTransfer(
    @Param('brandName') brandName: string,
    @Body() body: any,
  ) {
    const date = body?.date || body?.Date;

    if (!date || (typeof date === 'string' && date.trim() === '')) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 01NOV2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncStockTransfer(date, brandName);
      return {
        ...result,
        brand: brandName,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: `Lỗi khi đồng bộ stock transfer cho ${brandName}`,
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ báo cáo nộp quỹ cuối ca theo khoảng thời gian
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('shift-end-cash/range')
  async syncShiftEndCashByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('brand') brand?: string,
  ) {
    if (!startDate || !endDate) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncShiftEndCashByDateRange(
        startDate,
        endDate,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message:
            'Lỗi khi đồng bộ báo cáo nộp quỹ cuối ca theo khoảng thời gian',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ báo cáo nộp quỹ cuối ca
   * @param date - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('shift-end-cash')
  async syncShiftEndCash(
    @Body('date') date: string,
    @Body('brand') brand?: string,
  ) {
    if (!date) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 01NOV2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncShiftEndCash(date, brand);
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ báo cáo nộp quỹ cuối ca',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ tách gộp BOM theo khoảng thời gian
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('repack-formula/range')
  async syncRepackFormulaByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('brand') brand?: string,
  ) {
    if (!startDate || !endDate) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncRepackFormulaByDateRange(
        startDate,
        endDate,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ tách gộp BOM theo khoảng thời gian',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ danh sách CTKM theo khoảng thời gian
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('promotion/range')
  async syncPromotionByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('brand') brand?: string,
  ) {
    if (!startDate || !endDate) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncPromotionByDateRange(
        startDate,
        endDate,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ danh sách CTKM theo khoảng thời gian',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ danh sách Voucher Issue theo khoảng thời gian
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('voucher-issue/range')
  async syncVoucherIssueByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('brand') brand?: string,
  ) {
    if (!startDate || !endDate) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncVoucherIssueByDateRange(
        startDate,
        endDate,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message:
            'Lỗi khi đồng bộ danh sách Voucher Issue theo khoảng thời gian',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Lấy danh sách Voucher Issue với filter và pagination
   */
  @Get('voucher-issue')
  async getVoucherIssue(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('status') status?: string,
    @Query('code') code?: string,
    @Query('materialType') materialType?: string,
  ) {
    try {
      const result = await this.syncService.getVoucherIssue({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        brand,
        dateFrom,
        dateTo,
        status,
        code,
        materialType,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy danh sách Voucher Issue',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Lấy danh sách CTKM với filter và pagination
   */
  @Get('promotion')
  async getPromotion(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('ptype') ptype?: string,
    @Query('status') status?: string,
    @Query('code') code?: string,
  ) {
    try {
      const result = await this.syncService.getPromotion({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        brand,
        dateFrom,
        dateTo,
        ptype,
        status,
        code,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy danh sách CTKM',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Export promotions to Excel với chi tiết promotion lines
   */
  @Get('promotion/export')
  async exportPromotions(
    @Res() res: Response,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('ptype') ptype?: string,
    @Query('status') status?: string,
    @Query('code') code?: string,
  ) {
    try {
      const buffer = await this.syncService.exportPromotions({
        brand,
        dateFrom,
        dateTo,
        ptype,
        status,
        code,
      });

      // Generate filename
      const now = new Date();
      const dateStr = now.toISOString().split('T')[0].replace(/-/g, '');
      const brandSuffix = brand ? `_${brand.toUpperCase()}` : '';
      const fileName = `CTKM_ChiTiet_${dateStr}${brandSuffix}.xlsx`;

      // Set headers
      res.setHeader(
        'Content-Type',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      );
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="${encodeURIComponent(fileName)}"`,
      );
      res.setHeader('Content-Length', buffer.length);

      // Send buffer
      res.send(buffer);
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi xuất Excel CTKM',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Lấy danh sách tách gộp BOM với filter và pagination
   */
  @Get('repack-formula')
  async getRepackFormula(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('repackCatName') repackCatName?: string,
    @Query('itemcode') itemcode?: string,
  ) {
    try {
      const result = await this.syncService.getRepackFormula({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        brand,
        dateFrom,
        dateTo,
        repackCatName,
        itemcode,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy danh sách tách gộp BOM',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Lấy danh sách báo cáo nộp quỹ cuối ca với filter và pagination
   */
  @Get('shift-end-cash')
  async getShiftEndCash(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('branchCode') branchCode?: string,
    @Query('drawCode') drawCode?: string,
    @Query('apiId') apiId?: string,
  ) {
    try {
      const result = await this.syncService.getShiftEndCash({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        brand,
        dateFrom,
        dateTo,
        branchCode,
        drawCode,
        apiId: apiId ? parseInt(apiId, 10) : undefined,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy danh sách báo cáo nộp quỹ cuối ca',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Tạo phiếu chi tiền mặt từ báo cáo nộp quỹ cuối ca (double-click)
   */
  @Post('shift-end-cash/:id/create-payment')
  async createPaymentFromShiftEndCash(@Param('id') id: string) {
    try {
      const result = await this.syncService.createPaymentFromShiftEndCash(id);
      if (!result.success) {
        throw new HttpException(
          {
            success: false,
            message: result.message,
            error: result.error,
          },
          HttpStatus.BAD_REQUEST,
        );
      }
      return {
        success: true,
        message: result.message,
        data: result.data,
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi tạo phiếu chi tiền mặt từ báo cáo nộp quỹ cuối ca',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Lấy danh sách stock transfers với filter và pagination
   */
  @Get('stock-transfers')
  async getStockTransfers(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('branchCode') branchCode?: string,
    @Query('itemCode') itemCode?: string,
    @Query('soCode') soCode?: string,
    @Query('docCode') docCode?: string,
  ) {
    try {
      const result = await this.syncService.getStockTransfers({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        brand,
        dateFrom,
        dateTo,
        branchCode,
        itemCode,
        soCode,
        docCode,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy danh sách stock transfers',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ cashio theo ngày cho một brand hoặc tất cả brands
   * @param date - Date format: DDMMMYYYY (ví dụ: 02NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('cashio')
  async syncCashio(@Body('date') date: string, @Body('brand') brand?: string) {
    if (!date || (typeof date === 'string' && date.trim() === '')) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 02NOV2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncCashioByDate(date, brand);
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ cashio',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ cashio theo khoảng ngày cho một brand hoặc tất cả brands
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('cashio/range')
  async syncCashioByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('brand') brand?: string,
  ) {
    if (
      !startDate ||
      !endDate ||
      (typeof startDate === 'string' && startDate.trim() === '') ||
      (typeof endDate === 'string' && endDate.trim() === '')
    ) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01NOV2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncCashioByDateRange(
        startDate,
        endDate,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ cashio theo khoảng ngày',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Lấy danh sách cashio với filter và pagination
   */
  @Get('cashio')
  async getCashio(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('branchCode') branchCode?: string,
    @Query('soCode') soCode?: string,
    @Query('partnerCode') partnerCode?: string,
  ) {
    try {
      const result = await this.syncService.getCashio({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        brand,
        dateFrom,
        dateTo,
        branchCode,
        soCode,
        partnerCode,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy danh sách cashio',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Get('warehouse-processed')
  async getWarehouseProcessed(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('ioType') ioType?: string,
    @Query('success') success?: string,
    @Query('docCode') docCode?: string,
  ) {
    try {
      const result = await this.syncService.getWarehouseProcessed({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        dateFrom,
        dateTo,
        ioType,
        success: success !== undefined ? success === 'true' : undefined,
        docCode,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy thống kê warehouse processed',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  @Post('wsale/range')
  async syncWsaleByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('brand') brand?: string,
  ) {
    if (
      !startDate ||
      !endDate ||
      (typeof startDate === 'string' && startDate.trim() === '') ||
      (typeof endDate === 'string' && endDate.trim() === '')
    ) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 04DEC2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.getDailyWsaleByDateRange(
        startDate,
        endDate,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ bán buôn',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
