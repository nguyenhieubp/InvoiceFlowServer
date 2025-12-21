import { Controller, Post, Param, Get, Body, Query, HttpException, HttpStatus } from '@nestjs/common';
import { SyncService } from '../../services/sync.service';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Post('brand/:brandName')
  async syncBrand(@Param('brandName') brandName: string, @Body('date') date: string) {
    if (!date) {
      throw new HttpException(
        {
          success: false,
          message: 'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 04DEC2025)',
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

  @Post('brand/:brandName/t8')
  async syncBrandT8(@Param('brandName') brandName: string, @Body('date') date: string) {
    if (!date) {
      throw new HttpException(
        {
          success: false,
          message: 'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 04DEC2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    // T8 endpoint giờ cũng dùng Zappy API
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
   * Đồng bộ FaceID data từ API inout-customer theo ngày
   * @param date - Date format: DDMMMYYYY (ví dụ: 13DEC2025)
   * @param shopCodes - Optional array of shop codes. Nếu không có, sẽ lấy tất cả data
   */
  @Post('faceid')
  async syncFaceId(@Body('date') date: string, @Body('shopCodes') shopCodes?: string[]) {
    if (!date) {
      throw new HttpException(
        {
          success: false,
          message: 'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 13DEC2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncFaceIdByDate(date, shopCodes);
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ FaceID',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Đồng bộ stock transfer từ ngày đến ngày
   * Phải đặt trước route /:brandName để tránh conflict
   * @param dateFrom - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param dateTo - Date format: DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('stock-transfer/range')
  async syncStockTransferRange(
    @Body() body: any,
  ) {
    const dateFrom = body?.dateFrom || body?.DateFrom;
    const dateTo = body?.dateTo || body?.DateTo;
    const brand = body?.brand || body?.Brand;
    
    if (!dateFrom || !dateTo || (typeof dateFrom === 'string' && dateFrom.trim() === '') || (typeof dateTo === 'string' && dateTo.trim() === '')) {
      throw new HttpException(
        {
          success: false,
          message: 'Tham số dateFrom và dateTo là bắt buộc (format: DDMMMYYYY, ví dụ: 01NOV2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.syncService.syncStockTransferRange(dateFrom, dateTo, brand);
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
          message: 'Tham số date là bắt buộc (format: DDMMMYYYY, ví dụ: 01NOV2025)',
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
}

