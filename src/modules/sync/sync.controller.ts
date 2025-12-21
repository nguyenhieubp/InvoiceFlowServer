import { Controller, Post, Param, Get, Body, HttpException, HttpStatus } from '@nestjs/common';
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
}

