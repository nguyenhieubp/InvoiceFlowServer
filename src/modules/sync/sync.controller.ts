import { Controller, Post, Param, Get, Body, HttpException, HttpStatus } from '@nestjs/common';
import { SyncService } from '../../services/sync.service';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Post('all')
  async syncAll(@Body('date') date: string) {
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
      const result = await this.syncService.syncAllBrands(date);
      return { 
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ dữ liệu',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

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
   * Backfill toàn bộ đơn hàng cho khoảng ngày cố định 01/10/2025 - 30/11/2025.
   * Chạy tuần tự theo ngày, gọi syncAllBrands cho từng ngày.
   */
  @Post('all-range-oct-nov-2025')
  async syncAllRangeOctNov2025() {
    try {
      await this.syncService.syncAllBrandsRange_01OctTo30Nov2025();
      return {
        success: true,
        message: 'Đã kích hoạt đồng bộ tất cả brand cho khoảng 01/10/2025 - 30/11/2025 (vui lòng xem log server).',
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi đồng bộ khoảng 01/10/2025 - 30/11/2025',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}

