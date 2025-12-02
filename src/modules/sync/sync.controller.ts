import { Controller, Post, Param, Get, HttpException, HttpStatus } from '@nestjs/common';
import { SyncService } from '../../services/sync.service';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Post('all')
  async syncAll() {
    try {
      await this.syncService.syncAllBrands();
      return { 
        success: true,
        message: 'Đồng bộ dữ liệu từ tất cả nhãn hàng thành công',
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
  async syncBrand(@Param('brandName') brandName: string) {
    const brandMap: Record<string, string> = {
      chando: 'kh_chando',
      f3: 'kh_f3',
      labhair: 'kh_labhair',
      yaman: 'kh_yaman',
      menard: 'kh_menard',
    };

    const endpoint = brandMap[brandName.toLowerCase()];
    if (!endpoint) {
      throw new HttpException(
        {
          success: false,
          message: 'Nhãn hàng không hợp lệ',
          validBrands: Object.keys(brandMap),
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      await this.syncService.syncBrand(brandName, endpoint);
      return {
        success: true,
        message: `Đồng bộ ${brandName} thành công`,
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
  async syncBrandT8(@Param('brandName') brandName: string) {
    const brandMap: Record<string, string> = {
      chando: 'kh_chando',
      f3: 'kh_f3',
      labhair: 'kh_labhair',
      yaman: 'kh_yaman',
      menard: 'kh_menard',
    };

    const endpoint = brandMap[brandName.toLowerCase()];
    if (!endpoint) {
      throw new HttpException(
        {
          success: false,
          message: 'Nhãn hàng không hợp lệ',
          validBrands: Object.keys(brandMap),
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      await this.syncService.syncBrand(brandName, endpoint, true);
      return {
        success: true,
        message: `Đồng bộ ${brandName} từ t8 thành công`,
        brand: brandName,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      throw new HttpException(
        {
          success: false,
          message: `Lỗi khi đồng bộ ${brandName} từ t8`,
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
}

