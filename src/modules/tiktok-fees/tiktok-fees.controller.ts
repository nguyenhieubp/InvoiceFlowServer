import { Controller, Get, Query, Param } from '@nestjs/common';
import { TikTokFeesService } from './tiktok-fees.service';

@Controller('tiktok-fees')
export class TikTokFeesController {
  constructor(private readonly tiktokFeesService: TikTokFeesService) {}

  /**
   * GET /tiktok-fees
   * Get all TikTok fees with pagination and filters
   */
  @Get()
  async findAll(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('search') search?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    return this.tiktokFeesService.findAll({
      page: page ? parseInt(page) : undefined,
      limit: limit ? parseInt(limit) : undefined,
      brand,
      search,
      startDate,
      endDate,
    });
  }

  /**
   * GET /tiktok-fees/:erpCode
   * Get single TikTok fee by ERP code
   */
  @Get(':erpCode')
  async findByErpCode(@Param('erpCode') erpCode: string) {
    return this.tiktokFeesService.findByErpCode(erpCode);
  }
}
