import { Controller, Get, Query, Param } from '@nestjs/common';
import { ShopeeFeesService } from './shopee-fees.service';

@Controller('shopee-fees')
export class ShopeeFeesController {
  constructor(private readonly shopeeFeesService: ShopeeFeesService) {}

  /**
   * GET /shopee-fees
   * Get all Shopee fees with pagination and filters
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
    return this.shopeeFeesService.findAll({
      page: page ? parseInt(page) : undefined,
      limit: limit ? parseInt(limit) : undefined,
      brand,
      search,
      startDate,
      endDate,
    });
  }

  /**
   * GET /shopee-fees/:erpCode
   * Get single Shopee fee by ERP code
   */
  @Get(':erpCode')
  async findByErpCode(@Param('erpCode') erpCode: string) {
    return this.shopeeFeesService.findByErpCode(erpCode);
  }
}
