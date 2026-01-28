import { Controller, Get, Post, Query, Body } from '@nestjs/common';
import { GoodsReceiptService } from './goods-receipt.service';

@Controller('goods-receipts')
export class GoodsReceiptController {
  constructor(private readonly goodsReceiptService: GoodsReceiptService) {}

  @Post('sync')
  async syncGR(
    @Body() body: { startDate: string; endDate: string; brand?: string },
  ) {
    return this.goodsReceiptService.syncGoodsReceipts(
      body.startDate,
      body.endDate,
      body.brand,
    );
  }

  @Get()
  async getGRs(
    @Query('page') page: number,
    @Query('limit') limit: number,
    @Query('startDate') startDate: string,
    @Query('endDate') endDate: string,
    @Query('search') search: string,
  ) {
    return this.goodsReceiptService.getGoodsReceipts({
      page,
      limit,
      startDate,
      endDate,
      search,
    });
  }
}
