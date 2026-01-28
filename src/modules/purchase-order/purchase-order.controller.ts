import { Controller, Get, Post, Query, Body } from '@nestjs/common';
import { PurchaseOrderService } from './purchase-order.service';

@Controller('purchase-orders')
export class PurchaseOrderController {
  constructor(private readonly purchaseOrderService: PurchaseOrderService) {}

  @Post('sync')
  async syncPO(
    @Body() body: { startDate: string; endDate: string; brand?: string },
  ) {
    return this.purchaseOrderService.syncPurchaseOrders(
      body.startDate,
      body.endDate,
      body.brand,
    );
  }

  @Get()
  async getPOs(
    @Query('page') page: number,
    @Query('limit') limit: number,
    @Query('startDate') startDate: string,
    @Query('endDate') endDate: string,
    @Query('search') search: string,
    @Query('brand') brand: string,
  ) {
    return this.purchaseOrderService.getPurchaseOrders({
      page,
      limit,
      startDate,
      endDate,
      search,
      brand,
    });
  }
}
