import {
  Controller,
  Get,
  Put,
  Param,
  Body,
  Query,
  DefaultValuePipe,
  ParseIntPipe,
} from '@nestjs/common';
import { StockTransferService } from './stock-transfer.service';

@Controller('stock-transfers')
export class StockTransferController {
  constructor(private readonly stockTransferService: StockTransferService) {}

  @Get('missing-material')
  async getMissingMaterial(
    @Query('page', new DefaultValuePipe(1), ParseIntPipe) page: number,
    @Query('limit', new DefaultValuePipe(10), ParseIntPipe) limit: number,
    @Query('search') search?: string,
  ) {
    return this.stockTransferService.findMissingMaterial(page, limit, search);
  }

  @Put(':id')
  async updateMaterialCode(
    @Param('id') id: string,
    @Body('materialCode') materialCode: string,
  ) {
    return this.stockTransferService.updateMaterialCode(id, materialCode);
  }
}
