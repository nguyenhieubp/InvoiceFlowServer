import {
  Controller,
  Get,
  Query,
  Param,
} from '@nestjs/common';
import { WarehouseReleaseService } from '../../services/warehouse-release.service';

@Controller('warehouse-releases')
export class WarehouseReleasesController {
  constructor(private readonly warehouseReleaseService: WarehouseReleaseService) {}

  @Get('statistics')
  async getStatistics(
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Query('maDvcs') maDvcs?: string,
  ) {
    const options: any = {};

    if (startDate) {
      options.startDate = new Date(startDate);
    }

    if (endDate) {
      options.endDate = new Date(endDate);
    }

    if (maDvcs) {
      options.maDvcs = maDvcs;
    }

    return this.warehouseReleaseService.getStatistics(options);
  }

  @Get('doc-code/:docCode')
  async findByDocCode(@Param('docCode') docCode: string) {
    return this.warehouseReleaseService.findByDocCode(docCode);
  }

  @Get()
  async findAll(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('status') status?: string,
    @Query('docCode') docCode?: string,
    @Query('maKh') maKh?: string,
    @Query('tenKh') tenKh?: string,
    @Query('maDvcs') maDvcs?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    const options: any = {
      page: page ? parseInt(page) || 1 : 1,
      limit: limit ? parseInt(limit) || 50 : 50,
    };

    if (status !== undefined && status !== '') {
      options.status = parseInt(status);
    }

    if (docCode) {
      options.docCode = docCode;
    }

    if (maKh) {
      options.maKh = maKh;
    }

    if (tenKh) {
      options.tenKh = tenKh;
    }

    if (maDvcs) {
      options.maDvcs = maDvcs;
    }

    if (startDate) {
      options.startDate = new Date(startDate);
    }

    if (endDate) {
      options.endDate = new Date(endDate);
    }

    return this.warehouseReleaseService.findAll(options);
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    return this.warehouseReleaseService.findOne(id);
  }
}

