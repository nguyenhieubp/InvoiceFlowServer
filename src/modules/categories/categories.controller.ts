import {
  Controller,
  Get,
  Post,
  Put,
  Delete,
  Body,
  Param,
  Query,
  UseInterceptors,
  UploadedFile,
  BadRequestException,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { CategoriesService } from './categories.service';
import { CreateProductItemDto, UpdateProductItemDto } from '../../dto/create-product-item.dto';
import { CreatePromotionItemDto, UpdatePromotionItemDto } from '../../dto/create-promotion-item.dto';
import { CreateWarehouseItemDto, UpdateWarehouseItemDto } from '../../dto/create-warehouse-item.dto';

@Controller('categories')
export class CategoriesController {
  constructor(private readonly categoriesService: CategoriesService) {}

  @Get('products')
  async findAll(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
  ) {
    return this.categoriesService.findAll({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      search,
    });
  }

  @Get('products/:id')
  async findOne(@Param('id') id: string) {
    return this.categoriesService.findOne(id);
  }

  @Post('products')
  async create(@Body() createDto: CreateProductItemDto) {
    return this.categoriesService.create(createDto);
  }

  @Put('products/:id')
  async update(
    @Param('id') id: string,
    @Body() updateDto: UpdateProductItemDto,
  ) {
    return this.categoriesService.update(id, updateDto);
  }

  @Delete('products/:id')
  async delete(@Param('id') id: string) {
    await this.categoriesService.delete(id);
    return { message: 'Product deleted successfully' };
  }

  @Post('products/import')
  @UseInterceptors(FileInterceptor('file'))
  async importExcel(@UploadedFile() file: Express.Multer.File) {
    if (!file) {
      throw new BadRequestException('File không được tìm thấy');
    }

    // Validate file type
    const allowedMimes = [
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/csv',
    ];
    
    if (!allowedMimes.includes(file.mimetype)) {
      throw new BadRequestException(
        'File không hợp lệ. Vui lòng upload file Excel (.xlsx, .xls) hoặc CSV',
      );
    }

    try {
      const result = await this.categoriesService.importFromExcel(file);
      return {
        message: `Import thành công ${result.success}/${result.total} bản ghi`,
        ...result,
      };
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi import file Excel',
      );
    }
  }

  // ========== PROMOTION ENDPOINTS ==========

  @Get('promotions')
  async findAllPromotions(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
  ) {
    return this.categoriesService.findAllPromotions({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      search,
    });
  }

  @Get('promotions/:id')
  async findOnePromotion(@Param('id') id: string) {
    return this.categoriesService.findOnePromotion(id);
  }

  @Post('promotions')
  async createPromotion(@Body() createDto: CreatePromotionItemDto) {
    return this.categoriesService.createPromotion(createDto);
  }

  @Put('promotions/:id')
  async updatePromotion(
    @Param('id') id: string,
    @Body() updateDto: UpdatePromotionItemDto,
  ) {
    return this.categoriesService.updatePromotion(id, updateDto);
  }

  @Delete('promotions/:id')
  async deletePromotion(@Param('id') id: string) {
    await this.categoriesService.deletePromotion(id);
    return { message: 'Promotion deleted successfully' };
  }

  @Post('promotions/import')
  @UseInterceptors(FileInterceptor('file'))
  async importPromotionsExcel(@UploadedFile() file: Express.Multer.File) {
    if (!file) {
      throw new BadRequestException('File không được tìm thấy');
    }

    // Validate file type
    const allowedMimes = [
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/csv',
    ];
    
    if (!allowedMimes.includes(file.mimetype)) {
      throw new BadRequestException(
        'File không hợp lệ. Vui lòng upload file Excel (.xlsx, .xls) hoặc CSV',
      );
    }

    try {
      const result = await this.categoriesService.importPromotionsFromExcel(file);
      return {
        message: `Import thành công ${result.success}/${result.total} bản ghi`,
        ...result,
      };
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi import file Excel',
      );
    }
  }

  // ========== WAREHOUSE ENDPOINTS ==========

  @Get('warehouses')
  async findAllWarehouses(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
  ) {
    return this.categoriesService.findAllWarehouses({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      search,
    });
  }

  @Get('warehouses/:id')
  async findOneWarehouse(@Param('id') id: string) {
    return this.categoriesService.findOneWarehouse(id);
  }

  @Post('warehouses')
  async createWarehouse(@Body() createDto: CreateWarehouseItemDto) {
    return this.categoriesService.createWarehouse(createDto);
  }

  @Put('warehouses/:id')
  async updateWarehouse(
    @Param('id') id: string,
    @Body() updateDto: UpdateWarehouseItemDto,
  ) {
    return this.categoriesService.updateWarehouse(id, updateDto);
  }

  @Delete('warehouses/:id')
  async deleteWarehouse(@Param('id') id: string) {
    await this.categoriesService.deleteWarehouse(id);
    return { message: 'Warehouse deleted successfully' };
  }

  @Post('warehouses/import')
  @UseInterceptors(FileInterceptor('file'))
  async importWarehousesExcel(@UploadedFile() file: Express.Multer.File) {
    if (!file) {
      throw new BadRequestException('File không được tìm thấy');
    }

    // Validate file type
    const allowedMimes = [
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/csv',
    ];
    
    if (!allowedMimes.includes(file.mimetype)) {
      throw new BadRequestException(
        'File không hợp lệ. Vui lòng upload file Excel (.xlsx, .xls) hoặc CSV',
      );
    }

    try {
      const result = await this.categoriesService.importWarehousesFromExcel(file);
      return {
        message: `Import thành công ${result.success}/${result.total} bản ghi`,
        ...result,
      };
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi import file Excel',
      );
    }
  }
}

