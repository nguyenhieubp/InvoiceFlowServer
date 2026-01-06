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
import { CreateWarehouseCodeMappingDto, UpdateWarehouseCodeMappingDto } from '../../dto/create-warehouse-code-mapping.dto';
import { CreatePaymentMethodDto, UpdatePaymentMethodDto } from '../../dto/create-payment-method.dto';
import { CreateEcommerceCustomerDto, UpdateEcommerceCustomerDto } from '../../dto/create-ecommerce-customer.dto';

@Controller('categories')
export class CategoriesController {
  constructor(private readonly categoriesService: CategoriesService) { }

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

  // ========== WAREHOUSE CODE MAPPING ENDPOINTS ==========

  @Get('warehouse-code-mappings')
  async findAllWarehouseCodeMappings(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
  ) {
    return this.categoriesService.findAllWarehouseCodeMappings({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      search,
    });
  }

  // Các route cụ thể phải đặt TRƯỚC route có parameter :id để tránh conflict
  @Post('warehouse-code-mappings/map')
  async mapWarehouseCode(@Body() body: { maCu: string }) {
    const maMoi = await this.categoriesService.mapWarehouseCode(body.maCu);
    return {
      maCu: body.maCu,
      maMoi: maMoi,
      mapped: maMoi !== null,
    };
  }

  @Get('warehouse-code-mappings/map')
  async mapWarehouseCodeGet(@Query('maCu') maCu: string) {
    const maMoi = await this.categoriesService.mapWarehouseCode(maCu);
    return {
      maCu: maCu,
      maMoi: maMoi,
      mapped: maMoi !== null,
    };
  }

  @Get('warehouse-code-mappings/ma-cu/:maCu')
  async findWarehouseCodeMappingByMaCu(@Param('maCu') maCu: string) {
    return this.categoriesService.findWarehouseCodeMappingByMaCu(maCu);
  }

  @Get('warehouse-code-mappings/:id')
  async findOneWarehouseCodeMapping(@Param('id') id: string) {
    return this.categoriesService.findOneWarehouseCodeMapping(id);
  }

  @Post('warehouse-code-mappings')
  async createWarehouseCodeMapping(@Body() createDto: CreateWarehouseCodeMappingDto) {
    return this.categoriesService.createWarehouseCodeMapping(createDto);
  }

  @Put('warehouse-code-mappings/:id')
  async updateWarehouseCodeMapping(
    @Param('id') id: string,
    @Body() updateDto: UpdateWarehouseCodeMappingDto,
  ) {
    return this.categoriesService.updateWarehouseCodeMapping(id, updateDto);
  }

  @Delete('warehouse-code-mappings/:id')
  async deleteWarehouseCodeMapping(@Param('id') id: string) {
    await this.categoriesService.deleteWarehouseCodeMapping(id);
    return { message: 'Warehouse code mapping deleted successfully' };
  }

  @Post('warehouse-code-mappings/import')
  @UseInterceptors(FileInterceptor('file'))
  async importWarehouseCodeMappingsExcel(@UploadedFile() file: Express.Multer.File) {
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
      const result = await this.categoriesService.importWarehouseCodeMappingsFromExcel(file);
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

  // ========== PAYMENT METHOD ENDPOINTS ==========

  @Get('payment-methods')
  async findAllPaymentMethods(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
  ) {
    return this.categoriesService.findAllPaymentMethods({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      search,
    });
  }

  @Get('payment-methods/code/:code')
  async findPaymentMethodByCode(@Param('code') code: string) {
    return this.categoriesService.findPaymentMethodByCode(code);
  }

  @Get('payment-methods/:id')
  async findOnePaymentMethod(@Param('id') id: string) {
    return this.categoriesService.findOnePaymentMethod(id);
  }

  @Post('payment-methods')
  async createPaymentMethod(@Body() createDto: CreatePaymentMethodDto) {
    return this.categoriesService.createPaymentMethod(createDto);
  }

  @Put('payment-methods/:id')
  async updatePaymentMethod(
    @Param('id') id: string,
    @Body() updateDto: UpdatePaymentMethodDto,
  ) {
    return this.categoriesService.updatePaymentMethod(id, updateDto);
  }

  @Delete('payment-methods/:id')
  async deletePaymentMethod(@Param('id') id: string) {
    await this.categoriesService.deletePaymentMethod(id);
    return { message: 'Payment method deleted successfully' };
  }

  @Post('payment-methods/import')
  @UseInterceptors(FileInterceptor('file'))
  async importPaymentMethodsExcel(@UploadedFile() file: Express.Multer.File) {
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
      const result = await this.categoriesService.importPaymentMethodsFromExcel(file);
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

  // ========== CUSTOMER ENDPOINTS ==========

  @Get('customers')
  async findAllCustomers(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
  ) {
    return this.categoriesService.findAllCustomers({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      search,
    });
  }

  @Get('customers/:code')
  async findCustomerByCode(@Param('code') code: string) {
    return this.categoriesService.findCustomerByCode(code);
  }

  // ========== LOYALTY API PROXY ENDPOINTS ==========

  @Get('loyalty/products/code/:itemCode')
  async getProductByCode(@Param('itemCode') itemCode: string) {
    return this.categoriesService.getProductFromLoyaltyAPI(itemCode);
  }

  @Get('loyalty/departments')
  async getDepartmentByBranchCode(@Query('branchcode') branchcode: string) {
    if (!branchcode) {
      throw new BadRequestException('branchcode parameter is required');
    }
    return this.categoriesService.getDepartmentFromLoyaltyAPI(branchcode);
  }

  // ========== ECOMMERCE CUSTOMER ENDPOINTS ==========

  @Get('ecommerce-customers')
  async findAllEcommerceCustomers(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('search') search?: string,
  ) {
    return this.categoriesService.findAllEcommerceCustomers({
      page: page ? parseInt(page) : 1,
      limit: limit ? parseInt(limit) : 50,
      search,
    });
  }

  @Get('ecommerce-customers/active')
  async findActiveEcommerceCustomers() {
    return this.categoriesService.findActiveEcommerceCustomers();
  }

  @Get('ecommerce-customers/active/:code')
  async findActiveEcommerceCustomerByCode(@Param('code') code: string) {
    return this.categoriesService.findActiveEcommerceCustomerByCode(code);
  }

  @Get('ecommerce-customers/:id')
  async findOneEcommerceCustomer(@Param('id') id: string) {
    return this.categoriesService.findOneEcommerceCustomer(id);
  }

  @Post('ecommerce-customers')
  async createEcommerceCustomer(@Body() createDto: CreateEcommerceCustomerDto) {
    return this.categoriesService.createEcommerceCustomer(createDto);
  }

  @Put('ecommerce-customers/:id')
  async updateEcommerceCustomer(
    @Param('id') id: string,
    @Body() updateDto: UpdateEcommerceCustomerDto,
  ) {
    return this.categoriesService.updateEcommerceCustomer(id, updateDto);
  }

  @Delete('ecommerce-customers/:id')
  async deleteEcommerceCustomer(@Param('id') id: string) {
    await this.categoriesService.deleteEcommerceCustomer(id);
    return { message: 'Ecommerce customer deleted successfully' };
  }

  @Post('ecommerce-customers/import')
  @UseInterceptors(FileInterceptor('file'))
  async importEcommerceCustomersExcel(@UploadedFile() file: Express.Multer.File) {
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
      const result = await this.categoriesService.importEcommerceCustomersFromExcel(file);
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

