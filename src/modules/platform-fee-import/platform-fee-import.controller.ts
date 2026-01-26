import {
  Controller,
  Get,
  Post,
  Body,
  Query,
  Param,
  Res,
  UseInterceptors,
  UploadedFile,
  BadRequestException,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import type { Response } from 'express';
import { PlatformFeeImportService } from './platform-fee-import.service';
import { ImportPlatformFeeDto } from './dto/import-platform-fee.dto';

@Controller('platform-fee-import')
export class PlatformFeeImportController {
  constructor(
    private readonly platformFeeImportService: PlatformFeeImportService,
  ) {}

  @Post('import')
  @UseInterceptors(FileInterceptor('file'))
  async importExcel(
    @UploadedFile() file: Express.Multer.File,
    @Body() body: { platform: 'shopee' | 'tiktok' | 'lazada' },
  ) {
    if (!file) {
      throw new BadRequestException('File không được tìm thấy');
    }

    if (!body.platform) {
      throw new BadRequestException('Platform không được chỉ định');
    }

    if (!['shopee', 'tiktok', 'lazada'].includes(body.platform)) {
      throw new BadRequestException(
        'Platform không hợp lệ. Phải là shopee, tiktok hoặc lazada',
      );
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
      const result = await this.platformFeeImportService.importFromExcel(
        file,
        body.platform,
      );
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

  @Get()
  async findAll(
    @Query('platform') platform?: string,
    @Query('page') page?: number,
    @Query('limit') limit?: number,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Query('search') search?: string,
  ) {
    return this.platformFeeImportService.findAll({
      platform,
      page,
      limit,
      startDate,
      endDate,
      search,
    });
  }

  @Get('template/:platform')
  async downloadTemplate(
    @Param('platform') platform: string,
    @Res() res: Response,
  ) {
    if (!['shopee', 'tiktok', 'lazada'].includes(platform)) {
      throw new BadRequestException(
        'Platform không hợp lệ. Phải là shopee, tiktok hoặc lazada',
      );
    }

    try {
      const buffer = await this.platformFeeImportService.generateTemplate(
        platform as 'shopee' | 'tiktok' | 'lazada',
      );

      const fileName = `Mau_Import_Phi_San_${platform.toUpperCase()}.xlsx`;

      res.setHeader(
        'Content-Type',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      );
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="${encodeURIComponent(fileName)}"`,
      );
      res.setHeader('Content-Length', buffer.length);

      res.send(buffer);
    } catch (error: any) {
      throw new BadRequestException(
        error.message || 'Lỗi khi tạo file mẫu',
      );
    }
  }
}
