import {
  Controller,
  Post,
  Get,
  Body,
  Query,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { VoucherIssueService } from './voucher-issue.service';

@Controller('voucher-issue')
export class VoucherIssueController {
  constructor(private readonly voucherIssueService: VoucherIssueService) {}

  /**
   * Đồng bộ danh sách Voucher Issue theo khoảng thời gian
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   */
  @Post('sync/range')
  async syncVoucherIssueByDateRange(
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
    @Body('brand') brand?: string,
  ) {
    if (!startDate || !endDate) {
      throw new HttpException(
        {
          success: false,
          message:
            'Tham số startDate và endDate là bắt buộc (format: DDMMMYYYY, ví dụ: 01OCT2025)',
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    try {
      const result = await this.voucherIssueService.syncVoucherIssueByDateRange(
        startDate,
        endDate,
        brand,
      );
      return {
        ...result,
        timestamp: new Date().toISOString(),
      };
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message:
            'Lỗi khi đồng bộ danh sách Voucher Issue theo khoảng thời gian',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Lấy danh sách Voucher Issue với filter và pagination
   */
  @Get()
  async getVoucherIssue(
    @Query('page') page?: string,
    @Query('limit') limit?: string,
    @Query('brand') brand?: string,
    @Query('dateFrom') dateFrom?: string,
    @Query('dateTo') dateTo?: string,
    @Query('status') status?: string,
    @Query('serial') serial?: string,
    @Query('code') code?: string,
  ) {
    try {
      const result = await this.voucherIssueService.getVoucherIssue({
        page: page ? parseInt(page, 10) : 1,
        limit: limit ? parseInt(limit, 10) : 10,
        brand,
        dateFrom,
        dateTo,
        status,
        serial,
        code,
      });
      return result;
    } catch (error: any) {
      throw new HttpException(
        {
          success: false,
          message: 'Lỗi khi lấy danh sách Voucher Issue',
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
