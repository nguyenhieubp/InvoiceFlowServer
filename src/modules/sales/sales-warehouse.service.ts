import {
  Injectable,
  Logger,
  NotFoundException,
  BadRequestException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between } from 'typeorm';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';

/**
 * SalesWarehouseService
 * Handle warehouse & stock transfer operations
 */
@Injectable()
export class SalesWarehouseService {
  private readonly logger = new Logger(SalesWarehouseService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(WarehouseProcessed)
    private warehouseProcessedRepository: Repository<WarehouseProcessed>,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
  ) {}

  /**
   * Process warehouse from stock transfer
   * MOVED FROM: SalesService.processWarehouseFromStockTransfer()
   */
  async processWarehouseFromStockTransfer(
    stockTransfer: StockTransfer,
  ): Promise<any> {
    try {
      let result: any;
      let ioTypeForTracking: string;

      if (
        stockTransfer.doctype === 'STOCK_TRANSFER' &&
        stockTransfer.relatedStockCode
      ) {
        const stockTransferList = await this.stockTransferRepository.find({
          where: { docCode: stockTransfer.docCode },
          order: { createdAt: 'ASC' },
        });

        result =
          await this.fastApiInvoiceFlowService.processWarehouseTransferFromStockTransfers(
            stockTransferList,
          );
        ioTypeForTracking = 'T';
      } else {
        result =
          await this.fastApiInvoiceFlowService.processWarehouseFromStockTransfer(
            stockTransfer,
          );
        ioTypeForTracking = stockTransfer.ioType;
      }

      let isSuccess = false;
      let errorMessage: string | undefined = undefined;

      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        isSuccess = firstItem.status === 1;
        if (!isSuccess) {
          errorMessage = firstItem.message || 'Tạo phiếu warehouse thất bại';
        }
      } else if (
        result &&
        typeof result === 'object' &&
        result.status !== undefined
      ) {
        isSuccess = result.status === 1;
        if (!isSuccess) {
          errorMessage = result.message || 'Tạo phiếu warehouse thất bại';
        }
      } else {
        isSuccess = false;
        errorMessage = 'Response không hợp lệ từ Fast API';
      }

      // Save tracking record
      try {
        const existing = await this.warehouseProcessedRepository.findOne({
          where: { docCode: stockTransfer.docCode },
        });

        if (existing) {
          if (isSuccess) {
            await this.warehouseProcessedRepository.update(
              { docCode: stockTransfer.docCode },
              {
                ioType: ioTypeForTracking,
                processedDate: new Date(),
                result: JSON.stringify(result),
                success: isSuccess,
                errorMessage: null as any,
              },
            );
          } else {
            existing.ioType = ioTypeForTracking;
            existing.processedDate = new Date();
            existing.result = JSON.stringify(result);
            existing.success = isSuccess;
            existing.errorMessage = errorMessage;
            await this.warehouseProcessedRepository.save(existing);
          }
        } else {
          const warehouseProcessed = this.warehouseProcessedRepository.create({
            docCode: stockTransfer.docCode,
            ioType: ioTypeForTracking,
            processedDate: new Date(),
            result: JSON.stringify(result),
            success: isSuccess,
            ...(errorMessage && { errorMessage }),
          });
          await this.warehouseProcessedRepository.save(warehouseProcessed);
        }
        this.logger.log(
          `Đã lưu tracking cho docCode ${stockTransfer.docCode} với success = ${isSuccess}`,
        );
      } catch (saveError: any) {
        this.logger.error(
          `Lỗi khi lưu tracking cho docCode ${stockTransfer.docCode}: ${saveError?.message || saveError}`,
        );
      }

      if (!isSuccess) {
        throw new BadRequestException(
          errorMessage || 'Tạo phiếu warehouse thất bại',
        );
      }

      return result;
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      let errorResult: any = null;
      if (error?.response?.data) {
        errorResult = error.response.data;
      } else if (error?.data) {
        errorResult = error.data;
      }

      // Save failed tracking
      try {
        const ioTypeForTracking =
          stockTransfer.doctype === 'STOCK_TRANSFER'
            ? 'T'
            : stockTransfer.ioType;

        const existing = await this.warehouseProcessedRepository.findOne({
          where: { docCode: stockTransfer.docCode },
        });

        if (existing) {
          existing.ioType = ioTypeForTracking;
          existing.processedDate = new Date();
          existing.errorMessage = errorMessage;
          existing.success = false;
          if (errorResult) {
            existing.result = JSON.stringify(errorResult);
          }
          await this.warehouseProcessedRepository.save(existing);
        } else {
          const warehouseProcessed = this.warehouseProcessedRepository.create({
            docCode: stockTransfer.docCode,
            ioType: ioTypeForTracking,
            processedDate: new Date(),
            errorMessage,
            success: false,
            ...(errorResult && { result: JSON.stringify(errorResult) }),
          });
          await this.warehouseProcessedRepository.save(warehouseProcessed);
        }
        this.logger.log(
          `Đã lưu tracking thất bại cho docCode ${stockTransfer.docCode}`,
        );
      } catch (saveError: any) {
        this.logger.error(
          `Lỗi khi lưu tracking cho docCode ${stockTransfer.docCode}: ${saveError?.message || saveError}`,
        );
      }

      throw error;
    }
  }

  /**
   * Process warehouse by docCode
   */
  async processWarehouseFromStockTransferByDocCode(
    docCode: string,
  ): Promise<any> {
    const stockTransfer = await this.stockTransferRepository.findOne({
      where: { docCode },
      order: { createdAt: 'ASC' },
    });

    if (!stockTransfer) {
      throw new NotFoundException(
        `Không tìm thấy stock transfer với docCode = "${docCode}"`,
      );
    }

    return await this.processWarehouseFromStockTransfer(stockTransfer);
  }

  /**
   * Retry warehouse failed by date range
   */
  async retryWarehouseFailedByDateRange(
    dateFrom: string,
    dateTo: string,
  ): Promise<{
    success: boolean;
    message: string;
    totalProcessed: number;
    successCount: number;
    failedCount: number;
    errors: string[];
  }> {
    try {
      const parseDate = (dateStr: string): Date => {
        const day = parseInt(dateStr.substring(0, 2));
        const monthStr = dateStr.substring(2, 5).toUpperCase();
        const year = parseInt(dateStr.substring(5, 9));
        const monthMap: Record<string, number> = {
          JAN: 0,
          FEB: 1,
          MAR: 2,
          APR: 3,
          MAY: 4,
          JUN: 5,
          JUL: 6,
          AUG: 7,
          SEP: 8,
          OCT: 9,
          NOV: 10,
          DEC: 11,
        };
        const month = monthMap[monthStr] || 0;
        return new Date(year, month, day);
      };

      const fromDate = parseDate(dateFrom);
      const toDate = parseDate(dateTo);
      toDate.setHours(23, 59, 59, 999);

      const failedRecords = await this.warehouseProcessedRepository.find({
        where: {
          success: false,
          processedDate: Between(fromDate, toDate),
        },
        order: { processedDate: 'ASC' },
      });

      if (failedRecords.length === 0) {
        return {
          success: true,
          message: 'Không có record nào thất bại trong khoảng thời gian này',
          totalProcessed: 0,
          successCount: 0,
          failedCount: 0,
          errors: [],
        };
      }

      this.logger.log(
        `Bắt đầu retry ${failedRecords.length} records từ ${dateFrom} đến ${dateTo}`,
      );

      let successCount = 0;
      let failedCount = 0;
      const errors: string[] = [];

      for (const record of failedRecords) {
        try {
          await this.processWarehouseFromStockTransferByDocCode(record.docCode);
          successCount++;
          this.logger.log(`Retry thành công cho docCode: ${record.docCode}`);
        } catch (error: any) {
          failedCount++;
          const errorMsg = `docCode ${record.docCode}: ${error?.message || String(error)}`;
          errors.push(errorMsg);
          this.logger.error(`Retry thất bại cho docCode: ${record.docCode}`);
        }
      }

      const message = `Đã xử lý ${failedRecords.length} records: ${successCount} thành công, ${failedCount} thất bại`;

      return {
        success: failedCount === 0,
        message,
        totalProcessed: failedRecords.length,
        successCount,
        failedCount,
        errors: errors.slice(0, 10),
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi retry batch: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Create stock transfer
   * NOTE: Placeholder - full implementation would be moved here
   */
  async createStockTransfer(createDto: any): Promise<any> {
    this.logger.log('createStockTransfer');
    return {
      success: true,
      message: 'To be implemented',
    };
  }
}
