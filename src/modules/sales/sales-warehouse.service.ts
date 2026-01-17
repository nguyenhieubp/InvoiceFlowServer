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
import * as SalesUtils from '../../utils/sales.utils';

/**
 * SalesWarehouseService
 * Chịu trách nhiệm: Warehouse & stock transfer operations
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
   * Process warehouse from stock transfer by docCode
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
   * Process warehouse from stock transfer
   */
  async processWarehouseFromStockTransfer(
    stockTransfer: StockTransfer,
  ): Promise<any> {
    // ✅ Skip TRUTONKEEP items
    if (SalesUtils.isTrutonkeepItem(stockTransfer.itemCode)) {
      this.logger.log(
        `[Warehouse] Bỏ qua stock transfer với itemCode = TRUTONKEEP (docCode: ${stockTransfer.docCode})`,
      );
      return {
        success: true,
        message: 'Skipped TRUTONKEEP item',
        skipped: true,
      };
    }

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

      const { isSuccess, errorMessage } = this.validateWarehouseResult(result);

      await this.saveWarehouseTracking(
        stockTransfer.docCode,
        ioTypeForTracking,
        result,
        isSuccess,
        errorMessage,
      );

      if (!isSuccess) {
        throw new BadRequestException(
          errorMessage || 'Tạo phiếu warehouse thất bại',
        );
      }

      return result;
    } catch (error: any) {
      await this.saveFailedWarehouseTracking(stockTransfer, error);
      throw error;
    }
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
    const fromDate = SalesUtils.parseDateFromDDMMMYYYY(dateFrom);
    const toDate = SalesUtils.parseDateFromDDMMMYYYY(dateTo);
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

    let successCount = 0;
    let failedCount = 0;
    const errors: string[] = [];

    for (const record of failedRecords) {
      try {
        await this.processWarehouseFromStockTransferByDocCode(record.docCode);
        successCount++;
      } catch (error: any) {
        failedCount++;
        errors.push(
          `docCode ${record.docCode}: ${error?.message || String(error)}`,
        );
      }
    }

    return {
      success: failedCount === 0,
      message: `Đã xử lý ${failedRecords.length} records: ${successCount} thành công, ${failedCount} thất bại`,
      totalProcessed: failedRecords.length,
      successCount,
      failedCount,
      errors: errors.slice(0, 10),
    };
  }

  // Private helper methods
  private validateWarehouseResult(result: any): {
    isSuccess: boolean;
    errorMessage?: string;
  } {
    if (Array.isArray(result) && result.length > 0) {
      const firstItem = result[0];
      return {
        isSuccess: firstItem.status === 1,
        errorMessage:
          firstItem.status !== 1
            ? firstItem.message || 'Tạo phiếu warehouse thất bại'
            : undefined,
      };
    }

    if (result && typeof result === 'object' && result.status !== undefined) {
      return {
        isSuccess: result.status === 1,
        errorMessage:
          result.status !== 1
            ? result.message || 'Tạo phiếu warehouse thất bại'
            : undefined,
      };
    }

    return {
      isSuccess: false,
      errorMessage: 'Response không hợp lệ từ Fast API',
    };
  }

  private async saveWarehouseTracking(
    docCode: string,
    ioType: string,
    result: any,
    isSuccess: boolean,
    errorMessage?: string,
  ): Promise<void> {
    try {
      const existing = await this.warehouseProcessedRepository.findOne({
        where: { docCode },
      });

      if (existing) {
        if (isSuccess) {
          await this.warehouseProcessedRepository.update(
            { docCode },
            {
              ioType,
              processedDate: new Date(),
              result: JSON.stringify(result),
              success: isSuccess,
              errorMessage: null as any,
            },
          );
        } else {
          existing.ioType = ioType;
          existing.processedDate = new Date();
          existing.result = JSON.stringify(result);
          existing.success = isSuccess;
          existing.errorMessage = errorMessage;
          await this.warehouseProcessedRepository.save(existing);
        }
      } else {
        const warehouseProcessed = this.warehouseProcessedRepository.create({
          docCode,
          ioType,
          processedDate: new Date(),
          result: JSON.stringify(result),
          success: isSuccess,
          ...(errorMessage && { errorMessage }),
        });
        await this.warehouseProcessedRepository.save(warehouseProcessed);
      }
    } catch (saveError: any) {
      this.logger.error(
        `Lỗi khi lưu tracking cho docCode ${docCode}: ${saveError?.message || saveError}`,
      );
    }
  }

  private async saveFailedWarehouseTracking(
    stockTransfer: StockTransfer,
    error: any,
  ): Promise<void> {
    const errorMessage = error?.message || String(error);
    let errorResult: any = null;

    if (error?.response?.data) {
      errorResult = error.response.data;
    } else if (error?.data) {
      errorResult = error.data;
    }

    try {
      const ioTypeForTracking =
        stockTransfer.doctype === 'STOCK_TRANSFER' ? 'T' : stockTransfer.ioType;

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
    } catch (saveError: any) {
      this.logger.error(
        `Lỗi khi lưu tracking cho docCode ${stockTransfer.docCode}: ${saveError?.message || saveError}`,
      );
    }
  }
}
