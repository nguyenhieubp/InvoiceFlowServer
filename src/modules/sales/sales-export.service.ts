import { Injectable, Logger } from '@nestjs/common';
import { SalesQueryService } from './sales-query.service';
import * as SalesFormattingUtils from '../../utils/sales-formatting.utils';

/**
 * SalesExportService
 * Handle Excel export operations
 */
@Injectable()
export class SalesExportService {
  private readonly logger = new Logger(SalesExportService.name);

  constructor(private salesQueryService: SalesQueryService) {}

  /**
   * Export orders to Excel
   */
  async exportOrders(params: {
    brand?: string;
    processed?: boolean;
    date?: string;
    dateFrom?: string;
    dateTo?: string;
    search?: string;
    statusAsys?: boolean;
  }) {
    this.logger.log('[Export] Starting export orders to Excel');

    // Use SalesQueryService to get data
    const result = await this.salesQueryService.findAllOrders({
      brand: params.brand,
      isProcessed: params.processed,
      date: params.date,
      dateFrom: params.dateFrom,
      dateTo: params.dateTo,
      search: params.search,
      statusAsys: params.statusAsys,
      export: true, // Export mode: no pagination, return all sales
    });

    const sales = result.sales;
    if (!sales || sales.length === 0) {
      this.logger.warn('[Export] No sales data to export');
      return Buffer.from(''); // Return empty buffer
    }

    this.logger.log(`[Export] Formatting ${sales.length} sales for Excel`);

    // NOTE: formatOrdersForExcel function needs to be implemented in SalesFormattingUtils
    // For now, return a simple Excel buffer
    // TODO: Implement proper Excel formatting
    const excelBuffer = Buffer.from('Excel export to be implemented');

    this.logger.log('[Export] Excel export completed');

    return excelBuffer;
  }
}
