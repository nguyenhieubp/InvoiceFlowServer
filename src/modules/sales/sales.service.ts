import {
  Injectable,
  InternalServerErrorException,
  Logger,
  NotFoundException,
  BadRequestException,
} from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, Or, IsNull, Between } from 'typeorm';
import * as XLSX from 'xlsx-js-style';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { Invoice } from '../../entities/invoice.entity';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';
import { InvoiceService } from '../invoices/invoice.service';
import { ZappyApiService } from '../../services/zappy-api.service';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { CategoriesService } from '../categories/categories.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { InvoiceValidationService } from '../../services/invoice-validation.service';
import { Order, SaleItem } from '../../types/order.types';
import {
  CreateStockTransferDto,
  StockTransferItem,
} from '../../dto/create-stock-transfer.dto';
import * as _ from 'lodash';
import * as SalesUtils from '../../utils/sales.utils';
import * as VoucherUtils from '../../utils/voucher.utils';
import * as StockTransferUtils from '../../utils/stock-transfer.utils';
import * as SalesCalculationUtils from '../../utils/sales-calculation.utils';
import * as SalesFormattingUtils from '../../utils/sales-formatting.utils';
import * as ConvertUtils from '../../utils/convert.utils';
import { InvoiceLogicUtils } from '../../utils/invoice-logic.utils';
import { N8nService } from '../../services/n8n.service';

@Injectable()
export class SalesService {
  private readonly logger = new Logger(SalesService.name);
  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(WarehouseProcessed)
    private warehouseProcessedRepository: Repository<WarehouseProcessed>,
    private invoiceService: InvoiceService,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private fastApiService: FastApiClientService,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private categoriesService: CategoriesService,
    private loyaltyService: LoyaltyService,
    private invoiceValidationService: InvoiceValidationService,
    private n8nService: N8nService,
  ) {}

  /**
   * Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
   */
  private async getMaKhoFromStockTransfer(
    sale: any,
    docCode: string,
    stockTransfers: StockTransfer[],
    saleMaterialCode?: string | null,
    stockTransferMap?: Map<string, StockTransfer[]>,
  ): Promise<string> {
    const matched = StockTransferUtils.findMatchingStockTransfer(
      sale,
      docCode,
      stockTransfers,
      saleMaterialCode,
      stockTransferMap,
    );
    const stockCode = matched?.stockCode || '';
    if (!stockCode || stockCode.trim() === '') return '';

    try {
      const maMoi = await this.categoriesService.mapWarehouseCode(stockCode);
      return maMoi || stockCode;
    } catch (error: any) {
      this.logger.error(
        `Error mapping warehouse code ${stockCode}: ${error?.message || error}`,
      );
      return stockCode;
    }
  }

  /**
   * Enrich orders với cashio data
   */
  private async enrichOrdersWithCashio(orders: any[]): Promise<any[]> {
    const docCodes = orders.map((o) => o.docCode);
    if (docCodes.length === 0) return orders;

    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code IN (:...docCodes)', { docCodes })
      .orWhere('cashio.master_code IN (:...docCodes)', { docCodes })
      .getMany();

    const cashioMap = new Map<string, DailyCashio[]>();
    docCodes.forEach((docCode) => {
      const matchingCashios = cashioRecords.filter(
        (c) => c.so_code === docCode || c.master_code === docCode,
      );
      if (matchingCashios.length > 0) {
        cashioMap.set(docCode, matchingCashios);
      }
    });

    // Fetch stock transfers để thêm thông tin stock transfer
    // Join theo soCode (của stock transfer) = docCode (của order)
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodes) },
    });

    const stockTransferMap = new Map<string, StockTransfer[]>();
    docCodes.forEach((docCode) => {
      // Join theo soCode (của stock transfer) = docCode (của order)
      const matchingTransfers = stockTransfers.filter(
        (st) => st.soCode === docCode,
      );
      if (matchingTransfers.length > 0) {
        stockTransferMap.set(docCode, matchingTransfers);
      }
    });

    return orders.map((order) => {
      const cashioRecords = cashioMap.get(order.docCode) || [];
      const ecoinCashio = cashioRecords.find((c) => c.fop_syscode === 'ECOIN');
      const voucherCashio = cashioRecords.find(
        (c) => c.fop_syscode === 'VOUCHER',
      );
      const selectedCashio =
        ecoinCashio || voucherCashio || cashioRecords[0] || null;

      // Lấy thông tin stock transfer cho đơn hàng này
      const orderStockTransfers = stockTransferMap.get(order.docCode) || [];

      // Lọc chỉ lấy các stock transfer XUẤT KHO (SALE_STOCKOUT) với qty < 0
      // Bỏ qua các stock transfer nhập lại (RETURN) với qty > 0
      const stockOutTransfers = orderStockTransfers.filter((st) => {
        // Chỉ lấy các record có doctype = 'SALE_STOCKOUT' hoặc qty < 0 (xuất kho)
        const isStockOut =
          st.doctype === 'SALE_STOCKOUT' || Number(st.qty || 0) < 0;
        return isStockOut;
      });

      // Deduplicate stock transfers để tránh tính trùng
      // Group theo docCode + itemCode + stockCode + qty để đảm bảo chỉ tính một lần cho mỗi combination
      // (có thể có duplicate records trong database với id khác nhau nhưng cùng docCode, itemCode, stockCode, qty)
      const uniqueStockTransfersMap = new Map<string, StockTransfer>();
      stockOutTransfers.forEach((st) => {
        // Tạo key từ docCode + itemCode + stockCode + qty (giữ nguyên dấu âm để phân biệt ST và RT)
        // KHÔNG dùng Math.abs vì ST (qty=-11) và RT (qty=11) là 2 chứng từ khác nhau
        const qty = Number(st.qty || 0);
        const key = `${st.docCode || ''}_${st.itemCode || ''}_${st.stockCode || ''}_${qty}`;

        // Chỉ lưu nếu chưa có key này, hoặc nếu có thì giữ record có id (ưu tiên record có id)
        if (!uniqueStockTransfersMap.has(key)) {
          uniqueStockTransfersMap.set(key, st);
        } else {
          // Nếu đã có, chỉ thay thế nếu record hiện tại có id và record cũ không có id
          const existing = uniqueStockTransfersMap.get(key)!;
          if (st.id && !existing.id) {
            uniqueStockTransfersMap.set(key, st);
          }
        }
      });
      const uniqueStockTransfers = Array.from(uniqueStockTransfersMap.values());

      // Debug log nếu có duplicate hoặc có RT records bị loại bỏ
      if (orderStockTransfers.length > stockOutTransfers.length) {
        const returnCount =
          orderStockTransfers.length - stockOutTransfers.length;
        this.logger.debug(
          `[StockTransfer] Đơn hàng ${order.docCode}: Loại bỏ ${returnCount} records nhập lại (RETURN), chỉ tính ${stockOutTransfers.length} records xuất kho (ST)`,
        );
      }
      if (stockOutTransfers.length > uniqueStockTransfers.length) {
        this.logger.warn(
          `[StockTransfer] Đơn hàng ${order.docCode}: ${stockOutTransfers.length} records xuất kho → ${uniqueStockTransfers.length} unique (đã loại bỏ ${stockOutTransfers.length - uniqueStockTransfers.length} duplicates)`,
        );
      }

      // Tính tổng hợp thông tin stock transfer (chỉ tính từ unique records XUẤT KHO)
      // Lấy giá trị tuyệt đối của qty (vì qty xuất kho là số âm, nhưng số lượng xuất là số dương)
      const totalQty = uniqueStockTransfers.reduce((sum, st) => {
        const qty = Math.abs(Number(st.qty || 0));
        return sum + qty;
      }, 0);

      const stockTransferSummary = {
        totalItems: uniqueStockTransfers.length, // Số dòng stock transfer xuất kho (sau khi deduplicate)
        totalQty: totalQty, // Tổng số lượng xuất kho (lấy giá trị tuyệt đối vì qty xuất kho là số âm)
        uniqueItems: new Set(uniqueStockTransfers.map((st) => st.itemCode))
          .size, // Số sản phẩm khác nhau
        stockCodes: Array.from(
          new Set(
            uniqueStockTransfers.map((st) => st.stockCode).filter(Boolean),
          ),
        ), // Danh sách mã kho
        hasStockTransfer: uniqueStockTransfers.length > 0, // Có stock transfer xuất kho hay không
      };

      return {
        ...order,
        cashioData: cashioRecords.length > 0 ? cashioRecords : null,
        cashioFopSyscode: selectedCashio?.fop_syscode || null,
        cashioFopDescription: selectedCashio?.fop_description || null,
        cashioCode: selectedCashio?.code || null,
        cashioMasterCode: selectedCashio?.master_code || null,
        cashioTotalIn: selectedCashio?.total_in || null,
        cashioTotalOut: selectedCashio?.total_out || null,
        // Thông tin stock transfer
        stockTransferInfo: stockTransferSummary,
        stockTransfers:
          uniqueStockTransfers.length > 0
            ? uniqueStockTransfers.map((st) =>
                StockTransferUtils.formatStockTransferForFrontend(st),
              )
            : (order.stockTransfers || []).map((st: any) =>
                StockTransferUtils.formatStockTransferForFrontend(st),
              ), // Format để trả về materialCode
      };
    });
  }

  /**
   * Lấy stock transfer theo id
   */
  async getStockTransferById(id: string): Promise<StockTransfer | null> {
    return await this.stockTransferRepository.findOne({
      where: { id },
    });
  }

  /**
   * Xử lý warehouse receipt/release/transfer từ stock transfer theo docCode
   */
  async processWarehouseFromStockTransferByDocCode(
    docCode: string,
  ): Promise<any> {
    // Lấy stock transfer đầu tiên theo docCode
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
   * Retry batch các warehouse processed failed theo date range
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
      // Parse dates từ DDMMMYYYY
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
      toDate.setHours(23, 59, 59, 999); // Set to end of day

      // Tìm tất cả warehouse processed failed trong khoảng thời gian
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
        `[Warehouse Batch Retry] Bắt đầu retry ${failedRecords.length} records từ ${dateFrom} đến ${dateTo}`,
      );

      let successCount = 0;
      let failedCount = 0;
      const errors: string[] = [];

      // Retry từng record
      for (const record of failedRecords) {
        try {
          await this.processWarehouseFromStockTransferByDocCode(record.docCode);
          successCount++;
          this.logger.log(
            `[Warehouse Batch Retry] Retry thành công cho docCode: ${record.docCode}`,
          );
        } catch (error: any) {
          failedCount++;
          const errorMsg = `docCode ${record.docCode}: ${error?.message || String(error)}`;
          errors.push(errorMsg);
          this.logger.error(
            `[Warehouse Batch Retry] Retry thất bại cho docCode: ${record.docCode} - ${errorMsg}`,
          );
        }
      }

      const message = `Đã xử lý ${failedRecords.length} records: ${successCount} thành công, ${failedCount} thất bại`;

      return {
        success: failedCount === 0,
        message,
        totalProcessed: failedRecords.length,
        successCount,
        failedCount,
        errors: errors.slice(0, 10), // Chỉ trả về 10 lỗi đầu tiên
      };
    } catch (error: any) {
      this.logger.error(
        `[Warehouse Batch Retry] Lỗi khi retry batch: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Xử lý warehouse receipt/release/transfer từ stock transfer
   */
  async processWarehouseFromStockTransfer(
    stockTransfer: StockTransfer,
  ): Promise<any> {
    try {
      let result: any;
      let ioTypeForTracking: string;

      // Xử lý STOCK_TRANSFER với relatedStockCode
      if (
        stockTransfer.doctype === 'STOCK_TRANSFER' &&
        stockTransfer.relatedStockCode
      ) {
        // Lấy tất cả stock transfers cùng docCode
        const stockTransferList = await this.stockTransferRepository.find({
          where: { docCode: stockTransfer.docCode },
          order: { createdAt: 'ASC' },
        });

        // Gọi API warehouse transfer
        result =
          await this.fastApiInvoiceFlowService.processWarehouseTransferFromStockTransfers(
            stockTransferList,
          );
        ioTypeForTracking = 'T'; // T = Transfer
      } else {
        // Xử lý STOCK_IO
        result =
          await this.fastApiInvoiceFlowService.processWarehouseFromStockTransfer(
            stockTransfer,
          );
        ioTypeForTracking = stockTransfer.ioType;
      }

      // Kiểm tra result có status = 1 không để xác định success
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
        // Nếu result không có format mong đợi, coi như thất bại
        isSuccess = false;
        errorMessage = 'Response không hợp lệ từ Fast API';
      }

      // Lưu vào bảng tracking với success đúng (upsert - update nếu đã tồn tại)
      try {
        // Tìm record đã tồn tại
        const existing = await this.warehouseProcessedRepository.findOne({
          where: { docCode: stockTransfer.docCode },
        });

        if (existing) {
          // Update record đã tồn tại
          // Nếu thành công, xóa errorMessage bằng cách update trực tiếp
          if (isSuccess) {
            await this.warehouseProcessedRepository.update(
              { docCode: stockTransfer.docCode },
              {
                ioType: ioTypeForTracking,
                processedDate: new Date(),
                result: JSON.stringify(result),
                success: isSuccess,
                errorMessage: null as any, // Set null để xóa errorMessage trong database
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
          // Tạo mới nếu chưa tồn tại
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
          `[Warehouse Manual] Đã lưu tracking cho docCode ${stockTransfer.docCode} với success = ${isSuccess}`,
        );
      } catch (saveError: any) {
        this.logger.error(
          `[Warehouse Manual] Lỗi khi lưu tracking cho docCode ${stockTransfer.docCode}: ${saveError?.message || saveError}`,
        );
        // Không throw error để không ảnh hưởng đến response chính
      }

      // Nếu không thành công, throw error để controller xử lý
      if (!isSuccess) {
        throw new BadRequestException(
          errorMessage || 'Tạo phiếu warehouse thất bại',
        );
      }

      return result;
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      // Lấy result từ error response nếu có (để lưu vào database cho người dùng xem)
      let errorResult: any = null;
      if (error?.response?.data) {
        errorResult = error.response.data;
      } else if (error?.data) {
        errorResult = error.data;
      }

      // Lưu vào bảng tracking với success = false (upsert - update nếu đã tồn tại)
      try {
        const ioTypeForTracking =
          stockTransfer.doctype === 'STOCK_TRANSFER'
            ? 'T'
            : stockTransfer.ioType;

        // Tìm record đã tồn tại
        const existing = await this.warehouseProcessedRepository.findOne({
          where: { docCode: stockTransfer.docCode },
        });

        if (existing) {
          // Update record đã tồn tại
          existing.ioType = ioTypeForTracking;
          existing.processedDate = new Date();
          existing.errorMessage = errorMessage;
          existing.success = false;
          // Lưu result từ error response nếu có, nếu không thì giữ result cũ (nếu có)
          if (errorResult) {
            existing.result = JSON.stringify(errorResult);
          }
          // Nếu không có errorResult và existing cũng không có result, giữ null
          // Nếu existing đã có result, giữ nguyên
          await this.warehouseProcessedRepository.save(existing);
        } else {
          // Tạo mới nếu chưa tồn tại
          const warehouseProcessed = this.warehouseProcessedRepository.create({
            docCode: stockTransfer.docCode,
            ioType: ioTypeForTracking,
            processedDate: new Date(),
            errorMessage,
            success: false,
            // Lưu result từ error response nếu có
            ...(errorResult && { result: JSON.stringify(errorResult) }),
          });
          await this.warehouseProcessedRepository.save(warehouseProcessed);
        }
        this.logger.log(
          `[Warehouse Manual] Đã lưu tracking thất bại cho docCode ${stockTransfer.docCode}`,
        );
      } catch (saveError: any) {
        this.logger.error(
          `[Warehouse Manual] Lỗi khi lưu tracking cho docCode ${stockTransfer.docCode}: ${saveError?.message || saveError}`,
        );
      }

      // Throw error để controller xử lý
      throw error;
    }
  }

  async findAllOrders(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
    date?: string; // Format: DDMMMYYYY (ví dụ: 04DEC2025)
    dateFrom?: string; // Format: YYYY-MM-DD hoặc ISO string
    dateTo?: string; // Format: YYYY-MM-DD hoặc ISO string
    search?: string; // Search query để tìm theo docCode, customer name, code, mobile
    statusAsys?: boolean; // Filter theo statusAsys (true/false)
    export?: boolean; // Nếu true, trả về sales items riêng lẻ (không group, không paginate) để export Excel
    typeSale?: string; // Type sale: "WHOLESALE" or "RETAIL"
  }) {
    const {
      brand,
      isProcessed,
      page = 1,
      limit = 50,
      date,
      dateFrom,
      dateTo,
      search,
      statusAsys,
      export: isExport,
      typeSale,
    } = options;

    // Đếm tổng số sale items trước (để có total cho pagination)
    const countQuery = this.saleRepository
      .createQueryBuilder('sale')
      .select('COUNT(sale.id)', 'count');

    // Luôn join với customer để có thể search hoặc export
    countQuery.leftJoin('sale.customer', 'customer');

    // Helper function để apply filters chung cho cả count và full query
    const applyFilters = (query: any) => {
      if (isProcessed !== undefined) {
        query.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
      }
      if (statusAsys !== undefined) {
        query.andWhere('sale.statusAsys = :statusAsys', { statusAsys });
      }
      if (typeSale !== undefined && typeSale !== 'ALL') {
        query.andWhere('sale.type_sale = :type_sale', {
          type_sale: typeSale.toUpperCase(),
        });
      }
      if (brand) {
        query.andWhere('sale.brand = :brand', { brand });
      }
      if (search && search.trim() !== '') {
        const searchPattern = `%${search.trim().toLowerCase()}%`;
        query.andWhere(
          "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
          { search: searchPattern },
        );
      }

      // Date logic
      let hasFiltersWithDate = false;
      if (dateFrom || dateTo) {
        hasFiltersWithDate = true;
        if (dateFrom && dateTo) {
          const startDate = new Date(dateFrom);
          startDate.setHours(0, 0, 0, 0);
          const endDate = new Date(dateTo);
          endDate.setHours(23, 59, 59, 999);
          query.andWhere(
            'sale.docDate >= :dateFrom AND sale.docDate <= :dateTo',
            {
              dateFrom: startDate,
              dateTo: endDate,
            },
          );
        } else if (dateFrom) {
          const startDate = new Date(dateFrom);
          startDate.setHours(0, 0, 0, 0);
          query.andWhere('sale.docDate >= :dateFrom', { dateFrom: startDate });
        } else if (dateTo) {
          const endDate = new Date(dateTo);
          endDate.setHours(23, 59, 59, 999);
          query.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
        }
      } else if (date) {
        hasFiltersWithDate = true;
        const dateMatch = date.match(/^(\d{2})([A-Z]{3})(\d{4})$/i);
        if (dateMatch) {
          const [, day, monthStr, year] = dateMatch;
          const monthMap: { [key: string]: number } = {
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
          const month = monthMap[monthStr.toUpperCase()];
          if (month !== undefined) {
            const dateObj = new Date(parseInt(year), month, parseInt(day));
            const startOfDay = new Date(dateObj);
            startOfDay.setHours(0, 0, 0, 0);
            const endOfDay = new Date(dateObj);
            endOfDay.setHours(23, 59, 59, 999);
            query.andWhere(
              'sale.docDate >= :dateFrom AND sale.docDate <= :dateTo',
              {
                dateFrom: startOfDay,
                dateTo: endOfDay,
              },
            );
          }
        }
      } else if (brand && !hasFiltersWithDate) {
        const endDate = new Date();
        endDate.setHours(23, 59, 59, 999);
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - 30);
        startDate.setHours(0, 0, 0, 0);
        query.andWhere(
          'sale.docDate >= :dateFrom AND sale.docDate <= :dateTo',
          {
            dateFrom: startDate,
            dateTo: endDate,
          },
        );
      }
    };

    applyFilters(countQuery);
    const totalResult = await countQuery.getRawOne();
    const totalSaleItems = parseInt(totalResult?.count || '0', 10);

    const fullQuery = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer')
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.docCode', 'ASC')
      .addOrderBy('sale.id', 'ASC');

    applyFilters(fullQuery);

    if (!isExport) {
      const offset = (page - 1) * limit;
      fullQuery.skip(offset).take(limit);
    }

    const allSales = await fullQuery.getMany();

    if (isExport) {
      const salesWithCustomer = allSales.map((sale) => {
        return {
          ...sale,
          customer: sale.customer
            ? {
                code: sale.customer.code || sale.partnerCode || null,
                brand: sale.customer.brand || null,
                name: sale.customer.name || null,
                mobile: sale.customer.mobile || null,
              }
            : sale.partnerCode
              ? {
                  code: sale.partnerCode || null,
                  brand: null,
                  name: null,
                  mobile: null,
                }
              : null,
        };
      });

      return {
        sales: salesWithCustomer,
        total: totalSaleItems,
      };
    }

    const orderMap = new Map<string, any>();
    const allSalesData: any[] = [];

    for (const sale of allSales) {
      const docCode = sale.docCode;

      if (!orderMap.has(docCode)) {
        orderMap.set(docCode, {
          docCode: sale.docCode,
          docDate: sale.docDate,
          branchCode: sale.branchCode,
          docSourceType: sale.docSourceType,
          customer: sale.customer
            ? {
                code: sale.customer.code || sale.partnerCode || null,
                brand: sale.customer.brand || null,
                name: sale.customer.name || null,
                mobile: sale.customer.mobile || null,
              }
            : sale.partnerCode
              ? {
                  code: sale.partnerCode || null,
                  brand: null,
                  name: null,
                  mobile: null,
                }
              : null,
          totalRevenue: 0,
          totalQty: 0,
          totalItems: 0,
          isProcessed: sale.isProcessed,
          sales: [],
        });
      }

      const order = orderMap.get(docCode)!;
      order.totalRevenue += Number(sale.revenue || 0);
      order.totalQty += Number(sale.qty || 0);
      order.totalItems += 1;

      if (!sale.isProcessed) {
        order.isProcessed = false;
      }

      allSalesData.push(sale);
    }

    const itemCodes = Array.from(
      new Set(
        allSalesData
          .filter((sale) => sale.statusAsys !== false)
          .map((sale) => sale.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    const branchCodes = Array.from(
      new Set(
        allSalesData
          .map((sale) => sale.branchCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    const docCodes = Array.from(
      new Set(allSalesData.map((sale) => sale.docCode).filter(Boolean)),
    );
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer(docCodes);
    const stockTransfers =
      docCodesForStockTransfer.length > 0
        ? await this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          })
        : [];

    const stockTransferItemCodes = Array.from(
      new Set(
        stockTransfers
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );
    const allItemCodes = Array.from(
      new Set([...itemCodes, ...stockTransferItemCodes]),
    );

    const [loyaltyProductMap, departmentMap] = await Promise.all([
      this.loyaltyService.fetchProducts(allItemCodes),
      this.loyaltyService.fetchLoyaltyDepartments(branchCodes),
    ]);

    const { stockTransferMap, stockTransferByDocCodeMap } =
      StockTransferUtils.buildStockTransferMaps(
        stockTransfers,
        loyaltyProductMap,
        docCodes,
      );

    let getMaThe = new Map<string, string>();
    const [dataCard] = await this.n8nService.fetchCardData(docCodes[0]);
    for (const card of dataCard.data) {
      if (!card?.service_item_name || !card?.serial) {
        continue; // bỏ qua record lỗi / rỗng
      }
      const itemProduct = await this.loyaltyService.checkProduct(
        card.service_item_name,
      );
      if (itemProduct) {
        getMaThe.set(itemProduct.materialCode, card.serial);
      }
    }

    const enrichedSalesMap = new Map<string, any[]>();
    for (const sale of allSalesData) {
      const docCode = sale.docCode;
      if (!enrichedSalesMap.has(docCode)) {
        enrichedSalesMap.set(docCode, []);
      }

      const loyaltyProduct = sale.itemCode
        ? loyaltyProductMap.get(sale.itemCode)
        : null;
      const department = sale.branchCode
        ? departmentMap.get(sale.branchCode) || null
        : null;
      const maThe = getMaThe.get(loyaltyProduct?.materialCode || '') || '';
      sale.maThe = maThe;
      const saleMaterialCode = loyaltyProduct?.materialCode;
      let saleStockTransfers: StockTransfer[] = [];
      if (saleMaterialCode) {
        const stockTransferKey = `${docCode}_${saleMaterialCode}`;
        saleStockTransfers = stockTransferMap.get(stockTransferKey) || [];
      }
      if (saleStockTransfers.length === 0 && sale.itemCode) {
        saleStockTransfers = stockTransfers.filter(
          (st) => st.soCode === docCode && st.itemCode === sale.itemCode,
        );
      }

      const maKhoFromStockTransfer = await this.getMaKhoFromStockTransfer(
        sale,
        docCode,
        stockTransfers,
        saleMaterialCode,
        stockTransferMap,
      );
      const calculatedFields = SalesCalculationUtils.calculateSaleFields(
        sale,
        loyaltyProduct,
        department,
        sale.branchCode,
      );
      calculatedFields.maKho = maKhoFromStockTransfer;

      const order = orderMap.get(sale.docCode);
      const enrichedSale = await SalesFormattingUtils.formatSaleForFrontend(
        sale,
        loyaltyProduct,
        department,
        calculatedFields,
        order,
        this.categoriesService,
        this.loyaltyService,
        saleStockTransfers,
      );
      enrichedSale.stockTransfers = saleStockTransfers.map((st) =>
        StockTransferUtils.formatStockTransferForFrontend(st),
      );

      enrichedSalesMap.get(docCode)!.push(enrichedSale);
    }

    for (const [docCode, sales] of enrichedSalesMap.entries()) {
      const order = orderMap.get(docCode);
      if (order) {
        // Sử dụng n8nService trực tiếp thay cho private method
        const hasTachThe = sales.some((s: any) =>
          SalesUtils.isTachTheOrder(s.ordertype, s.ordertypeName),
        );
        if (hasTachThe) {
          try {
            const cardResponse =
              await this.n8nService.fetchCardDataWithRetry(docCode);
            const cardData = this.n8nService.parseCardData(cardResponse);
            this.n8nService.mapIssuePartnerCodeToSales(sales, cardData);
          } catch (e) {}
        }
        order.sales = sales;
        order.stockTransfers = (
          stockTransferByDocCodeMap.get(docCode) || []
        ).map((st) => StockTransferUtils.formatStockTransferForFrontend(st));
      }
    }

    const orders = Array.from(orderMap.values()).sort((a, b) => {
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });

    const enrichedOrders = await this.enrichOrdersWithCashio(orders);

    const maxOrders = limit * 2;
    const limitedOrders = enrichedOrders.slice(0, maxOrders);

    return {
      data: limitedOrders,
      total: totalSaleItems,
      page,
      limit,
      totalPages: Math.ceil(totalSaleItems / limit),
    };
  }

  async getStatusAsys(
    statusAsys?: string,
    page?: number,
    limit?: number,
    brand?: string,
    dateFrom?: string,
    dateTo?: string,
    search?: string,
  ) {
    try {
      // Parse statusAsys: 'true' -> true, 'false' -> false, undefined/empty -> undefined
      let statusAsysValue: boolean | undefined;
      if (statusAsys === 'true') {
        statusAsysValue = true;
      } else if (statusAsys === 'false') {
        statusAsysValue = false;
      } else {
        statusAsysValue = undefined;
      }

      const pageNumber = page || 1;
      const limitNumber = limit || 10;
      const skip = (pageNumber - 1) * limitNumber;

      // Sử dụng QueryBuilder để hỗ trợ filter phức tạp
      let query = this.saleRepository.createQueryBuilder('sale');

      // Luôn leftJoinAndSelect customer để load relation (cần cho response)
      query = query.leftJoinAndSelect('sale.customer', 'customer');

      // Filter statusAsys
      if (statusAsysValue !== undefined) {
        query = query.andWhere('sale.statusAsys = :statusAsys', {
          statusAsys: statusAsysValue,
        });
      }

      // Filter brand
      if (brand) {
        query = query.andWhere('customer.brand = :brand', { brand });
      }

      // Filter search (docCode, customer name, code, mobile)
      if (search && search.trim() !== '') {
        const searchPattern = `%${search.trim().toLowerCase()}%`;
        query = query.andWhere(
          "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
          { search: searchPattern },
        );
      }

      // Filter date range - dùng Date object (TypeORM sẽ convert tự động)
      let startDate: Date | undefined;
      let endDate: Date | undefined;

      if (dateFrom) {
        startDate = new Date(dateFrom);
        startDate.setHours(0, 0, 0, 0);
        query = query.andWhere('sale.docDate >= :dateFrom', {
          dateFrom: startDate,
        });
      }
      if (dateTo) {
        endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        query = query.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }

      // Tạo count query riêng (không dùng leftJoinAndSelect để tối ưu)
      const countQuery = this.saleRepository.createQueryBuilder('sale');

      // Apply cùng các filters như query chính nhưng chỉ dùng leftJoin (không Select)
      const needsCustomerJoin = brand || (search && search.trim() !== '');
      if (needsCustomerJoin) {
        countQuery.leftJoin('sale.customer', 'customer');
      }

      if (statusAsysValue !== undefined) {
        countQuery.andWhere('sale.statusAsys = :statusAsys', {
          statusAsys: statusAsysValue,
        });
      }

      if (brand) {
        countQuery.andWhere('customer.brand = :brand', { brand });
      }

      if (search && search.trim() !== '') {
        const searchPattern = `%${search.trim().toLowerCase()}%`;
        countQuery.andWhere(
          "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
          { search: searchPattern },
        );
      }

      if (startDate) {
        countQuery.andWhere('sale.docDate >= :dateFrom', {
          dateFrom: startDate,
        });
      }

      if (endDate) {
        countQuery.andWhere('sale.docDate <= :dateTo', { dateTo: endDate });
      }

      // Count total
      const totalCount = await countQuery.getCount();

      // Apply pagination và order
      query = query
        .orderBy('sale.createdAt', 'DESC')
        .skip(skip)
        .take(limitNumber);

      const sales = await query.getMany();

      return {
        data: sales,
        total: totalCount,
        page: pageNumber,
        limit: limitNumber,
        totalPages: Math.ceil(totalCount / limitNumber),
      };
    } catch (error: any) {
      this.logger.error(`[getStatusAsys] Error: ${error?.message || error}`);
      this.logger.error(
        `[getStatusAsys] Stack: ${error?.stack || 'No stack trace'}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ lại đơn lỗi - check lại với Loyalty API
   * Nếu tìm thấy trong Loyalty, cập nhật itemCode (mã vật tư) và statusAsys = true
   * Xử lý theo batch từ database để tránh load quá nhiều vào memory
   */
  async syncErrorOrders(): Promise<{
    total: number;
    success: number;
    failed: number;
    updated: Array<{
      id: string;
      docCode: string;
      itemCode: string;
      oldItemCode: string;
      newItemCode: string;
    }>;
  }> {
    let successCount = 0;
    let failCount = 0;
    const updated: Array<{
      id: string;
      docCode: string;
      itemCode: string;
      oldItemCode: string;
      newItemCode: string;
    }> = [];

    // Cấu hình batch size
    const DB_BATCH_SIZE = 500; // Load 500 records từ DB mỗi lần
    const PROCESS_BATCH_SIZE = 100; // Xử lý 100 sales mỗi batch trong memory
    const CONCURRENT_LIMIT = 10; // Chỉ gọi 10 API cùng lúc để tránh quá tải

    // Helper function để xử lý một sale
    const processSale = async (
      sale: any,
    ): Promise<{
      success: boolean;
      update?: {
        id: string;
        docCode: string;
        itemCode: string;
        oldItemCode: string;
        newItemCode: string;
      };
    }> => {
      try {
        const itemCode = sale.itemCode || '';
        if (!itemCode) {
          return { success: false };
        }

        const product = await this.loyaltyService.checkProduct(itemCode);

        if (product && product.materialCode) {
          // Tìm thấy trong Loyalty - cập nhật
          const newItemCode = product.materialCode;
          const oldItemCode = itemCode;

          // Cập nhật sale
          await this.saleRepository.update(sale.id, {
            itemCode: newItemCode,
            statusAsys: true,
          });

          return {
            success: true,
            update: {
              id: sale.id,
              docCode: sale.docCode || '',
              itemCode: sale.itemCode || '',
              oldItemCode,
              newItemCode,
            },
          };
        }
        return { success: false };
      } catch (error: any) {
        this.logger.error(
          `[syncErrorOrders] ❌ Lỗi khi check sale ${sale.id}: ${error?.message || error}`,
        );
        return { success: false };
      }
    };

    // Helper function để limit concurrent requests
    const processBatchConcurrent = async (sales: any[], limit: number) => {
      const results: Array<{ success: boolean; update?: any }> = [];
      for (let i = 0; i < sales.length; i += limit) {
        const batch = sales.slice(i, i + limit);
        const batchResults = await Promise.all(
          batch.map((sale) => processSale(sale)),
        );
        results.push(...batchResults);
      }
      return results;
    };

    // Xử lý từng batch từ database
    // Sau mỗi batch, query lại từ đầu vì các records đã xử lý (statusAsys = true)
    // sẽ không còn trong query nữa, nên không cần cursor
    let processedCount = 0;
    let dbBatchNumber = 0;

    while (true) {
      dbBatchNumber++;

      // Load batch từ database (luôn query từ đầu, vì records đã xử lý sẽ không còn trong query)
      const dbBatch = await this.saleRepository.find({
        where: [{ statusAsys: false }, { statusAsys: IsNull() }],
        order: { createdAt: 'DESC' },
        take: DB_BATCH_SIZE,
      });

      if (dbBatch.length === 0) {
        break; // Không còn records nào
      }

      // Xử lý batch này theo từng nhóm nhỏ
      for (let i = 0; i < dbBatch.length; i += PROCESS_BATCH_SIZE) {
        const processBatch = dbBatch.slice(i, i + PROCESS_BATCH_SIZE);
        const processBatchNumber = Math.floor(i / PROCESS_BATCH_SIZE) + 1;
        const totalProcessBatches = Math.ceil(
          dbBatch.length / PROCESS_BATCH_SIZE,
        );

        // Xử lý batch với giới hạn concurrent
        const batchResults = await processBatchConcurrent(
          processBatch,
          CONCURRENT_LIMIT,
        );

        // Cập nhật counters
        for (const result of batchResults) {
          if (result.success && result.update) {
            successCount++;
            updated.push(result.update);
          } else {
            failCount++;
          }
        }

        processedCount += processBatch.length;

        // Log progress - cập nhật totalCount vì có thể thay đổi khi có records mới
        const currentTotal = await this.saleRepository.count({
          where: [{ statusAsys: false }, { statusAsys: IsNull() }],
        });
      }

      // Nếu batch nhỏ hơn DB_BATCH_SIZE, có nghĩa là đã hết records
      if (dbBatch.length < DB_BATCH_SIZE) {
        break;
      }
    }

    return {
      total: processedCount,
      success: successCount,
      failed: failCount,
      updated,
    };
  }

  /**
   * Đồng bộ lại một đơn hàng cụ thể - check lại với Loyalty API
   * Nếu tìm thấy trong Loyalty, cập nhật itemCode (mã vật tư) và statusAsys = true
   */
  async syncErrorOrderByDocCode(docCode: string): Promise<{
    success: boolean;
    message: string;
    updated: number;
    failed: number;
    details: Array<{
      id: string;
      itemCode: string;
      oldItemCode: string;
      newItemCode: string;
    }>;
  }> {
    // Lấy tất cả sales của đơn hàng có statusAsys = false, null, hoặc undefined
    // Sử dụng Or để match cả false, null, và undefined
    const errorSales = await this.saleRepository.find({
      where: [
        { docCode, statusAsys: false },
        { docCode, statusAsys: IsNull() },
      ],
    });

    if (errorSales.length === 0) {
      return {
        success: true,
        message: `Đơn hàng ${docCode} không có dòng nào cần đồng bộ`,
        updated: 0,
        failed: 0,
        details: [],
      };
    }

    let successCount = 0;
    let failCount = 0;
    const details: Array<{
      id: string;
      itemCode: string;
      oldItemCode: string;
      newItemCode: string;
    }> = [];

    // Check lại từng sale với Loyalty API
    for (const sale of errorSales) {
      try {
        const itemCode = sale.itemCode || '';
        if (!itemCode) {
          failCount++;
          continue;
        }

        // Check với Loyalty API - sử dụng LoyaltyService
        const product = await this.loyaltyService.checkProduct(itemCode);

        if (product && product.materialCode) {
          // Tìm thấy trong Loyalty - cập nhật
          const newItemCode = product.materialCode; // Mã vật tư từ Loyalty
          const oldItemCode = itemCode;

          // Cập nhật sale
          await this.saleRepository.update(sale.id, {
            itemCode: newItemCode,
            statusAsys: true, // Đánh dấu đã có trong Loyalty
          });

          successCount++;
          details.push({
            id: sale.id,
            itemCode: sale.itemCode || '',
            oldItemCode,
            newItemCode,
          });
        } else {
          // Vẫn không tìm thấy trong Loyalty
          failCount++;
          this.logger.warn(
            `[syncErrorOrderByDocCode] ❌ Sale ${sale.id} (${docCode}): itemCode ${itemCode} vẫn không tồn tại trong Loyalty`,
          );
        }
      } catch (error: any) {
        failCount++;
        this.logger.error(
          `[syncErrorOrderByDocCode] ❌ Lỗi khi check sale ${sale.id}: ${error?.message || error}`,
        );
      }
    }

    const message =
      successCount > 0
        ? `Đồng bộ thành công: ${successCount} dòng đã được cập nhật${failCount > 0 ? `, ${failCount} dòng vẫn lỗi` : ''}`
        : `Không có dòng nào được cập nhật. ${failCount} dòng vẫn không tìm thấy trong Loyalty API`;

    return {
      success: successCount > 0,
      message,
      updated: successCount,
      failed: failCount,
      details,
    };
  }

  async findOne(id: string) {
    const sale = await this.saleRepository.findOne({
      where: { id },
      relations: ['customer'],
    });

    if (!sale) {
      throw new NotFoundException(`Sale with ID ${id} not found`);
    }

    return sale;
  }

  async findByOrderCode(docCode: string) {
    // Lấy tất cả sales có cùng docCode (cùng đơn hàng)
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    if (sales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // Join với daily_cashio để lấy cashio data
    // Join dựa trên: cashio.so_code = docCode HOẶC cashio.master_code = docCode
    const cashioRecords = await this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .where('cashio.so_code = :docCode', { docCode })
      .orWhere('cashio.master_code = :docCode', { docCode })
      .getMany();

    // Ưu tiên ECOIN, sau đó VOUCHER, sau đó các loại khác
    const ecoinCashio = cashioRecords.find((c) => c.fop_syscode === 'ECOIN');
    const voucherCashio = cashioRecords.find(
      (c) => c.fop_syscode === 'VOUCHER',
    );
    const selectedCashio =
      ecoinCashio || voucherCashio || cashioRecords[0] || null;

    // Lấy tất cả itemCode unique từ sales
    const itemCodes = SalesUtils.extractUniqueItemCodes(sales);

    // Load tất cả products một lần
    const products =
      itemCodes.length > 0
        ? await this.productItemRepository.find({
            where: { maERP: In(itemCodes) },
          })
        : [];

    // Tạo map để lookup nhanh
    const productMap = SalesUtils.createProductMap(products);

    // Fetch card data và tạo card code map
    const [dataCard] = await this.n8nService.fetchCardData(docCode);
    const cardCodeMap = SalesUtils.createCardCodeMap(dataCard);

    // Enrich sales với product information từ database và card code
    const enrichedSales = sales.map((sale) => {
      const saleWithProduct = SalesUtils.enrichSaleWithProduct(
        sale,
        productMap,
      );
      return SalesUtils.enrichSaleWithCardCode(saleWithProduct, cardCodeMap);
    });

    // Fetch products từ Loyalty API cho các itemCode không có trong database hoặc không có dvt
    // BỎ QUA các sale có statusAsys = false (đơn lỗi) - không fetch từ Loyalty API
    const loyaltyProductMap = new Map<string, any>();
    // Filter itemCodes: chỉ fetch cho các sale không phải đơn lỗi
    const validItemCodes = SalesUtils.filterValidItemCodes(itemCodes, sales);

    // Fetch products từ Loyalty API sử dụng LoyaltyService
    if (validItemCodes.length > 0) {
      const fetchedProducts =
        await this.loyaltyService.fetchProducts(validItemCodes);
      fetchedProducts.forEach((product, itemCode) => {
        loyaltyProductMap.set(itemCode, product);
      });
    }

    // Enrich sales với product từ Loyalty API (thêm dvt từ unit)
    const enrichedSalesWithLoyalty = enrichedSales.map((sale) =>
      SalesUtils.enrichSaleWithLoyaltyProduct(sale, loyaltyProductMap),
    );

    // Fetch departments để lấy ma_dvcs
    const branchCodes = SalesUtils.extractUniqueBranchCodes(sales);

    const departmentMap = new Map<string, any>();
    // Fetch departments parallel để tối ưu performance
    if (branchCodes.length > 0) {
      const departmentPromises = branchCodes.map(async (branchCode) => {
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
            { headers: { accept: 'application/json' } },
          );
          const department = response?.data?.data?.items?.[0];
          return { branchCode, department };
        } catch (error) {
          this.logger.warn(
            `Failed to fetch department for branchCode ${branchCode}: ${error}`,
          );
          return { branchCode, department: null };
        }
      });

      const departmentResults = await Promise.all(departmentPromises);
      departmentResults.forEach(({ branchCode, department }) => {
        if (department) {
          departmentMap.set(branchCode, department);
        }
      });
    }

    // Fetch stock transfers để lấy ma_nx (ST* và RT* từ stock transfer)
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    // Sử dụng materialCode đã được lưu trong database (đã được đồng bộ từ Loyalty API khi sync)
    // Nếu chưa có materialCode trong database, mới fetch từ Loyalty API
    const stockTransferItemCodesWithoutMaterialCode = Array.from(
      new Set(
        stockTransfers
          .filter((st) => st.itemCode && !st.materialCode)
          .map((st) => st.itemCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    // Chỉ fetch materialCode cho các itemCode chưa có materialCode trong database
    const stockTransferLoyaltyMap = new Map<string, any>();
    if (stockTransferItemCodesWithoutMaterialCode.length > 0) {
      const fetchedStockTransferProducts =
        await this.loyaltyService.fetchProducts(
          stockTransferItemCodesWithoutMaterialCode,
        );
      fetchedStockTransferProducts.forEach((product, itemCode) => {
        stockTransferLoyaltyMap.set(itemCode, product);
      });
    }

    // Tạo map: soCode_materialCode -> stock transfer (phân biệt ST và RT)
    // Match theo: Mã ĐH (soCode) = Số hóa đơn (docCode) VÀ Mã SP (itemCode) -> materialCode = Mã hàng (ma_vt)
    // Ưu tiên dùng materialCode đã lưu trong database, nếu chưa có thì lấy từ Loyalty API
    // Lưu ý: Dùng array để lưu tất cả stock transfers cùng key (tránh ghi đè khi có nhiều records giống nhau)
    const stockTransferMapBySoCodeAndMaterialCode = new Map<
      string,
      { st?: StockTransfer[]; rt?: StockTransfer[] }
    >();
    stockTransfers.forEach((st) => {
      // Ưu tiên dùng materialCode đã lưu trong database
      // Nếu chưa có thì lấy từ Loyalty API (đã fetch ở trên)
      const materialCode =
        st.materialCode ||
        stockTransferLoyaltyMap.get(st.itemCode)?.materialCode;
      if (!materialCode) {
        // Bỏ qua nếu không có materialCode (không match được)
        return;
      }

      // Key: soCode_materialCode (Mã ĐH_Mã hàng từ Loyalty API)
      const soCode = st.soCode || st.docCode || docCode;
      const key = `${soCode}_${materialCode}`;

      if (!stockTransferMapBySoCodeAndMaterialCode.has(key)) {
        stockTransferMapBySoCodeAndMaterialCode.set(key, {});
      }
      const itemMap = stockTransferMapBySoCodeAndMaterialCode.get(key)!;
      // ST* - dùng array để lưu tất cả
      if (st.docCode.startsWith('ST')) {
        if (!itemMap.st) {
          itemMap.st = [];
        }
        itemMap.st.push(st);
      }
      // RT* - dùng array để lưu tất cả
      if (st.docCode.startsWith('RT')) {
        if (!itemMap.rt) {
          itemMap.rt = [];
        }
        itemMap.rt.push(st);
      }
    });

    // Enrich sales với department information và lấy maKho từ stock transfer
    const enrichedSalesWithDepartment = await Promise.all(
      enrichedSalesWithLoyalty.map(async (sale) => {
        const department = sale.branchCode
          ? departmentMap.get(sale.branchCode) || null
          : null;
        const maBp = department?.ma_bp || sale.branchCode || null;

        // Lấy mã kho từ stock transfer (Mã kho xuất - stockCode)
        const saleLoyaltyProduct = sale.itemCode
          ? loyaltyProductMap.get(sale.itemCode)
          : null;
        const saleMaterialCode = saleLoyaltyProduct?.materialCode;
        const finalMaKho = await this.getMaKhoFromStockTransfer(
          sale,
          docCode,
          stockTransfers,
          saleMaterialCode,
        );

        // Lấy ma_nx từ stock transfer (phân biệt ST và RT)
        // Match stock transfer để lấy ma_nx
        const matchedStockTransfer = stockTransfers.find(
          (st) => st.soCode === docCode && st.itemCode === sale.itemCode,
        );
        const firstSt =
          matchedStockTransfer && matchedStockTransfer.docCode.startsWith('ST')
            ? matchedStockTransfer
            : null;
        const firstRt =
          matchedStockTransfer && matchedStockTransfer.docCode.startsWith('RT')
            ? matchedStockTransfer
            : null;

        return {
          ...sale,
          department: department,
          maKho: finalMaKho,
          // Thêm ma_nx từ stock transfer (lấy từ record đầu tiên)
          ma_nx_st: firstSt?.docCode || null, // ST* - mã nghiệp vụ từ stock transfer
          ma_nx_rt: firstRt?.docCode || null, // RT* - mã nghiệp vụ từ stock transfer
        };
      }),
    );

    // Tính tổng doanh thu của đơn hàng
    const totalRevenue = sales.reduce(
      (sum, sale) => sum + Number(sale.revenue),
      0,
    );
    const totalQty = sales.reduce((sum, sale) => sum + Number(sale.qty), 0);

    // Lấy thông tin chung từ sale đầu tiên
    const firstSale = sales[0];

    // Lấy thông tin khuyến mại từ Loyalty API cho các promCode trong đơn hàng
    // Fetch parallel để tối ưu performance
    const promotionsByCode: Record<string, any> = {};
    const uniquePromCodes = Array.from(
      new Set(
        sales
          .map((s) => s.promCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    if (uniquePromCodes.length > 0) {
      const promotionPromises = uniquePromCodes.map(async (promCode) => {
        try {
          // Gọi Loyalty API theo externalCode = promCode
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/promotions/item/external/${promCode}`,
            {
              headers: { accept: 'application/json' },
              timeout: 5000, // Timeout 5s để tránh chờ quá lâu
            },
          );

          const data = response?.data;
          return { promCode, data };
        } catch (error) {
          // Chỉ log error nếu không phải 404 (không tìm thấy promotion là bình thường)
          if ((error as any)?.response?.status !== 404) {
            this.logger.warn(
              `Lỗi khi lấy promotion cho promCode ${promCode}: ${(error as any)?.message || error}`,
            );
          }
          return { promCode, data: null };
        }
      });

      const promotionResults = await Promise.all(promotionPromises);
      promotionResults.forEach(({ promCode, data }) => {
        promotionsByCode[promCode] = {
          raw: data,
          main: data || null,
        };
      });
    }

    // Gắn promotion tương ứng vào từng dòng sale (chỉ để trả ra API, không lưu DB)
    // Và tính lại muaHangCkVip nếu chưa có hoặc cần override cho f3
    // Format sales giống findAllOrders để đảm bảo consistency với frontend
    // Format sales sau khi đã enrich promotion
    const formattedSales = await Promise.all(
      enrichedSalesWithDepartment.map(async (sale) => {
        const loyaltyProduct = sale.itemCode
          ? loyaltyProductMap.get(sale.itemCode)
          : null;
        const department = sale.branchCode
          ? departmentMap.get(sale.branchCode)
          : null;
        const calculatedFields = SalesCalculationUtils.calculateSaleFields(
          sale,
          loyaltyProduct,
          department,
          sale.branchCode,
        );

        const orderForFormatting = {
          customer: firstSale.customer || null,
          cashioData: cashioRecords,
          cashioFopSyscode: selectedCashio?.fop_syscode || null,
          cashioTotalIn: selectedCashio?.total_in || null,
          brand: firstSale.customer?.brand || null,
          docDate: firstSale.docDate,
        };

        const formattedSale = await SalesFormattingUtils.formatSaleForFrontend(
          sale,
          loyaltyProduct,
          department,
          calculatedFields,
          orderForFormatting,
          this.categoriesService,
          this.loyaltyService,
          stockTransfers,
        );

        // Thêm promotion info nếu có
        const promCode = sale.promCode;
        const promotion =
          promCode && promotionsByCode[promCode]
            ? promotionsByCode[promCode]
            : null;

        return {
          ...formattedSale,
          promotion,
          promotionDisplayCode: SalesUtils.getPromotionDisplayCode(promCode),
        };
      }),
    );

    // Format customer object để match với frontend interface
    const formattedCustomer = firstSale.customer
      ? {
          ...firstSale.customer,
          // Map mobile -> phone nếu phone chưa có
          phone: firstSale.customer.phone || firstSale.customer.mobile || null,
          // Map address -> street nếu street chưa có
          street:
            firstSale.customer.street || firstSale.customer.address || null,
        }
      : null;

    return {
      docCode: firstSale.docCode,
      docDate: firstSale.docDate,
      branchCode: firstSale.branchCode,
      docSourceType:
        firstSale.docSourceType || (firstSale as any).docSourceType || null,
      customer: formattedCustomer,
      totalRevenue,
      totalQty,
      totalItems: sales.length,
      sales: formattedSales,
      promotions: promotionsByCode,
      // Cashio data từ join với daily_cashio
      cashioData: cashioRecords.length > 0 ? cashioRecords : null,
      cashioFopSyscode: selectedCashio?.fop_syscode || null,
      cashioFopDescription: selectedCashio?.fop_description || null,
      cashioCode: selectedCashio?.code || null,
      cashioMasterCode: selectedCashio?.master_code || null,
      cashioTotalIn: selectedCashio?.total_in || null,
      cashioTotalOut: selectedCashio?.total_out || null,
    };
  }

  /**
   * Lưu hóa đơn vào bảng kê hóa đơn (FastApiInvoice)
   */
  private async saveFastApiInvoice(data: {
    docCode: string;
    maDvcs?: string;
    maKh?: string;
    tenKh?: string;
    ngayCt?: Date;
    status: number;
    message?: string;
    guid?: string | null;
    fastApiResponse?: string;
  }): Promise<FastApiInvoice> {
    try {
      // Kiểm tra xem đã có chưa
      const existing = await this.fastApiInvoiceRepository.findOne({
        where: { docCode: data.docCode },
      });

      if (existing) {
        // Cập nhật record hiện có
        existing.status = data.status;
        existing.message = data.message || existing.message;
        existing.guid = data.guid || existing.guid;
        existing.fastApiResponse =
          data.fastApiResponse || existing.fastApiResponse;
        if (data.maDvcs) existing.maDvcs = data.maDvcs;
        if (data.maKh) existing.maKh = data.maKh;
        if (data.tenKh) existing.tenKh = data.tenKh;
        if (data.ngayCt) existing.ngayCt = data.ngayCt;

        const saved = await this.fastApiInvoiceRepository.save(existing);
        return Array.isArray(saved) ? saved[0] : saved;
      } else {
        // Tạo mới
        const fastApiInvoice = this.fastApiInvoiceRepository.create({
          docCode: data.docCode,
          maDvcs: data.maDvcs ?? null,
          maKh: data.maKh ?? null,
          tenKh: data.tenKh ?? null,
          ngayCt: data.ngayCt ?? new Date(),
          status: data.status,
          message: data.message ?? null,
          guid: data.guid ?? null,
          fastApiResponse: data.fastApiResponse ?? null,
        } as Partial<FastApiInvoice>);

        const saved = await this.fastApiInvoiceRepository.save(fastApiInvoice);
        return Array.isArray(saved) ? saved[0] : saved;
      }
    } catch (error: any) {
      this.logger.error(
        `Error saving FastApiInvoice for ${data.docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  private async markOrderAsProcessed(docCode: string): Promise<void> {
    // Tìm tất cả các sale có cùng docCode
    const sales = await this.saleRepository.find({
      where: { docCode },
    });

    // Cập nhật isProcessed = true cho tất cả các sale
    if (sales.length > 0) {
      await this.saleRepository.update({ docCode }, { isProcessed: true });
    }
  }

  /**
   * Đánh dấu lại các đơn hàng đã có invoice là đã xử lý
   * Method này dùng để xử lý các invoice đã được tạo trước đó
   */
  async markProcessedOrdersFromInvoices(): Promise<{
    updated: number;
    message: string;
  }> {
    // Tìm tất cả các invoice đã được in (isPrinted = true)
    const invoices = await this.invoiceRepository.find({
      where: { isPrinted: true },
    });

    let updatedCount = 0;
    const processedDocCodes = new Set<string>();

    // Duyệt qua các invoice và tìm docCode từ key
    // Key có thể là docCode hoặc có format INV_xxx_xxx
    for (const invoice of invoices) {
      let docCode: string | null = null;

      // Thử 1: Key chính là docCode (cho các invoice mới)
      const salesByKey = await this.saleRepository.find({
        where: { docCode: invoice.key },
        take: 1,
      });
      if (salesByKey.length > 0) {
        docCode = invoice.key;
      } else {
        // Thử 2: Tìm trong printResponse xem có docCode không
        try {
          if (invoice.printResponse) {
            const printResponse = JSON.parse(invoice.printResponse);

            // Tìm trong Message (là JSON string chứa array)
            if (printResponse.Message) {
              try {
                const messageData = JSON.parse(printResponse.Message);
                if (Array.isArray(messageData) && messageData.length > 0) {
                  const data = messageData[0];
                  if (data.key) {
                    // Extract docCode từ key (format: SO52.00005808_X -> SO52.00005808)
                    const keyParts = data.key.split('_');
                    if (keyParts.length > 0) {
                      const potentialDocCode = keyParts[0];
                      const salesByPotentialKey =
                        await this.saleRepository.find({
                          where: { docCode: potentialDocCode },
                          take: 1,
                        });
                      if (salesByPotentialKey.length > 0) {
                        docCode = potentialDocCode;
                      }
                    }
                  }
                }
              } catch (msgError) {
                // Message không phải JSON string, bỏ qua
              }
            }

            // Thử tìm trong Data nếu có
            if (
              !docCode &&
              printResponse.Data &&
              Array.isArray(printResponse.Data) &&
              printResponse.Data.length > 0
            ) {
              const data = printResponse.Data[0];
              if (data.key) {
                const keyParts = data.key.split('_');
                if (keyParts.length > 0) {
                  const potentialDocCode = keyParts[0];
                  const salesByPotentialKey = await this.saleRepository.find({
                    where: { docCode: potentialDocCode },
                    take: 1,
                  });
                  if (salesByPotentialKey.length > 0) {
                    docCode = potentialDocCode;
                  }
                }
              }
            }
          }
        } catch (error) {
          // Ignore parse errors
        }
      }

      // Nếu tìm thấy docCode, đánh dấu các sale là đã xử lý
      if (docCode && !processedDocCodes.has(docCode)) {
        const updateResult = await this.saleRepository.update(
          { docCode },
          { isProcessed: true },
        );
        if (updateResult.affected && updateResult.affected > 0) {
          updatedCount += updateResult.affected;
          processedDocCodes.add(docCode);
        }
      }
    }

    return {
      updated: updatedCount,
      message: `Đã đánh dấu ${processedDocCodes.size} đơn hàng là đã xử lý (${updatedCount} sale records)`,
    };
  }

  /**
   * Đồng bộ dữ liệu từ Zappy API và lưu vào database
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 04DEC2025)
   * @param brand - Brand name (f3, labhair, yaman, menard). Nếu không có thì dùng default
   * @returns Kết quả đồng bộ
   */
  async syncFromZappy(
    date: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    ordersCount: number;
    salesCount: number;
    customersCount: number;
    errors?: string[];
  }> {
    try {
      // Lấy dữ liệu từ Zappy API
      const orders = await this.zappyApiService.getDailySales(date, brand);

      // Lấy dữ liệu cash/voucher từ get_daily_cash để enrich
      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date, brand);
      } catch (error) {}

      // Tạo map cash data theo so_code để dễ lookup
      const cashMapBySoCode = new Map<string, any[]>();
      cashData.forEach((cash) => {
        const soCode = cash.so_code || cash.master_code;
        if (soCode) {
          if (!cashMapBySoCode.has(soCode)) {
            cashMapBySoCode.set(soCode, []);
          }
          cashMapBySoCode.get(soCode)!.push(cash);
        }
      });

      if (orders.length === 0) {
        return {
          success: true,
          message: `Không có dữ liệu để đồng bộ cho ngày ${date}`,
          ordersCount: 0,
          salesCount: 0,
          customersCount: 0,
        };
      }

      let salesCount = 0;
      let customersCount = 0;
      const errors: string[] = [];

      // Collect tất cả branchCodes để fetch departments
      const branchCodes = Array.from(
        new Set(
          orders
            .map((o) => o.branchCode)
            .filter((code): code is string => !!code && code.trim() !== ''),
        ),
      );

      // Fetch departments để lấy company và map sang brand
      const departmentMap = new Map<string, { company?: string }>();
      if (!brand || brand !== 'chando') {
        for (const branchCode of branchCodes) {
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
              { headers: { accept: 'application/json' } },
            );
            const department = response?.data?.data?.items?.[0];
            if (department?.company) {
              departmentMap.set(branchCode, { company: department.company });
            }
          } catch (error) {
            this.logger.warn(
              `Failed to fetch department for branchCode ${branchCode}: ${error}`,
            );
          }
        }
      }
      // Map company sang brand
      const mapCompanyToBrand = (
        company: string | null | undefined,
      ): string => {
        if (!company) return '';
        const companyUpper = company.toUpperCase();
        const brandMap: Record<string, string> = {
          F3: 'f3',
          FACIALBAR: 'f3',
          MENARD: 'menard',
          LABHAIR: 'labhair',
          YAMAN: 'yaman',
          CHANDO: 'chando',
        };
        return brandMap[companyUpper] || company.toLowerCase();
      };

      // Xử lý từng order
      for (const order of orders) {
        try {
          // Lấy brand từ department.company
          const department = departmentMap.get(order.branchCode);
          const brandFromDepartment = department?.company
            ? mapCompanyToBrand(department.company)
            : order.customer.brand || '';

          // Tìm hoặc tạo customer
          let customer = await this.customerRepository.findOne({
            where: { code: order.customer.code },
          });

          if (!customer) {
            const newCustomer = this.customerRepository.create({
              code: order.customer.code,
              name: order.customer.name,
              brand: brandFromDepartment,
              mobile: order.customer.mobile,
              sexual: order.customer.sexual,
              idnumber: order.customer.idnumber,
              enteredat: order.customer.enteredat
                ? new Date(order.customer.enteredat)
                : null,
              crm_lead_source: order.customer.crm_lead_source,
              address: order.customer.address,
              province_name: order.customer.province_name,
              birthday: order.customer.birthday
                ? new Date(order.customer.birthday)
                : null,
              grade_name: order.customer.grade_name,
              branch_code: order.customer.branch_code,
            } as Partial<Customer>);
            customer = await this.customerRepository.save(newCustomer);
            customersCount++;
          } else {
            // Cập nhật thông tin customer nếu cần
            customer.name = order.customer.name || customer.name;
            customer.mobile = order.customer.mobile || customer.mobile;
            customer.grade_name =
              order.customer.grade_name || customer.grade_name;
            // Cập nhật brand từ department nếu có
            if (brandFromDepartment) {
              customer.brand = brandFromDepartment;
            }
            customer = await this.customerRepository.save(customer);
          }

          // Đảm bảo customer không null
          if (!customer) {
            const errorMsg = `Không thể tạo hoặc tìm customer với code ${order.customer.code}`;
            this.logger.error(errorMsg);
            errors.push(errorMsg);
            continue;
          }

          // Lấy cash/voucher data cho order này
          const orderCashData = cashMapBySoCode.get(order.docCode) || [];
          const voucherData = orderCashData.filter(
            (cash) => cash.fop_syscode === 'VOUCHER',
          );

          // Collect tất cả itemCodes từ order để fetch products từ Loyalty API và check 404
          // Đảm bảo trim ngay từ đầu để consistency
          const orderItemCodes = Array.from(
            new Set(
              (order.sales || [])
                .map((s) => s.itemCode?.trim())
                .filter((code): code is string => !!code && code !== ''),
            ),
          );

          // Fetch products từ Loyalty API để check sản phẩm không tồn tại (404)
          const notFoundItemCodes = new Set<string>();

          if (orderItemCodes.length > 0) {
            // Check products từ Loyalty API sử dụng LoyaltyService
            await Promise.all(
              orderItemCodes.map(async (trimmedItemCode) => {
                const product =
                  await this.loyaltyService.checkProduct(trimmedItemCode);
                if (!product) {
                  notFoundItemCodes.add(trimmedItemCode);
                }
              }),
            );
          }

          // Xử lý từng sale trong order - LƯU TẤT CẢ, đánh dấu statusAsys = false nếu sản phẩm không tồn tại (404)
          if (order.sales && order.sales.length > 0) {
            for (const saleItem of order.sales) {
              try {
                // Bỏ qua các item có itemcode = "TRUTONKEEP"
                const itemCode = saleItem.itemCode?.trim();
                if (itemCode && itemCode.toUpperCase() === 'TRUTONKEEP') {
                  this.logger.log(
                    `[SalesService] Bỏ qua sale item ${itemCode} (${saleItem.itemName || 'N/A'}) trong order ${order.docCode} - itemcode = TRUTONKEEP`,
                  );
                  continue;
                }

                // Kiểm tra xem sản phẩm có tồn tại trong Loyalty API không
                const isNotFound = itemCode && notFoundItemCodes.has(itemCode);
                // Set statusAsys: false nếu không tồn tại (404), true nếu tồn tại
                const statusAsys = !isNotFound;

                if (isNotFound) {
                  this.logger.warn(
                    `[SalesService] Sale item ${itemCode} (${saleItem.itemName || 'N/A'}) trong order ${order.docCode} - Sản phẩm không tồn tại trong Loyalty API (404), sẽ lưu với statusAsys = false`,
                  );
                }

                // Lấy productType: Ưu tiên từ Zappy API (producttype), nếu không có thì lấy từ Loyalty API
                // Kiểm tra cả producttype (chữ thường) và productType (camelCase) từ Zappy API
                const productTypeFromZappy =
                  saleItem.producttype || saleItem.productType || null;
                // Fetch productType từ Loyalty API nếu chưa có từ Zappy (đã có sẵn trong notFoundItemCodes check)
                let productTypeFromLoyalty: string | null = null;
                if (
                  !productTypeFromZappy &&
                  itemCode &&
                  !notFoundItemCodes.has(itemCode)
                ) {
                  try {
                    const loyaltyProduct =
                      await this.loyaltyService.checkProduct(itemCode);
                    if (loyaltyProduct) {
                      productTypeFromLoyalty =
                        loyaltyProduct.productType ||
                        loyaltyProduct.producttype ||
                        null;
                    }
                  } catch (error) {
                    // Ignore error, sẽ dùng null
                  }
                }
                const productType =
                  productTypeFromZappy || productTypeFromLoyalty || null;

                // Kiểm tra xem sale đã tồn tại chưa
                // Với đơn "08. Tách thẻ": cần thêm qty vào điều kiện vì có thể có 2 dòng cùng itemCode nhưng qty khác nhau (-1 và 1)
                // Với các đơn khác: chỉ cần docCode + itemCode + customer
                const ordertypeName =
                  saleItem.ordertype_name || saleItem.ordertype || '';
                const isTachThe =
                  ordertypeName.includes('08. Tách thẻ') ||
                  ordertypeName.includes('08.Tách thẻ') ||
                  ordertypeName.includes('08.  Tách thẻ');

                // Enrich voucher data từ get_daily_cash
                let voucherRefno: string | undefined;
                let voucherAmount: number | undefined;
                if (voucherData.length > 0) {
                  // Lấy voucher đầu tiên (có thể có nhiều voucher)
                  const firstVoucher = voucherData[0];
                  voucherRefno = firstVoucher.refno;
                  voucherAmount = firstVoucher.total_in || 0;
                }

                // Tạo sale mới
                // Tính toán ordertypeName trước
                let finalOrderTypeNameForNew: string | undefined = undefined;
                if (
                  saleItem.ordertype_name !== undefined &&
                  saleItem.ordertype_name !== null
                ) {
                  if (typeof saleItem.ordertype_name === 'string') {
                    const trimmed = saleItem.ordertype_name.trim();
                    finalOrderTypeNameForNew =
                      trimmed !== '' ? trimmed : undefined;
                  } else {
                    finalOrderTypeNameForNew =
                      String(saleItem.ordertype_name).trim() || undefined;
                  }
                }
                // Log để debug
                this.logger.log(
                  `[SalesService] Tạo mới sale ${order.docCode}/${saleItem.itemCode}: ` +
                    `ordertype_name raw="${saleItem.ordertype_name}" (type: ${typeof saleItem.ordertype_name}), final="${finalOrderTypeNameForNew}"`,
                );
                const newSale = this.saleRepository.create({
                  docCode: order.docCode,
                  docDate: new Date(order.docDate),
                  branchCode: order.branchCode,
                  docSourceType: order.docSourceType,
                  ordertype: saleItem.ordertype,
                  // Luôn lưu ordertypeName, kể cả khi là undefined (để lưu từ Zappy API)
                  // Nếu ordertypeName là empty string, set thành undefined
                  ordertypeName: finalOrderTypeNameForNew,
                  description: saleItem.description,
                  partnerCode: saleItem.partnerCode,
                  itemCode: saleItem.itemCode || '',
                  itemName: saleItem.itemName || '',
                  qty: saleItem.qty || 0,
                  revenue: saleItem.revenue || 0,
                  linetotal: saleItem.linetotal || saleItem.revenue || 0,
                  tienHang:
                    saleItem.tienHang ||
                    saleItem.linetotal ||
                    saleItem.revenue ||
                    0,
                  giaBan: saleItem.giaBan || 0,
                  promCode: saleItem.promCode,
                  serial: saleItem.serial,
                  soSerial: saleItem.serial,
                  disc_amt: saleItem.disc_amt,
                  grade_discamt: saleItem.grade_discamt,
                  other_discamt: saleItem.other_discamt,
                  chietKhauMuaHangGiamGia: saleItem.chietKhauMuaHangGiamGia,
                  paid_by_voucher_ecode_ecoin_bp:
                    saleItem.paid_by_voucher_ecode_ecoin_bp,
                  maCa: saleItem.shift_code,
                  // Validate saleperson_id để tránh NaN
                  saleperson_id: SalesUtils.validateInteger(
                    saleItem.saleperson_id,
                  ),
                  partner_name: saleItem.partner_name,
                  order_source: saleItem.order_source,
                  // Lưu mvc_serial vào maThe
                  maThe: saleItem.mvc_serial,
                  // Category fields
                  cat1: saleItem.cat1,
                  cat2: saleItem.cat2,
                  cat3: saleItem.cat3,
                  catcode1: saleItem.catcode1,
                  catcode2: saleItem.catcode2,
                  catcode3: saleItem.catcode3,
                  // Luôn lưu productType, kể cả khi là null (để lưu từ Zappy API)
                  // Nếu productType là empty string, set thành null
                  productType:
                    productType && productType.trim() !== ''
                      ? productType.trim()
                      : null,
                  // Enrich voucher data từ get_daily_cash
                  voucherDp1: voucherRefno,
                  thanhToanVoucher:
                    voucherAmount && voucherAmount > 0
                      ? voucherAmount
                      : undefined,
                  customer: customer,
                  brand: brand,
                  isProcessed: false,
                  statusAsys: statusAsys, // Set statusAsys: true nếu sản phẩm tồn tại, false nếu 404
                  type_sale: 'RETAIL',
                } as Partial<Sale>);
                await this.saleRepository.save(newSale);
                salesCount++;
              } catch (saleError: any) {
                const errorMsg = `Lỗi khi lưu sale ${order.docCode}/${saleItem.itemCode}: ${saleError?.message || saleError}`;
                this.logger.error(errorMsg);
                errors.push(errorMsg);
              }
            }
          }
        } catch (orderError: any) {
          const errorMsg = `Lỗi khi xử lý order ${order.docCode}: ${orderError?.message || orderError}`;
          this.logger.error(errorMsg);
          errors.push(errorMsg);
        }
      }

      return {
        success: errors.length === 0,
        message: `Đồng bộ thành công ${orders.length} đơn hàng cho ngày ${date}${brand ? ` (brand: ${brand})` : ''}`,
        ordersCount: orders.length,
        salesCount,
        customersCount,
        errors: errors.length > 0 ? errors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ từ Zappy API: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ sale từ khoảng thời gian cho tất cả các nhãn
   * @param startDate - Ngày bắt đầu theo format DDMMMYYYY (ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc theo format DDMMMYYYY (ví dụ: 01DEC2025)
   * @returns Kết quả đồng bộ tổng hợp
   */
  async syncSalesByDateRange(
    startDate: string,
    endDate: string,
  ): Promise<{
    success: boolean;
    message: string;
    totalOrdersCount: number;
    totalSalesCount: number;
    totalCustomersCount: number;
    brandResults: Array<{
      brand: string;
      ordersCount: number;
      salesCount: number;
      customersCount: number;
      errors?: string[];
    }>;
    errors?: string[];
  }> {
    const brands = ['f3', 'labhair', 'yaman', 'menard', 'chando'];
    const allErrors: string[] = [];
    const brandResults: Array<{
      brand: string;
      ordersCount: number;
      salesCount: number;
      customersCount: number;
      errors?: string[];
    }> = [];

    let totalOrdersCount = 0;
    let totalSalesCount = 0;
    let totalCustomersCount = 0;

    // Parse dates
    const parseDate = (dateStr: string): Date => {
      // Format: DDMMMYYYY (ví dụ: 01OCT2025)
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

      const month = monthMap[monthStr];
      if (month === undefined) {
        throw new Error(`Invalid month: ${monthStr}`);
      }

      return new Date(year, month, day);
    };

    const formatDate = (date: Date): string => {
      const day = date.getDate().toString().padStart(2, '0');
      const monthNames = [
        'JAN',
        'FEB',
        'MAR',
        'APR',
        'MAY',
        'JUN',
        'JUL',
        'AUG',
        'SEP',
        'OCT',
        'NOV',
        'DEC',
      ];
      const month = monthNames[date.getMonth()];
      const year = date.getFullYear();
      return `${day}${month}${year}`;
    };

    try {
      const start = parseDate(startDate);
      const end = parseDate(endDate);

      // Lặp qua từng brand
      for (const brand of brands) {
        this.logger.log(
          `[syncSalesByDateRange] Bắt đầu đồng bộ brand: ${brand}`,
        );
        let brandOrdersCount = 0;
        let brandSalesCount = 0;
        let brandCustomersCount = 0;
        const brandErrors: string[] = [];

        // Lặp qua từng ngày trong khoảng thời gian
        const currentDate = new Date(start);
        while (currentDate <= end) {
          const dateStr = formatDate(currentDate);
          try {
            this.logger.log(
              `[syncSalesByDateRange] Đồng bộ ${brand} - ngày ${dateStr}`,
            );
            const result = await this.syncFromZappy(dateStr, brand);

            brandOrdersCount += result.ordersCount;
            brandSalesCount += result.salesCount;
            brandCustomersCount += result.customersCount;

            if (result.errors && result.errors.length > 0) {
              brandErrors.push(
                ...result.errors.map((err) => `[${dateStr}] ${err}`),
              );
            }
          } catch (error: any) {
            const errorMsg = `[${brand}] Lỗi khi đồng bộ ngày ${dateStr}: ${error?.message || error}`;
            this.logger.error(errorMsg);
            brandErrors.push(errorMsg);
          }

          // Tăng ngày lên 1
          currentDate.setDate(currentDate.getDate() + 1);
        }

        totalOrdersCount += brandOrdersCount;
        totalSalesCount += brandSalesCount;
        totalCustomersCount += brandCustomersCount;

        brandResults.push({
          brand,
          ordersCount: brandOrdersCount,
          salesCount: brandSalesCount,
          customersCount: brandCustomersCount,
          errors: brandErrors.length > 0 ? brandErrors : undefined,
        });

        if (brandErrors.length > 0) {
          allErrors.push(...brandErrors);
        }

        this.logger.log(
          `[syncSalesByDateRange] Hoàn thành đồng bộ brand: ${brand} - ${brandOrdersCount} đơn, ${brandSalesCount} sale`,
        );
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ thành công từ ${startDate} đến ${endDate}: ${totalOrdersCount} đơn hàng, ${totalSalesCount} sale, ${totalCustomersCount} khách hàng`,
        totalOrdersCount,
        totalSalesCount,
        totalCustomersCount,
        brandResults,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ sale theo khoảng thời gian: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Tạo hóa đơn qua Fast API từ đơn hàng
   */
  async createInvoiceViaFastApi(
    docCode: string,
    forceRetry: boolean = false,
  ): Promise<any> {
    try {
      // ============================================
      // 1. CHECK INVOICE ĐÃ TẠO
      // ============================================
      if (!forceRetry) {
        const existingInvoice = await this.fastApiInvoiceRepository.findOne({
          where: { docCode },
        });

        if (existingInvoice && existingInvoice.status === 1) {
          return {
            success: true,
            message: `Đơn hàng ${docCode} đã được tạo hóa đơn thành công trước đó`,
            result: existingInvoice.fastApiResponse
              ? JSON.parse(existingInvoice.fastApiResponse)
              : null,
            alreadyExists: true,
          };
        }
      }

      // ============================================
      // 2. LẤY DỮ LIỆU ĐƠN HÀNG
      // ============================================
      const orderData = await this.findByOrderCode(docCode);
      const docCodesForStockTransfer =
        StockTransferUtils.getDocCodesForStockTransfer([docCode]);
      const stockTransfers = await this.stockTransferRepository.find({
        where: { soCode: In(docCodesForStockTransfer) },
      });

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(
          `Order ${docCode} not found or has no sales`,
        );
      }

      const hasX = /_X$/.test(docCode);

      if (_.isEmpty(stockTransfers)) {
        if (hasX) {
          return await this.handleSaleOrderWithUnderscoreX(
            orderData,
            docCode ?? '',
            1,
          );
        } else {
          return await this.handleSaleOrderWithUnderscoreX(
            orderData,
            docCode ?? '',
            0,
          );
        }
      }

      // Không có _X → xử lý bình thường
      return await this.processSingleOrder(docCode, forceRetry);
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi tạo hóa đơn cho ${docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Xử lý một đơn hàng đơn lẻ (được gọi từ createInvoiceViaFastApi)
   */
  private async processSingleOrder(
    docCode: string,
    forceRetry: boolean = false,
  ): Promise<any> {
    try {
      // Kiểm tra xem đơn hàng đã có trong bảng kê hóa đơn chưa (đã tạo thành công)
      // Nếu forceRetry = true, bỏ qua check này để cho phép retry
      if (!forceRetry) {
        const existingInvoice = await this.fastApiInvoiceRepository.findOne({
          where: { docCode },
        });

        if (existingInvoice && existingInvoice.status === 1) {
          return {
            success: true,
            message: `Đơn hàng ${docCode} đã được tạo hóa đơn thành công trước đó`,
            result: existingInvoice.fastApiResponse
              ? JSON.parse(existingInvoice.fastApiResponse)
              : null,
            alreadyExists: true,
          };
        }
      }

      // Lấy thông tin đơn hàng
      const orderData = await this.findByOrderCode(docCode);

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(
          `Order ${docCode} not found or has no sales`,
        );
      }

      // ============================================
      // BƯỚC 1: Kiểm tra docSourceType trước (ưu tiên cao nhất)
      // ============================================
      const firstSale =
        orderData.sales && orderData.sales.length > 0
          ? orderData.sales[0]
          : null;
      const docSourceTypeRaw =
        firstSale?.docSourceType ?? orderData.docSourceType ?? '';
      const docSourceType = docSourceTypeRaw
        ? String(docSourceTypeRaw).trim().toUpperCase()
        : '';

      // Xử lý SALE_RETURN
      // Nhưng vẫn phải validate chỉ cho phép "01.Thường" và "01. Thường"
      if (docSourceType === 'SALE_RETURN') {
        // Validate chỉ cho phép "01.Thường" và "01. Thường"
        const validationResult =
          this.invoiceValidationService.validateOrderForInvoice({
            docCode,
            sales: orderData.sales,
          });
        if (!validationResult.success) {
          const errorMessage =
            validationResult.message ||
            `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: undefined,
          });
          return {
            success: false,
            message: errorMessage,
            result: null,
          };
        }
        return await this.handleSaleReturnFlow(orderData, docCode);
      }

      // ============================================
      // BƯỚC 2: Validate điều kiện tạo hóa đơn TRƯỚC khi xử lý các case đặc biệt
      // ============================================
      // Validate chỉ cho phép "01.Thường" và "01. Thường" tạo hóa đơn
      // Các loại đơn đặc biệt (03. Đổi điểm, 04. Đổi DV, 05. Tặng sinh nhật) được xử lý riêng
      const sales = orderData.sales || [];
      const normalizeOrderType = (
        ordertypeName: string | null | undefined,
      ): string => {
        if (!ordertypeName) return '';
        return String(ordertypeName).trim().toLowerCase();
      };

      // Kiểm tra các loại đơn đặc biệt được phép xử lý
      const hasDoiDiemOrder = sales.some((s: any) =>
        SalesUtils.isDoiDiemOrder(s.ordertype, s.ordertypeName),
      );
      const hasDoiDvOrder = sales.some((s: any) =>
        SalesUtils.isDoiDvOrder(s.ordertype, s.ordertypeName),
      );
      const hasTangSinhNhatOrder = sales.some((s: any) =>
        SalesUtils.isTangSinhNhatOrder(s.ordertype, s.ordertypeName),
      );
      const hasDauTuOrder = sales.some((s: any) =>
        SalesUtils.isDauTuOrder(s.ordertype, s.ordertypeName),
      );
      const hasTachTheOrder = sales.some((s: any) =>
        SalesUtils.isTachTheOrder(s.ordertype, s.ordertypeName),
      );
      const hasDoiVoOrder = sales.some((s: any) =>
        SalesUtils.isDoiVoOrder(s.ordertype, s.ordertypeName),
      );
      const hasServiceOrder = sales.some((s: any) => {
        const normalized = normalizeOrderType(s.ordertypeName || s.ordertype);
        return (
          normalized === '02. làm dịch vụ' || normalized === '02.làm dịch vụ'
        );
      });

      // Nếu không phải các loại đơn đặc biệt được phép, validate chỉ cho phép "01.Thường"
      if (
        !hasDoiDiemOrder &&
        !hasDoiDvOrder &&
        !hasTangSinhNhatOrder &&
        !hasDauTuOrder &&
        !hasTachTheOrder &&
        !hasDoiVoOrder &&
        !hasServiceOrder
      ) {
        const validationResult =
          this.invoiceValidationService.validateOrderForInvoice({
            docCode,
            sales: orderData.sales,
          });

        if (!validationResult.success) {
          const errorMessage =
            validationResult.message ||
            `Đơn hàng ${docCode} không đủ điều kiện tạo hóa đơn`;
          // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: null,
          };
        }
      }

      // ============================================
      // BƯỚC 3: Xử lý các case đặc biệt (sau khi đã validate)
      // ============================================

      // Nếu là đơn dịch vụ, chạy flow dịch vụ -
      if (hasServiceOrder) {
        return await this.executeServiceOrderFlow(orderData, docCode);
      }

      // Nếu là đơn "03. Đổi điểm", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDoiDiemOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult =
            await this.fastApiInvoiceFlowService.createSalesOrder(
              {
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              },
              0,
            ); // action = 0 cho đơn "03. Đổi điểm"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult =
              await this.fastApiInvoiceFlowService.createSalesInvoice({
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage =
              'Tạo sales invoice thất bại (03. Đổi điểm)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage =
                  errorData[0].message ||
                  errorData[0].error ||
                  salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(
              `03. Đổi điểm sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`,
            );
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult
            ? 1
            : salesOrderResult
              ? 0
              : 0;
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (03. Đổi điểm)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (03. Đổi điểm)'
              : 'Tạo sales order và sales invoice thất bại (03. Đổi điểm)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (03. Đổi điểm)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (03. Đổi điểm)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (03. Đổi điểm)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage =
                errorData[0].message || errorData[0].error || errorMessage;
            } else if (errorData.message) {
              errorMessage = errorData.message;
            } else if (errorData.error) {
              errorMessage = errorData.error;
            } else if (typeof errorData === 'string') {
              errorMessage = errorData;
            }
          } else if (error?.message) {
            errorMessage = error.message;
          }

          this.logger.error(
            `03. Đổi điểm order creation failed for order ${docCode}: ${errorMessage}`,
          );

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data
              ? JSON.stringify(error.response.data)
              : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "04. Đổi DV", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDoiDvOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult =
            await this.fastApiInvoiceFlowService.createSalesOrder(
              {
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              },
              0,
            ); // action = 0 cho đơn "04. Đổi DV"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult =
              await this.fastApiInvoiceFlowService.createSalesInvoice({
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage =
              'Tạo sales invoice thất bại (04. Đổi DV)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage =
                  errorData[0].message ||
                  errorData[0].error ||
                  salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(
              `04. Đổi DV sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`,
            );
          }

          // Bước 3: Xử lý cashio payment (Phiếu thu tiền mặt/Giấy báo có) nếu salesInvoice thành công
          let cashioResult: any = null;
          if (salesInvoiceResult) {
            this.logger.log(
              `[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (04. Đổi DV)`,
            );
            cashioResult =
              await this.fastApiInvoiceFlowService.processCashioPayment(
                docCode,
                orderData,
                invoiceData,
              );

            if (
              cashioResult.cashReceiptResults &&
              cashioResult.cashReceiptResults.length > 0
            ) {
              this.logger.log(
                `[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (04. Đổi DV)`,
              );
            }
            if (
              cashioResult.creditAdviceResults &&
              cashioResult.creditAdviceResults.length > 0
            ) {
              this.logger.log(
                `[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (04. Đổi DV)`,
              );
            }

            // Xử lý payment (Phiếu chi tiền mặt) nếu có mã kho
            // CHỈ xử lý cho đơn có docSourceType = ORDER_RETURN hoặc SALE_RETURN
            try {
              // Kiểm tra docSourceType - chỉ xử lý payment cho ORDER_RETURN hoặc SALE_RETURN
              const firstSale =
                orderData.sales && orderData.sales.length > 0
                  ? orderData.sales[0]
                  : null;
              const docSourceTypeRaw =
                firstSale?.docSourceType || orderData.docSourceType || '';
              const docSourceType = docSourceTypeRaw
                ? String(docSourceTypeRaw).trim().toUpperCase()
                : '';

              if (
                docSourceType === 'ORDER_RETURN' ||
                docSourceType === 'SALE_RETURN'
              ) {
                const docCodesForStockTransfer =
                  StockTransferUtils.getDocCodesForStockTransfer([docCode]);
                const stockTransfers = await this.stockTransferRepository.find({
                  where: { soCode: In(docCodesForStockTransfer) },
                });
                const stockCodes = Array.from(
                  new Set(
                    stockTransfers.map((st) => st.stockCode).filter(Boolean),
                  ),
                );

                if (stockCodes.length > 0) {
                  this.logger.log(
                    `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (04. Đổi DV, docSourceType: ${docSourceType}) với ${stockCodes.length} mã kho`,
                  );
                  const paymentResult =
                    await this.fastApiInvoiceFlowService.processPayment(
                      docCode,
                      orderData,
                      invoiceData,
                      stockCodes,
                    );

                  if (
                    paymentResult.paymentResults &&
                    paymentResult.paymentResults.length > 0
                  ) {
                    this.logger.log(
                      `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (04. Đổi DV)`,
                    );
                  }
                  if (
                    paymentResult.debitAdviceResults &&
                    paymentResult.debitAdviceResults.length > 0
                  ) {
                    this.logger.log(
                      `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (04. Đổi DV)`,
                    );
                  }
                } else {
                  this.logger.debug(
                    `[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`,
                  );
                }
              } else {
                this.logger.debug(
                  `[Payment] Đơn hàng ${docCode} (04. Đổi DV) có docSourceType = "${docSourceType}", không phải ORDER_RETURN/SALE_RETURN, bỏ qua payment API`,
                );
              }
            } catch (paymentError: any) {
              // Log lỗi nhưng không fail toàn bộ flow
              this.logger.warn(
                `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${paymentError?.message || paymentError}`,
              );
            }
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult
            ? 1
            : salesOrderResult
              ? 0
              : 0;
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (04. Đổi DV)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (04. Đổi DV)'
              : 'Tạo sales order và sales invoice thất bại (04. Đổi DV)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
              cashio: cashioResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (04. Đổi DV)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (04. Đổi DV)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
              cashio: cashioResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (04. Đổi DV)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage =
                errorData[0].message || errorData[0].error || errorMessage;
            } else if (errorData.message) {
              errorMessage = errorData.message;
            } else if (errorData.error) {
              errorMessage = errorData.error;
            } else if (typeof errorData === 'string') {
              errorMessage = errorData;
            }
          } else if (error?.message) {
            errorMessage = error.message;
          }

          this.logger.error(
            `04. Đổi DV order creation failed for order ${docCode}: ${errorMessage}`,
          );

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data
              ? JSON.stringify(error.response.data)
              : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "05. Tặng sinh nhật", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasTangSinhNhatOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult =
            await this.fastApiInvoiceFlowService.createSalesOrder(
              {
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              },
              0,
            ); // action = 0 cho đơn "05. Tặng sinh nhật"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult =
              await this.fastApiInvoiceFlowService.createSalesInvoice({
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage =
              'Tạo sales invoice thất bại (05. Tặng sinh nhật)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage =
                  errorData[0].message ||
                  errorData[0].error ||
                  salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(
              `05. Tặng sinh nhật sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`,
            );
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult
            ? 1
            : salesOrderResult
              ? 0
              : 0;
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (05. Tặng sinh nhật)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (05. Tặng sinh nhật)'
              : 'Tạo sales order và sales invoice thất bại (05. Tặng sinh nhật)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (05. Tặng sinh nhật)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (05. Tặng sinh nhật)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (05. Tặng sinh nhật)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage =
                errorData[0].message || errorData[0].error || errorMessage;
            } else if (errorData.message) {
              errorMessage = errorData.message;
            } else if (errorData.error) {
              errorMessage = errorData.error;
            } else if (typeof errorData === 'string') {
              errorMessage = errorData;
            }
          } else if (error?.message) {
            errorMessage = error.message;
          }

          this.logger.error(
            `05. Tặng sinh nhật order creation failed for order ${docCode}: ${errorMessage}`,
          );

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data
              ? JSON.stringify(error.response.data)
              : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "06. Đầu tư", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDauTuOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult =
            await this.fastApiInvoiceFlowService.createSalesOrder(
              {
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              },
              0,
            ); // action = 0 cho đơn "06. Đầu tư"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult =
              await this.fastApiInvoiceFlowService.createSalesInvoice({
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage =
              'Tạo sales invoice thất bại (06. Đầu tư)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage =
                  errorData[0].message ||
                  errorData[0].error ||
                  salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(
              `06. Đầu tư sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`,
            );
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult
            ? 1
            : salesOrderResult
              ? 0
              : 0;
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (06. Đầu tư)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (06. Đầu tư)'
              : 'Tạo sales order và sales invoice thất bại (06. Đầu tư)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (06. Đầu tư)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (06. Đầu tư)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (06. Đầu tư)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage =
                errorData[0].message || errorData[0].error || errorMessage;
            } else if (errorData.message) {
              errorMessage = errorData.message;
            } else if (errorData.error) {
              errorMessage = errorData.error;
            } else if (typeof errorData === 'string') {
              errorMessage = errorData;
            }
          } else if (error?.message) {
            errorMessage = error.message;
          }

          this.logger.error(
            `06. Đầu tư order creation failed for order ${docCode}: ${errorMessage}`,
          );

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data
              ? JSON.stringify(error.response.data)
              : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "08. Tách thẻ", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasTachTheOrder) {
        // Gọi API get_card để lấy issue_partner_code cho đơn "08. Tách thẻ"
        try {
          const cardResponse =
            await this.n8nService.fetchCardDataWithRetry(docCode);
          const cardData = this.n8nService.parseCardData(cardResponse);
          this.n8nService.mapIssuePartnerCodeToSales(
            orderData.sales || [],
            cardData,
          );
        } catch (e) {}

        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult =
            await this.fastApiInvoiceFlowService.createSalesOrder(
              {
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              },
              0,
            ); // action = 0 cho đơn "08. Tách thẻ"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult =
              await this.fastApiInvoiceFlowService.createSalesInvoice({
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage =
              'Tạo sales invoice thất bại (08. Tách thẻ)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage =
                  errorData[0].message ||
                  errorData[0].error ||
                  salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(
              `08. Tách thẻ sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`,
            );
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult
            ? 1
            : salesOrderResult
              ? 0
              : 0;
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (08. Tách thẻ)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (08. Tách thẻ)'
              : 'Tạo sales order và sales invoice thất bại (08. Tách thẻ)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (08. Tách thẻ)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (08. Tách thẻ)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (08. Tách thẻ)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage =
                errorData[0].message || errorData[0].error || errorMessage;
            } else if (errorData.message) {
              errorMessage = errorData.message;
            } else if (errorData.error) {
              errorMessage = errorData.error;
            } else if (typeof errorData === 'string') {
              errorMessage = errorData;
            }
          } else if (error?.message) {
            errorMessage = error.message;
          }

          this.logger.error(
            `08. Tách thẻ order creation failed for order ${docCode}: ${errorMessage}`,
          );

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data
              ? JSON.stringify(error.response.data)
              : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu là đơn "Đổi vỏ", gọi Fast/salesOrder và Fast/salesInvoice
      if (hasDoiVoOrder) {
        const invoiceData = await this.buildFastApiInvoiceData(orderData);
        try {
          // Bước 1: Gọi Fast/salesOrder với action = 0
          const salesOrderResult =
            await this.fastApiInvoiceFlowService.createSalesOrder(
              {
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              },
              0,
            ); // action = 0 cho đơn "Đổi vỏ"

          // Bước 2: Gọi Fast/salesInvoice sau khi salesOrder thành công
          let salesInvoiceResult: any = null;
          try {
            salesInvoiceResult =
              await this.fastApiInvoiceFlowService.createSalesInvoice({
                ...invoiceData,
                customer: orderData.customer,
                ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
              });
          } catch (salesInvoiceError: any) {
            // Nếu salesInvoice thất bại, log lỗi nhưng vẫn lưu kết quả salesOrder
            let salesInvoiceErrorMessage =
              'Tạo sales invoice thất bại (Đổi vỏ)';
            if (salesInvoiceError?.response?.data) {
              const errorData = salesInvoiceError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                salesInvoiceErrorMessage =
                  errorData[0].message ||
                  errorData[0].error ||
                  salesInvoiceErrorMessage;
              } else if (errorData.message) {
                salesInvoiceErrorMessage = errorData.message;
              } else if (errorData.error) {
                salesInvoiceErrorMessage = errorData.error;
              } else if (typeof errorData === 'string') {
                salesInvoiceErrorMessage = errorData;
              }
            } else if (salesInvoiceError?.message) {
              salesInvoiceErrorMessage = salesInvoiceError.message;
            }
            this.logger.error(
              `Đổi vỏ sales invoice creation failed for order ${docCode}: ${salesInvoiceErrorMessage}`,
            );
          }

          // Lưu vào bảng kê hóa đơn
          const responseStatus = salesInvoiceResult
            ? 1
            : salesOrderResult
              ? 0
              : 0;
          const responseMessage = salesInvoiceResult
            ? 'Tạo sales order và sales invoice thành công (Đổi vỏ)'
            : salesOrderResult
              ? 'Tạo sales order thành công, nhưng sales invoice thất bại (Đổi vỏ)'
              : 'Tạo sales order và sales invoice thất bại (Đổi vỏ)';

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || invoiceData.ma_dvcs || '',
            maKh: orderData.customer?.code || invoiceData.ma_kh || '',
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: responseStatus,
            message: responseMessage,
            guid: salesInvoiceResult?.guid || salesOrderResult?.guid || null,
            fastApiResponse: JSON.stringify({
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            }),
          });

          return {
            success: !!salesInvoiceResult,
            message: salesInvoiceResult
              ? `Tạo sales order và sales invoice thành công cho đơn hàng ${docCode} (Đổi vỏ)`
              : `Tạo sales order thành công nhưng sales invoice thất bại cho đơn hàng ${docCode} (Đổi vỏ)`,
            result: {
              salesOrder: salesOrderResult,
              salesInvoice: salesInvoiceResult,
            },
          };
        } catch (error: any) {
          let errorMessage = 'Tạo sales order thất bại (Đổi vỏ)';
          if (error?.response?.data) {
            const errorData = error.response.data;
            if (Array.isArray(errorData) && errorData.length > 0) {
              errorMessage =
                errorData[0].message || errorData[0].error || errorMessage;
            } else if (errorData.message) {
              errorMessage = errorData.message;
            } else if (errorData.error) {
              errorMessage = errorData.error;
            } else if (typeof errorData === 'string') {
              errorMessage = errorData;
            }
          } else if (error?.message) {
            errorMessage = error.message;
          }

          this.logger.error(
            `Đổi vỏ order creation failed for order ${docCode}: ${errorMessage}`,
          );

          await this.saveFastApiInvoice({
            docCode,
            maDvcs: orderData.branchCode || '',
            maKh: orderData.customer?.code || '',
            tenKh: orderData.customer?.name || '',
            ngayCt: orderData.docDate
              ? new Date(orderData.docDate)
              : new Date(),
            status: 0,
            message: errorMessage,
            guid: null,
            fastApiResponse: error?.response?.data
              ? JSON.stringify(error.response.data)
              : undefined,
          });

          return {
            success: false,
            message: errorMessage,
            result: error?.response?.data || error,
          };
        }
      }

      // Nếu không phải các loại đơn đặc biệt, chạy flow bình thường (01.Thường)
      // Validation đã được thực hiện ở trên, nên ở đây chỉ cần xử lý flow bình thường

      // Build invoice data
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Gọi API tạo đơn hàng
      let result: any;
      try {
        result = await this.fastApiInvoiceFlowService.executeFullInvoiceFlow({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });
      } catch (error: any) {
        // Lấy thông báo lỗi chính xác từ Fast API response
        let errorMessage = 'Tạo hóa đơn thất bại';

        if (error?.response?.data) {
          // Fast API trả về lỗi trong response.data
          const errorData = error.response.data;
          if (Array.isArray(errorData) && errorData.length > 0) {
            errorMessage =
              errorData[0].message || errorData[0].error || errorMessage;
          } else if (errorData.message) {
            errorMessage = errorData.message;
          } else if (errorData.error) {
            errorMessage = errorData.error;
          } else if (typeof errorData === 'string') {
            errorMessage = errorData;
          }
        } else if (error?.message) {
          errorMessage = error.message;
        }

        // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
        await this.saveFastApiInvoice({
          docCode,
          maDvcs: invoiceData.ma_dvcs,
          maKh: invoiceData.ma_kh,
          tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
          ngayCt: invoiceData.ngay_ct
            ? new Date(invoiceData.ngay_ct)
            : new Date(),
          status: 0,
          message: errorMessage,
          guid: null,
          fastApiResponse: JSON.stringify(error?.response?.data || error),
        });

        this.logger.error(
          `Invoice creation failed for order ${docCode}: ${errorMessage}`,
        );

        return {
          success: false,
          message: errorMessage,
          result: error?.response?.data || error,
        };
      }

      // FIX: Check response từ Fast API
      // Response thành công: [{ status: 1, message: "OK", guid: [...] }]
      // Response lỗi: [] hoặc [{ status: 0, message: "..." }]
      let isSuccess = false;
      let responseStatus = 0;
      let responseMessage = 'Tạo hóa đơn thất bại';
      let responseGuid: string | null = null;

      if (Array.isArray(result)) {
        if (result.length === 0) {
          // Mảng rỗng = thất bại
          isSuccess = false;
          responseStatus = 0;
          responseMessage = 'Fast API trả về mảng rỗng - tạo hóa đơn thất bại';
        } else {
          // Kiểm tra phần tử đầu tiên
          const firstItem = result[0];
          if (firstItem.status === 1) {
            // status === 1 = thành công
            isSuccess = true;
            responseStatus = 1;
            const apiMessage = firstItem.message || '';
            const shouldUseApiMessage =
              apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
            responseMessage = shouldUseApiMessage
              ? `Tạo hóa đơn thành công cho đơn hàng ${docCode}. ${apiMessage}`
              : `Tạo hóa đơn thành công cho đơn hàng ${docCode}`;
            responseGuid = Array.isArray(firstItem.guid)
              ? firstItem.guid[0]
              : firstItem.guid || null;
          } else {
            // status === 0 hoặc khác = lỗi
            isSuccess = false;
            responseStatus = firstItem.status ?? 0;
            const apiMessage = firstItem.message || firstItem.error || '';
            const shouldUseApiMessage =
              apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
            responseMessage = shouldUseApiMessage
              ? `Tạo hóa đơn thất bại cho đơn hàng ${docCode}. ${apiMessage}`
              : `Tạo hóa đơn thất bại cho đơn hàng ${docCode}`;
          }
        }
      } else if (result && typeof result === 'object') {
        // Nếu result không phải mảng
        if (result.status === 1) {
          isSuccess = true;
          responseStatus = 1;
          const apiMessage = result.message || '';
          const shouldUseApiMessage =
            apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
          responseMessage = shouldUseApiMessage
            ? `Tạo hóa đơn thành công cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hóa đơn thành công cho đơn hàng ${docCode}`;
          responseGuid = result.guid || null;
        } else {
          isSuccess = false;
          responseStatus = result.status ?? 0;
          const apiMessage = result.message || result.error || '';
          const shouldUseApiMessage =
            apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
          responseMessage = shouldUseApiMessage
            ? `Tạo hóa đơn thất bại cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hóa đơn thất bại cho đơn hàng ${docCode}`;
        }
      } else {
        // Fallback: không có result hoặc result không hợp lệ
        isSuccess = false;
        responseStatus = 0;
        responseMessage = 'Fast API không trả về response hợp lệ';
      }

      // Lưu vào bảng kê hóa đơn (cả thành công và thất bại)
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: responseGuid || null,
        fastApiResponse: JSON.stringify(result),
      });

      // Xử lý cashio payment (cho đơn hàng "01. Thường", "07. Bán tài khoản" và khi tạo invoice thành công)
      let cashioResult: any = null;
      if (isSuccess) {
        const firstSale =
          orderData.sales && orderData.sales.length > 0
            ? orderData.sales[0]
            : null;
        const ordertypeName =
          firstSale?.ordertypeName || firstSale?.ordertype || '';
        const normalizedOrderType = String(ordertypeName).trim();
        const isNormalOrder =
          normalizedOrderType === '01.Thường' ||
          normalizedOrderType === '01. Thường';
        const isBanTaiKhoanOrder =
          normalizedOrderType.includes('07. Bán tài khoản') ||
          normalizedOrderType.includes('07.Bán tài khoản');

        if (isNormalOrder || isBanTaiKhoanOrder) {
          const orderTypeLabel = isNormalOrder
            ? '01. Thường'
            : '07. Bán tài khoản';
          this.logger.log(
            `[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (${orderTypeLabel})`,
          );
          cashioResult =
            await this.fastApiInvoiceFlowService.processCashioPayment(
              docCode,
              orderData,
              invoiceData,
            );

          if (
            cashioResult.cashReceiptResults &&
            cashioResult.cashReceiptResults.length > 0
          ) {
            this.logger.log(
              `[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (${orderTypeLabel})`,
            );
          }
          if (
            cashioResult.creditAdviceResults &&
            cashioResult.creditAdviceResults.length > 0
          ) {
            this.logger.log(
              `[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (${orderTypeLabel})`,
            );
          }

          // Xử lý payment (Phiếu chi tiền mặt) nếu có mã kho
          // CHỈ xử lý cho đơn có docSourceType = ORDER_RETURN hoặc SALE_RETURN
          try {
            // Kiểm tra docSourceType - chỉ xử lý payment cho ORDER_RETURN hoặc SALE_RETURN
            const docSourceTypeRaw =
              orderData.docSourceType || firstSale?.docSourceType || '';
            const docSourceType = docSourceTypeRaw
              ? String(docSourceTypeRaw).trim().toUpperCase()
              : '';

            if (
              docSourceType === 'ORDER_RETURN' ||
              docSourceType === 'SALE_RETURN'
            ) {
              const docCodesForStockTransfer =
                StockTransferUtils.getDocCodesForStockTransfer([docCode]);
              const stockTransfers = await this.stockTransferRepository.find({
                where: { soCode: In(docCodesForStockTransfer) },
              });
              const stockCodes = Array.from(
                new Set(
                  stockTransfers.map((st) => st.stockCode).filter(Boolean),
                ),
              );

              if (stockCodes.length > 0) {
                this.logger.log(
                  `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (${orderTypeLabel}, docSourceType: ${docSourceType}) với ${stockCodes.length} mã kho`,
                );
                const paymentResult =
                  await this.fastApiInvoiceFlowService.processPayment(
                    docCode,
                    orderData,
                    invoiceData,
                    stockCodes,
                  );

                if (
                  paymentResult.paymentResults &&
                  paymentResult.paymentResults.length > 0
                ) {
                  this.logger.log(
                    `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (${orderTypeLabel})`,
                  );
                }
                if (
                  paymentResult.debitAdviceResults &&
                  paymentResult.debitAdviceResults.length > 0
                ) {
                  this.logger.log(
                    `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (${orderTypeLabel})`,
                  );
                }
              } else {
                this.logger.debug(
                  `[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`,
                );
              }
            } else {
              this.logger.debug(
                `[Payment] Đơn hàng ${docCode} (${orderTypeLabel}) có docSourceType = "${docSourceType}", không phải ORDER_RETURN/SALE_RETURN, bỏ qua payment API`,
              );
            }
          } catch (paymentError: any) {
            // Log lỗi nhưng không fail toàn bộ flow
            this.logger.warn(
              `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${paymentError?.message || paymentError}`,
            );
          }
        }
      }

      if (!isSuccess) {
        // Có lỗi từ Fast API
        this.logger.error(
          `Invoice creation failed for order ${docCode}: ${responseMessage}`,
        );

        // Kiểm tra nếu là lỗi duplicate key - có thể đơn hàng đã tồn tại trong Fast API
        const isDuplicateError =
          responseMessage &&
          (responseMessage.toLowerCase().includes('duplicate') ||
            responseMessage.toLowerCase().includes('primary key constraint') ||
            responseMessage.toLowerCase().includes('pk_d81'));

        if (isDuplicateError) {
          // Cập nhật status thành 1 (thành công) vì có thể đã tồn tại trong Fast API
          await this.saveFastApiInvoice({
            docCode,
            maDvcs: invoiceData.ma_dvcs,
            maKh: invoiceData.ma_kh,
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: invoiceData.ngay_ct
              ? new Date(invoiceData.ngay_ct)
              : new Date(),
            status: 1, // Coi như thành công vì đã tồn tại
            message: `Đơn hàng đã tồn tại trong Fast API: ${responseMessage}`,
            guid: responseGuid || null,
            fastApiResponse: JSON.stringify(result),
          });

          return {
            success: true,
            message: `Đơn hàng ${docCode} đã tồn tại trong Fast API (có thể đã được tạo trước đó)`,
            result,
            alreadyExists: true,
          };
        }

        return {
          success: false,
          message: responseMessage,
          result,
        };
      }

      // Đánh dấu đơn hàng là đã xử lý
      const markOrderAsProcessedResult =
        await this.markOrderAsProcessed(docCode);
      console.log('markOrderAsProcessedResult', markOrderAsProcessedResult);
      return {
        success: true,
        message: `Tạo hóa đơn ${docCode} thành công`,
        result,
      };
    } catch (error: any) {
      this.logger.error(
        `Error creating invoice for order ${docCode}: ${error?.message || error}`,
      );
      this.logger.error(`Error stack: ${error?.stack || 'No stack trace'}`);

      throw error;
    }
  }

  /**
   * Xử lý flow tạo hóa đơn cho đơn hàng dịch vụ (02. Làm dịch vụ)
   * Flow:
   * 1. Customer (tạo/cập nhật)
   * 2. SalesOrder (tất cả dòng: I, S, V...)
   * 3. SalesInvoice (chỉ dòng productType = 'S')
   * 4. GxtInvoice (S → detail, I → ndetail)
   */
  private async executeServiceOrderFlow(
    orderData: any,
    docCode: string,
  ): Promise<any> {
    try {
      this.logger.log(
        `[ServiceOrderFlow] Bắt đầu xử lý đơn dịch vụ ${docCode}`,
      );

      const sales = orderData.sales || [];
      if (sales.length === 0) {
        throw new Error(`Đơn hàng ${docCode} không có sale item nào`);
      }

      // Step 1: Tạo/cập nhật Customer
      if (orderData.customer?.code) {
        await this.fastApiInvoiceFlowService.createOrUpdateCustomer({
          ma_kh: SalesUtils.normalizeMaKh(orderData.customer.code),
          ten_kh: orderData.customer.name || '',
          dia_chi: orderData.customer.address || undefined,
          dien_thoai:
            orderData.customer.mobile || orderData.customer.phone || undefined,
          so_cccd: orderData.customer.idnumber || undefined,
          ngay_sinh: orderData.customer?.birthday
            ? ConvertUtils.formatDateYYYYMMDD(orderData.customer.birthday)
            : undefined,
          gioi_tinh: orderData.customer.sexual || undefined,
        });
      }

      // Build invoice data cho tất cả sales (dùng để tạo SalesOrder)
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Step 2: Tạo SalesOrder cho TẤT CẢ dòng (I, S, V...)
      this.logger.log(
        `[ServiceOrderFlow] Tạo SalesOrder cho ${sales.length} dòng`,
      );
      await this.fastApiInvoiceFlowService.createSalesOrder({
        ...invoiceData,
        customer: orderData.customer,
        ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
      });

      // Step 3: Tạo SalesInvoice CHỈ cho productType = 'S'
      const serviceLines = sales.filter((s: any) => {
        const productType = s.producttype.toUpperCase().trim();
        return productType === 'S';
      });

      let salesInvoiceResult: any = null;
      if (serviceLines.length > 0) {
        this.logger.log(
          `[ServiceOrderFlow] Tạo SalesInvoice cho ${serviceLines.length} dòng dịch vụ (productType = 'S')`,
        );

        // Build invoice data chỉ cho service lines
        const serviceInvoiceData =
          await this.buildFastApiInvoiceDataForServiceLines(
            orderData,
            serviceLines,
          );

        salesInvoiceResult =
          await this.fastApiInvoiceFlowService.createSalesInvoice({
            ...serviceInvoiceData,
            customer: orderData.customer,
            ten_kh: orderData.customer?.name || serviceInvoiceData.ong_ba || '',
          });
      } else {
        this.logger.log(
          `[ServiceOrderFlow] Không có dòng dịch vụ (productType = 'S'), bỏ qua SalesInvoice`,
        );
      }

      // Step 3.5: Xử lý cashio payment (Phiếu thu tiền mặt/Giấy báo có) nếu salesInvoice thành công
      let cashioResult: any = null;
      if (salesInvoiceResult) {
        this.logger.log(
          `[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (02. Làm dịch vụ)`,
        );
        cashioResult =
          await this.fastApiInvoiceFlowService.processCashioPayment(
            docCode,
            orderData,
            invoiceData,
          );

        if (
          cashioResult.cashReceiptResults &&
          cashioResult.cashReceiptResults.length > 0
        ) {
          this.logger.log(
            `[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`,
          );
        }
        if (
          cashioResult.creditAdviceResults &&
          cashioResult.creditAdviceResults.length > 0
        ) {
          this.logger.log(
            `[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`,
          );
        }

        // Xử lý payment (Phiếu chi tiền mặt) nếu có mã kho
        // CHỈ xử lý cho đơn có docSourceType = ORDER_RETURN hoặc SALE_RETURN
        try {
          // Kiểm tra docSourceType - chỉ xử lý payment cho ORDER_RETURN hoặc SALE_RETURN
          const firstSale =
            orderData.sales && orderData.sales.length > 0
              ? orderData.sales[0]
              : null;
          const docSourceTypeRaw =
            firstSale?.docSourceType || orderData.docSourceType || '';
          const docSourceType = docSourceTypeRaw
            ? String(docSourceTypeRaw).trim().toUpperCase()
            : '';

          if (
            docSourceType === 'ORDER_RETURN' ||
            docSourceType === 'SALE_RETURN'
          ) {
            const docCodesForStockTransfer =
              StockTransferUtils.getDocCodesForStockTransfer([docCode]);
            const stockTransfers = await this.stockTransferRepository.find({
              where: { soCode: In(docCodesForStockTransfer) },
            });
            const stockCodes = Array.from(
              new Set(stockTransfers.map((st) => st.stockCode).filter(Boolean)),
            );

            if (stockCodes.length > 0) {
              this.logger.log(
                `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (02. Làm dịch vụ, docSourceType: ${docSourceType}) với ${stockCodes.length} mã kho`,
              );
              const paymentResult =
                await this.fastApiInvoiceFlowService.processPayment(
                  docCode,
                  orderData,
                  invoiceData,
                  stockCodes,
                );

              if (
                paymentResult.paymentResults &&
                paymentResult.paymentResults.length > 0
              ) {
                this.logger.log(
                  `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`,
                );
              }
              if (
                paymentResult.debitAdviceResults &&
                paymentResult.debitAdviceResults.length > 0
              ) {
                this.logger.log(
                  `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (02. Làm dịch vụ)`,
                );
              }
            } else {
              this.logger.debug(
                `[Payment] Đơn hàng ${docCode} không có mã kho, bỏ qua payment API`,
              );
            }
          } else {
            this.logger.debug(
              `[Payment] Đơn hàng ${docCode} (02. Làm dịch vụ) có docSourceType = "${docSourceType}", không phải ORDER_RETURN/SALE_RETURN, bỏ qua payment API`,
            );
          }
        } catch (paymentError: any) {
          // Log lỗi nhưng không fail toàn bộ flow
          this.logger.warn(
            `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode}: ${paymentError?.message || paymentError}`,
          );
        }
      }

      // Step 4: Tạo GxtInvoice (S → detail, I → ndetail)
      const importLines = sales.filter((s: any) => {
        const productType = (s.producttype || s.productType || '')
          .toUpperCase()
          .trim();
        return productType === 'S';
      });

      const exportLines = sales.filter((s: any) => {
        const productType = (s.producttype || s.productType || '')
          .toUpperCase()
          .trim();
        return productType === 'I';
      });

      // Log để đảm bảo tất cả dòng đều được xử lý
      this.logger.log(
        `[ServiceOrderFlow] Tổng số dòng: ${sales.length}, ` +
          `Dòng S (nhập): ${importLines.length}, ` +
          `Dòng I (xuất): ${exportLines.length}, ` +
          `Dòng khác: ${sales.length - importLines.length - exportLines.length}`,
      );

      let gxtInvoiceResult: any = null;
      if (importLines.length > 0 || exportLines.length > 0) {
        this.logger.log(
          `[ServiceOrderFlow] Tạo GxtInvoice: ${exportLines.length} dòng xuất (I) → detail, ${importLines.length} dòng nhập (S) → ndetail`,
        );

        const gxtInvoiceData = await this.buildGxtInvoiceData(
          orderData,
          importLines,
          exportLines,
        );

        gxtInvoiceResult =
          await this.fastApiInvoiceFlowService.createGxtInvoice(gxtInvoiceData);
      } else {
        this.logger.log(
          `[ServiceOrderFlow] Không có dòng S hoặc I, bỏ qua GxtInvoice`,
        );
      }

      // Lưu vào bảng kê hóa đơn
      const responseStatus = salesInvoiceResult ? 1 : 0;
      const responseMessage = salesInvoiceResult
        ? `Tạo hóa đơn dịch vụ ${docCode} thành công`
        : `Tạo SalesOrder thành công, không có dòng dịch vụ để tạo SalesInvoice`;

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: null,
        fastApiResponse: JSON.stringify({
          salesOrder: 'success',
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
        }),
      });

      // Đánh dấu đơn hàng là đã xử lý
      await this.markOrderAsProcessed(docCode);

      return {
        success: true,
        message: responseMessage,
        result: {
          salesOrder: 'success',
          salesInvoice: salesInvoiceResult,
          gxtInvoice: gxtInvoiceResult,
          cashio: cashioResult,
        },
      };
    } catch (error: any) {
      this.logger.error(
        `[ServiceOrderFlow] Lỗi khi xử lý đơn dịch vụ ${docCode}: ${error?.message || error}`,
      );

      // Lưu lỗi vào bảng kê hóa đơn
      const invoiceData = await this.buildFastApiInvoiceData(orderData).catch(
        () => ({
          ma_dvcs: orderData.branchCode || '',
          ma_kh: SalesUtils.normalizeMaKh(orderData.customer?.code),
          ong_ba: orderData.customer?.name || '',
          ngay_ct: orderData.docDate ? new Date(orderData.docDate) : new Date(),
        }),
      );

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs || orderData.branchCode || '',
        maKh:
          invoiceData.ma_kh ||
          SalesUtils.normalizeMaKh(orderData.customer?.code),
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: 0,
        message: error?.message || 'Tạo hóa đơn dịch vụ thất bại',
        guid: null,
        fastApiResponse: JSON.stringify(error?.response?.data || error),
      });

      throw error;
    }
  }

  /**
   * Build invoice data chỉ cho service lines (productType = 'S')
   */
  private async buildFastApiInvoiceDataForServiceLines(
    orderData: any,
    serviceLines: any[],
  ): Promise<any> {
    // Tạo orderData mới chỉ chứa service lines
    const serviceOrderData = {
      ...orderData,
      sales: serviceLines,
    };

    // Dùng lại logic buildFastApiInvoiceData nhưng với orderData đã filter
    return await this.buildFastApiInvoiceData(serviceOrderData);
  }

  /**
   * Build GxtInvoice data (Phiếu tạo gộp – xuất tách)
   * - detail: các dòng productType = 'I' (xuất)
   * - ndetail: các dòng productType = 'S' (nhập)
   */
  private async buildGxtInvoiceData(
    orderData: any,
    importLines: any[],
    exportLines: any[],
  ): Promise<any> {
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      if (isNaN(d.getTime())) {
        throw new Error('Invalid date');
      }
      return d.toISOString();
    };

    let docDate: Date;
    if (orderData.docDate instanceof Date) {
      docDate = orderData.docDate;
    } else if (typeof orderData.docDate === 'string') {
      docDate = new Date(orderData.docDate);
      if (isNaN(docDate.getTime())) {
        docDate = new Date();
      }
    } else {
      docDate = new Date();
    }

    const ngayCt = formatDateISO(docDate);
    const ngayLct = formatDateISO(docDate);

    const firstSale = orderData.sales?.[0] || {};
    const maDvcs =
      firstSale?.department?.ma_dvcs ||
      firstSale?.department?.ma_dvcs_ht ||
      orderData.customer?.brand ||
      orderData.branchCode ||
      '';

    // Helper để build detail/ndetail item
    const buildLineItem = async (sale: any, index: number): Promise<any> => {
      const toNumber = (value: any, defaultValue: number = 0): number => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        const num = Number(value);
        return isNaN(num) ? defaultValue : num;
      };

      const toString = (value: any, defaultValue: string = ''): string => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        return String(value);
      };

      const limitString = (value: string, maxLength: number): string => {
        if (!value) return '';
        const str = String(value);
        return str.length > maxLength ? str.substring(0, maxLength) : str;
      };

      const qty = toNumber(sale.qty, 0);
      const giaBan = toNumber(sale.giaBan, 0);
      const tienHang = toNumber(
        sale.tienHang || sale.linetotal || sale.revenue,
        0,
      );
      const giaNt2 = giaBan > 0 ? giaBan : qty > 0 ? tienHang / qty : 0;
      const tienNt2 = qty * giaNt2;

      // Lấy materialCode từ Loyalty API
      const materialCode =
        SalesUtils.getMaterialCode(sale, sale.product) || sale.itemCode || '';
      const dvt = toString(
        sale.product?.dvt || sale.product?.unit || sale.dvt,
        'Cái',
      );
      const maLo = toString(sale.maLo || sale.ma_lo, '');
      const maBp = toString(
        sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
        '',
      );

      return {
        ma_kho_n: firstSale?.maKho || '',
        ma_kho_x: firstSale?.maKho || '',
        ma_vt: limitString(materialCode, 16),
        dvt: limitString(dvt, 32),
        ma_lo: limitString(maLo, 16),
        so_luong: Math.abs(qty), // Lấy giá trị tuyệt đối
        gia_nt2: Number(giaNt2),
        tien_nt2: Number(tienNt2),
        ma_nx: 'NX01', // Fix cứng theo yêu cầu
        ma_bp: limitString(maBp, 8),
        dong: index + 1, // Số thứ tự dòng tăng dần (1, 2, 3...)
        dong_vt_goc: 1, // Dòng vật tư gốc luôn là 1
      };
    };

    // Build detail (xuất - productType = 'I')
    const detail = await Promise.all(
      exportLines.map((sale, index) => buildLineItem(sale, index)),
    );

    // Build ndetail (nhập - productType = 'S')
    const ndetail = await Promise.all(
      importLines.map((sale, index) => buildLineItem(sale, index)),
    );

    // Lấy kho nhập và kho xuất (có thể cần map từ branch/department)
    // Tạm thời dùng branchCode làm kho mặc định
    const maKhoN = firstSale?.maKho || '';
    const maKhoX = firstSale?.maKho || '';

    return {
      ma_dvcs: maDvcs,
      ma_kho_n: maKhoN,
      ma_kho_x: maKhoX,
      ong_ba: orderData.customer?.name || '',
      ma_gd: '2', // 1 = Tạo gộp, 2 = Xuất tách (có thể thay đổi theo rule)
      ngay_ct: ngayCt,
      ngay_lct: ngayLct,
      so_ct: orderData.docCode || '',
      dien_giai: orderData.docCode || '',
      action: 0, // 0: Mới, Sửa; 1: Xóa
      detail: detail,
      ndetail: ndetail,
    };
  }

  /**
   * Build invoice data cho Fast API (format mới)
   */
  private async buildFastApiInvoiceData(orderData: any): Promise<any> {
    try {
      // 1. Initialize and validate date
      const docDate = this.parseInvoiceDate(orderData.docDate);
      const ngayCt = this.formatDateKeepLocalDay(docDate);
      const ngayLct = ngayCt;

      const allSales = orderData.sales || [];
      if (allSales.length === 0) {
        throw new Error(
          `Đơn hàng ${orderData.docCode} không có sale item nào, bỏ qua không đồng bộ`,
        );
      }

      // 2. Determine order type (from first sale)
      const { isThuong: isNormalOrder } = InvoiceLogicUtils.getOrderTypes(
        allSales[0]?.ordertypeName || allSales[0]?.ordertype || '',
      );

      // 3. Load supporting data
      const { stockTransferMap, transDate } =
        await this.getInvoiceStockTransferMap(orderData.docCode, isNormalOrder);
      const cardSerialMap = await this.getInvoiceCardSerialMap(
        orderData.docCode,
      );

      // 4. Transform sales to details
      const detail = await Promise.all(
        allSales.map((sale: any, index: number) =>
          this.mapSaleToInvoiceDetail(sale, index, orderData, {
            isNormalOrder,
            stockTransferMap,
            cardSerialMap,
          }),
        ),
      );

      // 5. Build summary (cbdetail)
      const cbdetail = this.buildInvoiceCbDetail(detail);

      // 6. Assemble final payload
      return this.assembleInvoicePayload(orderData, detail, cbdetail, {
        ngayCt,
        ngayLct,
        transDate,
        maBp: detail[0]?.ma_bp || '',
      });
    } catch (error: any) {
      this.logInvoiceError(error, orderData);
      throw new Error(
        `Failed to build invoice data: ${error?.message || error}`,
      );
    }
  }

  /**
   * Build salesReturn data cho Fast API (Hàng bán trả lại)
   * Tương tự như buildFastApiInvoiceData nhưng có thêm các field đặc biệt cho salesReturn
   */
  private async buildSalesReturnData(
    orderData: any,
    stockTransfers: StockTransfer[],
  ): Promise<any> {
    try {
      // Sử dụng lại logic từ buildFastApiInvoiceData để build detail
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Format ngày theo ISO 8601
      const formatDateISO = (date: Date | string): string => {
        const d = typeof date === 'string' ? new Date(date) : date;
        if (isNaN(d.getTime())) {
          throw new Error('Invalid date');
        }
        return d.toISOString();
      };

      // Lấy ngày hóa đơn gốc (ngay_ct0) - có thể lấy từ sale đầu tiên hoặc orderData
      const firstSale = orderData.sales?.[0] || {};
      let ngayCt0: string | null = null;
      let soCt0: string | null = null;

      // Tìm hóa đơn gốc từ stock transfer hoặc sale
      // Nếu có stock transfer, có thể lấy từ soCode hoặc docCode
      if (stockTransfers && stockTransfers.length > 0) {
        const firstStockTransfer = stockTransfers.find(
          (stockTransfer) => stockTransfer.doctype === 'SALE_RETURN',
        );
        // soCode thường là mã đơn hàng gốc
        soCt0 = firstStockTransfer?.soCode || orderData.docCode || null;
        // Ngày có thể lấy từ stock transfer hoặc orderData
        if (firstStockTransfer?.transDate) {
          ngayCt0 = formatDateISO(firstStockTransfer?.transDate);
        } else if (orderData?.docDate) {
          ngayCt0 = formatDateISO(orderData?.docDate);
        }
      } else {
        // Nếu không có stock transfer, lấy từ orderData
        soCt0 = orderData.docCode || null;
        if (orderData?.docDate) {
          ngayCt0 = formatDateISO(orderData.docDate);
        }
      }

      // Format ngày hiện tại
      let docDate: Date;
      if (orderData.docDate instanceof Date) {
        docDate = orderData.docDate;
      } else if (typeof orderData.docDate === 'string') {
        docDate = new Date(orderData.docDate);
        if (isNaN(docDate.getTime())) {
          docDate = new Date();
        }
      } else {
        docDate = new Date();
      }

      const ngayCt = formatDateISO(docDate);
      const ngayLct = formatDateISO(docDate);

      // Lấy ma_dvcs
      const maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';

      // Lấy so_seri
      const soSeri =
        firstSale?.kyHieu ||
        firstSale?.branchCode ||
        orderData.branchCode ||
        'DEFAULT';

      // Gom số lượng trả lại theo mã vật tư
      const stockQtyMap = new Map<string, number>();

      (stockTransfers || [])
        .filter((st) => st.doctype === 'SALE_RETURN')
        .forEach((st) => {
          const maVt = st.materialCode;
          const qty = Number(st.qty || 0);

          if (!maVt || qty === 0) return;

          stockQtyMap.set(maVt, (stockQtyMap.get(maVt) || 0) + qty);
        });

      // Build detail từ invoiceData.detail, chỉ giữ các field cần thiết cho salesReturn
      const detail = (invoiceData.detail || [])
        .map((item: any, index: number) => {
          const soLuongFromStock = stockQtyMap.get(item.ma_vt) || 0;

          const detailItem: any = {
            // Field bắt buộc
            ma_vt: item.ma_vt,
            dvt: item.dvt,
            ma_kho: item.ma_kho,

            so_luong: soLuongFromStock,

            gia_ban: item.gia_ban,
            tien_hang: item.gia_ban * soLuongFromStock,

            // Field tài khoản
            tk_dt: item.tk_dt || '511',
            tk_gv: item.tk_gv || '632',

            // Field khuyến mãi
            is_reward_line: item.is_reward_line || 0,
            is_bundle_reward_line: item.is_bundle_reward_line || 0,
            km_yn: item.km_yn || 0,

            // CK
            ck01_nt: item.ck01_nt || 0,
            ck02_nt: item.ck02_nt || 0,
            ck03_nt: item.ck03_nt || 0,
            ck04_nt: item.ck04_nt || 0,
            ck05_nt: item.ck05_nt || 0,
            ck06_nt: item.ck06_nt || 0,
            ck07_nt: item.ck07_nt || 0,
            ck08_nt: item.ck08_nt || 0,
            ck09_nt: item.ck09_nt || 0,
            ck10_nt: item.ck10_nt || 0,
            ck11_nt: item.ck11_nt || 0,
            ck12_nt: item.ck12_nt || 0,
            ck13_nt: item.ck13_nt || 0,
            ck14_nt: item.ck14_nt || 0,
            ck15_nt: item.ck15_nt || 0,
            ck16_nt: item.ck16_nt || 0,
            ck17_nt: item.ck17_nt || 0,
            ck18_nt: item.ck18_nt || 0,
            ck19_nt: item.ck19_nt || 0,
            ck20_nt: item.ck20_nt || 0,
            ck21_nt: item.ck21_nt || 0,
            ck22_nt: item.ck22_nt || 0,

            // Thuế
            dt_tg_nt: item.dt_tg_nt || 0,
            ma_thue: item.ma_thue || '00',
            thue_suat: item.thue_suat || 0,
            tien_thue: item.tien_thue || 0,

            ma_bp: item.ma_bp,
            loai_gd: item.loai_gd || '01',
            dong: index + 1,
            id_goc_so: item.id_goc_so || 0,
            id_goc_ngay: item.id_goc_ngay || formatDateISO(new Date()),
          };

          return detailItem;
        })
        .filter(Boolean); // ❗ bỏ các dòng không có stock transfer

      // Build payload, chỉ thêm các field không null
      const salesReturnPayload: any = {
        ma_dvcs: maDvcs,
        ma_kh: invoiceData.ma_kh,
        ong_ba: invoiceData.ong_ba,
        ma_gd: '1', // Mã giao dịch (mặc định 1 - Hàng bán trả lại)
        tk_co: '131', // Tài khoản có (mặc định 131)
        ngay_lct: ngayLct,
        ngay_ct: ngayCt,
        so_ct: orderData.docCode || '',
        so_seri: soSeri,
        ma_nt: 'VND',
        ty_gia: 1.0,
        ma_kenh: 'ONLINE', // Mã kênh (mặc định ONLINE)
        detail: detail,
      };

      // Chỉ thêm các field optional nếu có giá trị
      if (firstSale?.maCa) {
        salesReturnPayload.ma_ca = firstSale.maCa;
      }
      if (soCt0) {
        salesReturnPayload.so_ct0 = soCt0;
      }
      if (ngayCt0) {
        salesReturnPayload.ngay_ct0 = ngayCt0;
      }
      if (orderData.docCode) {
        salesReturnPayload.dien_giai = orderData.docCode;
      }

      return salesReturnPayload;
    } catch (error: any) {
      this.logger.error(
        `Error building sales return data: ${error?.message || error}`,
      );
      throw new Error(
        `Failed to build sales return data: ${error?.message || error}`,
      );
    }
  }

  /**
   * Tạo stock transfer từ STOCK_TRANSFER data
   */
  async createStockTransfer(createDto: CreateStockTransferDto): Promise<any> {
    try {
      // Group theo doccode để xử lý từng phiếu
      const transferMap = new Map<string, StockTransferItem[]>();

      for (const item of createDto.data) {
        if (!transferMap.has(item.doccode)) {
          transferMap.set(item.doccode, []);
        }
        transferMap.get(item.doccode)!.push(item);
      }

      const results: Array<{
        doccode: string;
        success: boolean;
        result?: any;
        error?: string;
      }> = [];

      for (const [doccode, items] of transferMap.entries()) {
        try {
          // Lấy item đầu tiên để lấy thông tin chung
          const firstItem = items[0];

          // Join với order nếu có so_code
          let orderData: any = null;
          if (firstItem.so_code) {
            try {
              orderData = await this.findByOrderCode(firstItem.so_code);
            } catch (error) {}
          }

          // Build FastAPI stock transfer data
          const stockTransferData = await this.buildStockTransferData(
            items,
            orderData,
          );

          // Submit to FastAPI
          const result =
            await this.fastApiService.submitStockTransfer(stockTransferData);

          results.push({
            doccode,
            success: true,
            result,
          });
        } catch (error: any) {
          this.logger.error(
            `Error creating stock transfer for ${doccode}: ${error?.message || error}`,
          );
          results.push({
            doccode,
            success: false,
            error: error?.message || 'Unknown error',
          });
        }
      }

      return {
        success: true,
        results,
        total: results.length,
        successCount: results.filter((r) => r.success).length,
        failedCount: results.filter((r) => !r.success).length,
      };
    } catch (error: any) {
      this.logger.error(
        `Error creating stock transfers: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Xử lý đơn hàng có đuôi _X (ví dụ: SO45.01574458_X)
   * Gọi API salesOrder với action: 1
   * Cả đơn có _X và đơn gốc (bỏ _X) đều sẽ có action = 1
   */
  private async handleSaleOrderWithUnderscoreX(
    orderData: any,
    docCode: string,
    action: number,
  ): Promise<any> {
    // Đơn có đuôi _X → Gọi API salesOrder với action: 1
    const invoiceData = await this.buildFastApiInvoiceData(orderData);
    function removeSuffixX(code: string): string {
      return code.endsWith('_X') ? code.slice(0, -2) : code;
    }
    const docCodeWithoutX = removeSuffixX(docCode);

    // Gọi API salesOrder với action = 1 (không cần tạo/cập nhật customer)
    let result: any;
    let data = {
      ...invoiceData,
      dien_giai: docCodeWithoutX,
      so_ct: docCodeWithoutX,
      ma_kho: orderData?.maKho || '',
    };
    try {
      result = await this.fastApiInvoiceFlowService.createSalesOrder(
        {
          ...data,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        },
        action,
      ); // action = 1 cho đơn hàng có đuôi _X

      // Lưu vào bảng kê hóa đơn
      const responseStatus =
        Array.isArray(result) && result.length > 0 && result[0].status === 1
          ? 1
          : 0;
      const apiMessage =
        Array.isArray(result) && result.length > 0
          ? result[0].message || ''
          : '';
      const shouldUseApiMessage =
        apiMessage && apiMessage.trim().toUpperCase() !== 'OK';
      let responseMessage = '';
      if (responseStatus === 1) {
        responseMessage = shouldUseApiMessage
          ? `Tạo đơn hàng thành công cho đơn hàng ${docCode}. ${apiMessage}`
          : `Tạo đơn hàng thành công cho đơn hàng ${docCode}`;
      } else {
        responseMessage = shouldUseApiMessage
          ? `Tạo đơn hàng thất bại cho đơn hàng ${docCode}. ${apiMessage}`
          : `Tạo đơn hàng thất bại cho đơn hàng ${docCode}`;
      }
      const responseGuid =
        Array.isArray(result) &&
        result.length > 0 &&
        Array.isArray(result[0].guid)
          ? result[0].guid[0]
          : Array.isArray(result) && result.length > 0
            ? result[0].guid
            : null;

      // Xử lý cashio payment (Phiếu thu tiền mặt/Giấy báo có) nếu salesOrder thành công
      let cashioResult: any = null;
      let paymentResult: any = null;
      if (responseStatus === 1) {
        this.logger.log(
          `[Cashio] Bắt đầu xử lý cashio payment cho đơn hàng ${docCode} (đơn có đuôi _X)`,
        );
        cashioResult =
          await this.fastApiInvoiceFlowService.processCashioPayment(
            docCode,
            orderData,
            invoiceData,
          );

        if (
          cashioResult.cashReceiptResults &&
          cashioResult.cashReceiptResults.length > 0
        ) {
          this.logger.log(
            `[Cashio] Đã tạo ${cashioResult.cashReceiptResults.length} cashReceipt thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
          );
        }
        if (
          cashioResult.creditAdviceResults &&
          cashioResult.creditAdviceResults.length > 0
        ) {
          this.logger.log(
            `[Cashio] Đã tạo ${cashioResult.creditAdviceResults.length} creditAdvice thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
          );
        }

        // Xử lý Payment (Phiếu chi tiền mặt/Giấy báo nợ) cho đơn hủy (_X) - cho phép không có mã kho
        try {
          // Kiểm tra có stock transfer không
          const docCodesForStockTransfer =
            StockTransferUtils.getDocCodesForStockTransfer([docCode]);
          const stockTransfers = await this.stockTransferRepository.find({
            where: { soCode: In(docCodesForStockTransfer) },
          });
          const stockCodes = Array.from(
            new Set(stockTransfers.map((st) => st.stockCode).filter(Boolean)),
          );

          // Cho đơn _X: Gọi payment ngay cả khi không có mã kho (đơn hủy không có khái niệm xuất kho)
          const allowWithoutStockCodes = stockCodes.length === 0;

          if (allowWithoutStockCodes || stockCodes.length > 0) {
            this.logger.log(
              `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (đơn có đuôi _X) - ${allowWithoutStockCodes ? 'không có mã kho' : `với ${stockCodes.length} mã kho`}`,
            );
            paymentResult = await this.fastApiInvoiceFlowService.processPayment(
              docCode,
              orderData,
              invoiceData,
              stockCodes,
              allowWithoutStockCodes, // Cho phép gọi payment ngay cả khi không có mã kho
            );

            if (
              paymentResult.paymentResults &&
              paymentResult.paymentResults.length > 0
            ) {
              this.logger.log(
                `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
              );
            }
            if (
              paymentResult.debitAdviceResults &&
              paymentResult.debitAdviceResults.length > 0
            ) {
              this.logger.log(
                `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (đơn có đuôi _X)`,
              );
            }
          }
        } catch (paymentError: any) {
          // Log lỗi nhưng không fail toàn bộ flow
          this.logger.warn(
            `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode} (đơn có đuôi _X): ${paymentError?.message || paymentError}`,
          );
        }
      }

      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: responseGuid || null,
        fastApiResponse: JSON.stringify({
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        }),
      });

      return {
        success: responseStatus === 1,
        message: responseMessage,
        result: {
          salesOrder: result,
          cashio: cashioResult,
          payment: paymentResult,
        },
      };
    } catch (error: any) {
      // Lấy thông báo lỗi chính xác từ Fast API response
      let errorMessage = 'Tạo đơn hàng thất bại';

      if (error?.response?.data) {
        const errorData = error.response.data;
        if (Array.isArray(errorData) && errorData.length > 0) {
          errorMessage =
            errorData[0].message || errorData[0].error || errorMessage;
        } else if (errorData.message) {
          errorMessage = errorData.message;
        } else if (errorData.error) {
          errorMessage = errorData.error;
        } else if (typeof errorData === 'string') {
          errorMessage = errorData;
        }
      } else if (error?.message) {
        errorMessage = error.message;
      }

      // Format error message
      const shouldUseApiMessage =
        errorMessage && errorMessage.trim().toUpperCase() !== 'OK';
      const formattedErrorMessage = shouldUseApiMessage
        ? `Tạo đơn hàng thất bại cho đơn hàng ${docCode}. ${errorMessage}`
        : `Tạo đơn hàng thất bại cho đơn hàng ${docCode}`;

      // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct
          ? new Date(invoiceData.ngay_ct)
          : new Date(),
        status: 0,
        message: formattedErrorMessage,
        guid: null,
        fastApiResponse: JSON.stringify(error?.response?.data || error),
      });

      this.logger.error(
        `SALE_ORDER with _X suffix creation failed for order ${docCode}: ${formattedErrorMessage}`,
      );

      return {
        success: false,
        message: formattedErrorMessage,
        result: error?.response?.data || error,
      };
    }
  }

  private async handleSaleReturnFlow(
    orderData: any,
    docCode: string,
  ): Promise<any> {
    // Kiểm tra xem có stock transfer không
    // Xử lý đặc biệt cho đơn trả lại: fetch cả theo mã đơn gốc (SO)
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const stockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
    });

    // Case 1: Có stock transfer → Gọi API salesReturn
    if (stockTransfers && stockTransfers.length > 0) {
      // Build salesReturn data
      const salesReturnStockTransfers = stockTransfers.filter(
        (stockTransfer) => stockTransfer.doctype === 'SALE_RETURN',
      );
      const salesReturnData = await this.buildSalesReturnData(
        orderData,
        salesReturnStockTransfers,
      );

      // Gọi API salesReturn (không cần tạo/cập nhật customer)
      let result: any;
      try {
        result =
          await this.fastApiInvoiceFlowService.createSalesReturn(
            salesReturnData,
          );

        // Lưu vào bảng kê hóa đơn
        const responseStatus =
          Array.isArray(result) && result.length > 0 && result[0].status === 1
            ? 1
            : 0;
        let responseMessage = '';
        const apiMessage =
          Array.isArray(result) && result.length > 0 ? result[0].message : '';
        const shouldAppendApiMessage =
          apiMessage && apiMessage.trim().toUpperCase() !== 'OK';

        if (responseStatus === 1) {
          responseMessage = shouldAppendApiMessage
            ? `Tạo hàng bán trả lại thành công cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hàng bán trả lại thành công cho đơn hàng ${docCode}`;
        } else {
          responseMessage = shouldAppendApiMessage
            ? `Tạo hàng bán trả lại thất bại cho đơn hàng ${docCode}. ${apiMessage}`
            : `Tạo hàng bán trả lại thất bại cho đơn hàng ${docCode}`;
        }
        const responseGuid =
          Array.isArray(result) &&
          result.length > 0 &&
          Array.isArray(result[0].guid)
            ? result[0].guid[0]
            : Array.isArray(result) && result.length > 0
              ? result[0].guid
              : null;

        // Xử lý Payment (Phiếu chi tiền mặt) nếu có mã kho
        if (responseStatus === 1) {
          try {
            const stockCodes = Array.from(
              new Set(stockTransfers.map((st) => st.stockCode).filter(Boolean)),
            );

            if (stockCodes.length > 0) {
              // Build invoiceData để dùng cho payment (tương tự như các case khác)
              const invoiceData = await this.buildFastApiInvoiceData(orderData);

              this.logger.log(
                `[Payment] Bắt đầu xử lý payment cho đơn hàng ${docCode} (SALE_RETURN) với ${stockCodes.length} mã kho`,
              );
              const paymentResult =
                await this.fastApiInvoiceFlowService.processPayment(
                  docCode,
                  orderData,
                  invoiceData,
                  stockCodes,
                );

              if (
                paymentResult.paymentResults &&
                paymentResult.paymentResults.length > 0
              ) {
                this.logger.log(
                  `[Payment] Đã tạo ${paymentResult.paymentResults.length} payment thành công cho đơn hàng ${docCode} (SALE_RETURN)`,
                );
              }
              if (
                paymentResult.debitAdviceResults &&
                paymentResult.debitAdviceResults.length > 0
              ) {
                this.logger.log(
                  `[Payment] Đã tạo ${paymentResult.debitAdviceResults.length} debitAdvice thành công cho đơn hàng ${docCode} (SALE_RETURN)`,
                );
              }
            } else {
              this.logger.debug(
                `[Payment] Đơn hàng ${docCode} (SALE_RETURN) không có mã kho, bỏ qua payment API`,
              );
            }
          } catch (paymentError: any) {
            // Log lỗi nhưng không fail toàn bộ flow
            this.logger.warn(
              `[Payment] Lỗi khi xử lý payment cho đơn hàng ${docCode} (SALE_RETURN): ${paymentError?.message || paymentError}`,
            );
          }
        }

        await this.saveFastApiInvoice({
          docCode,
          maDvcs: salesReturnData.ma_dvcs,
          maKh: salesReturnData.ma_kh,
          tenKh: orderData.customer?.name || salesReturnData.ong_ba || '',
          ngayCt: salesReturnData.ngay_ct
            ? new Date(salesReturnData.ngay_ct)
            : new Date(),
          status: responseStatus,
          message: responseMessage,
          guid: responseGuid || null,
          fastApiResponse: JSON.stringify(result),
        });

        return {
          success: responseStatus === 1,
          message: responseMessage,
          result: result,
        };
      } catch (error: any) {
        // Lấy thông báo lỗi chính xác từ Fast API response
        let errorMessage = 'Tạo hàng bán trả lại thất bại';

        if (error?.response?.data) {
          const errorData = error.response.data;
          if (Array.isArray(errorData) && errorData.length > 0) {
            errorMessage =
              errorData[0].message || errorData[0].error || errorMessage;
          } else if (errorData.message) {
            errorMessage = errorData.message;
          } else if (errorData.error) {
            errorMessage = errorData.error;
          } else if (typeof errorData === 'string') {
            errorMessage = errorData;
          }
        } else if (error?.message) {
          errorMessage = error.message;
        }

        // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
        await this.saveFastApiInvoice({
          docCode,
          maDvcs: salesReturnData.ma_dvcs,
          maKh: salesReturnData.ma_kh,
          tenKh: orderData.customer?.name || salesReturnData.ong_ba || '',
          ngayCt: salesReturnData.ngay_ct
            ? new Date(salesReturnData.ngay_ct)
            : new Date(),
          status: 0,
          message: errorMessage,
          guid: null,
          fastApiResponse: JSON.stringify(error?.response?.data || error),
        });

        this.logger.error(
          `SALE_RETURN order creation failed for order ${docCode}: ${errorMessage}`,
        );

        return {
          success: false,
          message: errorMessage,
          result: error?.response?.data || error,
        };
      }
    }

    // Case 2: Không có stock transfer → Không xử lý (bỏ qua)
    // SALE_RETURN không có stock transfer không cần xử lý
    await this.saveFastApiInvoice({
      docCode,
      maDvcs: orderData.branchCode || '',
      maKh: orderData.customer?.code || '',
      tenKh: orderData.customer?.name || '',
      ngayCt: orderData.docDate ? new Date(orderData.docDate) : new Date(),
      status: 0,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
      guid: null,
      fastApiResponse: undefined,
    });

    return {
      success: false,
      message: 'SALE_RETURN không có stock transfer - không cần xử lý',
      result: null,
    };
  }

  /**
   * Build FastAPI stock transfer data từ STOCK_TRANSFER items
   */
  private async buildStockTransferData(
    items: StockTransferItem[],
    orderData: any,
  ): Promise<any> {
    const firstItem = items[0];

    // Lấy ma_dvcs từ order hoặc branch_code
    let maDvcs = '';
    if (orderData) {
      const firstSale = orderData.sales?.[0];
      maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';
    }
    if (!maDvcs) {
      maDvcs = firstItem.branch_code || '';
    }

    // Lấy ma_kh từ order và normalize (bỏ prefix "NV" nếu có)
    const maKh = SalesUtils.normalizeMaKh(orderData?.customer?.code);

    // Map iotype sang ma_nx (mã nhập xuất)
    // iotype: 'O' = xuất, 'I' = nhập
    // ma_nx: có thể là '1111' cho xuất, '1112' cho nhập (cần xác nhận với FastAPI)
    const getMaNx = (iotype: string): string => {
      if (iotype === 'O') {
        return '1111'; // Xuất nội bộ
      } else if (iotype === 'I') {
        return '1112'; // Nhập nội bộ
      }
      return '1111'; // Default
    };

    // Build detail items
    const detail = await Promise.all(
      items.map(async (item, index) => {
        // Fetch trackSerial và trackBatch từ Loyalty API để xác định dùng ma_lo hay so_serial
        let dvt = 'Cái'; // Default
        let trackSerial: boolean | null = null;
        let trackBatch: boolean | null = null;
        let productTypeFromLoyalty: string | null = null;

        try {
          const product = await this.productItemRepository.findOne({
            where: { maERP: item.item_code },
          });
          if (product?.dvt) {
            dvt = product.dvt;
          }
          // Fetch từ Loyalty API để lấy dvt, trackSerial, trackBatch và productType
          const loyaltyProduct = await this.loyaltyService.checkProduct(
            item.item_code,
          );
          if (loyaltyProduct) {
            if (loyaltyProduct?.unit) {
              dvt = loyaltyProduct.unit;
            }
            trackSerial = loyaltyProduct.trackSerial === true;
            trackBatch = loyaltyProduct.trackBatch === true;
            productTypeFromLoyalty =
              loyaltyProduct?.productType ||
              loyaltyProduct?.producttype ||
              null;
          }
        } catch (error) {}

        const productTypeUpper = productTypeFromLoyalty
          ? String(productTypeFromLoyalty).toUpperCase().trim()
          : null;

        // Debug log để kiểm tra trackSerial và trackBatch
        if (index === 0) {
        }

        // Xác định có dùng ma_lo hay so_serial dựa trên trackSerial và trackBatch từ Loyalty API
        const useBatch = SalesUtils.shouldUseBatch(trackBatch, trackSerial);

        let maLo: string | null = null;
        let soSerial: string | null = null;

        if (useBatch) {
          // trackBatch = true → dùng ma_lo với giá trị batchserial
          const batchSerial = item.batchserial || null;
          if (batchSerial) {
            // Vẫn cần productType để quyết định cắt bao nhiêu ký tự
            if (productTypeUpper === 'TPCN') {
              // Nếu productType là "TPCN", cắt lấy 8 ký tự cuối
              maLo =
                batchSerial.length >= 8 ? batchSerial.slice(-8) : batchSerial;
            } else if (
              productTypeUpper === 'SKIN' ||
              productTypeUpper === 'GIFT'
            ) {
              // Nếu productType là "SKIN" hoặc "GIFT", cắt lấy 4 ký tự cuối
              maLo =
                batchSerial.length >= 4 ? batchSerial.slice(-4) : batchSerial;
            } else {
              // Các trường hợp khác → giữ nguyên toàn bộ
              maLo = batchSerial;
            }
          } else {
            maLo = null;
          }
          soSerial = null;
        } else {
          // trackSerial = true và trackBatch = false → dùng so_serial, không set ma_lo
          maLo = null;
          soSerial = item.batchserial || null;
        }

        return {
          ma_vt: item.item_code,
          dvt: dvt,
          so_serial: soSerial,
          ma_kho: item.stock_code,
          so_luong: Math.abs(item.qty), // Lấy giá trị tuyệt đối
          gia_nt: 0, // Stock transfer thường không có giá
          tien_nt: 0, // Stock transfer thường không có tiền
          ma_lo: maLo,
          px_gia_dd: 0, // Mặc định 0
          ma_nx: getMaNx(item.iotype),
          ma_vv: null,
          ma_bp:
            orderData?.sales?.[0]?.department?.ma_bp ||
            item.branch_code ||
            null,
          so_lsx: null,
          ma_sp: null,
          ma_hd: null,
          ma_phi: null,
          ma_ku: null,
          ma_phi_hh: null,
          ma_phi_ttlk: null,
          tien_hh_nt: 0,
          tien_ttlk_nt: 0,
        };
      }),
    );

    // Format date
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      return d.toISOString();
    };

    const transDate = new Date(firstItem.transdate);
    const ngayCt = formatDateISO(transDate);
    const ngayLct = formatDateISO(transDate);

    // Lấy ma_nx từ item đầu tiên (tất cả items trong cùng 1 phiếu nên có cùng iotype)
    const maNx = getMaNx(firstItem.iotype);

    return {
      action: 0, // Thêm action field giống như salesInvoice
      ma_dvcs: maDvcs,
      ma_kh: maKh,
      ong_ba: orderData?.customer?.name || null,
      ma_gd: '1', // Mã giao dịch: 1
      ma_nx: maNx, // Thêm ma_nx vào header
      ngay_ct: ngayCt,
      so_ct: firstItem.doccode,
      ma_nt: 'VND',
      ty_gia: 1.0,
      dien_giai: firstItem.doc_desc || null,
      detail: detail,
    };
  }

  async cutCode(input: string): Promise<string> {
    return input?.split('-')[0] || '';
  }

  /**
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  ========== INVOICE REFACTOR HELPERS ==========
  */

  private toNumber(value: any, defaultValue: number = 0): number {
    if (value === null || value === undefined || value === '')
      return defaultValue;
    const num = Number(value);
    return isNaN(num) ? defaultValue : num;
  }

  private toString(value: any, defaultValue: string = ''): string {
    return value === null || value === undefined || value === ''
      ? defaultValue
      : String(value);
  }

  private limitString(value: string, maxLength: number): string {
    if (!value) return '';
    const str = String(value);
    return str.length > maxLength ? str.substring(0, maxLength) : str;
  }

  private formatDateISO(date: Date): string {
    if (isNaN(date.getTime())) throw new Error('Invalid date');
    return date.toISOString();
  }

  private formatDateKeepLocalDay(date: Date): string {
    if (isNaN(date.getTime())) throw new Error('Invalid date');

    const pad = (n: number) => String(n).padStart(2, '0');

    return (
      date.getFullYear() +
      '-' +
      pad(date.getMonth() + 1) +
      '-' +
      pad(date.getDate()) +
      'T' +
      pad(date.getHours()) +
      ':' +
      pad(date.getMinutes()) +
      ':' +
      pad(date.getSeconds()) +
      '.000Z'
    );
  }

  private parseInvoiceDate(inputDate: any): Date {
    let docDate: Date;
    if (inputDate instanceof Date) {
      docDate = inputDate;
    } else if (typeof inputDate === 'string') {
      docDate = new Date(inputDate);
      if (isNaN(docDate.getTime())) docDate = new Date();
    } else {
      docDate = new Date();
    }

    const minDate = new Date('1753-01-01T00:00:00');
    const maxDate = new Date('9999-12-31T23:59:59');
    if (docDate < minDate || docDate > maxDate) {
      throw new Error(
        `Date out of range for SQL Server: ${docDate.toISOString()}`,
      );
    }
    return docDate;
  }

  private async getInvoiceStockTransferMap(
    docCode: string,
    isNormalOrder: boolean,
  ) {
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const allStockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    const stockTransferMap = new Map<
      string,
      { st?: StockTransfer[]; rt?: StockTransfer[] }
    >();
    let transDate: Date | null = null;

    if (isNormalOrder && allStockTransfers.length > 0) {
      transDate = allStockTransfers[0].transDate || null;
      const itemCodes = Array.from(
        new Set(
          allStockTransfers
            .map((st) => st.itemCode)
            .filter((c): c is string => !!c && c.trim() !== ''),
        ),
      );

      const loyaltyMap = new Map<string, any>();
      if (itemCodes.length > 0) {
        const products = await this.loyaltyService.fetchProducts(itemCodes);
        products.forEach((p, c) => loyaltyMap.set(c, p));
      }

      allStockTransfers.forEach((st) => {
        const materialCode =
          st.materialCode || loyaltyMap.get(st.itemCode)?.materialCode;
        if (!materialCode) return;
        const key = `${st.soCode || st.docCode || docCode}_${materialCode}`;

        if (!stockTransferMap.has(key)) stockTransferMap.set(key, {});
        const m = stockTransferMap.get(key)!;
        if (st.docCode.startsWith('ST') || Number(st.qty || 0) < 0) {
          if (!m.st) m.st = [];
          m.st.push(st);
        } else if (st.docCode.startsWith('RT') || Number(st.qty || 0) > 0) {
          if (!m.rt) m.rt = [];
          m.rt.push(st);
        }
      });
    }
    return { stockTransferMap, allStockTransfers, transDate };
  }

  private async getInvoiceCardSerialMap(
    docCode: string,
  ): Promise<Map<string, string>> {
    const map = new Map<string, string>();
    const [dataCard] = await this.n8nService.fetchCardData(docCode);
    if (Array.isArray(dataCard?.data)) {
      for (const card of dataCard.data) {
        if (!card?.service_item_name || !card?.serial) continue;
        const product = await this.loyaltyService.checkProduct(
          card.service_item_name,
        );
        if (product) map.set(product.materialCode, card.serial);
      }
    }
    return map;
  }

  private async calculateInvoiceAmounts(
    sale: any,
    orderData: any,
    allocationRatio: number,
    isNormalOrder: boolean,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const headerOrderTypes = InvoiceLogicUtils.getOrderTypes(
      orderData.sales?.[0]?.ordertypeName ||
        orderData.sales?.[0]?.ordertype ||
        '',
    );

    const amounts: any = {
      tienThue: this.toNumber(sale.tienThue, 0),
      dtTgNt: this.toNumber(sale.dtTgNt, 0),
      ck01_nt: this.toNumber(
        sale.other_discamt || sale.chietKhauMuaHangGiamGia,
        0,
      ),
      ck02_nt: this.toNumber(sale.chietKhauCkTheoChinhSach, 0),
      ck03_nt: this.toNumber(
        sale.chietKhauMuaHangCkVip || sale.grade_discamt,
        0,
      ),
      ck04_nt: this.toNumber(
        sale.chietKhauThanhToanCoupon || sale.chietKhau09,
        0,
      ),
      ck05_nt:
        this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0
          ? this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0)
          : 0,
      ck07_nt: this.toNumber(sale.chietKhauVoucherDp2, 0),
      ck08_nt: this.toNumber(sale.chietKhauVoucherDp3, 0),
    };

    // Fill others with default 0 or from sale fields
    for (let i = 9; i <= 22; i++) {
      if (i === 11) continue; // ck11 handled separately
      const key = `ck${i.toString().padStart(2, '0')}_nt`;
      const saleKey = `chietKhau${i.toString().padStart(2, '0')}`;
      amounts[key] = this.toNumber(sale[saleKey] || sale[key], 0);
    }
    amounts.ck06_nt = 0;

    // ck11 (ECOIN) logic
    let ck11_nt = this.toNumber(
      sale.chietKhauThanhToanTkTienAo || sale.chietKhau11,
      0,
    );
    if (
      ck11_nt === 0 &&
      this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0 &&
      orderData.cashioData
    ) {
      const ecoin = orderData.cashioData.find(
        (c: any) => c.fop_syscode === 'ECOIN',
      );
      if (ecoin?.total_in) ck11_nt = this.toNumber(ecoin.total_in, 0);
    }
    amounts.ck11_nt = ck11_nt;

    // Allocation
    if (
      isNormalOrder &&
      allocationRatio !== 1 &&
      allocationRatio > 0 &&
      !orderTypes.isDoiDiem &&
      !headerOrderTypes.isDoiDiem
    ) {
      Object.keys(amounts).forEach((k) => {
        if (k.endsWith('_nt') || k === 'tienThue' || k === 'dtTgNt') {
          amounts[k] *= allocationRatio;
        }
      });
    }

    if (orderTypes.isDoiDiem || headerOrderTypes.isDoiDiem) amounts.ck05_nt = 0;

    // promCode logic
    let promCode = sale.promCode || sale.prom_code || null;
    promCode = await this.cutCode(promCode);
    if (sale.productType === 'I') {
      promCode = promCode + '.I';
    } else if (sale.productType === 'S') {
      promCode = promCode + '.S';
    } else if (sale.productType === 'V') {
      promCode = promCode + '.V';
    }
    amounts.promCode = promCode;

    return amounts;
  }

  private async resolveInvoicePromotionCodes(
    sale: any,
    orderData: any,
    giaBan: number,
    promCode: string | null,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const isTangHang =
      Math.abs(giaBan) < 0.01 &&
      Math.abs(this.toNumber(sale.linetotal || sale.revenue, 0)) < 0.01;
    const maDvcs = this.toString(
      sale.department?.ma_dvcs || sale.department?.ma_dvcs_ht || '',
      '',
    );
    const productTypeUpper = (sale.productType || '')?.toUpperCase().trim();

    return InvoiceLogicUtils.resolvePromotionCodes({
      sale,
      orderTypes,
      isTangHang,
      maDvcs,
      productTypeUpper,
      promCode,
    });
  }

  private resolveInvoiceAccounts(
    sale: any,
    loyaltyProduct: any,
    giaBan: number,
    maCk01: string | null,
    maCtkmTangHang: string | null,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const isTangHang =
      Math.abs(giaBan) < 0.01 &&
      Math.abs(this.toNumber(sale.linetotal || sale.revenue, 0)) < 0.01;
    const isGiaBanZero = Math.abs(sale.giaBanGoc || 0) < 0.01;

    return InvoiceLogicUtils.resolveAccountingAccounts({
      sale,
      loyaltyProduct,
      orderTypes,
      isTangHang,
      isGiaBanZero,
      hasMaCtkm: !!(maCk01 || maCtkmTangHang),
      hasMaCtkmTangHang: !!maCtkmTangHang,
    });
  }

  private resolveInvoiceLoaiGd(sale: any): string {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertype || sale.ordertypeName || '',
    );
    return InvoiceLogicUtils.resolveLoaiGd({ sale, orderTypes });
  }

  private async resolveInvoiceBatchSerial(
    sale: any,
    saleMaterialCode: string,
    cardSerialMap: Map<string, string>,
    stockTransferMap: Map<string, any>,
    docCode: string,
    loyaltyProduct: any,
  ) {
    let batchSerial: string | null = null;
    if (saleMaterialCode) {
      const sts = stockTransferMap.get(`${docCode}_${saleMaterialCode}`)?.st;
      if (sts?.[0]?.batchSerial) batchSerial = sts[0].batchSerial;
    }

    return InvoiceLogicUtils.resolveBatchSerial({
      batchSerialFromST: batchSerial,
      trackBatch: loyaltyProduct?.trackBatch === true,
      trackSerial: loyaltyProduct?.trackSerial === true,
    });
  }

  private calculateInvoiceQty(
    sale: any,
    docCode: string,
    saleMaterialCode: string,
    isNormalOrder: boolean,
    stockTransferMap: Map<string, any>,
  ) {
    let qty = this.toNumber(sale.qty, 0);
    const saleQty = this.toNumber(sale.qty, 0);
    let allocationRatio = 1;

    if (isNormalOrder && saleMaterialCode) {
      const key = `${docCode}_${saleMaterialCode}`;
      const firstSt = stockTransferMap.get(key)?.st?.[0];
      if (firstSt && saleQty !== 0) {
        qty = Math.abs(Number(firstSt.qty || 0));
        allocationRatio = qty / saleQty;
      }
    }
    return { qty, saleQty, allocationRatio };
  }

  private calculateInvoicePrices(
    sale: any,
    qty: number,
    allocationRatio: number,
    isNormalOrder: boolean,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    return InvoiceLogicUtils.calculatePrices({
      sale,
      orderTypes,
      allocationRatio,
      qtyFromStock: qty,
    });
  }

  private resolveInvoiceMaKhHeader(orderData: any): string {
    let maKh = SalesUtils.normalizeMaKh(orderData.customer?.code);
    const firstSale = orderData.sales?.[0];
    const { isTachThe } = InvoiceLogicUtils.getOrderTypes(
      firstSale?.ordertype || firstSale?.ordertypeName || '',
    );

    if (isTachThe && Array.isArray(orderData.sales)) {
      const saleWithIssue =
        orderData.sales.find(
          (s: any) => Number(s.qty || 0) < 0 && s.issuePartnerCode,
        ) || orderData.sales.find((s: any) => s.issuePartnerCode);
      if (saleWithIssue) {
        maKh = SalesUtils.normalizeMaKh(saleWithIssue.issuePartnerCode);
      }
    }
    return maKh;
  }

  private async resolveInvoiceMaKho(
    sale: any,
    saleMaterialCode: string,
    stockTransferMap: Map<string, any>,
    docCode: string,
    maBp: string,
    isTachThe: boolean,
  ): Promise<string> {
    let maKhoFromST: string | null = null;
    if (saleMaterialCode) {
      const sts = stockTransferMap.get(`${docCode}_${saleMaterialCode}`)?.st;
      if (sts?.[0]?.stockCode) maKhoFromST = sts[0].stockCode;
    }

    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertype || sale.ordertypeName || '',
    );

    const maKho = InvoiceLogicUtils.resolveMaKho({
      maKhoFromST,
      maKhoFromSale: sale.maKho || null,
      maBp,
      orderTypes,
    });

    const maKhoMap = await this.categoriesService.mapWarehouseCode(maKho);
    return maKhoMap || maKho || '';
  }

  private fillInvoiceChietKhauFields(
    detailItem: any,
    amounts: any,
    sale: any,
    orderData: any,
  ) {
    for (let i = 1; i <= 22; i++) {
      const idx = i.toString().padStart(2, '0');
      const key = `ck${idx}_nt`;
      const maKey = `ma_ck${idx}`;
      detailItem[key] = Number(amounts[key] || 0);

      // Special ma_ck logic
      if (i === 3) {
        const brand = orderData.customer?.brand || orderData.brand || '';
        detailItem[maKey] = this.limitString(
          this.toString(
            SalesCalculationUtils.calculateMuaHangCkVip(
              sale,
              sale.product,
              brand,
            ),
            '',
          ),
          32,
        );
      } else if (i === 4) {
        detailItem[maKey] = this.limitString(
          detailItem.ck04_nt > 0 || sale.thanhToanCoupon
            ? this.toString(sale.maCk04 || 'COUPON', '')
            : '',
          32,
        );
      } else if (i === 5) {
        const { isDoiDiem } = InvoiceLogicUtils.getOrderTypes(
          sale.ordertype || sale.ordertypeName,
        );
        const { isDoiDiem: isDoiDiemHeader } = InvoiceLogicUtils.getOrderTypes(
          orderData.sales?.[0]?.ordertype ||
            orderData.sales?.[0]?.ordertypeName ||
            '',
        );

        if (isDoiDiem || isDoiDiemHeader) {
          detailItem[maKey] = '';
        } else if (detailItem.ck05_nt > 0) {
          const maKh = sale.partnerCode;
          // Note: using logic from buildFastApiInvoiceData
          detailItem[maKey] = this.limitString(
            this.toString(
              InvoiceLogicUtils.resolveVoucherCode({
                sale: {
                  ...sale,
                  customer: sale.customer || orderData.customer,
                },
                customer: null, // Resolution happens inside resolveVoucherCode
                brand: orderData.customer?.brand || orderData.brand || '',
              }),
              sale.maCk05 || 'VOUCHER',
            ),
            32,
          );
        }
      } else if (i === 7) {
        detailItem[maKey] = this.limitString(
          sale.voucherDp2 ? 'VOUCHER_DP2' : '',
          32,
        );
      } else if (i === 8) {
        detailItem[maKey] = this.limitString(
          sale.voucherDp3 ? 'VOUCHER_DP3' : '',
          32,
        );
      } else if (i === 11) {
        detailItem[maKey] = this.limitString(
          detailItem.ck11_nt > 0 || sale.thanhToanTkTienAo
            ? this.toString(
                sale.maCk11 ||
                  SalesUtils.generateTkTienAoLabel(
                    orderData.docDate,
                    orderData.customer?.brand ||
                      orderData.sales?.[0]?.customer?.brand,
                  ),
                '',
              )
            : '',
          32,
        );
      } else {
        // Default mapping for other ma_ck fields
        const saleMaKey = `maCk${idx}`;
        detailItem[maKey] = this.limitString(
          this.toString(sale[saleMaKey] || '', ''),
          32,
        );
      }
    }
  }

  private buildInvoiceCbDetail(detail: any[]) {
    return detail.map((item: any) => {
      let tongChietKhau = 0;
      for (let i = 1; i <= 22; i++) {
        tongChietKhau += Number(
          item[`ck${i.toString().padStart(2, '0')}_nt`] || 0,
        );
      }

      return {
        ma_vt: item.ma_vt || '',
        dvt: item.dvt || '',
        so_luong: Number(item.so_luong || 0),
        ck_nt: Number(tongChietKhau),
        gia_nt: Number(item.gia_ban || 0),
        tien_nt: Number(item.tien_hang || 0),
      };
    });
  }

  private async mapSaleToInvoiceDetail(
    sale: any,
    index: number,
    orderData: any,
    context: any,
  ): Promise<any> {
    const { isNormalOrder, stockTransferMap, cardSerialMap } = context;
    const saleMaterialCode =
      sale.product?.materialCode ||
      sale.product?.maVatTu ||
      sale.product?.maERP;

    // 1. Qty & Allocation
    const { qty, allocationRatio } = this.calculateInvoiceQty(
      sale,
      orderData.docCode,
      saleMaterialCode,
      isNormalOrder,
      stockTransferMap,
    );

    // 2. Prices
    const { giaBan, tienHang, tienHangGoc } = this.calculateInvoicePrices(
      sale,
      qty,
      allocationRatio,
      isNormalOrder,
    );

    // 3. Amounts (Discounts, Tax, Subsidy)
    const amounts = await this.calculateInvoiceAmounts(
      sale,
      orderData,
      allocationRatio,
      isNormalOrder,
    );

    // 4. Resolve Codes & Accounts
    const materialCode =
      SalesUtils.getMaterialCode(sale, sale.product) || sale.itemCode;
    const loyaltyProduct = await this.loyaltyService.checkProduct(materialCode);

    const { maCk01, maCtkmTangHang } = await this.resolveInvoicePromotionCodes(
      sale,
      orderData,
      giaBan,
      amounts.promCode,
    );

    const { tkChietKhau, tkChiPhi, maPhi } = this.resolveInvoiceAccounts(
      sale,
      loyaltyProduct,
      giaBan,
      maCk01,
      maCtkmTangHang,
    );

    // 5. Build Detail Item
    const maBp = this.limitString(
      this.toString(
        sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
        '',
      ),
      8,
    );
    const loaiGd = this.resolveInvoiceLoaiGd(sale);
    const { maLo, soSerial } = await this.resolveInvoiceBatchSerial(
      sale,
      saleMaterialCode,
      cardSerialMap,
      stockTransferMap,
      orderData.docCode,
      loyaltyProduct,
    );

    const detailItem: any = {
      tk_chiet_khau: this.limitString(this.toString(tkChietKhau, ''), 16),
      tk_chi_phi: this.limitString(this.toString(tkChiPhi, ''), 16),
      ma_phi: this.limitString(this.toString(maPhi, ''), 16),
      tien_hang: Number(sale.qty) * Number(sale.giaBan),
      so_luong: Number(sale.qty),
      ma_kh_i: this.limitString(this.toString(sale.issuePartnerCode, ''), 16),
      ma_vt: this.limitString(
        this.toString(
          loyaltyProduct?.materialCode || sale.product?.maVatTu || '',
        ),
        16,
      ),
      dvt: this.limitString(
        this.toString(
          sale.product?.dvt || sale.product?.unit || sale.dvt,
          'Cái',
        ),
        32,
      ),
      loai: this.limitString(this.toString(sale.loai || sale.cat1, ''), 2),
      loai_gd: this.limitString(loaiGd, 2),
      ma_ctkm_th: this.limitString(maCtkmTangHang, 32),
    };

    const finalMaKho = await this.resolveInvoiceMaKho(
      sale,
      saleMaterialCode,
      stockTransferMap,
      orderData.docCode,
      maBp,
      loaiGd === '11' || loaiGd === '12',
    );
    if (finalMaKho && finalMaKho.trim() !== '') {
      detailItem.ma_kho = this.limitString(finalMaKho, 16);
    }

    Object.assign(detailItem, {
      gia_ban: Number(giaBan),
      is_reward_line: sale.isRewardLine ? 1 : 0,
      is_bundle_reward_line: sale.isBundleRewardLine ? 1 : 0,
      km_yn:
        maCtkmTangHang === 'TT DAU TU'
          ? 0
          : Math.abs(giaBan) < 0.01 && Math.abs(tienHang) < 0.01
            ? 1
            : 0,
      dong_thuoc_goi: this.limitString(
        this.toString(sale.dongThuocGoi, ''),
        32,
      ),
      trang_thai: this.limitString(this.toString(sale.trangThai, ''), 32),
      barcode: this.limitString(this.toString(sale.barcode, ''), 32),
      ma_ck01: this.limitString(maCk01, 32),
      dt_tg_nt: Number(amounts.dtTgNt),
      tien_thue: Number(amounts.tienThue),
      ma_thue: this.limitString(this.toString(sale.maThue, '00'), 8),
      thue_suat: Number(this.toNumber(sale.thueSuat, 0)),
      tk_thue: this.limitString(this.toString(sale.tkThueCo, ''), 16),
      tk_cpbh: this.limitString(this.toString(sale.tkCpbh, ''), 16),
      ma_bp: maBp,
      ma_the: this.limitString(cardSerialMap.get(saleMaterialCode) || '', 256),
      dong: index + 1,
      id_goc_ngay: sale.idGocNgay
        ? this.formatDateISO(new Date(sale.idGocNgay))
        : this.formatDateISO(new Date()),
      id_goc: this.limitString(this.toString(sale.idGoc, ''), 70),
      id_goc_ct: this.limitString(this.toString(sale.idGocCt, ''), 16),
      id_goc_so: Number(this.toNumber(sale.idGocSo, 0)),
      id_goc_dv: this.limitString(this.toString(sale.idGocDv, ''), 8),
      ma_combo: this.limitString(this.toString(sale.maCombo, ''), 16),
      ma_nx_st: this.limitString(this.toString(sale.ma_nx_st, ''), 32),
      ma_nx_rt: this.limitString(this.toString(sale.ma_nx_rt, ''), 32),
      ...(soSerial && soSerial.trim() !== ''
        ? { so_serial: this.limitString(soSerial, 64) }
        : maLo && maLo.trim() !== ''
          ? { ma_lo: this.limitString(maLo, 16) }
          : {}),
    });

    this.fillInvoiceChietKhauFields(detailItem, amounts, sale, orderData);

    return detailItem;
  }

  private assembleInvoicePayload(
    orderData: any,
    detail: any[],
    cbdetail: any[],
    context: any,
  ) {
    const { ngayCt, ngayLct, transDate, maBp } = context;
    const firstSale = orderData.sales?.[0];

    return {
      action: 0,
      ma_dvcs:
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '',
      ma_kh: this.resolveInvoiceMaKhHeader(orderData),
      ong_ba: orderData.customer?.name || null,
      ma_gd: '1',
      ma_tt: null,
      ma_ca: firstSale?.maCa || null,
      hinh_thuc: '0',
      dien_giai: orderData.docCode || null,
      ngay_lct: ngayLct,
      ngay_ct: ngayCt,
      so_ct: orderData.docCode || '',
      so_seri: orderData.branchCode || 'DEFAULT',
      ma_nt: 'VND',
      ty_gia: 1.0,
      ma_bp: maBp,
      tk_thue_no: '131111',
      ma_kenh: 'ONLINE',
      trans_date: transDate
        ? this.formatDateKeepLocalDay(new Date(transDate))
        : null,
      detail,
      cbdetail,
    };
  }

  private logInvoiceError(error: any, orderData: any) {
    this.logger.error(
      `Error building Fast API invoice data: ${error?.message || error}`,
    );
    this.logger.error(`Error stack: ${error?.stack || 'No stack trace'}`);
    this.logger.error(
      `Order data: ${JSON.stringify({
        docCode: orderData?.docCode,
        docDate: orderData?.docDate,
        salesCount: orderData?.sales?.length,
        customer: orderData?.customer
          ? { code: orderData.customer.code, name: orderData.customer.name }
          : null,
      })}`,
    );
  }
}
