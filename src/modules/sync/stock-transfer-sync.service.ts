import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { HttpService } from '@nestjs/axios';

import { StockTransfer } from '../../entities/stock-transfer.entity';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { parseDDMMMYYYY } from '../../utils/date-parser.util';

@Injectable()
export class StockTransferSyncService {
  private readonly logger = new Logger(StockTransferSyncService.name);

  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(WarehouseProcessed)
    private warehouseProcessedRepository: Repository<WarehouseProcessed>,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private loyaltyService: LoyaltyService,
  ) {}

  /**
   * Helper function để validate integer value
   * Chuyển NaN, undefined, null thành undefined
   */
  private validateInteger(value: any): number | undefined {
    if (value === null || value === undefined) {
      return undefined;
    }
    const num = Number(value);
    if (isNaN(num) || !isFinite(num)) {
      return undefined;
    }
    return Math.floor(num);
  }

  /**
   * Đồng bộ dữ liệu xuất kho từ Zappy API
   * @param date - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard)
   */
  async syncStockTransfer(
    date: string,
    brand: string,
    options?: { skipWarehouseProcessing?: boolean },
  ): Promise<{
    success: boolean;
    message: string;
    recordsCount: number;
    savedCount: number;
    updatedCount: number;
    errors?: string[];
  }> {
    try {
      this.logger.log(
        `[Stock Transfer] Bắt đầu đồng bộ dữ liệu xuất kho cho brand ${brand} ngày ${date}`,
      );

      // Gọi API với P_PART=1,2,3 tuần tự để tránh quá tải
      const parts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
      const allStockTransData: any[] = [];

      for (const part of parts) {
        try {
          this.logger.log(
            `[Stock Transfer] Đang lấy dữ liệu part ${part} cho brand ${brand} ngày ${date}`,
          );
          const partData = await this.zappyApiService.getDailyStockTrans(
            date,
            brand,
            part,
          );
          if (partData && partData.length > 0) {
            allStockTransData.push(...partData);
            this.logger.log(
              `[Stock Transfer] Nhận được ${partData.length} records từ part ${part} cho brand ${brand} ngày ${date}`,
            );
          } else {
            this.logger.log(
              `[Stock Transfer] Không có dữ liệu từ part ${part} cho brand ${brand} ngày ${date}`,
            );
          }
        } catch (error: any) {
          this.logger.error(
            `[Stock Transfer] Lỗi khi lấy dữ liệu part ${part} cho brand ${brand} ngày ${date}: ${error?.message || error}`,
          );
          // Tiếp tục với part tiếp theo, không throw error
        }
      }

      if (!allStockTransData || allStockTransData.length === 0) {
        this.logger.log(
          `[Stock Transfer] Không có dữ liệu xuất kho cho brand ${brand} ngày ${date}`,
        );
        return {
          success: true,
          message: `Không có dữ liệu xuất kho cho brand ${brand} ngày ${date}`,
          recordsCount: 0,
          savedCount: 0,
          updatedCount: 0,
        };
      }

      this.logger.log(
        `[Stock Transfer] Tổng cộng nhận được ${allStockTransData.length} records xuất kho cho brand ${brand} ngày ${date}`,
      );

      // KHÔNG deduplicate - giữ lại tất cả records để lưu vào database
      // Mỗi record sẽ có compositeKey unique với timestamp khi lưu
      const stockTransData = allStockTransData;

      this.logger.log(
        `[Stock Transfer] Giữ lại tất cả ${stockTransData.length} records (không deduplicate)`,
      );

      // Parse date từ format "01/11/2025 19:00" sang Date object
      const parseTransDate = (dateStr: string): Date => {
        if (!dateStr) return new Date();
        try {
          // Format: "01/11/2025 19:00"
          const [datePart, timePart] = dateStr.split(' ');
          const [day, month, year] = datePart.split('/');

          if (timePart) {
            const [hours, minutes] = timePart.split(':');
            return new Date(
              parseInt(year),
              parseInt(month) - 1,
              parseInt(day),
              parseInt(hours),
              parseInt(minutes),
            );
          } else {
            return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
          }
        } catch (error) {
          this.logger.warn(
            `Failed to parse transDate: ${dateStr}, using current date`,
          );
          return new Date();
        }
      };

      // Fetch materialCode từ Loyalty API cho tất cả itemCode
      const uniqueItemCodes = Array.from(
        new Set(
          stockTransData
            .map((item) => item.item_code)
            .filter((code): code is string => !!code && code.trim() !== ''),
        ),
      );

      const materialCodeMap = new Map<string, string>();
      if (uniqueItemCodes.length > 0) {
        try {
          // fetchProducts sẽ tự động thử cả /code/ và /old-code/ cho mỗi itemCode
          const loyaltyProducts =
            await this.loyaltyService.fetchProducts(uniqueItemCodes);
          loyaltyProducts.forEach((product, itemCode) => {
            if (product?.materialCode) {
              materialCodeMap.set(itemCode, product.materialCode);
            }
          });
        } catch (error: any) {
          this.logger.warn(
            `[Stock Transfer] Lỗi khi fetch materialCode từ Loyalty API: ${error?.message || error}`,
          );
        }
      }

      let savedCount = 0;
      const errors: string[] = [];

      // Map all data to entities first
      const entitiesToSave: StockTransfer[] = [];
      const conversionErrors: string[] = [];

      for (const item of stockTransData) {
        try {
          // Tạo compositeKey dựa trên nội dung để đảm bảo tính duy nhất (Deterministic)
          // Loại bỏ timestamp và random để đảm bảo idempotency
          const compositeKey = [
            item.doccode || '',
            item.item_code || '',
            (item.qty || 0).toString(),
            item.stock_code || '',
            item.iotype || '', // Thêm iotype để phân biệt nhập/xuất
            item.batchserial || '', // Thêm batchserial nếu có
          ].join('|');

          // Lấy materialCode từ Loyalty API
          const materialCode = item.item_code
            ? materialCodeMap.get(item.item_code)
            : undefined;

          const stockTransferData: Partial<StockTransfer> = {
            doctype: item.doctype || '',
            docCode: item.doccode || '',
            transDate: parseTransDate(item.transdate),
            docDesc: item.doc_desc || undefined,
            branchCode: item.branch_code || '',
            brandCode: item.brand_code || '',
            itemCode: item.item_code || '',
            itemName: item.item_name || '',
            materialCode: materialCode || undefined,
            stockCode: item.stock_code || '',
            relatedStockCode: item.related_stock_code || undefined,
            ioType: item.iotype || '',
            qty: item.qty || 0,
            batchSerial: item.batchserial || undefined,
            lineInfo1: item.line_info1 || undefined,
            lineInfo2: item.line_info2 || undefined,
            soCode: item.so_code || undefined,
            syncDate: date,
            brand: brand,
            compositeKey: compositeKey,
          };

          const entity = this.stockTransferRepository.create(stockTransferData);
          entitiesToSave.push(entity);
        } catch (err: any) {
          const errorMsg = `Lỗi khi convert stock transfer ${item.doccode}/${item.item_code}: ${err?.message || err}`;
          this.logger.error(`[Stock Transfer] ${errorMsg}`);
          conversionErrors.push(errorMsg);
        }
      }

      if (conversionErrors.length > 0) {
        errors.push(...conversionErrors);
      }

      // Save in chunks
      const chunkSize = 500; // Tăng lên 500 để nhanh hơn
      this.logger.log(
        `[Stock Transfer] Bắt đầu lưu ${entitiesToSave.length} records (Chunk size: ${chunkSize})`,
      );

      for (let i = 0; i < entitiesToSave.length; i += chunkSize) {
        const chunk = entitiesToSave.slice(i, i + chunkSize);
        try {
          await this.stockTransferRepository.save(chunk);
          savedCount += chunk.length;
          this.logger.log(
            `[Stock Transfer] Đã lưu chunk ${Math.floor(i / chunkSize) + 1}/${Math.ceil(entitiesToSave.length / chunkSize)} (${chunk.length} records)`,
          );
        } catch (chunkError: any) {
          const errorMsg = `Lỗi khi lưu chunk ${Math.floor(i / chunkSize) + 1}: ${chunkError?.message || chunkError}`;
          this.logger.error(`[Stock Transfer] ${errorMsg}`);
          // Nếu fails cả chunk, thử lưu từng item trong chunk để cứu vớt
          this.logger.log(
            `[Stock Transfer] Thử lưu lẻ từng item trong chunk lỗi...`,
          );
          for (const entity of chunk) {
            try {
              await this.stockTransferRepository.save(entity);
              savedCount++;
            } catch (itemError: any) {
              const itemErrorMsg = `Lỗi khi lưu item ${entity.docCode}/${entity.itemCode}: ${itemError?.message || itemError}`;
              errors.push(itemErrorMsg);
            }
          }
        }
      }

      this.logger.log(
        `[Stock Transfer] Đã lưu ${savedCount} records mới cho brand ${brand} ngày ${date}`,
      );

      // Tự động xử lý warehouse cho các stock transfers mới (chỉ cho các docCode chưa được xử lý)
      // Nếu skipWarehouseProcessing = true thì bỏ qua bước này
      // DISABLED per user request: Không tự động tạo phiếu nhập/xuất kho khi sync
      // if (!options?.skipWarehouseProcessing) {
      //   try {
      //     await this.processWarehouseForStockTransfers(date, brand);
      //   } catch (warehouseError: any) {
      //     this.logger.warn(
      //       `[Stock Transfer] Lỗi khi xử lý warehouse tự động cho brand ${brand} ngày ${date}: ${warehouseError?.message || warehouseError}`,
      //     );
      //     // Không throw error để không chặn flow sync chính
      //   }
      // }

      return {
        success: errors.length === 0,
        message: `Đồng bộ thành công ${stockTransData.length} records xuất kho cho brand ${brand} ngày ${date}. Đã lưu ${savedCount} records mới`,
        recordsCount: stockTransData.length,
        savedCount,
        updatedCount: 0,
        errors: errors.length > 0 ? errors : undefined,
      };
    } catch (error: any) {
      const errorMsg = `Lỗi khi đồng bộ stock transfer cho brand ${brand} ngày ${date}: ${error?.message || error}`;
      this.logger.error(`[Stock Transfer] ${errorMsg}`);
      return {
        success: false,
        message: errorMsg,
        recordsCount: 0,
        savedCount: 0,
        updatedCount: 0,
        errors: [errorMsg],
      };
    }
  }

  /**
   * Đồng bộ dữ liệu xuất kho từ ngày đến ngày
   * @param dateFrom - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param dateTo - Date format: DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard). Nếu không có thì đồng bộ tất cả brands
   */
  async syncStockTransferRange(
    dateFrom: string,
    dateTo: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    totalRecordsCount: number;
    totalSavedCount: number;
    totalUpdatedCount: number;
    errors?: string[];
    details?: Array<{
      date: string;
      brand: string;
      recordsCount: number;
      savedCount: number;
      updatedCount: number;
    }>;
  }> {
    try {
      this.logger.log(
        `[Stock Transfer Range] Bắt đầu đồng bộ dữ liệu xuất kho từ ${dateFrom} đến ${dateTo}${brand ? ` cho brand ${brand}` : ' cho tất cả brands'}`,
      );

      // Parse dates từ DDMMMYYYY sang Date object
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

      const formatToDDMMMYYYY = (d: Date): string => {
        const day = d.getDate().toString().padStart(2, '0');
        const monthIdx = d.getMonth();
        const year = d.getFullYear();
        const months = [
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
        const monthStr = months[monthIdx];
        return `${day}${monthStr}${year}`;
      };

      const startDate = parseDate(dateFrom);
      const endDate = parseDate(dateTo);

      if (startDate > endDate) {
        throw new Error('dateFrom phải nhỏ hơn hoặc bằng dateTo');
      }

      const brands = brand ? [brand] : ['f3', 'labhair', 'yaman', 'menard'];
      let totalRecordsCount = 0;
      let totalSavedCount = 0;
      let totalUpdatedCount = 0;
      const errors: string[] = [];
      const details: Array<{
        date: string;
        brand: string;
        recordsCount: number;
        savedCount: number;
        updatedCount: number;
      }> = [];

      // Phase 1: Đồng bộ dữ liệu từ Zappy (Bỏ qua xử lý Warehouse)
      const dateList: string[] = [];
      const currentDate = new Date(startDate.getTime());
      while (currentDate <= endDate) {
        dateList.push(formatToDDMMMYYYY(currentDate));
        // Tăng 1 ngày
        currentDate.setDate(currentDate.getDate() + 1);
      }

      // Loop qua từng ngày để sync từ Zappy
      for (const dateStr of dateList) {
        for (const brandItem of brands) {
          try {
            this.logger.log(
              `[Stock Transfer Range] Phase 1: Đang đồng bộ Zappy brand ${brandItem} cho ngày ${dateStr}`,
            );
            // Gọi sync với skipWarehouseProcessing = true
            const result = await this.syncStockTransfer(dateStr, brandItem, {
              skipWarehouseProcessing: true,
            });

            totalRecordsCount += result.recordsCount;
            totalSavedCount += result.savedCount;
            totalUpdatedCount += result.updatedCount;

            details.push({
              date: dateStr,
              brand: brandItem,
              recordsCount: result.recordsCount,
              savedCount: result.savedCount,
              updatedCount: result.updatedCount,
            });

            if (result.errors && result.errors.length > 0) {
              errors.push(...result.errors);
            }
          } catch (error: any) {
            const errorMsg = `Lỗi khi đồng bộ stock transfer (Phase 1) cho brand ${brandItem} ngày ${dateStr}: ${error?.message || error}`;
            this.logger.error(`[Stock Transfer Range] ${errorMsg}`);
            errors.push(errorMsg);
          }
        }
      }

      this.logger.log(
        `[Stock Transfer Range] Phase 1 hoàn tất. Bắt đầu Phase 2: Xử lý Warehouse...`,
      );

      // Phase 2: Xử lý Warehouse (Sau khi đã có đủ dữ liệu)
      // DISABLED per user request: Không tự động xử lý warehouse (Push to Fast)
      // for (const dateStr of dateList) {
      //   for (const brandItem of brands) {
      //     try {
      //       this.logger.log(
      //         `[Stock Transfer Range] Phase 2: Đang xử lý Warehouse brand ${brandItem} cho ngày ${dateStr}`,
      //       );
      //       await this.processWarehouseForStockTransfers(dateStr, brandItem);
      //       this.logger.log(
      //         `[Stock Transfer Range] Phase 2: Hoàn thành xử lý Warehouse brand ${brandItem} cho ngày ${dateStr}`,
      //       );
      //     } catch (error: any) {
      //       const errorMsg = `Lỗi khi xử lý warehouse (Phase 2) cho brand ${brandItem} ngày ${dateStr}: ${error?.message || error}`;
      //       this.logger.error(`[Stock Transfer Range] ${errorMsg}`);
      //       errors.push(errorMsg);
      //     }
      //   }
      // }

      this.logger.log(
        `[Stock Transfer Range] Hoàn thành đồng bộ dữ liệu xuất kho từ ${dateFrom} đến ${dateTo}. Tổng: ${totalRecordsCount} records, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
      );

      return {
        success: errors.length === 0,
        message: `Đồng bộ thành công dữ liệu xuất kho từ ${dateFrom} đến ${dateTo}. Tổng: ${totalRecordsCount} records, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        totalRecordsCount,
        totalSavedCount,
        totalUpdatedCount,
        errors: errors.length > 0 ? errors : undefined,
        details,
      };
    } catch (error: any) {
      const errorMsg = `Lỗi khi đồng bộ stock transfer range từ ${dateFrom} đến ${dateTo}: ${error?.message || error}`;
      this.logger.error(`[Stock Transfer Range] ${errorMsg}`);
      throw new Error(errorMsg);
    }
  }

  /**
   * Lấy danh sách stock transfers với filter và pagination
   */
  async getStockTransfers(params: {
    page?: number;
    limit?: number;
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    branchCode?: string;
    itemCode?: string;
    soCode?: string;
    docCode?: string;
    doctype?: string;
  }): Promise<{
    success: boolean;
    data: StockTransfer[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
    };
  }> {
    try {
      const page = params.page || 1;
      const limit = params.limit || 10;
      const skip = (page - 1) * limit;

      const queryBuilder =
        this.stockTransferRepository.createQueryBuilder('st');

      // Apply filters
      if (params.brand) {
        queryBuilder.andWhere('st.brand = :brand', { brand: params.brand });
      }
      if (params.branchCode) {
        queryBuilder.andWhere('st.branchCode = :branchCode', {
          branchCode: params.branchCode,
        });
      }
      if (params.itemCode) {
        // Use prefix search instead of full-text search to utilize index
        // Changed from LIKE '%value%' to LIKE 'value%'
        queryBuilder.andWhere('st.itemCode LIKE :itemCode', {
          itemCode: `${params.itemCode}%`,
        });
      }
      if (params.soCode) {
        queryBuilder.andWhere('st.soCode = :soCode', { soCode: params.soCode });
      }
      if (params.docCode) {
        // Use prefix LIKE instead of POSITION to utilize index
        // Changed from POSITION(:docCode IN st.docCode) > 0 to LIKE 'value%'
        queryBuilder.andWhere('st.docCode LIKE :docCode', {
          docCode: `${params.docCode}%`,
        });
      }
      if (params.dateFrom) {
        // Use date-parser utility instead of inline function
        const fromDate = parseDDMMMYYYY(params.dateFrom);
        queryBuilder.andWhere('st.transDate >= :dateFrom', {
          dateFrom: fromDate,
        });
      }
      if (params.dateTo) {
        // Use date-parser utility with endOfDay flag
        const toDate = parseDDMMMYYYY(params.dateTo, true);
        queryBuilder.andWhere('st.transDate <= :dateTo', { dateTo: toDate });
      }

      if (params.doctype) {
        queryBuilder.andWhere('st.doctype = :doctype', {
          doctype: params.doctype,
        });
      }

      // Filter by doctype
      if (params.doctype) {
        queryBuilder.andWhere('st.doctype = :doctype', {
          doctype: params.doctype,
        });
      }

      // Order by transDate DESC
      queryBuilder.orderBy('st.transDate', 'DESC');

      // Apply pagination
      queryBuilder.skip(skip).take(limit);

      // Use getManyAndCount() instead of separate getCount() and getMany()
      // This executes a single optimized query instead of two separate queries
      const [data, total] = await queryBuilder.getManyAndCount();

      const totalPages = Math.ceil(total / limit);

      return {
        success: true,
        data,
        pagination: {
          page,
          limit,
          total,
          totalPages,
        },
      };
    } catch (error: any) {
      this.logger.error(
        `Error getting stock transfers: ${error?.message || error}`,
      );
      throw error;
    }
  }

  async getWarehouseProcessed(params: {
    page?: number;
    limit?: number;
    dateFrom?: string;
    dateTo?: string;
    ioType?: string;
    success?: boolean;
    docCode?: string;
    doctype?: string;
  }): Promise<{
    success: boolean;
    data: WarehouseProcessed[];
    statistics: {
      total: number;
      success: number;
      failed: number;
      byIoType: {
        I: number;
        O: number;
      };
    };
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
    };
  }> {
    try {
      // 1. Validate Date Range (Mandatory & Max 31 days)
      if (!params.dateFrom || !params.dateTo) {
        throw new Error(
          'Vui lòng chọn Từ ngày và Đến ngày (bắt buộc để đảm bảo hiệu năng).',
        );
      }

      const fromDate = parseDDMMMYYYY(params.dateFrom);
      const toDate = parseDDMMMYYYY(params.dateTo, true); // End of day

      const diffTime = Math.abs(toDate.getTime() - fromDate.getTime());
      const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

      if (diffDays > 32) {
        // Allow slightly more than 31 to cover month transitions safe margin
        throw new Error(
          'Khoảng thời gian tối đa là 1 tháng (31 ngày). Vui lòng chọn lại.',
        );
      }

      const page = params.page || 1;
      const limit = params.limit || 10;
      const skip = (page - 1) * limit;

      const queryBuilder = this.warehouseProcessedRepository
        .createQueryBuilder('wp')
        .orderBy('wp.transDate', 'DESC')
        .addOrderBy('wp.docCode', 'ASC');

      // Filter by ioType
      if (params.ioType) {
        // Use TRIM to avoid issues with potential whitespace in DB
        queryBuilder.andWhere('TRIM(wp.ioType) = :ioType', {
          ioType: params.ioType.trim(),
        });
      }

      // Filter by success
      if (params.success !== undefined) {
        queryBuilder.andWhere('wp.success = :success', {
          success: params.success,
        });
      }

      // Filter by docCode
      if (params.docCode) {
        queryBuilder.andWhere('wp.docCode LIKE :docCode', {
          docCode: `%${params.docCode}%`,
        });
      }

      // Filter by doctype
      if (params.doctype) {
        queryBuilder.andWhere('wp.doctype = :doctype', {
          doctype: params.doctype,
        });
      }

      // Filter by dateFrom & dateTo (Already parsed)
      queryBuilder.andWhere('wp.transDate >= :dateFrom', {
        dateFrom: fromDate,
      });
      queryBuilder.andWhere('wp.transDate <= :dateTo', { dateTo: toDate });

      // Get list data & total count for pagination
      // getManyAndCount is better than getCount + getMany separately
      const [data, total] = await queryBuilder
        .skip(skip)
        .take(limit)
        .getManyAndCount();

      // OPTIMIZED STATISTICS CALCULATION
      // Instead of fetching ALL records to memory, use SQL Aggregation
      const statsQueryBuilder = this.warehouseProcessedRepository
        .createQueryBuilder('wp')
        .select('COUNT(wp.id)', 'total')
        .addSelect(
          `SUM(CASE WHEN wp.success = TRUE THEN 1 ELSE 0 END)`,
          'success_count',
        )
        .addSelect(
          `SUM(CASE WHEN wp.success = FALSE THEN 1 ELSE 0 END)`,
          'failed_count',
        )
        .addSelect(
          `SUM(CASE WHEN wp.ioType = 'I' THEN 1 ELSE 0 END)`,
          'i_type_count',
        )
        .addSelect(
          `SUM(CASE WHEN wp.ioType = 'O' THEN 1 ELSE 0 END)`,
          'o_type_count',
        )
        .addSelect(
          `SUM(CASE WHEN wp.ioType = 'T' THEN 1 ELSE 0 END)`,
          't_type_count',
        );

      // Apply SAME filters to stats query (except pagination)
      if (params.ioType) {
        statsQueryBuilder.andWhere('TRIM(wp.ioType) = :ioType', {
          ioType: params.ioType.trim(),
        });
      }
      if (params.success !== undefined) {
        statsQueryBuilder.andWhere('wp.success = :success', {
          success: params.success,
        });
      }
      if (params.docCode) {
        statsQueryBuilder.andWhere('wp.docCode LIKE :docCode', {
          docCode: `%${params.docCode}%`,
        });
      }
      if (params.doctype) {
        statsQueryBuilder.andWhere('wp.doctype = :doctype', {
          doctype: params.doctype,
        });
      }

      // Date filters are mandatory now
      statsQueryBuilder.andWhere('wp.transDate >= :dateFrom', {
        dateFrom: fromDate,
      });
      statsQueryBuilder.andWhere('wp.transDate <= :dateTo', {
        dateTo: toDate,
      });

      const rawStats = await statsQueryBuilder.getRawOne();
      // Parse results (SQL returns strings for counts usually)
      const statistics = {
        total: parseInt(rawStats.total || '0', 10),
        success: parseInt(rawStats.success_count || '0', 10),
        failed: parseInt(rawStats.failed_count || '0', 10),
        byIoType: {
          I: parseInt(rawStats.i_type_count || '0', 10),
          O: parseInt(rawStats.o_type_count || '0', 10),
          T: parseInt(rawStats.t_type_count || '0', 10),
        },
      };

      const totalPages = Math.ceil(total / limit);

      return {
        success: true,
        data,
        statistics,
        pagination: {
          page,
          limit,
          total,
          totalPages,
        },
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi lấy danh sách warehouse processed: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Tự động xử lý warehouse cho các stock transfers của một ngày
   * Chỉ xử lý các docCode chưa được gọi API warehouse
   * @param date - Ngày sync (format: DDMMMYYYY)
   * @param brand - Brand name
   */
  async processWarehouseForStockTransfers(
    date: string,
    brand: string,
  ): Promise<void> {
    try {
      this.logger.log(
        `[Warehouse Auto] Bắt đầu xử lý warehouse tự động cho brand ${brand} ngày ${date}`,
      );

      // Lấy tất cả stock transfers của ngày đó
      const stockTransfers = await this.stockTransferRepository.find({
        where: {
          syncDate: date,
          brand: brand,
        },
        order: {
          docCode: 'ASC',
          createdAt: 'ASC',
        },
      });

      if (!stockTransfers || stockTransfers.length === 0) {
        this.logger.log(
          `[Warehouse Auto] Không có stock transfers để xử lý cho brand ${brand} ngày ${date}`,
        );
        return;
      }

      // Nhóm theo docCode và doctype
      const docCodeMap = new Map<string, StockTransfer[]>();
      for (const st of stockTransfers) {
        if (!docCodeMap.has(st.docCode)) {
          docCodeMap.set(st.docCode, []);
        }
        docCodeMap.get(st.docCode)!.push(st);
      }

      let processedCount = 0;
      let skippedCount = 0;
      let errorCount = 0;

      // Xử lý từng docCode
      for (const [docCode, stockTransferList] of docCodeMap) {
        try {
          // Lấy record đầu tiên để kiểm tra điều kiện
          const firstStockTransfer = stockTransferList[0];

          // Xử lý STOCK_TRANSFER với relatedStockCode
          if (
            firstStockTransfer.doctype === 'STOCK_TRANSFER' &&
            firstStockTransfer.relatedStockCode
          ) {
            // Kiểm tra relatedStockCode phải có
            if (
              !firstStockTransfer.relatedStockCode ||
              firstStockTransfer.relatedStockCode.trim() === ''
            ) {
              this.logger.debug(
                `[Warehouse Auto] DocCode ${docCode} có doctype = "STOCK_TRANSFER" nhưng không có relatedStockCode, bỏ qua`,
              );
              skippedCount++;
              continue;
            }

            // Gọi processWarehouse (đang được implement bởi người khác hoặc ở module khác)
            // Hiện tại disable theo yêu cầu user
            this.logger.debug(
              `[Warehouse Auto] Skip processing warehouse for ${docCode} (Disabled feature)`,
            );
            skippedCount++;
          } else {
            // Các trường hợp khác ??
            skippedCount++;
          }
        } catch (error: any) {
          errorCount++;
          this.logger.error(
            `[Warehouse Auto] Lỗi khi xử lý docCode ${docCode}: ${error?.message || error}`,
          );
        }
      }

      this.logger.log(
        `[Warehouse Auto] Hoàn thành xử lý cho brand ${brand} ngày ${date}: ${processedCount} processed, ${skippedCount} skipped, ${errorCount} errors`,
      );
    } catch (error: any) {
      this.logger.error(
        `[Warehouse Auto] Lỗi khi processWarehouseForStockTransfers: ${error?.message || error}`,
      );
      // Không throw để không block sync process
    }
  }

  async syncErrorStockTransferBySoCode(soCode: string): Promise<{
    success: boolean;
    message: string;
    updated: number;
    failed: number;
    details: Array<{
      id: string;
      itemCode: string;
      oldMaterialCode: string;
      newMaterialCode: string;
    }>;
  }> {
    if (!soCode) {
      return {
        success: false,
        message: 'Thiếu tham số soCode',
        updated: 0,
        failed: 0,
        details: [],
      };
    }

    try {
      const stockTransfers = await this.stockTransferRepository.find({
        where: { soCode },
      });

      if (stockTransfers.length === 0) {
        return {
          success: false,
          message: `Không tìm thấy stock transfer nào với soCode ${soCode}`,
          updated: 0,
          failed: 0,
          details: [],
        };
      }

      this.logger.log(
        `[SyncErrorStockTransfer] Found ${stockTransfers.length} records for ${soCode}`,
      );

      const itemCodes = new Set<string>();
      stockTransfers.forEach((st) => {
        if (st.itemCode) {
          itemCodes.add(st.itemCode);
        }
      });

      const loyaltyProductMap = await this.loyaltyService.fetchProducts(
        Array.from(itemCodes),
      );

      let updatedCount = 0;
      let failedCount = 0;
      const details: Array<{
        id: string;
        itemCode: string;
        oldMaterialCode: string;
        newMaterialCode: string;
      }> = [];

      for (const st of stockTransfers) {
        if (!st.itemCode) continue;

        const loyaltyProduct = loyaltyProductMap.get(st.itemCode);
        const newMaterialCode = loyaltyProduct?.materialCode;

        if (newMaterialCode && st.materialCode !== newMaterialCode) {
          const oldMaterialCode = st.materialCode;
          st.materialCode = newMaterialCode;

          try {
            await this.stockTransferRepository.save(st);
            updatedCount++;
            details.push({
              id: st.id,
              itemCode: st.itemCode,
              oldMaterialCode: oldMaterialCode || 'NULL',
              newMaterialCode: newMaterialCode,
            });
            this.logger.log(
              `[SyncErrorStockTransfer] Updated ${st.docCode} item ${st.itemCode}: ${oldMaterialCode} -> ${newMaterialCode}`,
            );
          } catch (error) {
            failedCount++;
            this.logger.error(
              `[SyncErrorStockTransfer] Failed to update ${st.docCode} item ${st.itemCode}: ${error}`,
            );
          }
        } else {
          if (!newMaterialCode && !st.materialCode) {
            failedCount++;
          }
        }
      }

      return {
        success: true,
        message: `Đã xử lý ${stockTransfers.length} dòng. Cập nhật: ${updatedCount}, Không đổi/Lỗi: ${stockTransfers.length - updatedCount}`,
        updated: updatedCount,
        failed: failedCount,
        details,
      };
    } catch (error: any) {
      this.logger.error(
        `[SyncErrorStockTransfer] Error processing ${soCode}: ${error?.message || error}`,
      );
      return {
        success: false,
        message: `Lỗi xử lý: ${error?.message || error}`,
        updated: 0,
        failed: 0,
        details: [],
      };
    }
  }
}
