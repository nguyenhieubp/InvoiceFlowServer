import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { VoucherIssue } from '../../entities/voucher-issue.entity';
import { ZappyApiService } from '../../services/zappy-api.service';

@Injectable()
export class VoucherIssueService {
  private readonly logger = new Logger(VoucherIssueService.name);

  constructor(
    @InjectRepository(VoucherIssue)
    private voucherIssueRepository: Repository<VoucherIssue>,
    private zappyApiService: ZappyApiService,
  ) {}

  /**
   * Helper: Parse voucher date string to Date object
   */
  private parseVoucherDate(dateStr: string | null | undefined): Date | null {
    if (!dateStr || typeof dateStr !== 'string') return null;
    try {
      const trimmed = dateStr.trim();
      if (!trimmed) return null;

      if (trimmed.includes('T') || trimmed.includes('Z')) {
        const isoDate = new Date(trimmed);
        if (isNaN(isoDate.getTime())) {
          return null;
        }
        return isoDate;
      }

      // Format: DD/MM/YYYY HH:mm or DD/MM/YYYY
      const parts = trimmed.split(' ');
      const datePart = parts[0];
      const timePart = parts[1] || '00:00';

      if (!datePart || !datePart.includes('/')) {
        return null;
      }

      const [day, month, year] = datePart.split('/');
      const [hours, minutes] = timePart.split(':');

      const yearNum = parseInt(year, 10);
      const monthNum = parseInt(month, 10);
      const dayNum = parseInt(day, 10);
      const hoursNum = parseInt(hours || '0', 10);
      const minutesNum = parseInt(minutes || '0', 10);

      if (isNaN(yearNum) || isNaN(monthNum) || isNaN(dayNum)) {
        return null;
      }

      const date = new Date(
        yearNum,
        monthNum - 1,
        dayNum,
        isNaN(hoursNum) ? 0 : hoursNum,
        isNaN(minutesNum) ? 0 : minutesNum,
      );

      if (isNaN(date.getTime())) {
        return null;
      }

      return date;
    } catch (error) {
      return null;
    }
  }

  /**
   * Đồng bộ dữ liệu danh sách Voucher Issue từ API
   * Refactored: Flatten structure - One row per detail line
   */
  async syncVoucherIssue(
    dateFrom: string,
    dateTo: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    recordsCount: number;
    savedCount: number;
    updatedCount: number;
    errors?: string[];
  }> {
    try {
      const brands = brand ? [brand] : ['f3', 'labhair', 'yaman', 'menard'];
      let totalRecordsCount = 0;
      let totalSavedCount = 0;
      let totalUpdatedCount = 0;
      const allErrors: string[] = [];

      for (const brandName of brands) {
        try {
          this.logger.log(
            `[VoucherIssue] Đang đồng bộ ${brandName} cho khoảng ${dateFrom} - ${dateTo}`,
          );

          // 1. Fetch summary list
          const voucherIssueData = await this.zappyApiService.getVoucherIssue(
            dateFrom,
            dateTo,
            brandName,
          );

          if (!voucherIssueData || voucherIssueData.length === 0) {
            this.logger.log(
              `[VoucherIssue] Không có dữ liệu cho ${brandName} - khoảng ${dateFrom} - ${dateTo}`,
            );
            continue;
          }

          this.logger.log(
            `[VoucherIssue] Tìm thấy ${voucherIssueData.length} vouchers summary. Bắt đầu fetch details...`,
          );

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          const BATCH_SIZE = 10;
          for (let i = 0; i < voucherIssueData.length; i += BATCH_SIZE) {
            const batch = voucherIssueData.slice(i, i + BATCH_SIZE);

            // Fetch details concurrently for the batch
            const batchResults = await Promise.all(
              batch.map(async (voucherData) => {
                const apiId =
                  typeof voucherData.id === 'number'
                    ? voucherData.id
                    : parseInt(String(voucherData.id), 10);

                if (isNaN(apiId)) {
                  return { voucherData, details: null, error: 'Invalid ID' };
                }

                try {
                  const details =
                    await this.zappyApiService.getVoucherIssueDetail(
                      apiId,
                      brandName,
                    );
                  return { voucherData, details, error: null };
                } catch (e: any) {
                  return {
                    voucherData,
                    details: null,
                    error: e?.message || e,
                  };
                }
              }),
            );

            // Process batch results
            for (const result of batchResults) {
              const { voucherData, details, error } = result;
              const apiId = Number(voucherData.id);

              if (error) {
                const msg = `Lỗi khi lấy details voucher ${apiId}: ${error}`;
                this.logger.error(msg);
                brandErrors.push(msg);
                continue;
              }

              // Flatten logic:
              // Iterate through 'series' in contents.
              // Use series or items? User example showed "series".
              // details usually has structure: { items: [], series: [...] } or just [...]?
              // Assuming details is the object containing series.

              let lines: any[] = [];
              if (details && details.series && Array.isArray(details.series)) {
                lines = details.series;
              } else if (
                details &&
                details.items &&
                Array.isArray(details.items)
              ) {
                // Fallback to items if series is empty/missing but items exist?
                // User said "series". Let's stick to series first.
                // If both are empty, we might want to create 1 dummy row or skip?
                // User requirement: "theo dạng line". If no line, maybe no record?
                // Or maybe just create one record with empty serial?
                // Implementation: If lines exist, create rows. If NO lines, create 1 row with null serial.
              }

              for (const line of lines) {
                try {
                  const serial = line.serial || null;
                  // Unique key logic: api_id + serial + brand
                  // If serial is null, it's just api_id + brand (which might duplicate if we have multiple nulls?)
                  // Ideally serial should be unique per voucher.

                  const existingRecord =
                    await this.voucherIssueRepository.findOne({
                      where: {
                        api_id: apiId,
                        brand: brandName,
                        serial: serial, // Check logic matches Index
                      },
                    });

                  // Map common fields from parent
                  const docdate = this.parseVoucherDate(voucherData.docdate);
                  const validFromdate = this.parseVoucherDate(
                    voucherData.valid_fromdate,
                  );
                  const validTodate = this.parseVoucherDate(
                    voucherData.valid_todate,
                  );
                  const enteredat = this.parseVoucherDate(
                    voucherData.enteredat,
                  );

                  // Line specific dates (user mentioned valid_todate in series)
                  const lineValidFrom = this.parseVoucherDate(
                    line.valid_fromdate,
                  );
                  const lineValidTo = this.parseVoucherDate(line.valid_todate);

                  const entityData: Partial<VoucherIssue> = {
                    api_id: apiId,
                    brand: brandName,
                    code: voucherData.code,
                    status_lov: voucherData.status_lov,
                    docdate: docdate || undefined,
                    description: voucherData.description,
                    brand_code: voucherData.brand_code,
                    apply_for_branch_types: voucherData.apply_for_branch_types,
                    val: Number(voucherData.val || 0),
                    percent:
                      voucherData.percent !== undefined
                        ? Number(voucherData.percent)
                        : 0,
                    max_value: Number(voucherData.max_value || 0),
                    saletype: voucherData.saletype,
                    enable_precost: voucherData.enable_precost,
                    supplier_support_fee: Number(
                      voucherData.supplier_support_fee || 0,
                    ),
                    valid_fromdate: validFromdate || undefined,
                    valid_todate: validTodate || undefined,
                    valid_days_from_so: Number(
                      voucherData.valid_days_from_so || 0,
                    ),
                    check_ownership: voucherData.check_ownership,
                    allow_cashback: voucherData.allow_cashback,
                    prom_for_employee: voucherData.prom_for_employee,
                    bonus_for_sale_employee:
                      voucherData.bonus_for_sale_employee,
                    so_percent:
                      voucherData.so_percent !== undefined
                        ? Number(voucherData.so_percent)
                        : null,
                    r_total_scope: voucherData.r_total_scope,
                    ecode_item_code: voucherData.ecode_item_code,
                    voucher_item_code: voucherData.voucher_item_code,
                    voucher_item_name: voucherData.voucher_item_name,
                    cost_for_gl: Number(voucherData.cost_for_gl || 0),
                    buy_items_by_date_range:
                      voucherData.buy_items_by_date_range,
                    buy_items_option_name: voucherData.buy_items_option_name,
                    disable_bonus_point_for_sale:
                      voucherData.disable_bonus_point_for_sale,
                    disable_bonus_point: voucherData.disable_bonus_point,
                    for_mkt_kol: voucherData.for_mkt_kol,
                    for_mkt_prom: voucherData.for_mkt_prom,
                    allow_apply_for_promoted_so:
                      voucherData.allow_apply_for_promoted_so,
                    campaign_code: voucherData.campaign_code,
                    sl_max_sudung_cho_1_kh: Number(
                      voucherData.sl_max_sudung_cho_1_kh || 0,
                    ),
                    is_locked: voucherData.is_locked,
                    enteredat: enteredat || undefined,
                    enteredby: voucherData.enteredby,
                    material_type: voucherData.material_type,
                    applyfor_wso: voucherData.applyfor_wso,
                    partnership: voucherData.partnership,
                    sync_date_from: dateFrom,
                    sync_date_to: dateTo,

                    // FLATTENED FIELDS
                    serial: serial,
                    console_code: line.console_code || undefined,
                    valid_fromdate_detail: lineValidFrom || undefined,
                    valid_todate_detail: lineValidTo || undefined,
                  };

                  if (existingRecord) {
                    await this.voucherIssueRepository.update(
                      existingRecord.id,
                      entityData,
                    );
                    brandUpdatedCount++;
                  } else {
                    const newEntity =
                      this.voucherIssueRepository.create(entityData);
                    await this.voucherIssueRepository.save(newEntity);
                    brandSavedCount++;
                  }
                  totalRecordsCount++;
                } catch (saveError: any) {
                  const errorMsg = `Lỗi lưu voucher ${apiId} (serial: ${line.serial}): ${saveError?.message || saveError}`;
                  this.logger.error(errorMsg);
                  brandErrors.push(errorMsg);
                }
              }
            }
          }

          totalSavedCount += brandSavedCount;
          totalUpdatedCount += brandUpdatedCount;
          allErrors.push(...brandErrors);

          this.logger.log(
            `[VoucherIssue] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`,
          );
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ voucher issue: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ danh sách Voucher Issue thành công: ${totalRecordsCount} records, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        recordsCount: totalRecordsCount,
        savedCount: totalSavedCount,
        updatedCount: totalUpdatedCount,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ voucher issue: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ danh sách Voucher Issue theo khoảng thời gian
   * @param startDate - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncVoucherIssueByDateRange(
    startDate: string,
    endDate: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    totalRecordsCount: number;
    totalSavedCount: number;
    totalUpdatedCount: number;
    brandResults?: Array<{
      brand: string;
      recordsCount: number;
      savedCount: number;
      updatedCount: number;
      errors?: string[];
    }>;
    errors?: string[];
  }> {
    try {
      const brands = brand ? [brand] : ['f3', 'labhair', 'yaman', 'menard'];
      let totalRecordsCount = 0;
      let totalSavedCount = 0;
      let totalUpdatedCount = 0;
      const allErrors: string[] = [];
      const brandResults: Array<{
        brand: string;
        recordsCount: number;
        savedCount: number;
        updatedCount: number;
        errors?: string[];
      }> = [];

      // API này nhận date range, không cần lặp từng ngày
      for (const brandName of brands) {
        try {
          this.logger.log(
            `[syncVoucherIssueByDateRange] Bắt đầu đồng bộ brand: ${brandName}`,
          );
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          try {
            this.logger.log(
              `[syncVoucherIssueByDateRange] Đồng bộ ${brandName} - khoảng ${startDate} - ${endDate}`,
            );
            const result = await this.syncVoucherIssue(
              startDate,
              endDate,
              brandName,
            );

            brandRecordsCount = result.recordsCount;
            brandSavedCount = result.savedCount;
            brandUpdatedCount = result.updatedCount;

            if (result.errors && result.errors.length > 0) {
              brandErrors.push(...result.errors);
            }
          } catch (error: any) {
            const errorMsg = `[${brandName}] Lỗi khi đồng bộ khoảng ${startDate} - ${endDate}: ${error?.message || error}`;
            this.logger.error(errorMsg);
            brandErrors.push(errorMsg);
          }

          totalRecordsCount += brandRecordsCount;
          totalSavedCount += brandSavedCount;
          totalUpdatedCount += brandUpdatedCount;
          allErrors.push(...brandErrors);

          brandResults.push({
            brand: brandName,
            recordsCount: brandRecordsCount,
            savedCount: brandSavedCount,
            updatedCount: brandUpdatedCount,
            errors: brandErrors.length > 0 ? brandErrors : undefined,
          });

          this.logger.log(
            `[syncVoucherIssueByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} voucher`,
          );
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ voucher issue: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ danh sách Voucher Issue thành công từ ${startDate} đến ${endDate}: ${totalRecordsCount} voucher, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        totalRecordsCount,
        totalSavedCount,
        totalUpdatedCount,
        brandResults: brandResults.length > 0 ? brandResults : undefined,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ voucher issue theo khoảng thời gian: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Lấy danh sách Voucher Issue với filter và pagination
   */
  async getVoucherIssue(params: {
    page?: number;
    limit?: number;
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    status?: string;
    serial?: string;
    code?: string;
  }): Promise<{
    success: boolean;
    data: any[];
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

      const queryBuilder = this.voucherIssueRepository
        .createQueryBuilder('vi')

        .orderBy('vi.docdate', 'DESC')
        .addOrderBy('vi.code', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('vi.brand = :brand', { brand: params.brand });
      }

      // Filter by status
      if (params.status) {
        queryBuilder.andWhere('vi.status_lov = :status', {
          status: params.status,
        });
      }

      // Filter by serial
      if (params.serial) {
        queryBuilder.andWhere('vi.serial LIKE :serial', {
          serial: `%${params.serial}%`,
        });
      }

      // Filter by code
      if (params.code) {
        queryBuilder.andWhere('vi.code LIKE :code', {
          code: `%${params.code}%`,
        });
      }

      // Filter by dateFrom
      if (params.dateFrom) {
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
        const fromDate = parseDate(params.dateFrom);
        queryBuilder.andWhere('vi.docdate >= :dateFrom', {
          dateFrom: fromDate,
        });
      }

      // Filter by dateTo
      if (params.dateTo) {
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
          return new Date(year, month, day, 23, 59, 59);
        };
        const toDate = parseDate(params.dateTo);
        queryBuilder.andWhere('vi.docdate <= :dateTo', { dateTo: toDate });
      }

      // Get total count
      const total = await queryBuilder.getCount();

      // Apply pagination
      queryBuilder.skip(skip).take(limit);

      // Get data
      const data = await queryBuilder.getMany();

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
        `Error getting voucher issue: ${error?.message || error}`,
      );
      throw error;
    }
  }
}
