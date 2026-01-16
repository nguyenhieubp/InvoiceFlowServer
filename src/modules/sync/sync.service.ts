import {
  Injectable,
  Logger,
  Inject,
  forwardRef,
  InternalServerErrorException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import * as XLSX from 'xlsx-js-style';
import { Customer } from '../../entities/customer.entity';
import { Sale } from '../../entities/sale.entity';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { StockTransfer } from '../../entities/stock-transfer.entity';
import { WarehouseProcessed } from '../../entities/warehouse-processed.entity';
import { ShiftEndCash } from '../../entities/shift-end-cash.entity';
import { ShiftEndCashLine } from '../../entities/shift-end-cash-line.entity';
import { RepackFormula } from '../../entities/repack-formula.entity';
import { RepackFormulaItem } from '../../entities/repack-formula-item.entity';
import { Promotion } from '../../entities/promotion.entity';
import { PromotionLine } from '../../entities/promotion-line.entity';
import { VoucherIssue } from '../../entities/voucher-issue.entity';

import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import { SalesSyncService } from '../sales/sales-sync.service';

import { SalesService } from '../sales/sales.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { FastApiClientService } from '../../services/fast-api-client.service';
import { Order } from 'src/types/order.types';
import { formatZappyDate, parseZappyDate } from 'src/utils/convert.utils';

@Injectable()
export class SyncService {
  private readonly logger = new Logger(SyncService.name);

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

  constructor(
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    @InjectRepository(WarehouseProcessed)
    private warehouseProcessedRepository: Repository<WarehouseProcessed>,
    @InjectRepository(ShiftEndCash)
    private shiftEndCashRepository: Repository<ShiftEndCash>,
    @InjectRepository(ShiftEndCashLine)
    private shiftEndCashLineRepository: Repository<ShiftEndCashLine>,
    @InjectRepository(RepackFormula)
    private repackFormulaRepository: Repository<RepackFormula>,
    @InjectRepository(RepackFormulaItem)
    private repackFormulaItemRepository: Repository<RepackFormulaItem>,
    @InjectRepository(Promotion)
    private promotionRepository: Repository<Promotion>,
    @InjectRepository(PromotionLine)
    private promotionLineRepository: Repository<PromotionLine>,
    @InjectRepository(VoucherIssue)
    private voucherIssueRepository: Repository<VoucherIssue>,

    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private loyaltyService: LoyaltyService,
    @Inject(forwardRef(() => SalesService))
    private salesService: SalesService,
    @Inject(forwardRef(() => FastApiInvoiceFlowService))
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    private fastApiClientService: FastApiClientService,
    private salesSyncService: SalesSyncService,
  ) {}

  async syncBrand(
    brandName: string,
    date: string,
  ): Promise<{
    success: boolean;
    message: string;
    ordersCount: number;
    salesCount: number;
    customersCount: number;
    invoiceSuccessCount?: number;
    invoiceFailureCount?: number;
    errors?: string[];
    invoiceErrors?: string[];
  }> {
    // Sync từ Zappy API với brand cụ thể
    this.logger.log(
      `Đang đồng bộ dữ liệu từ Zappy API cho ngày ${date} (brand: ${brandName})`,
    );
    return this.syncFromZappy(date, brandName);
  }

  async syncFromZappy(
    date: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    ordersCount: number;
    salesCount: number;
    customersCount: number;
    invoiceSuccessCount?: number;
    invoiceFailureCount?: number;
    errors?: string[];
    invoiceErrors?: string[];
  }> {
    this.logger.log(
      `[SyncService] Checkpoint: Delegating syncFromZappy to SalesSyncService`,
    );
    return this.salesSyncService.syncFromZappy(date, brand);
  }

  /**
   * Đồng bộ dữ liệu xuất kho từ Zappy API
   * @param date - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard)
   */
  async syncStockTransfer(
    date: string,
    brand: string,
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
      const parts = [1, 2, 3];
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
      try {
        await this.processWarehouseForStockTransfers(date, brand);
      } catch (warehouseError: any) {
        this.logger.warn(
          `[Stock Transfer] Lỗi khi xử lý warehouse tự động cho brand ${brand} ngày ${date}: ${warehouseError?.message || warehouseError}`,
        );
        // Không throw error để không chặn flow sync chính
      }

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

  async getDailyWsaleByDateRange(
    startDate: string,
    endDate: string,
    brand?: string,
  ): Promise<any> {
    try {
      let ordersCount = 0;
      let salesCount = 0;
      let customersCount = 0;

      const start = parseZappyDate(startDate);
      const end = parseZappyDate(endDate);

      for (
        let date = new Date(start);
        date <= end;
        date.setDate(date.getDate() + 1)
      ) {
        const zappyDate = formatZappyDate(date);

        const dailyResult = await this.getDailyWsale(zappyDate, brand);

        ordersCount += dailyResult.ordersCount || 0;
        salesCount += dailyResult.salesCount || 0;
        customersCount += dailyResult.customersCount || 0;
      }

      return {
        success: true,
        message: `Đồng bộ bán buôn từ ${startDate} đến ${endDate} (brand: ${brand}) thành công`,
        ordersCount,
        salesCount,
        customersCount,
      };
    } catch (error: any) {
      this.logger.error(
        `Error getting daily Wsale by date range: ${error?.message || error}`,
      );
      throw error;
    }
  }

  async getDailyWsale(date: string, brand?: string): Promise<any> {
    try {
      const orders = await this.zappyApiService.getDailyWsale(date, brand);

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
            customer = (await this.customerRepository.save(
              newCustomer,
            )) as unknown as Customer;
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
            customer = (await this.customerRepository.save(
              customer,
            )) as unknown as Customer;
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
                  saleperson_id: this.validateInteger(saleItem.saleperson_id),
                  partner_name: saleItem.partner_name,
                  order_source: saleItem.order_source,
                  // Lưu mvc_serial vào maThe
                  maThe: saleItem.mvc_serial,
                  // Category fields

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
                  type_sale: 'WHOLESALE',
                  disc_tm: saleItem.disc_tm,
                  disc_ctkm: saleItem.disc_ctkm,
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
        `Error getting daily Wsale: ${error?.message || error}`,
      );
      throw error;
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

      // Lặp qua từng ngày
      const currentDate = new Date(startDate.getTime());
      while (currentDate <= endDate) {
        const dateStr = formatToDDMMMYYYY(currentDate);

        // Đồng bộ cho từng brand
        for (const brandItem of brands) {
          try {
            this.logger.log(
              `[Stock Transfer Range] Đang đồng bộ brand ${brandItem} cho ngày ${dateStr}`,
            );
            const result = await this.syncStockTransfer(dateStr, brandItem);

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

            this.logger.log(
              `[Stock Transfer Range] Hoàn thành đồng bộ brand ${brandItem} cho ngày ${dateStr}`,
            );
          } catch (error: any) {
            const errorMsg = `Lỗi khi đồng bộ stock transfer cho brand ${brandItem} ngày ${dateStr}: ${error?.message || error}`;
            this.logger.error(`[Stock Transfer Range] ${errorMsg}`);
            errors.push(errorMsg);
          }
        }

        // Tăng 1 ngày
        currentDate.setDate(currentDate.getDate() + 1);
      }

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
        queryBuilder.andWhere('st.itemCode LIKE :itemCode', {
          itemCode: `%${params.itemCode}%`,
        });
      }
      if (params.soCode) {
        queryBuilder.andWhere('st.soCode = :soCode', { soCode: params.soCode });
      }
      if (params.docCode) {
        // Use POSITION function instead of LIKE to avoid escaping issues with _ and %
        // POSITION returns > 0 if substring is found
        queryBuilder.andWhere('POSITION(:docCode IN st.docCode) > 0', {
          docCode: params.docCode,
        });
      }
      if (params.dateFrom) {
        // Parse DDMMMYYYY to Date
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
        queryBuilder.andWhere('st.transDate >= :dateFrom', {
          dateFrom: fromDate,
        });
      }
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
        queryBuilder.andWhere('st.transDate <= :dateTo', { dateTo: toDate });
      }

      // Order by transDate DESC
      queryBuilder.orderBy('st.transDate', 'DESC');

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
        `Error getting stock transfers: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ báo cáo nộp quỹ cuối ca từ ERP API
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard). Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncShiftEndCash(
    date: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    message: string;
    recordsCount: number;
    savedCount: number;
    updatedCount: number;
    errors?: string[];
    newRecordIds?: string[]; // Danh sách ID các records mới được tạo
  }> {
    try {
      const brands = brand ? [brand] : ['f3', 'labhair', 'yaman', 'menard'];
      let totalRecordsCount = 0;
      let totalSavedCount = 0;
      let totalUpdatedCount = 0;
      const allErrors: string[] = [];
      const allNewRecordIds: string[] = []; // Track các record mới được tạo

      for (const brandName of brands) {
        try {
          this.logger.log(
            `[ShiftEndCash] Đang đồng bộ ${brandName} cho ngày ${date}`,
          );

          // Lấy dữ liệu từ API
          const shiftEndCashData = await this.zappyApiService.getShiftEndCash(
            date,
            brandName,
          );

          if (!shiftEndCashData || shiftEndCashData.length === 0) {
            this.logger.log(
              `[ShiftEndCash] Không có dữ liệu cho ${brandName} - ngày ${date}`,
            );
            continue;
          }

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];
          const brandNewRecordIds: string[] = []; // Track các record mới được tạo trong brand này

          // Parse date string sang Date object
          const parseDateString = (
            dateStr: string | null | undefined,
          ): Date | null => {
            if (!dateStr) return null;
            try {
              // Format: "01/11/2025 10:16" hoặc ISO string
              if (dateStr.includes('T') || dateStr.includes('Z')) {
                return new Date(dateStr);
              }
              // Format: "01/11/2025 10:16"
              const parts = dateStr.split(' ');
              const datePart = parts[0]; // "01/11/2025"
              const timePart = parts[1] || '00:00'; // "10:16"
              const [day, month, year] = datePart.split('/');
              const [hours, minutes] = timePart.split(':');
              return new Date(
                parseInt(year),
                parseInt(month) - 1,
                parseInt(day),
                parseInt(hours) || 0,
                parseInt(minutes) || 0,
              );
            } catch (error) {
              this.logger.warn(`Failed to parse date: ${dateStr}`);
              return null;
            }
          };

          // Xử lý từng record
          for (const record of shiftEndCashData) {
            try {
              // Kiểm tra xem đã có record với api_id và brand này chưa
              const existingRecord = await this.shiftEndCashRepository.findOne({
                where: { api_id: record.id, brand: brandName },
              });

              // Parse dates - đảm bảo lưu đúng giá trị, kể cả null
              const openat =
                record.openat !== undefined &&
                record.openat !== null &&
                record.openat !== ''
                  ? new Date(record.openat)
                  : null;
              const closedat =
                record.closedat !== undefined &&
                record.closedat !== null &&
                record.closedat !== ''
                  ? new Date(record.closedat)
                  : null;
              const docdate = parseDateString(record.docdate);
              const gl_date = parseDateString(record.gl_date);
              const enteredat = parseDateString(record.enteredat);

              if (existingRecord) {
                // Update existing record - lưu TẤT CẢ giá trị từ API, kể cả null, empty string, 0
                existingRecord.draw_code =
                  record.draw_code !== undefined && record.draw_code !== null
                    ? record.draw_code
                    : existingRecord.draw_code;
                existingRecord.branch_code =
                  record.branch_code !== undefined &&
                  record.branch_code !== null
                    ? record.branch_code
                    : existingRecord.branch_code;
                existingRecord.status =
                  record.status !== undefined && record.status !== null
                    ? record.status
                    : existingRecord.status;
                existingRecord.teller_code =
                  record.teller_code !== undefined &&
                  record.teller_code !== null
                    ? record.teller_code
                    : existingRecord.teller_code;
                existingRecord.openat =
                  openat !== undefined && openat !== null
                    ? openat
                    : existingRecord.openat;
                existingRecord.closedat =
                  closedat !== undefined && closedat !== null
                    ? closedat
                    : existingRecord.closedat;
                existingRecord.shift_status =
                  record.shift_status !== undefined &&
                  record.shift_status !== null
                    ? record.shift_status
                    : existingRecord.shift_status;
                existingRecord.docdate =
                  docdate !== undefined && docdate !== null
                    ? docdate
                    : existingRecord.docdate;
                existingRecord.gl_date =
                  gl_date !== undefined && gl_date !== null
                    ? gl_date
                    : existingRecord.gl_date;
                existingRecord.description =
                  record.description !== undefined &&
                  record.description !== null
                    ? record.description
                    : existingRecord.description;
                existingRecord.total =
                  record.total !== undefined && record.total !== null
                    ? Number(record.total)
                    : existingRecord.total;
                existingRecord.enteredat =
                  enteredat !== undefined && enteredat !== null
                    ? enteredat
                    : existingRecord.enteredat;
                existingRecord.enteredby =
                  record.enteredby !== undefined && record.enteredby !== null
                    ? record.enteredby
                    : existingRecord.enteredby;
                existingRecord.sync_date = date;

                // Xóa các lines cũ và tạo mới
                if (record.lines && Array.isArray(record.lines)) {
                  await this.shiftEndCashLineRepository.delete({
                    shiftEndCashId: existingRecord.id,
                  });

                  const linesToCreate = record.lines.map((line: any) => ({
                    shiftEndCashId: existingRecord.id,
                    fop_code:
                      line.fop_code !== undefined && line.fop_code !== null
                        ? line.fop_code
                        : null,
                    fop_name:
                      line.fop_name !== undefined && line.fop_name !== null
                        ? line.fop_name
                        : null,
                    system_amt:
                      line.system_amt !== undefined && line.system_amt !== null
                        ? Number(line.system_amt)
                        : 0,
                    sys_acct_code:
                      line.sys_acct_code !== undefined &&
                      line.sys_acct_code !== null
                        ? line.sys_acct_code
                        : null,
                    actual_amt:
                      line.actual_amt !== undefined && line.actual_amt !== null
                        ? Number(line.actual_amt)
                        : 0,
                    actual_acct_code:
                      line.actual_acct_code !== undefined &&
                      line.actual_acct_code !== null
                        ? line.actual_acct_code
                        : null,
                    diff_amount:
                      line.diff_amount !== undefined &&
                      line.diff_amount !== null
                        ? Number(line.diff_amount)
                        : 0,
                    diff_acct_code:
                      line.diff_acct_code !== undefined &&
                      line.diff_acct_code !== null
                        ? line.diff_acct_code
                        : null,
                    template_id:
                      line.template_id !== undefined &&
                      line.template_id !== null
                        ? Number(line.template_id)
                        : null,
                  }));
                  const lines =
                    this.shiftEndCashLineRepository.create(linesToCreate);
                  await this.shiftEndCashLineRepository.save(lines);
                }

                await this.shiftEndCashRepository.save(existingRecord);
                brandUpdatedCount++;
              } else {
                // Tạo record mới - lưu TẤT CẢ giá trị từ API
                const newRecord = this.shiftEndCashRepository.create({
                  api_id: record.id,
                  draw_code:
                    record.draw_code !== undefined && record.draw_code !== null
                      ? record.draw_code
                      : '',
                  branch_code:
                    record.branch_code !== undefined &&
                    record.branch_code !== null
                      ? record.branch_code
                      : null,
                  status:
                    record.status !== undefined && record.status !== null
                      ? record.status
                      : null,
                  teller_code:
                    record.teller_code !== undefined &&
                    record.teller_code !== null
                      ? record.teller_code
                      : null,
                  openat:
                    openat !== undefined && openat !== null ? openat : null,
                  closedat:
                    closedat !== undefined && closedat !== null
                      ? closedat
                      : null,
                  shift_status:
                    record.shift_status !== undefined &&
                    record.shift_status !== null
                      ? record.shift_status
                      : null,
                  docdate:
                    docdate !== undefined && docdate !== null ? docdate : null,
                  gl_date:
                    gl_date !== undefined && gl_date !== null ? gl_date : null,
                  description:
                    record.description !== undefined &&
                    record.description !== null
                      ? record.description
                      : null,
                  total:
                    record.total !== undefined && record.total !== null
                      ? Number(record.total)
                      : 0,
                  enteredat:
                    enteredat !== undefined && enteredat !== null
                      ? enteredat
                      : null,
                  enteredby:
                    record.enteredby !== undefined && record.enteredby !== null
                      ? record.enteredby
                      : null,
                  sync_date: date,
                  brand: brandName,
                } as any);

                const savedRecord = (await this.shiftEndCashRepository.save(
                  newRecord,
                )) as unknown as ShiftEndCash;

                // Tạo lines
                if (record.lines && Array.isArray(record.lines)) {
                  const linesToCreate = record.lines.map((line: any) => ({
                    shiftEndCashId: savedRecord.id,
                    fop_code:
                      line.fop_code !== undefined && line.fop_code !== null
                        ? line.fop_code
                        : null,
                    fop_name:
                      line.fop_name !== undefined && line.fop_name !== null
                        ? line.fop_name
                        : null,
                    system_amt:
                      line.system_amt !== undefined && line.system_amt !== null
                        ? Number(line.system_amt)
                        : 0,
                    sys_acct_code:
                      line.sys_acct_code !== undefined &&
                      line.sys_acct_code !== null
                        ? line.sys_acct_code
                        : null,
                    actual_amt:
                      line.actual_amt !== undefined && line.actual_amt !== null
                        ? Number(line.actual_amt)
                        : 0,
                    actual_acct_code:
                      line.actual_acct_code !== undefined &&
                      line.actual_acct_code !== null
                        ? line.actual_acct_code
                        : null,
                    diff_amount:
                      line.diff_amount !== undefined &&
                      line.diff_amount !== null
                        ? Number(line.diff_amount)
                        : 0,
                    diff_acct_code:
                      line.diff_acct_code !== undefined &&
                      line.diff_acct_code !== null
                        ? line.diff_acct_code
                        : null,
                    template_id:
                      line.template_id !== undefined &&
                      line.template_id !== null
                        ? Number(line.template_id)
                        : null,
                  }));
                  const lines =
                    this.shiftEndCashLineRepository.create(linesToCreate);
                  await this.shiftEndCashLineRepository.save(lines);
                }

                brandSavedCount++;
                brandNewRecordIds.push(savedRecord.id); // Track record mới
              }
            } catch (recordError: any) {
              const errorMsg = `[${brandName}] Lỗi khi xử lý record ${record.id || record.draw_code}: ${recordError?.message || recordError}`;
              this.logger.error(errorMsg);
              brandErrors.push(errorMsg);
            }
          }

          totalRecordsCount += shiftEndCashData.length;
          totalSavedCount += brandSavedCount;
          totalUpdatedCount += brandUpdatedCount;
          allErrors.push(...brandErrors);
          allNewRecordIds.push(...brandNewRecordIds); // Collect các record mới từ brand này

          this.logger.log(
            `[ShiftEndCash] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`,
          );
        } catch (brandError: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ shift end cash: ${brandError?.message || brandError}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ thành công ${totalRecordsCount} báo cáo nộp quỹ cuối ca: ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        recordsCount: totalRecordsCount,
        savedCount: totalSavedCount,
        updatedCount: totalUpdatedCount,
        errors: allErrors.length > 0 ? allErrors : undefined,
        newRecordIds: allNewRecordIds.length > 0 ? allNewRecordIds : undefined, // Trả về danh sách ID các records mới được tạo
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ shift end cash: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Lấy danh sách báo cáo nộp quỹ cuối ca với filter và pagination
   */
  async getShiftEndCash(params: {
    page?: number;
    limit?: number;
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    branchCode?: string;
    drawCode?: string;
    apiId?: number;
    onlyProcessed?: boolean;
    paymentSuccess?: boolean;
  }): Promise<{
    success: boolean;
    data: any[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
    };
    stats?: {
      success: number;
      failure: number;
      total: number;
    };
  }> {
    try {
      const page = params.page || 1;
      const limit = params.limit || 10;
      const skip = (page - 1) * limit;

      const queryBuilder = this.shiftEndCashRepository
        .createQueryBuilder('sec')
        .leftJoinAndSelect('sec.lines', 'lines')
        .orderBy('sec.docdate', 'DESC')
        .addOrderBy('sec.draw_code', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('sec.brand = :brand', { brand: params.brand });
      }

      // Filter by branchCode (sử dụng trường branch_code từ database)
      if (params.branchCode) {
        queryBuilder.andWhere('sec.branch_code = :branchCode', {
          branchCode: params.branchCode,
        });
      }

      // Filter by drawCode
      if (params.drawCode) {
        queryBuilder.andWhere('sec.draw_code = :drawCode', {
          drawCode: params.drawCode,
        });
      }

      // Filter by apiId
      if (params.apiId !== undefined && params.apiId !== null) {
        queryBuilder.andWhere('sec.api_id = :apiId', { apiId: params.apiId });
      }

      // Filter only processed (payment_success IS NOT NULL)
      if (params.onlyProcessed) {
        queryBuilder.andWhere('sec.payment_success IS NOT NULL');
      }

      // Filter by payment success status
      if (params.paymentSuccess !== undefined) {
        queryBuilder.andWhere('sec.payment_success = :paymentSuccess', {
          paymentSuccess: params.paymentSuccess,
        });
      }

      // Filter by dateFrom và dateTo - filter theo openat và closedat
      // Filter các record có openat hoặc closedat nằm trong khoảng dateFrom-dateTo
      if (params.dateFrom || params.dateTo) {
        const parseDate = (
          dateStr: string,
          isEndOfDay: boolean = false,
        ): Date => {
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
          if (isEndOfDay) {
            return new Date(year, month, day, 23, 59, 59, 999);
          }
          return new Date(year, month, day, 0, 0, 0, 0);
        };

        if (params.dateFrom && params.dateTo) {
          // Cả hai đều có: Filter record có openat HOẶC closedat nằm trong khoảng
          const fromDate = parseDate(params.dateFrom);
          const toDate = parseDate(params.dateTo, true);
          queryBuilder.andWhere(
            '(sec.openat BETWEEN :dateFrom AND :dateTo OR sec.closedat BETWEEN :dateFrom AND :dateTo)',
            { dateFrom: fromDate, dateTo: toDate },
          );
        } else if (params.dateFrom) {
          // Chỉ có dateFrom: Filter record có openat >= dateFrom HOẶC closedat >= dateFrom
          const fromDate = parseDate(params.dateFrom);
          queryBuilder.andWhere(
            '(sec.openat >= :dateFrom OR sec.closedat >= :dateFrom)',
            { dateFrom: fromDate },
          );
        } else if (params.dateTo) {
          // Chỉ có dateTo: Filter record có openat <= dateTo HOẶC closedat <= dateTo
          const toDate = parseDate(params.dateTo, true);
          queryBuilder.andWhere(
            '(sec.openat <= :dateTo OR sec.closedat <= :dateTo)',
            { dateTo: toDate },
          );
        }
      }

      // Clone query builder for stats (without pagination)
      const statsQueryBuilder = queryBuilder.clone();

      // Get total count
      const total = await queryBuilder.getCount();

      // Apply pagination
      queryBuilder.skip(skip).take(limit);

      // Get data
      const data = await queryBuilder.getMany();

      // Calculate stats
      const successCount = await statsQueryBuilder
        .clone()
        .andWhere('sec.payment_success = :successStatus', {
          successStatus: true,
        })
        .getCount();

      const failureCount = await statsQueryBuilder
        .clone()
        .andWhere('sec.payment_success = :failureStatus', {
          failureStatus: false,
        })
        .getCount();

      return {
        success: true,
        data,
        pagination: {
          page,
          limit,
          total,
          totalPages: Math.ceil(total / limit),
        },
        stats: {
          success: successCount,
          failure: failureCount,
          total,
        },
      };

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
        `Error getting shift end cash: ${error?.message || error}`,
      );
      throw error;
    }
  }

  async syncOdoo(dateFrom: string, dateTo: string): Promise<void> {
    try {
      this.logger.log(`Bắt đầu đồng bộ odoo từ ${dateFrom} đến ${dateTo}`);
      const response = await this.httpService.axiosRef.get(
        'https://ecs.vmt.vn/api/sale-orders',
        {
          headers: {
            accept: 'application/json',
            'Content-Type': 'application/json',
          },
          params: {
            token: 'chHIqq7u8bhm5rFD68be',
            date_from: dateFrom,
            date_to: dateTo,
          },
        },
      );
      const data = response?.data?.data;
      this.logger.log(`Đồng bộ odoo thành công: ${data}`);
      // return {
      //   success: true,
      //   message: 'Đồng bộ odoo thành công',
      //   data: data,
      // };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ odoo: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo phiếu chi tiền mặt (payment) từ báo cáo nộp quỹ cuối ca
   * @param shiftEndCashId - ID của báo cáo nộp quỹ cuối ca
   * @returns Kết quả tạo payment
   */
  async createPaymentFromShiftEndCash(shiftEndCashId: string): Promise<{
    success: boolean;
    message: string;
    data?: any;
    error?: string;
  }> {
    try {
      // Lấy shift end cash record
      const shiftEndCash = await this.shiftEndCashRepository.findOne({
        where: { id: shiftEndCashId },
        relations: ['lines'],
      });

      if (!shiftEndCash) {
        throw new Error(
          `Không tìm thấy báo cáo nộp quỹ cuối ca với ID: ${shiftEndCashId}`,
        );
      }

      // Ưu tiên dùng branch_code từ database, nếu không có thì extract từ draw_code
      let branchCode = shiftEndCash.branch_code;
      if (!branchCode) {
        const drawCode = shiftEndCash.draw_code || '';
        const branchCodeMatch = drawCode.match(/^([A-Z0-9]+)_/);
        branchCode = branchCodeMatch ? branchCodeMatch[1] : drawCode;
      }

      // Fetch department để lấy ma_dvcs và ma_bp
      let maDvcs = '';
      let maBp = '';
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
          { headers: { accept: 'application/json' } },
        );
        const department = response?.data?.data?.items?.[0];
        if (department) {
          maDvcs = department.ma_dvcs || department.ma_dvcs_ht || branchCode;
          maBp = department.ma_bp || branchCode;
        } else {
          maDvcs = branchCode;
          maBp = branchCode;
        }
      } catch (error) {
        this.logger.warn(
          `Không thể lấy department cho branchCode ${branchCode}, dùng giá trị mặc định`,
        );
        maDvcs = branchCode;
        maBp = branchCode;
      }

      // Build payment payload
      const docDate =
        shiftEndCash.docdate || shiftEndCash.gl_date || new Date();
      const totalAmount = Number(shiftEndCash.total || 0);

      // Format date to ISO string
      const formatDateISO = (date: Date): string => {
        if (!date) return new Date().toISOString();
        if (typeof date === 'string') {
          return new Date(date).toISOString();
        }
        return date.toISOString();
      };

      // Tìm dòng tiền mặt (CASH) từ lines để lấy số tiền
      const cashLine = shiftEndCash.lines?.find(
        (line: any) =>
          line.fop_code?.toUpperCase() === 'CASH' ||
          line.fop_name?.toLowerCase().includes('tiền mặt'),
      );

      // Ưu tiên dùng actual_amt từ cashLine, nếu không có thì dùng system_amt, cuối cùng là total
      const paymentAmount = cashLine
        ? Number(cashLine.actual_amt || cashLine.system_amt || 0)
        : totalAmount;

      const paymentPayload: any = {
        action: 0,
        ma_dvcs: maDvcs,
        ma_kh: branchCode || '', // Mã khách hàng = branchCode (chi nội bộ cho chi nhánh)
        loai_ct: '2', // 2 - Chi cho khách hàng
        dept_id: maBp,
        ngay_lct: formatDateISO(docDate),
        so_ct: shiftEndCash.draw_code || '', // Mã chứng từ = draw_code
        httt: 'CASH', // Hình thức thanh toán = CASH
        status: '0',
        dien_giai:
          shiftEndCash.description || `Chi tiền cho ${shiftEndCash.draw_code}`,
        detail: [
          {
            tien: paymentAmount,
            ma_bp: maBp,
          },
        ],
      };

      // Gọi payment API
      const result =
        await this.fastApiClientService.submitPayment(paymentPayload);

      // Validate response
      let isSuccess = false;
      let resultMessage = 'Tạo payment thất bại';

      if (Array.isArray(result) && result.length > 0) {
        const firstItem = result[0];
        if (firstItem.status === 1) {
          isSuccess = true;
          resultMessage = 'Tạo phiếu chi tiền mặt thành công';
        } else {
          resultMessage = firstItem.message || 'Tạo payment thất bại';
        }
      } else if (
        result &&
        typeof result === 'object' &&
        result.status !== undefined
      ) {
        if (result.status === 1) {
          isSuccess = true;
          resultMessage = 'Tạo phiếu chi tiền mặt thành công';
        } else {
          resultMessage = result.message || 'Tạo payment thất bại';
        }
      }

      // Update shift end cash status
      shiftEndCash.payment_success = isSuccess;
      shiftEndCash.payment_message = resultMessage;
      shiftEndCash.payment_date = new Date();
      shiftEndCash.payment_response = JSON.stringify(result);
      await this.shiftEndCashRepository.save(shiftEndCash);

      if (!isSuccess) {
        this.logger.error(
          `[ShiftEndCash Payment] Payment API trả về lỗi: ${resultMessage}`,
        );
        throw new Error(resultMessage);
      }

      this.logger.log(
        `[ShiftEndCash Payment] Tạo payment thành công cho shift end cash ${shiftEndCashId}`,
      );

      return {
        success: true,
        message: resultMessage,
        data: result,
      };
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      this.logger.error(
        `[ShiftEndCash Payment] Lỗi khi tạo payment: ${errorMessage}`,
      );

      // Update fail status if possible (and not already saved)
      try {
        const shiftEndCash = await this.shiftEndCashRepository.findOne({
          where: { id: shiftEndCashId },
        });
        if (shiftEndCash) {
          shiftEndCash.payment_success = false;
          shiftEndCash.payment_message = errorMessage;
          shiftEndCash.payment_date = new Date();
          await this.shiftEndCashRepository.save(shiftEndCash);
        }
      } catch (saveError) {
        this.logger.error(`Failed to save payment error status: ${saveError}`);
      }

      return {
        success: false,
        message: 'Lỗi khi tạo phiếu chi tiền mặt',
        error: errorMessage,
      };
    }
  }

  /**
   * Đồng bộ báo cáo nộp quỹ cuối ca theo khoảng thời gian
   * @param startDate - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncShiftEndCashByDateRange(
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
      // Parse dates
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

      const formatDate = (date: Date): string => {
        const day = date.getDate().toString().padStart(2, '0');
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
        const month = months[date.getMonth()];
        const year = date.getFullYear();
        return `${day}${month}${year}`;
      };

      const start = parseDate(startDate);
      const end = parseDate(endDate);

      if (start > end) {
        throw new Error('Ngày bắt đầu phải nhỏ hơn hoặc bằng ngày kết thúc');
      }

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

      for (const brandName of brands) {
        try {
          this.logger.log(
            `[syncShiftEndCashByDateRange] Bắt đầu đồng bộ brand: ${brandName}`,
          );
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Lặp qua từng ngày trong khoảng thời gian
          const currentDate = new Date(start);
          while (currentDate <= end) {
            const dateStr = formatDate(currentDate);
            try {
              this.logger.log(
                `[syncShiftEndCashByDateRange] Đồng bộ ${brandName} - ngày ${dateStr}`,
              );
              const result = await this.syncShiftEndCash(dateStr, brandName);

              brandRecordsCount += result.recordsCount;
              brandSavedCount += result.savedCount;
              brandUpdatedCount += result.updatedCount;

              if (result.errors && result.errors.length > 0) {
                brandErrors.push(
                  ...result.errors.map((err) => `[${dateStr}] ${err}`),
                );
              }
            } catch (error: any) {
              const errorMsg = `[${brandName}] Lỗi khi đồng bộ ngày ${dateStr}: ${error?.message || error}`;
              this.logger.error(errorMsg);
              brandErrors.push(errorMsg);
            }

            // Tăng ngày lên 1
            currentDate.setDate(currentDate.getDate() + 1);
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
            `[syncShiftEndCashByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} báo cáo`,
          );
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ shift end cash: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ báo cáo nộp quỹ cuối ca thành công từ ${startDate} đến ${endDate}: ${totalRecordsCount} báo cáo, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        totalRecordsCount,
        totalSavedCount,
        totalUpdatedCount,
        brandResults: brandResults.length > 0 ? brandResults : undefined,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ shift end cash theo khoảng thời gian: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ dữ liệu tách gộp BOM (Repack Formula) từ API
   * @param dateFrom - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01NOV2025)
   * @param dateTo - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncRepackFormula(
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
            `[RepackFormula] Đang đồng bộ ${brandName} cho khoảng ${dateFrom} - ${dateTo}`,
          );

          // Lấy dữ liệu từ API
          const repackFormulaData = await this.zappyApiService.getRepackFormula(
            dateFrom,
            dateTo,
            brandName,
          );

          if (!repackFormulaData || repackFormulaData.length === 0) {
            this.logger.log(
              `[RepackFormula] Không có dữ liệu cho ${brandName} - khoảng ${dateFrom} - ${dateTo}`,
            );
            continue;
          }

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Parse date string sang Date object
          const parseDateString = (
            dateStr: string | null | undefined,
          ): Date | null => {
            if (!dateStr) return null;
            try {
              // Format: "05/11/2025 00:00" hoặc ISO string
              if (dateStr.includes('T') || dateStr.includes('Z')) {
                return new Date(dateStr);
              }
              // Format: "05/11/2025 00:00"
              const parts = dateStr.split(' ');
              const datePart = parts[0]; // "05/11/2025"
              const timePart = parts[1] || '00:00'; // "00:00"
              const [day, month, year] = datePart.split('/');
              const [hours, minutes] = timePart.split(':');
              return new Date(
                parseInt(year),
                parseInt(month) - 1,
                parseInt(day),
                parseInt(hours) || 0,
                parseInt(minutes) || 0,
              );
            } catch (error) {
              this.logger.warn(`Không thể parse date: ${dateStr}`);
              return null;
            }
          };

          for (const formulaData of repackFormulaData) {
            try {
              // Kiểm tra xem đã tồn tại chưa (dựa trên api_id và brand)
              const existingRecord = await this.repackFormulaRepository.findOne(
                {
                  where: {
                    api_id: formulaData.id,
                    brand: brandName,
                  },
                },
              );

              const validFromdate = parseDateString(formulaData.valid_fromdate);
              const validTodate = parseDateString(formulaData.valid_todate);
              const enteredat = parseDateString(formulaData.enteredat);

              if (existingRecord) {
                // Cập nhật record đã tồn tại
                existingRecord.name = formulaData.name || existingRecord.name;
                existingRecord.check_qty_constraint =
                  formulaData.check_qty_constraint ||
                  existingRecord.check_qty_constraint;
                existingRecord.depr_pct =
                  formulaData.depr_pct !== undefined
                    ? formulaData.depr_pct
                    : existingRecord.depr_pct;
                existingRecord.branch_codes =
                  formulaData.branch_codes !== undefined
                    ? formulaData.branch_codes
                    : existingRecord.branch_codes;
                existingRecord.repack_cat_name =
                  formulaData.repack_cat_name || existingRecord.repack_cat_name;
                existingRecord.valid_fromdate =
                  validFromdate || existingRecord.valid_fromdate;
                existingRecord.valid_todate =
                  validTodate !== null
                    ? validTodate
                    : existingRecord.valid_todate;
                existingRecord.enteredby =
                  formulaData.enteredby || existingRecord.enteredby;
                existingRecord.enteredat =
                  enteredat || existingRecord.enteredat;
                existingRecord.locked =
                  formulaData.locked || existingRecord.locked;
                existingRecord.sync_date_from = dateFrom;
                existingRecord.sync_date_to = dateTo;

                // Xóa các items cũ
                await this.repackFormulaItemRepository.delete({
                  repackFormulaId: existingRecord.id,
                });

                // Tạo lại items từ from_items và to_items
                const items: RepackFormulaItem[] = [];
                if (
                  formulaData.from_items &&
                  Array.isArray(formulaData.from_items)
                ) {
                  for (const fromItem of formulaData.from_items) {
                    const item = this.repackFormulaItemRepository.create({
                      repackFormula: existingRecord,
                      item_type: 'from',
                      itemcode: fromItem.itemcode || null,
                      qty: fromItem.qty !== undefined ? fromItem.qty : 0,
                    });
                    items.push(item);
                  }
                }
                if (
                  formulaData.to_items &&
                  Array.isArray(formulaData.to_items)
                ) {
                  for (const toItem of formulaData.to_items) {
                    const item = this.repackFormulaItemRepository.create({
                      repackFormula: existingRecord,
                      item_type: 'to',
                      itemcode: toItem.itemcode || null,
                      qty: toItem.qty !== undefined ? toItem.qty : 0,
                    });
                    items.push(item);
                  }
                }

                existingRecord.items = items;
                await this.repackFormulaRepository.save(existingRecord);
                brandUpdatedCount++;
              } else {
                // Tạo record mới
                const newRecord = this.repackFormulaRepository.create({
                  api_id: formulaData.id,
                  name: formulaData.name || undefined,
                  check_qty_constraint:
                    formulaData.check_qty_constraint || undefined,
                  depr_pct:
                    formulaData.depr_pct !== undefined
                      ? formulaData.depr_pct
                      : 0,
                  branch_codes:
                    formulaData.branch_codes !== undefined
                      ? formulaData.branch_codes
                      : undefined,
                  repack_cat_name: formulaData.repack_cat_name || undefined,
                  valid_fromdate: validFromdate || undefined,
                  valid_todate: validTodate || undefined,
                  enteredby: formulaData.enteredby || undefined,
                  enteredat: enteredat || undefined,
                  locked: formulaData.locked || undefined,
                  sync_date_from: dateFrom,
                  sync_date_to: dateTo,
                  brand: brandName,
                });

                const savedRecord = (await this.repackFormulaRepository.save(
                  newRecord,
                )) as unknown as RepackFormula;

                // Tạo items từ from_items và to_items
                const items: RepackFormulaItem[] = [];
                if (
                  formulaData.from_items &&
                  Array.isArray(formulaData.from_items)
                ) {
                  for (const fromItem of formulaData.from_items) {
                    const item = this.repackFormulaItemRepository.create({
                      repackFormula: savedRecord,
                      item_type: 'from',
                      itemcode: fromItem.itemcode || undefined,
                      qty: fromItem.qty !== undefined ? fromItem.qty : 0,
                    });
                    items.push(item);
                  }
                }
                if (
                  formulaData.to_items &&
                  Array.isArray(formulaData.to_items)
                ) {
                  for (const toItem of formulaData.to_items) {
                    const item = this.repackFormulaItemRepository.create({
                      repackFormula: savedRecord,
                      item_type: 'to',
                      itemcode: toItem.itemcode || undefined,
                      qty: toItem.qty !== undefined ? toItem.qty : 0,
                    });
                    items.push(item);
                  }
                }

                if (items.length > 0) {
                  await this.repackFormulaItemRepository.save(items);
                }

                brandSavedCount++;
              }
              totalRecordsCount++;
            } catch (error: any) {
              const errorMsg = `Lỗi khi xử lý repack formula id ${formulaData.id}: ${error?.message || error}`;
              this.logger.error(errorMsg);
              brandErrors.push(errorMsg);
            }
          }

          totalSavedCount += brandSavedCount;
          totalUpdatedCount += brandUpdatedCount;
          allErrors.push(...brandErrors);

          this.logger.log(
            `[RepackFormula] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`,
          );
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ repack formula: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ tách gộp BOM thành công: ${totalRecordsCount} công thức, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        recordsCount: totalRecordsCount,
        savedCount: totalSavedCount,
        updatedCount: totalUpdatedCount,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ repack formula: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ tách gộp BOM theo khoảng thời gian
   * @param startDate - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncRepackFormulaByDateRange(
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
      // Parse dates
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

      const start = parseDate(startDate);
      const end = parseDate(endDate);

      if (start > end) {
        throw new Error('Ngày bắt đầu phải nhỏ hơn hoặc bằng ngày kết thúc');
      }

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
      // Gọi trực tiếp với startDate và endDate
      for (const brandName of brands) {
        try {
          this.logger.log(
            `[syncRepackFormulaByDateRange] Bắt đầu đồng bộ brand: ${brandName}`,
          );
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          try {
            this.logger.log(
              `[syncRepackFormulaByDateRange] Đồng bộ ${brandName} - khoảng ${startDate} - ${endDate}`,
            );
            const result = await this.syncRepackFormula(
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
            `[syncRepackFormulaByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} công thức`,
          );
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ repack formula: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ tách gộp BOM thành công từ ${startDate} đến ${endDate}: ${totalRecordsCount} công thức, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        totalRecordsCount,
        totalSavedCount,
        totalUpdatedCount,
        brandResults: brandResults.length > 0 ? brandResults : undefined,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ repack formula theo khoảng thời gian: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Lấy danh sách tách gộp BOM với filter và pagination
   */
  async getRepackFormula(params: {
    page?: number;
    limit?: number;
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    repackCatName?: string;
    itemcode?: string;
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

      const queryBuilder = this.repackFormulaRepository
        .createQueryBuilder('rf')
        .leftJoinAndSelect('rf.items', 'items')
        .orderBy('rf.valid_fromdate', 'DESC')
        .addOrderBy('rf.name', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('rf.brand = :brand', { brand: params.brand });
      }

      // Filter by repack_cat_name
      if (params.repackCatName) {
        queryBuilder.andWhere('rf.repack_cat_name = :repackCatName', {
          repackCatName: params.repackCatName,
        });
      }

      // Filter by itemcode (trong items)
      if (params.itemcode) {
        queryBuilder.andWhere('items.itemcode = :itemcode', {
          itemcode: params.itemcode,
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
        queryBuilder.andWhere('rf.valid_fromdate >= :dateFrom', {
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
        queryBuilder.andWhere('rf.valid_fromdate <= :dateTo', {
          dateTo: toDate,
        });
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
        `Error getting repack formula: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ dữ liệu danh sách CTKM (Promotion) từ API
   * @param dateFrom - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01NOV2025)
   * @param dateTo - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncPromotion(
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
            `[Promotion] Đang đồng bộ ${brandName} cho khoảng ${dateFrom} - ${dateTo}`,
          );

          // Lấy danh sách promotion từ API
          const promotionData = await this.zappyApiService.getPromotion(
            dateFrom,
            dateTo,
            brandName,
          );

          if (!promotionData || promotionData.length === 0) {
            this.logger.log(
              `[Promotion] Không có dữ liệu cho ${brandName} - khoảng ${dateFrom} - ${dateTo}`,
            );
            continue;
          }

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Parse date string sang Date object
          const parseDateString = (
            dateStr: string | null | undefined,
          ): Date | null => {
            if (!dateStr || typeof dateStr !== 'string') return null;
            try {
              const trimmed = dateStr.trim();
              if (!trimmed) return null;

              // Format: "07/11/2025 21:29" hoặc ISO string
              if (trimmed.includes('T') || trimmed.includes('Z')) {
                const isoDate = new Date(trimmed);
                if (isNaN(isoDate.getTime())) {
                  this.logger.warn(`Invalid ISO date: ${dateStr}`);
                  return null;
                }
                return isoDate;
              }

              // Format: "07/11/2025 21:29"
              const parts = trimmed.split(' ');
              const datePart = parts[0]; // "07/11/2025"
              const timePart = parts[1] || '00:00'; // "21:29"

              if (!datePart || !datePart.includes('/')) {
                this.logger.warn(`Invalid date format (no /): ${dateStr}`);
                return null;
              }

              const [day, month, year] = datePart.split('/');
              const [hours, minutes] = timePart.split(':');

              // Validate các giá trị
              const yearNum = parseInt(year, 10);
              const monthNum = parseInt(month, 10);
              const dayNum = parseInt(day, 10);
              const hoursNum = parseInt(hours || '0', 10);
              const minutesNum = parseInt(minutes || '0', 10);

              if (isNaN(yearNum) || isNaN(monthNum) || isNaN(dayNum)) {
                this.logger.warn(
                  `Invalid date values: ${dateStr} (year: ${year}, month: ${month}, day: ${day})`,
                );
                return null;
              }

              if (monthNum < 1 || monthNum > 12) {
                this.logger.warn(`Invalid month: ${monthNum} in ${dateStr}`);
                return null;
              }

              if (dayNum < 1 || dayNum > 31) {
                this.logger.warn(`Invalid day: ${dayNum} in ${dateStr}`);
                return null;
              }

              const date = new Date(
                yearNum,
                monthNum - 1,
                dayNum,
                isNaN(hoursNum) ? 0 : hoursNum,
                isNaN(minutesNum) ? 0 : minutesNum,
              );

              // Kiểm tra Date có hợp lệ không
              if (isNaN(date.getTime())) {
                this.logger.warn(
                  `Invalid Date object created from: ${dateStr}`,
                );
                return null;
              }

              return date;
            } catch (error) {
              this.logger.warn(`Không thể parse date: ${dateStr} - ${error}`);
              return null;
            }
          };

          for (const promoData of promotionData) {
            // Đảm bảo api_id là number
            const apiId =
              typeof promoData.id === 'number'
                ? promoData.id
                : parseInt(String(promoData.id), 10);

            if (isNaN(apiId)) {
              this.logger.warn(`Invalid api_id for promotion: ${promoData.id}`);
              continue;
            }

            try {
              // Kiểm tra xem đã tồn tại chưa (dựa trên api_id và brand)
              const existingRecord = await this.promotionRepository.findOne({
                where: {
                  api_id: apiId,
                  brand: brandName,
                },
              });

              const fromdate = parseDateString(promoData.fromdate);
              const todate = parseDateString(promoData.todate);
              const enteredat = parseDateString(promoData.enteredat);

              if (existingRecord) {
                this.logger.log(
                  `[Promotion] Cập nhật promotion ${apiId} (${promoData.code || 'N/A'}) cho brand ${brandName}`,
                );
                // Cập nhật record đã tồn tại
                existingRecord.code = promoData.code || existingRecord.code;
                existingRecord.seq =
                  promoData.seq !== undefined
                    ? promoData.seq
                    : existingRecord.seq;
                existingRecord.name = promoData.name || existingRecord.name;
                existingRecord.fromdate = fromdate || existingRecord.fromdate;
                existingRecord.todate =
                  todate !== null ? todate : existingRecord.todate;
                existingRecord.ptype = promoData.ptype || existingRecord.ptype;
                existingRecord.pricetype =
                  promoData.pricetype || existingRecord.pricetype;
                existingRecord.brand_code =
                  promoData.brand_code || existingRecord.brand_code;
                existingRecord.locked =
                  promoData.locked || existingRecord.locked;
                existingRecord.status =
                  promoData.status || existingRecord.status;
                existingRecord.enteredby =
                  promoData.enteredby || existingRecord.enteredby;
                existingRecord.enteredat =
                  enteredat || existingRecord.enteredat;
                existingRecord.sync_date_from = dateFrom;
                existingRecord.sync_date_to = dateTo;

                // Xóa các lines cũ
                await this.promotionLineRepository.delete({
                  promotionId: existingRecord.id,
                });

                // Lấy chi tiết lines từ API
                let lines: PromotionLine[] = [];
                try {
                  const lineData = await this.zappyApiService.getPromotionLine(
                    promoData.id,
                    brandName,
                  );

                  // Tạo lại lines từ i_lines và v_lines
                  lines = [];
                  if (lineData.i_lines && Array.isArray(lineData.i_lines)) {
                    for (const iLine of lineData.i_lines) {
                      const line = this.promotionLineRepository.create({
                        promotion: existingRecord,
                        line_type: 'i_lines',
                        seq: iLine.seq !== undefined ? iLine.seq : undefined,
                        buy_items: iLine.buy_items || undefined,
                        buy_qty:
                          iLine.buy_qty !== undefined ? iLine.buy_qty : 0,
                        buy_type: iLine.buy_type || undefined,
                        buy_combined_qty:
                          iLine.buy_combined_qty !== undefined
                            ? iLine.buy_combined_qty
                            : undefined,
                        buy_fromtotal:
                          iLine.buy_fromtotal !== undefined
                            ? iLine.buy_fromtotal
                            : undefined,
                        buy_tototal:
                          iLine.buy_tototal !== undefined
                            ? iLine.buy_tototal
                            : undefined,
                        prom_group: iLine.prom_group || undefined,
                        card_pattern: iLine.card_pattern || undefined,
                        get_items: iLine.get_items || undefined,
                        get_item_price:
                          iLine.get_item_price !== undefined
                            ? iLine.get_item_price
                            : undefined,
                        get_qty:
                          iLine.get_qty !== undefined ? iLine.get_qty : 0,
                        get_discamt:
                          iLine.get_discamt !== undefined
                            ? iLine.get_discamt
                            : undefined,
                        get_max_discamt:
                          iLine.get_max_discamt !== undefined
                            ? iLine.get_max_discamt
                            : 0,
                        get_discpct:
                          iLine.get_discpct !== undefined
                            ? iLine.get_discpct
                            : undefined,
                        get_value_range:
                          iLine.get_value_range !== undefined
                            ? iLine.get_value_range
                            : undefined,
                        get_vouchertype: iLine.get_vouchertype || undefined,
                        get_item_option: iLine.get_item_option || undefined,
                        svc_card_months:
                          iLine.svc_card_months !== undefined
                            ? iLine.svc_card_months
                            : undefined,
                        guideline: iLine.guideline || undefined,
                      });
                      lines.push(line);
                    }
                  }
                  if (lineData.v_lines && Array.isArray(lineData.v_lines)) {
                    for (const vLine of lineData.v_lines) {
                      const line = this.promotionLineRepository.create({
                        promotion: existingRecord,
                        line_type: 'v_lines',
                        seq: vLine.seq !== undefined ? vLine.seq : undefined,
                        buy_items: vLine.buy_items || undefined,
                        buy_qty:
                          vLine.buy_qty !== undefined ? vLine.buy_qty : 0,
                        buy_type: vLine.buy_type || undefined,
                        buy_combined_qty:
                          vLine.buy_combined_qty !== undefined
                            ? vLine.buy_combined_qty
                            : undefined,
                        buy_fromtotal:
                          vLine.buy_fromtotal !== undefined
                            ? vLine.buy_fromtotal
                            : undefined,
                        buy_tototal:
                          vLine.buy_tototal !== undefined
                            ? vLine.buy_tototal
                            : undefined,
                        prom_group: vLine.prom_group || undefined,
                        card_pattern: vLine.card_pattern || undefined,
                        get_items: vLine.get_items || undefined,
                        get_item_price:
                          vLine.get_item_price !== undefined
                            ? vLine.get_item_price
                            : undefined,
                        get_qty:
                          vLine.get_qty !== undefined ? vLine.get_qty : 0,
                        get_discamt:
                          vLine.get_discamt !== undefined
                            ? vLine.get_discamt
                            : undefined,
                        get_max_discamt:
                          vLine.get_max_discamt !== undefined
                            ? vLine.get_max_discamt
                            : 0,
                        get_discpct:
                          vLine.get_discpct !== undefined
                            ? vLine.get_discpct
                            : undefined,
                        get_value_range:
                          vLine.get_value_range !== undefined
                            ? vLine.get_value_range
                            : undefined,
                        get_vouchertype: vLine.get_vouchertype || undefined,
                        get_item_option: vLine.get_item_option || undefined,
                        svc_card_months:
                          vLine.svc_card_months !== undefined
                            ? vLine.svc_card_months
                            : undefined,
                        guideline: vLine.guideline || undefined,
                      });
                      lines.push(line);
                    }
                  }

                  if (lines.length > 0) {
                    await this.promotionLineRepository.save(lines);
                  }
                } catch (lineError: any) {
                  this.logger.warn(
                    `Không thể lấy lines cho promotion ${apiId}: ${lineError?.message || lineError}`,
                  );
                  // Vẫn tiếp tục, không throw error
                }

                existingRecord.lines = lines;
                await this.promotionRepository.save(existingRecord);
                brandUpdatedCount++;
              } else {
                // Tạo record mới
                this.logger.log(
                  `[Promotion] Tạo mới promotion ${apiId} (${promoData.code || 'N/A'}) cho brand ${brandName}`,
                );
                const newRecord = this.promotionRepository.create({
                  api_id: apiId,
                  code: promoData.code || undefined,
                  seq: promoData.seq !== undefined ? promoData.seq : undefined,
                  name: promoData.name || undefined,
                  fromdate: fromdate || undefined,
                  todate: todate || undefined,
                  ptype: promoData.ptype || undefined,
                  pricetype: promoData.pricetype || undefined,
                  brand_code: promoData.brand_code || undefined,
                  locked: promoData.locked || undefined,
                  status: promoData.status || undefined,
                  enteredby: promoData.enteredby || undefined,
                  enteredat: enteredat || undefined,
                  sync_date_from: dateFrom,
                  sync_date_to: dateTo,
                  brand: brandName,
                });

                const savedRecord = (await this.promotionRepository.save(
                  newRecord,
                )) as unknown as Promotion;

                // Lấy chi tiết lines từ API
                try {
                  const lineData = await this.zappyApiService.getPromotionLine(
                    apiId,
                    brandName,
                  );

                  // Tạo lines từ i_lines và v_lines
                  const lines: PromotionLine[] = [];
                  if (lineData.i_lines && Array.isArray(lineData.i_lines)) {
                    for (const iLine of lineData.i_lines) {
                      const line = this.promotionLineRepository.create({
                        promotion: savedRecord,
                        line_type: 'i_lines',
                        seq: iLine.seq !== undefined ? iLine.seq : undefined,
                        buy_items: iLine.buy_items || undefined,
                        buy_qty:
                          iLine.buy_qty !== undefined ? iLine.buy_qty : 0,
                        buy_type: iLine.buy_type || undefined,
                        buy_combined_qty:
                          iLine.buy_combined_qty !== undefined
                            ? iLine.buy_combined_qty
                            : undefined,
                        buy_fromtotal:
                          iLine.buy_fromtotal !== undefined
                            ? iLine.buy_fromtotal
                            : undefined,
                        buy_tototal:
                          iLine.buy_tototal !== undefined
                            ? iLine.buy_tototal
                            : undefined,
                        prom_group: iLine.prom_group || undefined,
                        card_pattern: iLine.card_pattern || undefined,
                        get_items: iLine.get_items || undefined,
                        get_item_price:
                          iLine.get_item_price !== undefined
                            ? iLine.get_item_price
                            : undefined,
                        get_qty:
                          iLine.get_qty !== undefined ? iLine.get_qty : 0,
                        get_discamt:
                          iLine.get_discamt !== undefined
                            ? iLine.get_discamt
                            : undefined,
                        get_max_discamt:
                          iLine.get_max_discamt !== undefined
                            ? iLine.get_max_discamt
                            : 0,
                        get_discpct:
                          iLine.get_discpct !== undefined
                            ? iLine.get_discpct
                            : undefined,
                        get_value_range:
                          iLine.get_value_range !== undefined
                            ? iLine.get_value_range
                            : undefined,
                        get_vouchertype: iLine.get_vouchertype || undefined,
                        get_item_option: iLine.get_item_option || undefined,
                        svc_card_months:
                          iLine.svc_card_months !== undefined
                            ? iLine.svc_card_months
                            : undefined,
                        guideline: iLine.guideline || undefined,
                      });
                      lines.push(line);
                    }
                  }
                  if (lineData.v_lines && Array.isArray(lineData.v_lines)) {
                    for (const vLine of lineData.v_lines) {
                      const line = this.promotionLineRepository.create({
                        promotion: savedRecord,
                        line_type: 'v_lines',
                        seq: vLine.seq !== undefined ? vLine.seq : undefined,
                        buy_items: vLine.buy_items || undefined,
                        buy_qty:
                          vLine.buy_qty !== undefined ? vLine.buy_qty : 0,
                        buy_type: vLine.buy_type || undefined,
                        buy_combined_qty:
                          vLine.buy_combined_qty !== undefined
                            ? vLine.buy_combined_qty
                            : undefined,
                        buy_fromtotal:
                          vLine.buy_fromtotal !== undefined
                            ? vLine.buy_fromtotal
                            : undefined,
                        buy_tototal:
                          vLine.buy_tototal !== undefined
                            ? vLine.buy_tototal
                            : undefined,
                        prom_group: vLine.prom_group || undefined,
                        card_pattern: vLine.card_pattern || undefined,
                        get_items: vLine.get_items || undefined,
                        get_item_price:
                          vLine.get_item_price !== undefined
                            ? vLine.get_item_price
                            : undefined,
                        get_qty:
                          vLine.get_qty !== undefined ? vLine.get_qty : 0,
                        get_discamt:
                          vLine.get_discamt !== undefined
                            ? vLine.get_discamt
                            : undefined,
                        get_max_discamt:
                          vLine.get_max_discamt !== undefined
                            ? vLine.get_max_discamt
                            : 0,
                        get_discpct:
                          vLine.get_discpct !== undefined
                            ? vLine.get_discpct
                            : undefined,
                        get_value_range:
                          vLine.get_value_range !== undefined
                            ? vLine.get_value_range
                            : undefined,
                        get_vouchertype: vLine.get_vouchertype || undefined,
                        get_item_option: vLine.get_item_option || undefined,
                        svc_card_months:
                          vLine.svc_card_months !== undefined
                            ? vLine.svc_card_months
                            : undefined,
                        guideline: vLine.guideline || undefined,
                      });
                      lines.push(line);
                    }
                  }

                  if (lines.length > 0) {
                    await this.promotionLineRepository.save(lines);
                  }
                } catch (lineError: any) {
                  this.logger.warn(
                    `Không thể lấy lines cho promotion ${apiId}: ${lineError?.message || lineError}`,
                  );
                  // Vẫn tiếp tục, không throw error
                }

                brandSavedCount++;
              }
              totalRecordsCount++;
            } catch (error: any) {
              const errorMsg = `Lỗi khi xử lý promotion id ${apiId}: ${error?.message || error}`;
              this.logger.error(errorMsg);
              brandErrors.push(errorMsg);
            }
          }

          totalSavedCount += brandSavedCount;
          totalUpdatedCount += brandUpdatedCount;
          allErrors.push(...brandErrors);

          this.logger.log(
            `[Promotion] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`,
          );
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ promotion: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ danh sách CTKM thành công: ${totalRecordsCount} chương trình, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        recordsCount: totalRecordsCount,
        savedCount: totalSavedCount,
        updatedCount: totalUpdatedCount,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ promotion: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ danh sách CTKM theo khoảng thời gian
   * @param startDate - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 31OCT2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncPromotionByDateRange(
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
      // Gọi trực tiếp với startDate và endDate
      for (const brandName of brands) {
        try {
          this.logger.log(
            `[syncPromotionByDateRange] Bắt đầu đồng bộ brand: ${brandName}`,
          );
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          try {
            this.logger.log(
              `[syncPromotionByDateRange] Đồng bộ ${brandName} - khoảng ${startDate} - ${endDate}`,
            );
            const result = await this.syncPromotion(
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
            `[syncPromotionByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} chương trình`,
          );
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ promotion: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ danh sách CTKM thành công từ ${startDate} đến ${endDate}: ${totalRecordsCount} chương trình, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        totalRecordsCount,
        totalSavedCount,
        totalUpdatedCount,
        brandResults: brandResults.length > 0 ? brandResults : undefined,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lỗi khi đồng bộ promotion theo khoảng thời gian: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Lấy danh sách CTKM với filter và pagination
   */
  async getPromotion(params: {
    page?: number;
    limit?: number;
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    ptype?: string;
    status?: string;
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

      const queryBuilder = this.promotionRepository
        .createQueryBuilder('p')
        .leftJoinAndSelect('p.lines', 'lines')
        .orderBy('p.fromdate', 'DESC')
        .addOrderBy('p.code', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('p.brand = :brand', { brand: params.brand });
      }

      // Filter by ptype
      if (params.ptype) {
        queryBuilder.andWhere('p.ptype = :ptype', { ptype: params.ptype });
      }

      // Filter by status
      if (params.status) {
        queryBuilder.andWhere('p.status = :status', { status: params.status });
      }

      // Filter by code
      if (params.code) {
        queryBuilder.andWhere('p.code LIKE :code', {
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
        queryBuilder.andWhere('p.fromdate >= :dateFrom', {
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
        queryBuilder.andWhere('p.fromdate <= :dateTo', { dateTo: toDate });
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
      this.logger.error(`Error getting promotion: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Export promotions to Excel file với chi tiết promotion lines
   */
  async exportPromotions(params: {
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    ptype?: string;
    status?: string;
    code?: string;
  }): Promise<Buffer> {
    try {
      // Lấy tất cả promotions (không pagination)
      const queryBuilder = this.promotionRepository
        .createQueryBuilder('p')
        .leftJoinAndSelect('p.lines', 'lines')
        .orderBy('p.fromdate', 'DESC')
        .addOrderBy('p.code', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('p.brand = :brand', { brand: params.brand });
      }

      // Filter by ptype
      if (params.ptype) {
        queryBuilder.andWhere('p.ptype = :ptype', { ptype: params.ptype });
      }

      // Filter by status
      if (params.status) {
        queryBuilder.andWhere('p.status = :status', { status: params.status });
      }

      // Filter by code
      if (params.code) {
        queryBuilder.andWhere('p.code LIKE :code', {
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
        queryBuilder.andWhere('p.fromdate >= :dateFrom', {
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
        queryBuilder.andWhere('p.fromdate <= :dateTo', { dateTo: toDate });
      }

      const promotions = await queryBuilder.getMany();

      // Format date helper
      const formatDate = (date: Date | null | undefined): string => {
        if (!date) return '-';
        return new Date(date).toLocaleDateString('vi-VN');
      };

      // Build Excel data - mỗi promotion line là một row
      const excelData: any[] = [];

      for (const promotion of promotions) {
        const detailCodeBase = promotion.code || `ID: ${promotion.api_id}`;

        if (promotion.lines && promotion.lines.length > 0) {
          for (const line of promotion.lines) {
            const detailCode =
              line.seq !== null
                ? `${detailCodeBase}.${String(line.seq).padStart(2, '0')}`
                : detailCodeBase;

            excelData.push({
              'Mã gốc': promotion.code || promotion.api_id,
              'Mã chi tiết': detailCode,
              'API ID': promotion.api_id,
              Tên: promotion.name || '-',
              Loại: promotion.ptype || '-',
              Brand: promotion.brand || '-',
              'Từ ngày': formatDate(promotion.fromdate),
              'Đến ngày': formatDate(promotion.todate),
              'Trạng thái': promotion.status || '-',
              'Người tạo': promotion.enteredby || '-',
              'Ngày tạo': formatDate(promotion.enteredat),
              'Brand Code': promotion.brand_code || '-',
              'Line Type': line.line_type === 'i_lines' ? 'I Lines' : 'V Lines',
              Seq: line.seq !== null ? line.seq : '-',
              'Buy Items': line.buy_items || '-',
              'Buy Qty': line.buy_qty || 0,
              'Buy From Total':
                line.buy_fromtotal !== null && line.buy_fromtotal !== undefined
                  ? line.buy_fromtotal
                  : '-',
              'Buy To Total':
                line.buy_tototal !== null && line.buy_tototal !== undefined
                  ? line.buy_tototal
                  : '-',
              'Buy Type': line.buy_type || '-',
              'Buy Combined Qty':
                line.buy_combined_qty !== null &&
                line.buy_combined_qty !== undefined
                  ? line.buy_combined_qty
                  : '-',
              'Prom Group': line.prom_group || '-',
              'Card Pattern': line.card_pattern || '-',
              'Get Items': line.get_items || '-',
              'Get Item Price':
                line.get_item_price !== null &&
                line.get_item_price !== undefined
                  ? line.get_item_price
                  : '-',
              'Get Qty': line.get_qty || 0,
              'Get Disc Amt':
                line.get_discamt !== null && line.get_discamt !== undefined
                  ? line.get_discamt
                  : '-',
              'Get Max Disc Amt':
                line.get_max_discamt !== null &&
                line.get_max_discamt !== undefined
                  ? line.get_max_discamt
                  : '-',
              'Get Disc Pct':
                line.get_discpct !== null && line.get_discpct !== undefined
                  ? `${line.get_discpct}%`
                  : '-',
              'Get Value Range':
                line.get_value_range !== null &&
                line.get_value_range !== undefined
                  ? line.get_value_range
                  : '-',
              'Get Voucher Type': line.get_vouchertype || '-',
              'Get Item Option': line.get_item_option || '-',
              'Svc Card Months':
                line.svc_card_months !== null &&
                line.svc_card_months !== undefined
                  ? line.svc_card_months
                  : '-',
              Guideline: line.guideline || '-',
            });
          }
        } else {
          // Nếu không có lines, vẫn thêm một row cho promotion
          excelData.push({
            'Mã gốc': promotion.code || promotion.api_id,
            'Mã chi tiết': detailCodeBase,
            'API ID': promotion.api_id,
            Tên: promotion.name || '-',
            Loại: promotion.ptype || '-',
            Brand: promotion.brand || '-',
            'Từ ngày': formatDate(promotion.fromdate),
            'Đến ngày': formatDate(promotion.todate),
            'Trạng thái': promotion.status || '-',
            'Người tạo': promotion.enteredby || '-',
            'Ngày tạo': formatDate(promotion.enteredat),
            'Brand Code': promotion.brand_code || '-',
            'Line Type': '-',
            Seq: '-',
            'Buy Items': '-',
            'Buy Qty': '-',
            'Buy From Total': '-',
            'Buy To Total': '-',
            'Buy Type': '-',
            'Buy Combined Qty': '-',
            'Prom Group': '-',
            'Card Pattern': '-',
            'Get Items': '-',
            'Get Item Price': '-',
            'Get Qty': '-',
            'Get Disc Amt': '-',
            'Get Max Disc Amt': '-',
            'Get Disc Pct': '-',
            'Get Value Range': '-',
            'Get Voucher Type': '-',
            'Get Item Option': '-',
            'Svc Card Months': '-',
            Guideline: '-',
          });
        }
      }

      // Create workbook
      const wb = XLSX.utils.book_new();
      const ws = XLSX.utils.json_to_sheet(excelData);

      // Style header row
      const range = XLSX.utils.decode_range(ws['!ref'] || 'A1');
      for (let col = range.s.c; col <= range.e.c; col++) {
        const colLetter = XLSX.utils.encode_col(col);
        const headerCellAddress = colLetter + '1';
        if (ws[headerCellAddress]) {
          ws[headerCellAddress].s = {
            fill: {
              fgColor: { rgb: 'E5E7EB' },
              patternType: 'solid',
            },
            font: {
              bold: true,
              color: { rgb: '000000' },
            },
            alignment: {
              horizontal: 'left',
              vertical: 'center',
            },
          };
        }
      }

      // Set column widths
      const colWidths = [
        { wch: 15 }, // Mã gốc
        { wch: 18 }, // Mã chi tiết
        { wch: 10 }, // API ID
        { wch: 20 }, // Tên
        { wch: 8 }, // Loại
        { wch: 10 }, // Brand
        { wch: 12 }, // Từ ngày
        { wch: 12 }, // Đến ngày
        { wch: 15 }, // Trạng thái
        { wch: 25 }, // Người tạo
        { wch: 12 }, // Ngày tạo
        { wch: 12 }, // Brand Code
        { wch: 12 }, // Line Type
        { wch: 8 }, // Seq
        { wch: 15 }, // Buy Items
        { wch: 10 }, // Buy Qty
        { wch: 15 }, // Buy From Total
        { wch: 15 }, // Buy To Total
        { wch: 12 }, // Buy Type
        { wch: 15 }, // Buy Combined Qty
        { wch: 12 }, // Prom Group
        { wch: 15 }, // Card Pattern
        { wch: 15 }, // Get Items
        { wch: 15 }, // Get Item Price
        { wch: 10 }, // Get Qty
        { wch: 15 }, // Get Disc Amt
        { wch: 15 }, // Get Max Disc Amt
        { wch: 12 }, // Get Disc Pct
        { wch: 15 }, // Get Value Range
        { wch: 15 }, // Get Voucher Type
        { wch: 15 }, // Get Item Option
        { wch: 15 }, // Svc Card Months
        { wch: 40 }, // Guideline
      ];
      ws['!cols'] = colWidths;

      // Add worksheet to workbook
      XLSX.utils.book_append_sheet(wb, ws, 'CTKM Chi tiết');

      // Convert to buffer
      const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

      return buffer;
    } catch (error: any) {
      this.logger.error(`[exportPromotions] Error: ${error?.message || error}`);
      this.logger.error(
        `[exportPromotions] Stack: ${error?.stack || 'No stack trace'}`,
      );
      throw new InternalServerErrorException(
        `Error exporting promotions to Excel: ${error?.message || 'Unknown error'}`,
      );
    }
  }

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

  /**
   * Helper method để lưu cashio data vào database
   * @param cashData - Array of cashio records từ API
   * @param date - Ngày sync (format: DDMMMYYYY)
   * @param brand - Brand name
   * @returns Object chứa savedCount và skippedCount
   */
  private async saveCashioData(
    cashData: any[],
    date: string,
    brand?: string,
  ): Promise<{
    savedCount: number;
    skippedCount: number;
    updatedCount: number;
    errors: string[];
  }> {
    const errors: string[] = [];
    let savedCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;

    if (cashData.length === 0) {
      return { savedCount: 0, skippedCount: 0, updatedCount: 0, errors: [] };
    }

    try {
      // Parse docdate từ string sang Date
      const parseDocdate = (docdateStr: string): Date => {
        // Format: "03-10-2025 10:30"
        const parts = docdateStr.split(' ');
        const datePart = parts[0]; // "03-10-2025"
        const timePart = parts[1] || '00:00'; // "10:30"
        const [day, month, year] = datePart.split('-');
        const [hour, minute] = timePart.split(':');
        return new Date(
          parseInt(year),
          parseInt(month) - 1,
          parseInt(day),
          parseInt(hour),
          parseInt(minute),
        );
      };

      const parseRefnoIdate = (refnoIdateStr: string): Date | null => {
        if (
          !refnoIdateStr ||
          refnoIdateStr === '00:00' ||
          refnoIdateStr.includes('00:00')
        ) {
          return null;
        }
        // Format: "03-10-2025 00:00"
        const parts = refnoIdateStr.split(' ');
        const datePart = parts[0]; // "03-10-2025"
        const [day, month, year] = datePart.split('-');
        return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
      };

      // Lưu từng cashio record vào database (chỉ insert nếu chưa có, update nếu đã có)
      for (const cash of cashData) {
        try {
          // Tìm xem đã có record với api_id chưa (api_id là unique từ API)
          const existingCashio = await this.dailyCashioRepository.findOne({
            where: { api_id: cash.id },
          });

          const parsedRefnoIdate = cash.refno_idate
            ? parseRefnoIdate(cash.refno_idate)
            : undefined;

          const cashioData: Partial<DailyCashio> = {
            api_id: cash.id,
            code: cash.code,
            fop_syscode: cash.fop_syscode || undefined,
            fop_description: cash.fop_description || undefined,
            so_code: cash.so_code || '',
            master_code: cash.master_code || undefined,
            docdate: parseDocdate(cash.docdate),
            branch_code: cash.branch_code || undefined,
            partner_code: cash.partner_code || undefined,
            partner_name: cash.partner_name || undefined,
            refno: cash.refno || undefined,
            refno_idate: parsedRefnoIdate || undefined,
            total_in: cash.total_in ? Number(cash.total_in) : 0,
            total_out: cash.total_out ? Number(cash.total_out) : 0,
            sync_date: date,
            brand: brand || undefined,
            bank_code: cash.bank_code || undefined,
            period_code: cash.period_code || undefined,
          };

          if (existingCashio) {
            // Update existing record
            await this.dailyCashioRepository.update(
              { id: existingCashio.id },
              cashioData,
            );
            updatedCount++;
          } else {
            // Insert new record
            const newCashio = this.dailyCashioRepository.create(cashioData);
            await this.dailyCashioRepository.save(newCashio);
            savedCount++;
          }
        } catch (cashioError: any) {
          const errorMsg = `Failed to save cashio record ${cash.code}: ${cashioError?.message || cashioError}`;
          this.logger.warn(errorMsg);
          errors.push(errorMsg);
        }
      }

      this.logger.log(
        `[Cashio] Đã lưu ${savedCount} cashio records mới, cập nhật ${updatedCount} records, bỏ qua ${skippedCount} records (tổng ${cashData.length} records từ API)`,
      );
    } catch (error: any) {
      const errorMsg = `Failed to save cashio data to database: ${error?.message || error}`;
      this.logger.error(errorMsg);
      errors.push(errorMsg);
    }

    return { savedCount, skippedCount, updatedCount, errors };
  }

  /**
   * Đồng bộ cashio theo ngày cho một brand hoặc tất cả brands
   * @param date - Date format: DDMMMYYYY (ví dụ: 02NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncCashioByDate(
    date: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    totalRecordsCount: number;
    totalSavedCount: number;
    totalSkippedCount: number;
    totalUpdatedCount: number;
    brandResults: Array<{
      brand: string;
      recordsCount: number;
      savedCount: number;
      skippedCount: number;
      updatedCount: number;
      errors?: string[];
    }>;
    errors?: string[];
  }> {
    try {
      this.logger.log(
        `[Cashio] Bắt đầu đồng bộ cashio cho ngày ${date}${brand ? ` cho brand ${brand}` : ' cho tất cả brands'}`,
      );

      const brands = brand ? [brand] : ['f3', 'labhair', 'yaman', 'menard'];
      let totalRecordsCount = 0;
      let totalSavedCount = 0;
      let totalSkippedCount = 0;
      let totalUpdatedCount = 0;
      const brandResults: Array<{
        brand: string;
        recordsCount: number;
        savedCount: number;
        skippedCount: number;
        updatedCount: number;
        errors?: string[];
      }> = [];
      const allErrors: string[] = [];

      for (const brandName of brands) {
        try {
          this.logger.log(
            `[Cashio] Đang đồng bộ ${brandName} cho ngày ${date}`,
          );

          // Gọi API để lấy cashio data
          const cashData = await this.zappyApiService.getDailyCash(
            date,
            brandName,
          );
          const recordsCount = cashData.length;
          totalRecordsCount += recordsCount;

          // Lưu vào database
          const saveResult = await this.saveCashioData(
            cashData,
            date,
            brandName,
          );
          totalSavedCount += saveResult.savedCount;
          totalSkippedCount += saveResult.skippedCount;
          totalUpdatedCount += saveResult.updatedCount;

          if (saveResult.errors.length > 0) {
            allErrors.push(...saveResult.errors);
          }

          brandResults.push({
            brand: brandName,
            recordsCount,
            savedCount: saveResult.savedCount,
            skippedCount: saveResult.skippedCount,
            updatedCount: saveResult.updatedCount,
            errors:
              saveResult.errors.length > 0 ? saveResult.errors : undefined,
          });

          this.logger.log(
            `[Cashio] Hoàn thành đồng bộ ${brandName}: ${saveResult.savedCount} mới, ${saveResult.updatedCount} cập nhật, ${saveResult.skippedCount} đã tồn tại (tổng ${recordsCount} records)`,
          );
        } catch (brandError: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ cashio: ${brandError?.message || brandError}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);

          brandResults.push({
            brand: brandName,
            recordsCount: 0,
            savedCount: 0,
            skippedCount: 0,
            updatedCount: 0,
            errors: [errorMsg],
          });
        }
      }

      this.logger.log(
        `[Cashio] Hoàn thành đồng bộ cashio cho ngày ${date}: ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật, ${totalSkippedCount} đã tồn tại (tổng ${totalRecordsCount} records từ tất cả brands)`,
      );

      return {
        success: true,
        totalRecordsCount,
        totalSavedCount,
        totalSkippedCount,
        totalUpdatedCount,
        brandResults,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Error syncing cashio by date: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đồng bộ cashio theo khoảng ngày cho một brand hoặc tất cả brands
   * @param startDate - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param endDate - Date format: DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncCashioByDateRange(
    startDate: string,
    endDate: string,
    brand?: string,
  ): Promise<{
    success: boolean;
    totalRecordsCount: number;
    totalSavedCount: number;
    totalSkippedCount: number;
    totalUpdatedCount: number;
    brandResults: Array<{
      brand: string;
      recordsCount: number;
      savedCount: number;
      skippedCount: number;
      updatedCount: number;
      errors?: string[];
    }>;
    errors?: string[];
  }> {
    try {
      this.logger.log(
        `[Cashio Range] Bắt đầu đồng bộ cashio từ ${startDate} đến ${endDate}${brand ? ` cho brand ${brand}` : ' cho tất cả brands'}`,
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

      const fromDate = parseDate(startDate);
      const toDate = parseDate(endDate);

      if (fromDate > toDate) {
        throw new Error('startDate phải nhỏ hơn hoặc bằng endDate');
      }

      // Generate danh sách các ngày cần sync
      const datesToSync: string[] = [];
      const currentDate = new Date(fromDate);
      while (currentDate <= toDate) {
        const day = String(currentDate.getDate()).padStart(2, '0');
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
        const month = monthNames[currentDate.getMonth()];
        const year = currentDate.getFullYear();
        const dateStr = `${day}${month}${year}`;
        datesToSync.push(dateStr);
        currentDate.setDate(currentDate.getDate() + 1);
      }

      this.logger.log(
        `[Cashio Range] Sẽ đồng bộ ${datesToSync.length} ngày từ ${startDate} đến ${endDate}`,
      );

      const brands = brand ? [brand] : ['f3', 'labhair', 'yaman', 'menard'];
      let totalRecordsCount = 0;
      let totalSavedCount = 0;
      let totalSkippedCount = 0;
      let totalUpdatedCount = 0;
      const brandResults: Array<{
        brand: string;
        recordsCount: number;
        savedCount: number;
        skippedCount: number;
        updatedCount: number;
        errors?: string[];
      }> = [];
      const allErrors: string[] = [];

      // Đồng bộ từng brand
      for (const brandName of brands) {
        let brandRecordsCount = 0;
        let brandSavedCount = 0;
        let brandSkippedCount = 0;
        let brandUpdatedCount = 0;
        const brandErrors: string[] = [];

        try {
          this.logger.log(`[Cashio Range] Bắt đầu đồng bộ brand: ${brandName}`);

          // Đồng bộ từng ngày
          for (const dateStr of datesToSync) {
            try {
              this.logger.log(
                `[Cashio Range] Đang đồng bộ ${brandName} cho ngày ${dateStr}`,
              );

              // Gọi API để lấy cashio data
              const cashData = await this.zappyApiService.getDailyCash(
                dateStr,
                brandName,
              );
              brandRecordsCount += cashData.length;

              // Lưu vào database
              const saveResult = await this.saveCashioData(
                cashData,
                dateStr,
                brandName,
              );
              brandSavedCount += saveResult.savedCount;
              brandSkippedCount += saveResult.skippedCount;
              brandUpdatedCount += saveResult.updatedCount;

              if (saveResult.errors.length > 0) {
                brandErrors.push(...saveResult.errors);
              }

              // Thêm delay nhỏ giữa các request để tránh quá tải API
              await new Promise((resolve) => setTimeout(resolve, 100));
            } catch (dateError: any) {
              const errorMsg = `[${brandName}] Lỗi khi đồng bộ ngày ${dateStr}: ${dateError?.message || dateError}`;
              this.logger.error(errorMsg);
              brandErrors.push(errorMsg);
            }
          }

          totalRecordsCount += brandRecordsCount;
          totalSavedCount += brandSavedCount;
          totalSkippedCount += brandSkippedCount;
          totalUpdatedCount += brandUpdatedCount;

          if (brandErrors.length > 0) {
            allErrors.push(...brandErrors);
          }

          brandResults.push({
            brand: brandName,
            recordsCount: brandRecordsCount,
            savedCount: brandSavedCount,
            skippedCount: brandSkippedCount,
            updatedCount: brandUpdatedCount,
            errors: brandErrors.length > 0 ? brandErrors : undefined,
          });

          this.logger.log(
            `[Cashio Range] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật, ${brandSkippedCount} đã tồn tại (tổng ${brandRecordsCount} records)`,
          );
        } catch (brandError: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ cashio range: ${brandError?.message || brandError}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);

          brandResults.push({
            brand: brandName,
            recordsCount: 0,
            savedCount: 0,
            skippedCount: 0,
            updatedCount: 0,
            errors: [errorMsg],
          });
        }
      }

      this.logger.log(
        `[Cashio Range] Hoàn thành đồng bộ cashio từ ${startDate} đến ${endDate}: ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật, ${totalSkippedCount} đã tồn tại (tổng ${totalRecordsCount} records từ tất cả brands)`,
      );

      return {
        success: true,
        totalRecordsCount,
        totalSavedCount,
        totalSkippedCount,
        totalUpdatedCount,
        brandResults,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Error syncing cashio by date range: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Lấy danh sách cashio với filter và pagination
   */
  async getCashio(params: {
    page?: number;
    limit?: number;
    brand?: string;
    dateFrom?: string;
    dateTo?: string;
    branchCode?: string;
    soCode?: string;
    partnerCode?: string;
  }): Promise<{
    success: boolean;
    data: DailyCashio[];
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

      const queryBuilder = this.dailyCashioRepository
        .createQueryBuilder('cashio')
        .orderBy('cashio.docdate', 'DESC')
        .addOrderBy('cashio.code', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('cashio.brand = :brand', { brand: params.brand });
      }

      // Filter by branchCode
      if (params.branchCode) {
        queryBuilder.andWhere('cashio.branch_code = :branchCode', {
          branchCode: params.branchCode,
        });
      }

      // Filter by soCode
      if (params.soCode) {
        queryBuilder.andWhere(
          '(cashio.so_code = :soCode OR cashio.master_code = :soCode)',
          { soCode: params.soCode },
        );
      }

      // Filter by partnerCode
      if (params.partnerCode) {
        queryBuilder.andWhere('cashio.partner_code = :partnerCode', {
          partnerCode: params.partnerCode,
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
        queryBuilder.andWhere('cashio.docdate >= :dateFrom', {
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
        queryBuilder.andWhere('cashio.docdate <= :dateTo', { dateTo: toDate });
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
      this.logger.error(`Error getting cashio: ${error?.message || error}`);
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
      const page = params.page || 1;
      const limit = params.limit || 10;
      const skip = (page - 1) * limit;

      const queryBuilder = this.warehouseProcessedRepository
        .createQueryBuilder('wp')
        .orderBy('wp.processedDate', 'DESC')
        .addOrderBy('wp.docCode', 'ASC');

      // Filter by ioType
      if (params.ioType) {
        queryBuilder.andWhere('wp.ioType = :ioType', { ioType: params.ioType });
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
        queryBuilder.andWhere('wp.processedDate >= :dateFrom', {
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
        queryBuilder.andWhere('wp.processedDate <= :dateTo', {
          dateTo: toDate,
        });
      }

      // Get total count
      const total = await queryBuilder.getCount();

      // Apply pagination
      queryBuilder.skip(skip).take(limit);

      // Get data
      const data = await queryBuilder.getMany();

      // Calculate statistics
      const statsQueryBuilder =
        this.warehouseProcessedRepository.createQueryBuilder('wp');

      // Apply same filters to stats query
      if (params.ioType) {
        statsQueryBuilder.andWhere('wp.ioType = :ioType', {
          ioType: params.ioType,
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
        statsQueryBuilder.andWhere('wp.processedDate >= :dateFrom', {
          dateFrom: fromDate,
        });
      }

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
        statsQueryBuilder.andWhere('wp.processedDate <= :dateTo', {
          dateTo: toDate,
        });
      }

      const allRecords = await statsQueryBuilder.getMany();
      const statistics = {
        total: allRecords.length,
        success: allRecords.filter((r) => r.success).length,
        failed: allRecords.filter((r) => !r.success).length,
        byIoType: {
          I: allRecords.filter((r) => r.ioType === 'I').length,
          O: allRecords.filter((r) => r.ioType === 'O').length,
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

            // Gọi API warehouse transfer (group tất cả items cùng docCode)
            this.logger.log(
              `[Warehouse Auto] Đang xử lý warehouse transfer cho docCode ${docCode}`,
            );
            const result =
              await this.fastApiInvoiceFlowService.processWarehouseTransferFromStockTransfers(
                stockTransferList,
              );

            // Lưu vào bảng tracking (upsert - update nếu đã tồn tại)
            const existingRecord =
              await this.warehouseProcessedRepository.findOne({
                where: { docCode },
              });
            if (existingRecord) {
              // Dùng update để set errorMessage = null khi thành công
              await this.warehouseProcessedRepository.update(
                { docCode },
                {
                  ioType: 'T', // T = Transfer
                  processedDate: new Date(),
                  result: JSON.stringify(result),
                  success: true,
                  errorMessage: null as any, // Set null để xóa errorMessage trong database
                },
              );
            } else {
              const warehouseProcessed =
                this.warehouseProcessedRepository.create({
                  docCode,
                  ioType: 'T', // T = Transfer
                  processedDate: new Date(),
                  result: JSON.stringify(result),
                  success: true,
                });
              await this.warehouseProcessedRepository.save(warehouseProcessed);
            }

            processedCount++;
            this.logger.log(
              `[Warehouse Auto] Đã xử lý warehouse transfer thành công cho docCode ${docCode}`,
            );
            continue;
          }

          // Xử lý STOCK_IO
          // Chỉ xử lý record đầu tiên (vì các record cùng docCode có thể khác nhau)
          const stockTransfer = firstStockTransfer;

          // Kiểm tra doctype phải là "STOCK_IO" mới gọi API warehouse (kiểm tra đầu tiên để tránh check các điều kiện khác)
          if (stockTransfer.doctype !== 'STOCK_IO') {
            this.logger.debug(
              `[Warehouse Auto] DocCode ${docCode} có doctype = "${stockTransfer.doctype}", bỏ qua (chỉ xử lý doctype = "STOCK_IO" hoặc "STOCK_TRANSFER")`,
            );
            skippedCount++;
            continue;
          }

          // Kiểm tra điều kiện: soCode phải là "null" hoặc null
          if (
            stockTransfer.soCode !== 'null' &&
            stockTransfer.soCode !== null
          ) {
            this.logger.debug(
              `[Warehouse Auto] DocCode ${docCode} có soCode = "${stockTransfer.soCode}", bỏ qua (chỉ xử lý soCode = "null" hoặc null)`,
            );
            skippedCount++;
            continue;
          }

          // Kiểm tra ioType phải là "I" hoặc "O"
          if (stockTransfer.ioType !== 'I' && stockTransfer.ioType !== 'O') {
            this.logger.debug(
              `[Warehouse Auto] DocCode ${docCode} có ioType = "${stockTransfer.ioType}", bỏ qua (chỉ xử lý "I" hoặc "O")`,
            );
            skippedCount++;
            continue;
          }

          // Gọi API warehouse
          this.logger.log(
            `[Warehouse Auto] Đang xử lý warehouse cho docCode ${docCode} (doctype: ${stockTransfer.doctype}, ioType: ${stockTransfer.ioType})`,
          );
          const result =
            await this.fastApiInvoiceFlowService.processWarehouseFromStockTransfer(
              stockTransfer,
            );

          // Lưu vào bảng tracking (upsert - update nếu đã tồn tại)
          const existingRecord =
            await this.warehouseProcessedRepository.findOne({
              where: { docCode },
            });
          if (existingRecord) {
            // Dùng update để set errorMessage = null khi thành công
            await this.warehouseProcessedRepository.update(
              { docCode },
              {
                ioType: stockTransfer.ioType,
                processedDate: new Date(),
                result: JSON.stringify(result),
                success: true,
                errorMessage: null as any, // Set null để xóa errorMessage trong database
              },
            );
          } else {
            const warehouseProcessed = this.warehouseProcessedRepository.create(
              {
                docCode,
                ioType: stockTransfer.ioType,
                processedDate: new Date(),
                result: JSON.stringify(result),
                success: true,
              },
            );
            await this.warehouseProcessedRepository.save(warehouseProcessed);
          }

          processedCount++;
          this.logger.log(
            `[Warehouse Auto] Đã xử lý warehouse thành công cho docCode ${docCode}`,
          );
        } catch (error: any) {
          errorCount++;
          const errorMessage = error?.message || String(error);

          // Lưu vào bảng tracking với success = false (upsert - update nếu đã tồn tại)
          try {
            const firstStockTransfer = stockTransferList[0];
            const existingRecord =
              await this.warehouseProcessedRepository.findOne({
                where: { docCode },
              });
            if (existingRecord) {
              existingRecord.ioType = firstStockTransfer.ioType || 'T'; // T = Transfer nếu không có ioType
              existingRecord.processedDate = new Date();
              existingRecord.errorMessage = errorMessage;
              existingRecord.success = false;
              await this.warehouseProcessedRepository.save(existingRecord);
            } else {
              const warehouseProcessed =
                this.warehouseProcessedRepository.create({
                  docCode,
                  ioType: firstStockTransfer.ioType || 'T', // T = Transfer nếu không có ioType
                  processedDate: new Date(),
                  errorMessage,
                  success: false,
                });
              await this.warehouseProcessedRepository.save(warehouseProcessed);
            }
          } catch (saveError: any) {
            this.logger.error(
              `[Warehouse Auto] Lỗi khi lưu tracking cho docCode ${docCode}: ${saveError?.message || saveError}`,
            );
          }

          this.logger.warn(
            `[Warehouse Auto] Lỗi khi xử lý warehouse cho docCode ${docCode}: ${errorMessage}`,
          );
        }
      }

      this.logger.log(
        `[Warehouse Auto] Hoàn thành xử lý warehouse cho brand ${brand} ngày ${date}: ` +
          `${processedCount} thành công, ${skippedCount} bỏ qua, ${errorCount} lỗi`,
      );
    } catch (error: any) {
      this.logger.error(
        `[Warehouse Auto] Lỗi khi xử lý warehouse tự động cho brand ${brand} ngày ${date}: ${error?.message || error}`,
      );
      throw error;
    }
  }
}
