import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull, In } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import * as SalesUtils from '../../utils/sales.utils';

/**
 * SalesSyncService
 * Chịu trách nhiệm: Sync operations với external APIs (Zappy, Loyalty)
 */
@Injectable()
export class SalesSyncService {
  private readonly logger = new Logger(SalesSyncService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private loyaltyService: LoyaltyService,
  ) {}

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

    // Xử lý từng batch từ database
    let processedCount = 0;

    while (true) {
      // Load batch từ database
      const dbBatch = await this.saleRepository.find({
        where: [{ statusAsys: false }, { statusAsys: IsNull() }],
        order: { createdAt: 'DESC' },
        take: DB_BATCH_SIZE,
      });

      if (dbBatch.length === 0) {
        break;
      }

      // Xử lý batch này theo từng nhóm nhỏ
      for (let i = 0; i < dbBatch.length; i += PROCESS_BATCH_SIZE) {
        const processBatch = dbBatch.slice(i, i + PROCESS_BATCH_SIZE);

        // --- BATCH FETCHING OPTIMIZATION ---
        // 1. Collect all unique itemCodes from the batch
        const itemCodes = Array.from(
          new Set(
            processBatch
              .map((s) => s.itemCode?.trim())
              .filter((code): code is string => !!code && code !== ''),
          ),
        );

        // 2. Fetch all products at once
        const productMap = await this.loyaltyService.fetchProducts(itemCodes);

        // 3. Process each sale using the map (Synchronous loop)
        const updatePromises = processBatch.map(async (sale) => {
          try {
            const itemCode = sale.itemCode || '';
            if (!itemCode) return { success: false };

            const product = productMap.get(itemCode);

            if (product && product.materialCode) {
              const newItemCode = product.materialCode;
              const oldItemCode = itemCode;

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
        });

        const batchResults = await Promise.all(updatePromises);

        // Update counters
        for (const result of batchResults) {
          if (result.success && result.update) {
            successCount++;
            updated.push(result.update);
          } else {
            failCount++;
          }
        }

        processedCount += processBatch.length;
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

    // --- BATCH FETCHING FIX ---
    // 1. Collect unique item codes
    const itemCodes = Array.from(
      new Set(
        errorSales
          .map((s) => s.itemCode?.trim())
          .filter((code): code is string => !!code && code !== ''),
      ),
    );

    // 2. Batch fetch from Loyalty API
    const productMap = await this.loyaltyService.fetchProducts(itemCodes);

    // 3. Process using Map
    for (const sale of errorSales) {
      try {
        const itemCode = sale.itemCode || '';
        if (!itemCode) {
          failCount++;
          continue;
        }

        const product = productMap.get(itemCode);

        if (product && product.materialCode) {
          const newItemCode = product.materialCode;
          const oldItemCode = itemCode;

          await this.saleRepository.update(sale.id, {
            itemCode: newItemCode,
            statusAsys: true,
          });

          successCount++;
          details.push({
            id: sale.id,
            itemCode: sale.itemCode || '',
            oldItemCode,
            newItemCode,
          });
        } else {
          failCount++;
          this.logger.warn(
            `[syncErrorOrderByDocCode] ❌ Sale ${sale.id} (${docCode}): itemCode ${itemCode} vẫn không tồn tại trong Loyalty API`,
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

  /**
   * Đồng bộ sales theo khoảng thời gian cho tất cả brands
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
   * Đồng bộ sales từ Zappy API cho một ngày cụ thể
   * NOTE: Method này rất dài (~400 dòng), cần refactor thêm
   * TODO: Tách thành các private helper methods
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
      // 1. Fetch data from Zappy
      const orders = await this.zappyApiService.getDailySales(date, brand);

      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date, brand);
      } catch (error) {}

      // 2. Process cash data map
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
          message: `Sync sales from Zappy API for ${date}`,
          ordersCount: 0,
          salesCount: 0,
          customersCount: 0,
        };
      }

      let salesCount = 0;
      let customersCount = 0;
      const errors: string[] = [];

      // 3. Fetch departments for all branches
      const branchCodes = Array.from(
        new Set(
          orders
            .map((o) => o.branchCode)
            .filter((code): code is string => !!code && code.trim() !== ''),
        ),
      );
      const departmentMap = await this.fetchDepartmentsForBranches(
        branchCodes,
        brand,
      );

      // --- BATCH FETCHING OPTIMIZATION ---
      // Collect all itemCodes from ALL orders to batch fetch
      const allItemCodes = new Set<string>();
      orders.forEach((order) => {
        if (order.sales) {
          order.sales.forEach((s: any) => {
            const code = s.itemCode?.trim();
            if (code) allItemCodes.add(code);
          });
        }
      });

      // Batch fetch from Loyalty
      const loyaltyProductMap = await this.loyaltyService.fetchProducts(
        Array.from(allItemCodes),
      );

      // 4. Process each order
      for (const order of orders) {
        try {
          // Determine brand
          const department = departmentMap.get(order.branchCode);
          const brandFromDepartment = department?.company
            ? this.mapCompanyToBrand(department.company)
            : order.customer.brand || '';

          // Process Customer
          const customer = await this.processCustomer(
            order,
            brandFromDepartment,
            (isNew) => {
              if (isNew) customersCount++;
            },
          );

          if (!customer) {
            const errorMsg = `Customer not found or customer code ${order.customer.code}`;
            this.logger.error(errorMsg);
            errors.push(errorMsg);
            continue;
          }

          // Check Loyalty Products using Batch Map
          // We can now determine "notFound" using the pre-fetched map
          const notFoundItemCodes = this.getNotFoundItemCodesFromMap(
            order,
            loyaltyProductMap,
          );

          // Process Sales
          const orderSalesCount = await this.processOrderSales(
            order,
            customer,
            brandFromDepartment, // Use mapped brand
            notFoundItemCodes,
            cashMapBySoCode,
            loyaltyProductMap, // Pass the map!
            (errorMsg) => errors.push(errorMsg),
          );
          salesCount += orderSalesCount;
        } catch (orderError: any) {
          const errorMsg = `Error processing order ${order.docCode}: ${orderError?.message || orderError}`;
          this.logger.error(errorMsg);
          errors.push(errorMsg);
        }
      }

      return {
        success: errors.length === 0,
        message: `Sync sales from Zappy API for ${date}`,
        ordersCount: orders.length,
        salesCount,
        customersCount,
        errors: errors.length > 0 ? errors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Error syncing sales from Zappy API for ${date}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  //Private Helpers

  private async fetchDepartmentsForBranches(
    branchCodes: string[],
    targetBrand?: string,
  ): Promise<Map<string, { company?: string }>> {
    const departmentMap = new Map<string, { company?: string }>();
    if (!targetBrand || targetBrand !== 'chando') {
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
    return departmentMap;
  }

  private mapCompanyToBrand(company: string | null | undefined): string {
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
  }

  private async processCustomer(
    order: any,
    brandFromDepartment: string,
    onNewCustomer?: (isNew: boolean) => void,
  ): Promise<Customer | null> {
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
      } as any);
      customer = (await this.customerRepository.save(
        newCustomer,
      )) as unknown as Customer;
      if (onNewCustomer) onNewCustomer(true);
    } else {
      customer.name = order.customer.name || customer.name;
      customer.mobile = order.customer.mobile || customer.mobile;
      customer.grade_name = order.customer.grade_name || customer.grade_name;
      if (brandFromDepartment) {
        customer.brand = brandFromDepartment;
      }
      customer = (await this.customerRepository.save(
        customer,
      )) as unknown as Customer;
    }
    return customer;
  }

  // OPTIMIZED: Uses Map lookup instead of API calls
  private getNotFoundItemCodesFromMap(
    order: any,
    productMap: Map<string, any>,
  ): Set<string> {
    const notFoundItemCodes = new Set<string>();
    if (order.sales) {
      order.sales.forEach((s: any) => {
        const code = s.itemCode?.trim();
        if (code && !productMap.has(code)) {
          notFoundItemCodes.add(code);
        }
      });
    }
    return notFoundItemCodes;
  }

  // LEGACY METHOD: Kept but unused refactoring in syncFromZappy replaces it
  private async getNotFoundItemCodesForOrder(order: any): Promise<Set<string>> {
    // Replaced by getNotFoundItemCodesFromMap in optimizer logic
    // But keeping it if called elsewhere (it is private so likely safe to remove if unused)
    // For now, let's optimize it too just in case
    const orderItemCodes: string[] = Array.from(
      new Set(
        (order.sales || [])
          .map((s: any) => s.itemCode?.trim())
          .filter((code: any): code is string => !!code && code !== ''),
      ),
    );

    const notFoundItemCodes = new Set<string>();
    if (orderItemCodes.length > 0) {
      // BATCH FETCHING FIX
      const productMap =
        await this.loyaltyService.fetchProducts(orderItemCodes);
      orderItemCodes.forEach((code: string) => {
        if (!productMap.has(code)) {
          notFoundItemCodes.add(code);
        }
      });
    }
    return notFoundItemCodes;
  }

  private async processOrderSales(
    order: any,
    customer: Customer,
    brand: string,
    notFoundItemCodes: Set<string>,
    cashMapBySoCode: Map<string, any[]>,
    loyaltyProductMap: Map<string, any>, // Added parameter
    onError?: (msg: string) => void,
  ): Promise<number> {
    let salesCount = 0;
    if (order.sales && order.sales.length > 0) {
      const orderCashData = cashMapBySoCode.get(order.docCode) || [];
      const voucherData = orderCashData.filter(
        (cash) => cash.fop_syscode === 'VOUCHER',
      );

      for (const saleItem of order.sales) {
        try {
          const itemCode = saleItem.itemCode?.trim();
          if (itemCode && itemCode.toUpperCase() === 'TRUTONKEEP') {
            continue;
          }

          const isNotFound = itemCode && notFoundItemCodes.has(itemCode);
          const statusAsys = !isNotFound;

          if (isNotFound) {
            this.logger.warn(
              `[SalesSyncService] Sale item ${itemCode} trong order ${order.docCode} - Sản phẩm không tồn tại trong Loyalty API (404), sẽ lưu với statusAsys = false`,
            );
          }

          // Use Map instead of Async call
          const productType = this.resolveProductTypeFromMap(
            saleItem,
            itemCode,
            notFoundItemCodes,
            loyaltyProductMap,
          );

          // Enrich voucher data
          let voucherRefno: string | undefined;
          let voucherAmount: number | undefined;
          if (voucherData.length > 0) {
            const firstVoucher = voucherData[0];
            voucherRefno = firstVoucher.refno;
            voucherAmount = firstVoucher.total_in || 0;
          }

          const ordertypeName = this.resolveOrderTypeName(saleItem) || '';

          // Check idempotency
          // Với đơn "08. Tách thẻ": cần thêm qty vào điều kiện vì có thể có 2 dòng cùng itemCode nhưng qty khác nhau (-1 và 1)
          const isTachThe =
            ordertypeName.includes('08. Tách thẻ') ||
            ordertypeName.includes('08.Tách thẻ') ||
            ordertypeName.includes('08.  Tách thẻ');

          let existingSale: Sale | null = null;
          if (isTachThe) {
            existingSale = await this.saleRepository.findOne({
              where: {
                docCode: order.docCode,
                itemCode: saleItem.itemCode || '',
                qty: saleItem.qty || 0,
              },
            });
          } else {
            existingSale = await this.saleRepository.findOne({
              where: {
                docCode: order.docCode,
                itemCode: saleItem.itemCode || '',
              },
            });
          }

          const saleData: any = {
            docCode: order.docCode,
            docDate: new Date(order.docDate),
            branchCode: order.branchCode,
            docSourceType: order.docSourceType,
            ordertype: saleItem.ordertype,
            ordertypeName: ordertypeName,
            description: saleItem.description,
            partnerCode: saleItem.partnerCode,
            itemCode: saleItem.itemCode || '',
            itemName: saleItem.itemName || '',
            qty: saleItem.qty || 0,
            revenue: saleItem.revenue || 0,
            linetotal: saleItem.linetotal || saleItem.revenue || 0,
            tienHang:
              saleItem.tienHang || saleItem.linetotal || saleItem.revenue || 0,
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
            saleperson_id: SalesUtils.validateInteger(saleItem.saleperson_id),
            partner_name: saleItem.partner_name,
            order_source: saleItem.order_source,
            maThe: saleItem.mvc_serial,

            productType: productType,
            voucherDp1: voucherRefno,
            thanhToanVoucher:
              voucherAmount && voucherAmount > 0 ? voucherAmount : undefined,
            customer: customer,
            brand: brand,
            // Keep existing isProcessed status if update
            isProcessed: existingSale ? existingSale.isProcessed : false,
            statusAsys: statusAsys,
            svc_code: saleItem.svc_code,
            type_sale: 'RETAIL',
            disc_tm: saleItem.disc_tm,
            disc_ctkm: saleItem.disc_ctkm,
          };

          if (existingSale) {
            // UPDATE
            this.logger.log(
              `[SalesSyncService] Cập nhật sale ${order.docCode}/${saleItem.itemCode} (ID: ${existingSale.id})`,
            );
            await this.saleRepository.update(existingSale.id, saleData);
          } else {
            // CREATE MỚI
            this.logger.log(
              `[SalesSyncService] Tạo mới sale ${order.docCode}/${saleItem.itemCode}`,
            );
            const newSale = this.saleRepository.create(saleData);
            await this.saleRepository.save(newSale);
          }
          salesCount++;
        } catch (saleError: any) {
          const errorMsg = `Lỗi khi lưu sale ${order.docCode}/${saleItem.itemCode}: ${saleError?.message || saleError}`;
          if (onError) onError(errorMsg);
        }
      }
    }
    return salesCount;
  }

  // OPTIMIZED: Synchronous resolution from Map
  private resolveProductTypeFromMap(
    saleItem: any,
    itemCode: string,
    notFoundItemCodes: Set<string>,
    loyaltyProductMap: Map<string, any>,
  ): string | null {
    const productTypeFromZappy =
      saleItem.producttype || saleItem.productType || null;
    let productTypeFromLoyalty: string | null = null;

    if (!productTypeFromZappy && itemCode && !notFoundItemCodes.has(itemCode)) {
      const loyaltyProduct = loyaltyProductMap.get(itemCode);
      if (loyaltyProduct) {
        productTypeFromLoyalty =
          loyaltyProduct.productType || loyaltyProduct.producttype || null;
      }
    }
    return productTypeFromZappy || productTypeFromLoyalty || null;
  }

  // LEGACY: Keeping original signature just in case, but unused in main flow now
  private async resolveProductType(
    saleItem: any,
    itemCode: string,
    notFoundItemCodes: Set<string>,
  ): Promise<string | null> {
    const productTypeFromZappy =
      saleItem.producttype || saleItem.productType || null;
    let productTypeFromLoyalty: string | null = null;
    if (!productTypeFromZappy && itemCode && !notFoundItemCodes.has(itemCode)) {
      try {
        const loyaltyProduct = await this.loyaltyService.checkProduct(itemCode);
        if (loyaltyProduct) {
          productTypeFromLoyalty =
            loyaltyProduct.productType || loyaltyProduct.producttype || null;
        }
      } catch (error) {
        // Ignore
      }
    }
    return productTypeFromZappy || productTypeFromLoyalty || null;
  }

  private resolveOrderTypeName(saleItem: any): string | undefined {
    if (
      saleItem.ordertype_name !== undefined &&
      saleItem.ordertype_name !== null
    ) {
      if (typeof saleItem.ordertype_name === 'string') {
        const trimmed = saleItem.ordertype_name.trim();
        return trimmed !== '' ? trimmed : undefined;
      } else {
        return String(saleItem.ordertype_name).trim() || undefined;
      }
    }
    return undefined;
  }
}
