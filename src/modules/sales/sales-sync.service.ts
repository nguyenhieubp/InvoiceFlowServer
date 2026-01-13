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
 * Chá»‹u trÃ¡ch nhiá»‡m: Sync operations vá»›i external APIs (Zappy, Loyalty)
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
   * Äá»“ng bá»™ láº¡i Ä‘Æ¡n lá»—i - check láº¡i vá»›i Loyalty API
   * Náº¿u tÃ¬m tháº¥y trong Loyalty, cáº­p nháº­t itemCode (mÃ£ váº­t tÆ°) vÃ  statusAsys = true
   * Xá»­ lÃ½ theo batch tá»« database Ä‘á»ƒ trÃ¡nh load quÃ¡ nhiá»u vÃ o memory
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

    // Cáº¥u hÃ¬nh batch size
    const DB_BATCH_SIZE = 500; // Load 500 records tá»« DB má»—i láº§n
    const PROCESS_BATCH_SIZE = 100; // Xá»­ lÃ½ 100 sales má»—i batch trong memory
    const CONCURRENT_LIMIT = 10; // Chá»‰ gá»i 10 API cÃ¹ng lÃºc Ä‘á»ƒ trÃ¡nh quÃ¡ táº£i

    // Helper function Ä‘á»ƒ xá»­ lÃ½ má»™t sale
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
          // TÃ¬m tháº¥y trong Loyalty - cáº­p nháº­t
          const newItemCode = product.materialCode;
          const oldItemCode = itemCode;

          // Cáº­p nháº­t sale
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
          `[syncErrorOrders] âŒ Lá»—i khi check sale ${sale.id}: ${error?.message || error}`,
        );
        return { success: false };
      }
    };

    // Helper function Ä‘á»ƒ limit concurrent requests
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

    // Xá»­ lÃ½ tá»«ng batch tá»« database
    let processedCount = 0;
    let dbBatchNumber = 0;

    while (true) {
      dbBatchNumber++;

      // Load batch tá»« database
      const dbBatch = await this.saleRepository.find({
        where: [{ statusAsys: false }, { statusAsys: IsNull() }],
        order: { createdAt: 'DESC' },
        take: DB_BATCH_SIZE,
      });

      if (dbBatch.length === 0) {
        break;
      }

      // Xá»­ lÃ½ batch nÃ y theo tá»«ng nhÃ³m nhá»
      for (let i = 0; i < dbBatch.length; i += PROCESS_BATCH_SIZE) {
        const processBatch = dbBatch.slice(i, i + PROCESS_BATCH_SIZE);

        // Xá»­ lÃ½ batch vá»›i giá»›i háº¡n concurrent
        const batchResults = await processBatchConcurrent(
          processBatch,
          CONCURRENT_LIMIT,
        );

        // Cáº­p nháº­t counters
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

      // Náº¿u batch nhá» hÆ¡n DB_BATCH_SIZE, cÃ³ nghÄ©a lÃ  Ä‘Ã£ háº¿t records
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
   * Äá»“ng bá»™ láº¡i má»™t Ä‘Æ¡n hÃ ng cá»¥ thá»ƒ - check láº¡i vá»›i Loyalty API
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
        message: `ÄÆ¡n hÃ ng ${docCode} khÃ´ng cÃ³ dÃ²ng nÃ o cáº§n Ä‘á»“ng bá»™`,
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

    // Check láº¡i tá»«ng sale vá»›i Loyalty API
    for (const sale of errorSales) {
      try {
        const itemCode = sale.itemCode || '';
        if (!itemCode) {
          failCount++;
          continue;
        }

        const product = await this.loyaltyService.checkProduct(itemCode);

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
            `[syncErrorOrderByDocCode] âŒ Sale ${sale.id} (${docCode}): itemCode ${itemCode} váº«n khÃ´ng tá»“n táº¡i trong Loyalty`,
          );
        }
      } catch (error: any) {
        failCount++;
        this.logger.error(
          `[syncErrorOrderByDocCode] âŒ Lá»—i khi check sale ${sale.id}: ${error?.message || error}`,
        );
      }
    }

    const message =
      successCount > 0
        ? `Äá»“ng bá»™ thÃ nh cÃ´ng: ${successCount} dÃ²ng Ä‘Ã£ Ä‘Æ°á»£c cáº­p nháº­t${failCount > 0 ? `, ${failCount} dÃ²ng váº«n lá»—i` : ''}`
        : `KhÃ´ng cÃ³ dÃ²ng nÃ o Ä‘Æ°á»£c cáº­p nháº­t. ${failCount} dÃ²ng váº«n khÃ´ng tÃ¬m tháº¥y trong Loyalty API`;

    return {
      success: successCount > 0,
      message,
      updated: successCount,
      failed: failCount,
      details,
    };
  }

  /**
   * Äá»“ng bá»™ sales theo khoáº£ng thá»i gian cho táº¥t cáº£ brands
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

      // Láº·p qua tá»«ng brand
      for (const brand of brands) {
        this.logger.log(
          `[syncSalesByDateRange] Báº¯t Ä‘áº§u Ä‘á»“ng bá»™ brand: ${brand}`,
        );
        let brandOrdersCount = 0;
        let brandSalesCount = 0;
        let brandCustomersCount = 0;
        const brandErrors: string[] = [];

        // Láº·p qua tá»«ng ngÃ y trong khoáº£ng thá»i gian
        const currentDate = new Date(start);
        while (currentDate <= end) {
          const dateStr = formatDate(currentDate);
          try {
            this.logger.log(
              `[syncSalesByDateRange] Äá»“ng bá»™ ${brand} - ngÃ y ${dateStr}`,
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
            const errorMsg = `[${brand}] Lá»—i khi Ä‘á»“ng bá»™ ngÃ y ${dateStr}: ${error?.message || error}`;
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
          `[syncSalesByDateRange] HoÃ n thÃ nh Ä‘á»“ng bá»™ brand: ${brand} - ${brandOrdersCount} Ä‘Æ¡n, ${brandSalesCount} sale`,
        );
      }

      return {
        success: allErrors.length === 0,
        message: `Äá»“ng bá»™ thÃ nh cÃ´ng tá»« ${startDate} Ä‘áº¿n ${endDate}: ${totalOrdersCount} Ä‘Æ¡n hÃ ng, ${totalSalesCount} sale, ${totalCustomersCount} khÃ¡ch hÃ ng`,
        totalOrdersCount,
        totalSalesCount,
        totalCustomersCount,
        brandResults,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(
        `Lá»—i khi Ä‘á»“ng bá»™ sale theo khoáº£ng thá»i gian: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Äá»“ng bá»™ sales tá»« Zappy API cho má»™t ngÃ y cá»¥ thá»ƒ
   * NOTE: Method nÃ y ráº¥t dÃ i (~400 dÃ²ng), cáº§n refactor thÃªm
   * TODO: TÃ¡ch thÃ nh cÃ¡c private helper methods
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

          // Check Loyalty Products
          const notFoundItemCodes =
            await this.getNotFoundItemCodesForOrder(order);

          // Process Sales
          const orderSalesCount = await this.processOrderSales(
            order,
            customer,
            brandFromDepartment, // Use mapped brand
            notFoundItemCodes,
            cashMapBySoCode,
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

  private async getNotFoundItemCodesForOrder(order: any): Promise<Set<string>> {
    const orderItemCodes = Array.from(
      new Set(
        (order.sales || [])
          .map((s: any) => s.itemCode?.trim())
          .filter((code: any): code is string => !!code && code !== ''),
      ),
    );

    const notFoundItemCodes = new Set<string>();
    if (orderItemCodes.length > 0) {
      await Promise.all(
        orderItemCodes.map(async (trimmedItemCode: string) => {
          const product =
            await this.loyaltyService.checkProduct(trimmedItemCode);
          if (!product) {
            notFoundItemCodes.add(trimmedItemCode);
          }
        }),
      );
    }
    return notFoundItemCodes;
  }

  private async processOrderSales(
    order: any,
    customer: Customer,
    brand: string,
    notFoundItemCodes: Set<string>,
    cashMapBySoCode: Map<string, any[]>,
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
              `[SalesSyncService] Sale item ${itemCode} trong order ${order.docCode} - Sáº£n pháº©m khÃ´ng tá»“n táº¡i trong Loyalty API (404), sáº½ lÆ°u vá»›i statusAsys = false`,
            );
          }

          const productType = await this.resolveProductType(
            saleItem,
            itemCode,
            notFoundItemCodes,
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
            type_sale: 'RETAIL',
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
          const errorMsg = `Lá»—i khi lÆ°u sale ${order.docCode}/${saleItem.itemCode}: ${saleError?.message || saleError}`;
          if (onError) onError(errorMsg);
        }
      }
    }
    return salesCount;
  }

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
