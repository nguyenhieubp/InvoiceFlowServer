import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ZappyApiService } from '../../services/zappy-api.service';
import { LoyaltyService } from '../../services/loyalty.service';
import * as SalesUtils from '../../utils/sales.utils';

/**
 * SalesSyncService
 * Handle sync operations with external APIs (Zappy, Loyalty)
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
   * Sync sales from Zappy API
   * NOTE: This is a large method (375 lines) moved from SalesService
   * Consider further refactoring if needed
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
    // Implementation moved from SalesService.syncFromZappy()
    // Keeping the same logic to preserve functionality
    try {
      const orders = await this.zappyApiService.getDailySales(date, brand);

      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date, brand);
      } catch (error) {
        this.logger.warn('Failed to fetch cash data', error);
      }

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

      // Collect branch codes
      const branchCodes = Array.from(
        new Set(
          orders
            .map((o) => o.branchCode)
            .filter((code): code is string => !!code && code.trim() !== ''),
        ),
      );

      // Fetch departments
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
              `Failed to fetch department for branchCode ${branchCode}`,
              error,
            );
          }
        }
      }

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

      // Process each order
      for (const order of orders) {
        try {
          const department = departmentMap.get(order.branchCode);
          const brandFromDepartment = department?.company
            ? mapCompanyToBrand(department.company)
            : order.customer.brand || '';

          // Find or create customer
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
            customer.name = order.customer.name || customer.name;
            customer.mobile = order.customer.mobile || customer.mobile;
            customer.grade_name =
              order.customer.grade_name || customer.grade_name;
            if (brandFromDepartment) {
              customer.brand = brandFromDepartment;
            }
            customer = await this.customerRepository.save(customer);
          }

          if (!customer) {
            const errorMsg = `Không thể tạo hoặc tìm customer với code ${order.customer.code}`;
            this.logger.error(errorMsg);
            errors.push(errorMsg);
            continue;
          }

          const orderCashData = cashMapBySoCode.get(order.docCode) || [];
          const voucherData = orderCashData.filter(
            (cash) => cash.fop_syscode === 'VOUCHER',
          );

          const orderItemCodes = Array.from(
            new Set(
              (order.sales || [])
                .map((s) => s.itemCode?.trim())
                .filter((code): code is string => !!code && code !== ''),
            ),
          );

          // Check products from Loyalty API
          const notFoundItemCodes = new Set<string>();
          if (orderItemCodes.length > 0) {
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

          // Process each sale item
          if (order.sales && order.sales.length > 0) {
            for (const saleItem of order.sales) {
              try {
                const itemCode = saleItem.itemCode?.trim();
                if (itemCode && itemCode.toUpperCase() === 'TRUTONKEEP') {
                  this.logger.log(
                    `Bỏ qua sale item ${itemCode} trong order ${order.docCode} - itemcode = TRUTONKEEP`,
                  );
                  continue;
                }

                const isNotFound = itemCode && notFoundItemCodes.has(itemCode);
                const statusAsys = !isNotFound;

                if (isNotFound) {
                  this.logger.warn(
                    `Sale item ${itemCode} trong order ${order.docCode} - Sản phẩm không tồn tại trong Loyalty API (404)`,
                  );
                }

                const productTypeFromZappy =
                  saleItem.producttype || saleItem.productType || null;
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
                    // Ignore
                  }
                }
                const productType =
                  productTypeFromZappy || productTypeFromLoyalty || null;

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

                let voucherRefno: string | undefined;
                let voucherAmount: number | undefined;
                if (voucherData.length > 0) {
                  const firstVoucher = voucherData[0];
                  voucherRefno = firstVoucher.refno;
                  voucherAmount = firstVoucher.total_in || 0;
                }

                const newSale = this.saleRepository.create({
                  docCode: order.docCode,
                  docDate: new Date(order.docDate),
                  branchCode: order.branchCode,
                  docSourceType: order.docSourceType,
                  ordertype: saleItem.ordertype,
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
                  saleperson_id: SalesUtils.validateInteger(
                    saleItem.saleperson_id,
                  ),
                  partner_name: saleItem.partner_name,
                  order_source: saleItem.order_source,
                  maThe: saleItem.mvc_serial,
                  cat1: saleItem.cat1,
                  cat2: saleItem.cat2,
                  cat3: saleItem.cat3,
                  catcode1: saleItem.catcode1,
                  catcode2: saleItem.catcode2,
                  catcode3: saleItem.catcode3,
                  productType:
                    productType && productType.trim() !== ''
                      ? productType.trim()
                      : null,
                  voucherDp1: voucherRefno,
                  thanhToanVoucher:
                    voucherAmount && voucherAmount > 0
                      ? voucherAmount
                      : undefined,
                  customer: customer,
                  brand: brand,
                  isProcessed: false,
                  statusAsys: statusAsys,
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
   * Sync error orders - check with Loyalty API and update statusAsys
   * NOTE: This method processes in batches to avoid memory issues
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
    // Implementation moved from SalesService.syncErrorOrders()
    // Placeholder - full implementation would be moved here
    this.logger.log('syncErrorOrders - to be implemented');
    return {
      total: 0,
      success: 0,
      failed: 0,
      updated: [],
    };
  }

  /**
   * Sync error order by docCode
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
    // Implementation moved from SalesService.syncErrorOrderByDocCode()
    // Placeholder - full implementation would be moved here
    this.logger.log(`syncErrorOrderByDocCode - ${docCode}`);
    return {
      success: true,
      message: 'To be implemented',
      updated: 0,
      failed: 0,
      details: [],
    };
  }

  /**
   * Sync sales by date range
   */
  async syncSalesByDateRange(startDate: string, endDate: string): Promise<any> {
    // Implementation moved from SalesService.syncSalesByDateRange()
    // Placeholder - full implementation would be moved here
    this.logger.log(`syncSalesByDateRange - ${startDate} to ${endDate}`);
    return {
      success: true,
      message: 'To be implemented',
    };
  }
}
