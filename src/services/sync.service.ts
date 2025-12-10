import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { Customer } from '../entities/customer.entity';
import { Sale } from '../entities/sale.entity';
import { ZappyApiService } from './zappy-api.service';
import { Order } from '../types/order.types';
import { SalesService } from '../modules/sales/sales.service';

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
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    @Inject(forwardRef(() => SalesService))
    private salesService: SalesService,
  ) {}

  async syncAllBrands(date: string): Promise<{
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
    this.logger.log(`Bắt đầu đồng bộ dữ liệu từ Zappy API cho tất cả nhãn hàng cho ngày ${date}`);
    const brands = ['f3', 'labhair', 'yaman', 'menard']; // Các brand có Zappy API
    const allErrors: string[] = [];
    let totalOrdersCount = 0;
    let totalSalesCount = 0;
    let totalCustomersCount = 0;
    let totalInvoiceSuccessCount = 0;
    let totalInvoiceFailureCount = 0;
    const totalInvoiceErrors: string[] = [];

    for (const brand of brands) {
      try {
        const result = await this.syncBrand(brand, date);
        totalOrdersCount += result.ordersCount;
        totalSalesCount += result.salesCount;
        totalCustomersCount += result.customersCount;
        totalInvoiceSuccessCount += result.invoiceSuccessCount || 0;
        totalInvoiceFailureCount += result.invoiceFailureCount || 0;
        if (result.invoiceErrors) {
          totalInvoiceErrors.push(...result.invoiceErrors);
        }
        if (result.errors) {
          allErrors.push(...result.errors);
        }
      } catch (error: any) {
        const errorMsg = `Lỗi khi đồng bộ ${brand} cho ngày ${date}: ${error?.message || error}`;
        this.logger.error(errorMsg);
        allErrors.push(errorMsg);
      }
    }

    this.logger.log(
      `Hoàn thành đồng bộ tất cả nhãn hàng cho ngày ${date}: ${totalOrdersCount} orders, ${totalSalesCount} sales mới, ${totalCustomersCount} customers mới. ` +
      `Hóa đơn Fast API: ${totalInvoiceSuccessCount} thành công, ${totalInvoiceFailureCount} thất bại.`,
    );

    return {
      success: allErrors.length === 0,
      message: allErrors.length === 0
        ? `Đồng bộ tất cả nhãn hàng thành công cho ngày ${date}`
        : `Đồng bộ tất cả nhãn hàng hoàn thành với ${allErrors.length} lỗi cho ngày ${date}`,
      ordersCount: totalOrdersCount,
      salesCount: totalSalesCount,
      customersCount: totalCustomersCount,
      invoiceSuccessCount: totalInvoiceSuccessCount,
      invoiceFailureCount: totalInvoiceFailureCount,
      invoiceErrors: totalInvoiceErrors,
      errors: allErrors.length > 0 ? allErrors : undefined,
    };
  }

  async syncBrand(brandName: string, date: string): Promise<{
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
    this.logger.log(`Đang đồng bộ dữ liệu từ Zappy API cho ngày ${date} (brand: ${brandName})`);
    return this.syncFromZappy(date, brandName);
  }

  async syncFromZappy(date: string, brand?: string): Promise<{
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
    this.logger.log(`Bắt đầu đồng bộ dữ liệu từ Zappy API cho ngày ${date}${brand ? ` (brand: ${brand})` : ''}`);

    try {
      // Lấy dữ liệu từ Zappy API với brand cụ thể
      const orders = await this.zappyApiService.getDailySales(date, brand);
      
      // Lấy dữ liệu cash/voucher từ get_daily_cash/get_daily_cashio để enrich
      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date, brand);
        this.logger.log(`Fetched ${cashData.length} cash records for date ${date}${brand ? ` (brand: ${brand})` : ''}`);
      } catch (error) {
        this.logger.warn(`Failed to fetch daily cash data: ${error}`);
      }

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
            .filter((code): code is string => !!code && code.trim() !== '')
        )
      );

      // Fetch departments để lấy company và map sang brand
      const departmentMap = new Map<string, { company?: string }>();
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
          this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
        }
      }

      // Map company sang brand
      const mapCompanyToBrand = (company: string | null | undefined): string => {
        if (!company) return '';
        const companyUpper = company.toUpperCase();
        const brandMap: Record<string, string> = {
          'F3': 'f3',
          'FACIALBAR': 'f3',
          'MENARD': 'menard',
          'CHANDO': 'chando',
          'LABHAIR': 'labhair',
          'YAMAN': 'yaman',
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
              enteredat: order.customer.enteredat ? new Date(order.customer.enteredat) : null,
              crm_lead_source: order.customer.crm_lead_source,
              address: order.customer.address,
              province_name: order.customer.province_name,
              birthday: order.customer.birthday ? new Date(order.customer.birthday) : null,
              grade_name: order.customer.grade_name,
              branch_code: order.customer.branch_code,
            } as Partial<Customer>);
            customer = await this.customerRepository.save(newCustomer);
            customersCount++;
          } else {
            // Cập nhật thông tin customer nếu cần
            customer.name = order.customer.name || customer.name;
            customer.mobile = order.customer.mobile || customer.mobile;
            customer.grade_name = order.customer.grade_name || customer.grade_name;
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
          const voucherData = orderCashData.filter((cash) => cash.fop_syscode === 'VOUCHER');
          
          // Collect tất cả itemCodes từ order để fetch products từ Loyalty API
          const orderItemCodes = Array.from(
            new Set(
              (order.sales || [])
                .map((s) => s.itemCode)
                .filter((code): code is string => !!code && code.trim() !== '')
            )
          );

          // Fetch products từ Loyalty API để kiểm tra dvt và productType (song song)
          const productDvtMap = new Map<string, string>();
          const productTypeMap = new Map<string, string>();
          if (orderItemCodes.length > 0) {
            await Promise.all(
              orderItemCodes.map(async (itemCode) => {
                try {
                  const response = await this.httpService.axiosRef.get(
                    `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
                    { headers: { accept: 'application/json' } },
                  );
                  const loyaltyProduct = response?.data?.data?.item || response?.data;
                  if (loyaltyProduct?.unit) {
                    productDvtMap.set(itemCode, loyaltyProduct.unit);
                  }
                  // Lưu productType từ Loyalty API
                  if (loyaltyProduct?.productType || loyaltyProduct?.producttype) {
                    productTypeMap.set(itemCode, loyaltyProduct.productType || loyaltyProduct.producttype);
                  }
                } catch (error) {
                  // Không có dvt hoặc productType từ Loyalty API
                }
              }),
            );
          }
          
          // Xử lý từng sale trong order - TRUYỀN MẤY LƯU NẤY (lưu tất cả các dòng từ Zappy API)
          if (order.sales && order.sales.length > 0) {
            this.logger.log(`Order ${order.docCode} có ${order.sales.length} sale items, bắt đầu lưu tất cả`);
            for (const saleItem of order.sales) {
              try {
                // Enrich voucher data từ get_daily_cash
                let voucherRefno: string | undefined;
                let voucherAmount: number | undefined;
                if (voucherData.length > 0) {
                  // Lấy voucher đầu tiên (có thể có nhiều voucher)
                  const firstVoucher = voucherData[0];
                  voucherRefno = firstVoucher.refno;
                  voucherAmount = firstVoucher.total_in || 0;
                }

                // Luôn tạo sale mới - TRUYỀN MẤY LƯU NẤY (không check duplicate, lưu tất cả)
                // Lấy productType từ Loyalty API
                const productType = productTypeMap.get(saleItem.itemCode || '');
                // Lấy dvt từ Loyalty API nếu không có
                const dvt = saleItem.dvt || productDvtMap.get(saleItem.itemCode || '') || null;
                this.logger.debug(`Đang lưu sale: ${order.docCode}/${saleItem.itemCode}, promCode: ${saleItem.promCode || 'null'}, serial: ${saleItem.serial || 'null'}`);
                const newSale = this.saleRepository.create({
                    docCode: order.docCode,
                    docDate: new Date(order.docDate),
                    branchCode: order.branchCode,
                    docSourceType: order.docSourceType,
                    ordertype: saleItem.ordertype,
                    description: saleItem.description,
                    partnerCode: saleItem.partnerCode,
                    itemCode: saleItem.itemCode || '',
                    itemName: saleItem.itemName || '',
                    qty: saleItem.qty || 0,
                    revenue: saleItem.revenue || 0,
                    linetotal: saleItem.linetotal || saleItem.revenue || 0,
                    productType: productType || undefined,
                    tienHang: saleItem.tienHang || saleItem.linetotal || saleItem.revenue || 0,
                    giaBan: saleItem.giaBan || 0,
                    promCode: saleItem.promCode,
                    serial: saleItem.serial,
                    soSerial: saleItem.serial,
                    dvt: dvt, // Dvt từ Loyalty API nếu có
                    disc_amt: saleItem.disc_amt,
                    grade_discamt: saleItem.grade_discamt,
                    other_discamt: saleItem.other_discamt,
                    chietKhauMuaHangGiamGia: saleItem.chietKhauMuaHangGiamGia,
                    paid_by_voucher_ecode_ecoin_bp: saleItem.paid_by_voucher_ecode_ecoin_bp,
                    maCa: saleItem.shift_code,
                    saleperson_id: this.validateInteger(saleItem.saleperson_id),
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
                    // Enrich voucher data từ get_daily_cash
                    voucherDp1: voucherRefno,
                    thanhToanVoucher: voucherAmount && voucherAmount > 0 ? voucherAmount : undefined,
                    customer: customer,
                    isProcessed: false,
                  } as Partial<Sale>);
                  const savedSale = await this.saleRepository.save(newSale);
                  salesCount++;
                  this.logger.debug(`Đã lưu thành công sale: ${order.docCode}/${saleItem.itemCode}, promCode: ${saleItem.promCode || 'null'}, id: ${savedSale.id}`);
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

      this.logger.log(
        `Hoàn thành đồng bộ: ${orders.length} orders, ${salesCount} sales mới, ${customersCount} customers mới`,
      );

      // Tự động tạo hóa đơn cho tất cả các đơn hàng vừa đồng bộ
      // Lấy docCodes từ database (chỉ các đơn hàng thực sự có sales được lưu)
      // để tránh lỗi khi order không có sales nào được lưu (do filter dvt)
      const savedDocCodes = await this.saleRepository
        .createQueryBuilder('sale')
        .select('DISTINCT sale.docCode', 'docCode')
        .where('sale.isProcessed = :isProcessed', { isProcessed: false })
        .getRawMany();
      
      const docCodes = savedDocCodes.map((item: any) => item.docCode).filter((code: string) => code);
      
      let invoiceSuccessCount = 0;
      let invoiceFailureCount = 0;
      const invoiceErrors: string[] = [];

      this.logger.log(`Bắt đầu tự động tạo hóa đơn cho ${docCodes.length} đơn hàng (có sales trong DB)...`);
      
      for (const docCode of docCodes) {
          try {
          this.logger.log(`Đang tạo hóa đơn cho đơn hàng: ${docCode}`);
          const result = await this.salesService.createInvoiceViaFastApi(docCode);
          if (result.success) {
            invoiceSuccessCount++;
            this.logger.log(`✓ Tạo hóa đơn thành công cho đơn hàng: ${docCode}`);
          } else {
            invoiceFailureCount++;
            const errorMsg = `Tạo hóa đơn thất bại cho ${docCode}: ${result.message || 'Unknown error'}`;
            invoiceErrors.push(errorMsg);
            this.logger.warn(`✗ ${errorMsg}`);
          }
        } catch (error: any) {
          invoiceFailureCount++;
          const errorMsg = `Lỗi khi tạo hóa đơn cho đơn hàng ${docCode}: ${error?.message || error}`;
          invoiceErrors.push(errorMsg);
          this.logger.error(`✗ ${errorMsg}`);
        }
      }

      this.logger.log(
        `Hoàn thành tự động tạo hóa đơn: ${invoiceSuccessCount} thành công, ${invoiceFailureCount} thất bại`,
      );

      return {
        success: errors.length === 0,
        message: `Đồng bộ thành công ${orders.length} đơn hàng cho ngày ${date}. Tự động tạo hóa đơn: ${invoiceSuccessCount} thành công, ${invoiceFailureCount} thất bại`,
        ordersCount: orders.length,
        salesCount,
        customersCount,
        invoiceSuccessCount,
        invoiceFailureCount,
        errors: errors.length > 0 ? errors : undefined,
        invoiceErrors: invoiceErrors.length > 0 ? invoiceErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ từ Zappy API: ${error?.message || error}`);
      throw error;
    }
  }

  // Method cũ không còn dùng - đã chuyển sang Zappy API
  // Giữ lại để tương thích nếu cần, nhưng không được gọi
  private async processCustomerData_OLD(
    customerData: any,
    brand: string,
  ): Promise<void> {
    const personalInfo = customerData?.Personal_Info;
    const sales = customerData?.Sales || [];

    // Tìm hoặc tạo customer
    let customer = await this.customerRepository.findOne({
      where: { code: personalInfo.code, brand },
    });

    if (!customer) {
      const newCustomerData: Partial<Customer> = {
        code: personalInfo.code,
        name: personalInfo.name,
        brand,
      };
      
      // Xử lý địa chỉ: ưu tiên street, nếu không có thì dùng address
      if (personalInfo.street) {
        newCustomerData.street = personalInfo.street;
      } else if (personalInfo.address) {
        newCustomerData.street = personalInfo.address;
        newCustomerData.address = personalInfo.address;
      }
      
      if (personalInfo.birthday) newCustomerData.birthday = new Date(personalInfo.birthday);
      if (personalInfo.sexual) newCustomerData.sexual = personalInfo.sexual;
      
      // Xử lý số điện thoại: lưu cả phone và mobile
      if (personalInfo.phone) {
        newCustomerData.phone = personalInfo.phone;
      }
      if (personalInfo.mobile) {
        newCustomerData.mobile = personalInfo.mobile;
        // Nếu chưa có phone thì dùng mobile
        if (!newCustomerData.phone) {
          newCustomerData.phone = personalInfo.mobile;
        }
      }
      
      // Lưu các trường mới
      if (personalInfo.idnumber) newCustomerData.idnumber = personalInfo.idnumber;
      if (personalInfo.enteredat) newCustomerData.enteredat = new Date(personalInfo.enteredat);
      if (personalInfo.crm_lead_source) newCustomerData.crm_lead_source = personalInfo.crm_lead_source;
      if (personalInfo.province_name) newCustomerData.province_name = personalInfo.province_name;
      if (personalInfo.grade_name) newCustomerData.grade_name = personalInfo.grade_name;
      if (personalInfo.branch_code) newCustomerData.branch_code = personalInfo.branch_code;

      customer = this.customerRepository.create(newCustomerData);
      customer = await this.customerRepository.save(customer);
    } else {
      // Cập nhật thông tin nếu có thay đổi
      customer.name = personalInfo.name;
      
      // Xử lý địa chỉ
      if (personalInfo.street !== undefined) {
        customer.street = personalInfo.street;
      }
      if (personalInfo.address !== undefined) {
        customer.address = personalInfo.address;
        if (!customer.street) {
          customer.street = personalInfo.address;
        }
      }
      
      // Xử lý số điện thoại: lưu cả phone và mobile
      if (personalInfo.phone !== undefined) {
        customer.phone = personalInfo.phone;
      }
      if (personalInfo.mobile !== undefined) {
        customer.mobile = personalInfo.mobile;
        // Nếu chưa có phone thì dùng mobile
        if (!customer.phone) {
          customer.phone = personalInfo.mobile;
        }
      }
      
      if (personalInfo.birthday) {
        customer.birthday = new Date(personalInfo.birthday);
      }
      
      // Cập nhật các trường mới
      if (personalInfo.idnumber !== undefined) customer.idnumber = personalInfo.idnumber;
      if (personalInfo.enteredat) customer.enteredat = new Date(personalInfo.enteredat);
      if (personalInfo.crm_lead_source !== undefined) customer.crm_lead_source = personalInfo.crm_lead_source;
      if (personalInfo.province_name !== undefined) customer.province_name = personalInfo.province_name;
      if (personalInfo.grade_name !== undefined) customer.grade_name = personalInfo.grade_name;
      if (personalInfo.branch_code !== undefined) customer.branch_code = personalInfo.branch_code;
      
      await this.customerRepository.save(customer);
    }

    // Xử lý sales
    // LOGIC SALE:
    // Sale = Dòng bán hàng (KHÔNG phải đơn hàng)
    // Order = Đơn hàng (được nhận diện bởi docCode)
    // 
    // Mối quan hệ: 1 Order (docCode) = Nhiều Sale (nhiều sản phẩm)
    //
    // 1. Mỗi sale đại diện cho 1 dòng bán hàng (1 sản phẩm trong 1 đơn hàng)
    // 2. Sale được lưu trực tiếp từ API với các thông tin:
    //    - qty: Số lượng sản phẩm (từ API, không tính toán)
    //    - revenue: Doanh thu của dòng này (từ API, có thể = 0 nếu là hàng khuyến mãi/tặng)
    //    - docCode: Mã đơn hàng (Order) - cùng 1 docCode = cùng 1 đơn hàng
    //    - itemCode: Mã sản phẩm
    // 3. Tránh trùng lặp: Kiểm tra sale đã tồn tại dựa trên (docCode + itemCode + customerId)
    //    - Nếu đã tồn tại: Bỏ qua (không cập nhật)
    //    - Nếu chưa tồn tại: Tạo mới với isProcessed = false
    // 4. Lưu ý: 
    //    - 1 Order (docCode) có thể có nhiều Sale (nhiều sản phẩm trong đơn)
    //    - Cùng 1 docCode + itemCode có thể có nhiều sale nếu qty khác nhau (tách dòng)
    //    - revenue có thể = 0 (hàng tặng, khuyến mãi)
    //    - isProcessed: Đánh dấu sale đã được dùng để tạo invoice chưa
    
    let salesCreated = 0;
    let salesSkipped = 0;
    let salesError = 0;
    
    for (const saleData of sales) {
      try {
        // Kiểm tra xem sale đã tồn tại chưa
        // Kiểm tra dựa trên: docCode + itemCode + qty + revenue + customerId
        // Điều này cho phép lưu nhiều sale với cùng docCode + itemCode nếu qty hoặc revenue khác nhau
        const existingSale = await this.saleRepository.findOne({
          where: {
            docCode: saleData.doccode,
            itemCode: saleData.itemcode,
            qty: saleData.qty,
            revenue: saleData.revenue,
            customerId: customer.id,
          },
        });

        if (!existingSale) {
          // Tạo sale mới - lưu trực tiếp qty và revenue từ API
          // KHÔNG tính toán lại, chỉ lưu giá trị từ API
          const saleDataToCreate: Partial<Sale> = {
            branchCode: saleData.branch_code,
            docCode: saleData.doccode, // Mã đơn hàng
            docDate: new Date(saleData.docdate),
            docSourceType: saleData.docsourcetype || 'sale', // Mặc định 'sale' nếu không có
            partnerCode: saleData.partner_code || personalInfo.code, // Dùng customer code nếu không có partner_code
            itemCode: saleData.itemcode, // Mã sản phẩm
            itemName: saleData.itemname,
            qty: saleData.qty, // Số lượng - lưu trực tiếp từ API
            revenue: saleData.revenue || 0, // Doanh thu - lưu trực tiếp từ API (có thể = 0)
            customerId: customer.id,
            isProcessed: false, // Mặc định chưa xử lý (chưa tạo invoice)
          };
          
          // Các trường optional
          if (saleData.description) saleDataToCreate.description = saleData.description;
          if (saleData.kenh) saleDataToCreate.kenh = saleData.kenh;
          if (saleData.prom_code) saleDataToCreate.promCode = saleData.prom_code;
          if (saleData.ordertype) saleDataToCreate.ordertype = saleData.ordertype;
          
          // Các trường bổ sung từ API
          if (saleData.cat1 !== undefined) saleDataToCreate.cat1 = saleData.cat1;
          if (saleData.cat2 !== undefined) saleDataToCreate.cat2 = saleData.cat2;
          if (saleData.cat3 !== undefined) saleDataToCreate.cat3 = saleData.cat3;
          if (saleData.catcode1 !== undefined) saleDataToCreate.catcode1 = saleData.catcode1;
          if (saleData.catcode2 !== undefined) saleDataToCreate.catcode2 = saleData.catcode2;
          if (saleData.catcode3 !== undefined) saleDataToCreate.catcode3 = saleData.catcode3;
          if (saleData.ck_tm !== undefined && saleData.ck_tm !== null) saleDataToCreate.ck_tm = saleData.ck_tm;
          if (saleData.ck_dly !== undefined && saleData.ck_dly !== null) saleDataToCreate.ck_dly = saleData.ck_dly;
          if (saleData.docid !== undefined) saleDataToCreate.docid = this.validateInteger(saleData.docid);
          if (saleData.serial !== undefined && saleData.serial !== null) saleDataToCreate.serial = saleData.serial;
          if (saleData.cm_code !== undefined && saleData.cm_code !== null) saleDataToCreate.cm_code = saleData.cm_code;
          if (saleData.line_id !== undefined) saleDataToCreate.line_id = this.validateInteger(saleData.line_id);
          if (saleData.disc_amt !== undefined) saleDataToCreate.disc_amt = saleData.disc_amt;
          if (saleData.docmonth !== undefined) saleDataToCreate.docmonth = saleData.docmonth;
          if (saleData.itemcost !== undefined) saleDataToCreate.itemcost = saleData.itemcost;
          if (saleData.linetotal !== undefined) saleDataToCreate.linetotal = saleData.linetotal;
          if (saleData.totalcost !== undefined) saleDataToCreate.totalcost = saleData.totalcost;
          if (saleData.crm_emp_id !== undefined) saleDataToCreate.crm_emp_id = this.validateInteger(saleData.crm_emp_id);
          if (saleData.doctype_name !== undefined) saleDataToCreate.doctype_name = saleData.doctype_name;
          if (saleData.order_source !== undefined && saleData.order_source !== null) saleDataToCreate.order_source = saleData.order_source;
          if (saleData.partner_name !== undefined) saleDataToCreate.partner_name = saleData.partner_name;
          if (saleData.crm_branch_id !== undefined) saleDataToCreate.crm_branch_id = this.validateInteger(saleData.crm_branch_id);
          if (saleData.grade_discamt !== undefined) saleDataToCreate.grade_discamt = saleData.grade_discamt;
          if (saleData.revenue_wsale !== undefined) saleDataToCreate.revenue_wsale = saleData.revenue_wsale;
          if (saleData.saleperson_id !== undefined) saleDataToCreate.saleperson_id = this.validateInteger(saleData.saleperson_id);
          if (saleData.revenue_retail !== undefined) saleDataToCreate.revenue_retail = saleData.revenue_retail;
          if (saleData.paid_by_voucher_ecode_ecoin_bp !== undefined) saleDataToCreate.paid_by_voucher_ecode_ecoin_bp = saleData.paid_by_voucher_ecode_ecoin_bp;

          const sale = this.saleRepository.create(saleDataToCreate);
          await this.saleRepository.save(sale);
          salesCreated++;
          this.logger.debug(
            `Đã tạo sale: ${saleData.doccode}/${saleData.itemcode} - qty: ${saleData.qty}, revenue: ${saleData.revenue}`,
          );
        } else {
          salesSkipped++;
          this.logger.debug(
            `Bỏ qua sale (đã tồn tại): ${saleData.doccode}/${saleData.itemcode} - qty: ${saleData.qty}, revenue: ${saleData.revenue}`,
          );
        }
      } catch (error) {
        salesError++;
        this.logger.error(
          `Lỗi khi lưu sale ${saleData.doccode}/${saleData.itemcode} (qty: ${saleData.qty}, revenue: ${saleData.revenue}): ${error.message}`,
        );
        // Tiếp tục xử lý sale tiếp theo, không dừng toàn bộ
      }
    }
    
    if (sales.length > 0) {
      this.logger.log(
        `Customer ${personalInfo.code}: Tổng ${sales.length} sales - Đã tạo: ${salesCreated}, Bỏ qua: ${salesSkipped}, Lỗi: ${salesError}`,
      );
    }
  }
}

