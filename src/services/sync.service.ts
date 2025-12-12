import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { Customer } from '../entities/customer.entity';
import { Sale } from '../entities/sale.entity';
import { DailyCashio } from '../entities/daily-cashio.entity';
import { ZappyApiService } from './zappy-api.service';
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
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
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
    const brands = ['f3', 'labhair', 'yaman', 'menard']; // Các brand có Zappy API
    const allErrors: string[] = [];
    let totalOrdersCount = 0;
    let totalSalesCount = 0;
    let totalCustomersCount = 0;
    let totalInvoiceSuccessCount = 0;
    let totalInvoiceFailureCount = 0;
    const totalInvoiceErrors: string[] = [];

    // Delay giữa các brand để giảm tải (1 phút)
    const delayBetweenBrands = 60000; // 60 seconds (1 minute)

    for (let i = 0; i < brands.length; i++) {
      const brand = brands[i];
      try {
        this.logger.log(`Bắt đầu đồng bộ brand ${brand} (${i + 1}/${brands.length}) cho ngày ${date}`);
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
        this.logger.log(`Hoàn thành đồng bộ brand ${brand} cho ngày ${date}`);
        
        // Thêm delay trước khi đồng bộ brand tiếp theo (trừ brand cuối cùng)
        if (i < brands.length - 1) {
          this.logger.log(`Chờ ${delayBetweenBrands / 1000} giây trước khi đồng bộ brand tiếp theo...`);
          await new Promise(resolve => setTimeout(resolve, delayBetweenBrands));
        }
      } catch (error: any) {
        const errorMsg = `Lỗi khi đồng bộ ${brand} cho ngày ${date}: ${error?.message || error}`;
        this.logger.error(errorMsg);
        allErrors.push(errorMsg);
        
        // Vẫn thêm delay ngay cả khi có lỗi để tránh quá tải
        if (i < brands.length - 1) {
          this.logger.log(`Chờ ${delayBetweenBrands / 1000} giây trước khi đồng bộ brand tiếp theo...`);
          await new Promise(resolve => setTimeout(resolve, delayBetweenBrands));
        }
      }
    }

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

    try {
      // Lấy dữ liệu từ Zappy API với brand cụ thể
      const orders = await this.zappyApiService.getDailySales(date, brand);
      
      // Lấy dữ liệu cash/voucher từ get_daily_cash/get_daily_cashio để enrich
      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date, brand);
      } catch (error) {
        this.logger.warn(`Failed to fetch daily cash data: ${error}`);
      }

      // Lưu TẤT CẢ cashio records vào database
      if (cashData.length > 0) {
        try {
          // Parse docdate từ string sang Date
          const parseDocdate = (docdateStr: string): Date => {
            // Format: "03-10-2025 10:30"
            const parts = docdateStr.split(' ');
            const datePart = parts[0]; // "03-10-2025"
            const timePart = parts[1] || '00:00'; // "10:30"
            const [day, month, year] = datePart.split('-');
            const [hour, minute] = timePart.split(':');
            return new Date(parseInt(year), parseInt(month) - 1, parseInt(day), parseInt(hour), parseInt(minute));
          };

          const parseRefnoIdate = (refnoIdateStr: string): Date | null => {
            if (!refnoIdateStr || refnoIdateStr === '00:00' || refnoIdateStr.includes('00:00')) {
              return null;
            }
            // Format: "03-10-2025 00:00"
            const parts = refnoIdateStr.split(' ');
            const datePart = parts[0]; // "03-10-2025"
            const [day, month, year] = datePart.split('-');
            return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
          };

          // Lưu từng cashio record vào database (chỉ insert nếu chưa có, skip nếu đã có)
          let savedCount = 0;
          let skippedCount = 0;
          for (const cash of cashData) {
            try {
              // Tìm xem đã có record với api_id chưa (api_id là unique từ API)
              const existingCashio = await this.dailyCashioRepository.findOne({
                where: { api_id: cash.id },
              });

              // Nếu đã có record với api_id này rồi → skip, không lưu nữa
              if (existingCashio) {
                skippedCount++;
                continue;
              }

              const parsedRefnoIdate = cash.refno_idate ? parseRefnoIdate(cash.refno_idate) : undefined;
              
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
              };

              // Insert new record
              const newCashio = this.dailyCashioRepository.create(cashioData);
              await this.dailyCashioRepository.save(newCashio);
              savedCount++;
            } catch (cashioError: any) {
              this.logger.warn(`Failed to save cashio record ${cash.code}: ${cashioError?.message || cashioError}`);
            }
          }
          this.logger.log(`[Sync] Đã lưu ${savedCount} cashio records mới, bỏ qua ${skippedCount} records đã tồn tại (tổng ${cashData.length} records từ API)`);
        } catch (error: any) {
          this.logger.error(`Failed to save cashio data to database: ${error?.message || error}`);
        }
      }

      // Tạo map cash data theo so_code để dễ lookup (dùng cho enrich vào sale items)
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
            { 
              headers: { accept: 'application/json' },
              timeout: 5000, // 5 seconds timeout
            },
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

          // Lấy cash/voucher/ECOIN data cho order này
          const orderCashData = cashMapBySoCode.get(order.docCode) || [];
          const voucherData = orderCashData.filter((cash) => cash.fop_syscode === 'VOUCHER');
          const ecoinData = orderCashData.filter((cash) => cash.fop_syscode === 'ECOIN');
          
          // Collect tất cả itemCodes từ order để fetch products từ Loyalty API
          const orderItemCodes = Array.from(
            new Set(
              (order.sales || [])
                .map((s) => s.itemCode)
                .filter((code): code is string => !!code && code.trim() !== '')
            )
          );

          // Fetch products từ Loyalty API để kiểm tra dvt, productType, materialCode, trackInventory, trackSerial (song song)
          const productDvtMap = new Map<string, string>();
          const productTypeMap = new Map<string, string>();
          const productMaterialCodeMap = new Map<string, string>();
          const productTrackInventoryMap = new Map<string, boolean>();
          const productTrackSerialMap = new Map<string, boolean>();
          if (orderItemCodes.length > 0) {
            await Promise.all(
              orderItemCodes.map(async (itemCode) => {
                try {
                  const response = await this.httpService.axiosRef.get(
                    `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
                    { 
                      headers: { accept: 'application/json' },
                      timeout: 5000, // 5 seconds timeout
                    },
                  );
                  const loyaltyProduct = response?.data?.data?.item || response?.data;
                  if (loyaltyProduct?.unit) {
                    productDvtMap.set(itemCode, loyaltyProduct.unit);
                  }
                  // Lưu productType từ Loyalty API
                  if (loyaltyProduct?.productType || loyaltyProduct?.producttype) {
                    productTypeMap.set(itemCode, loyaltyProduct.productType || loyaltyProduct.producttype);
                  }
                  // Lưu materialCode từ Loyalty API
                  if (loyaltyProduct?.materialCode) {
                    productMaterialCodeMap.set(itemCode, loyaltyProduct.materialCode);
                  }
                  // Lưu trackInventory và trackSerial từ Loyalty API
                  if (loyaltyProduct?.trackInventory !== undefined) {
                    productTrackInventoryMap.set(itemCode, loyaltyProduct.trackInventory === true);
                  }
                  if (loyaltyProduct?.trackSerial !== undefined) {
                    productTrackSerialMap.set(itemCode, loyaltyProduct.trackSerial === true);
                  }
                } catch (error) {
                  // Không có dữ liệu từ Loyalty API - bỏ qua lỗi
                }
              }),
            );
          }
          
          // Xử lý từng sale trong order - TRUYỀN MẤY LƯU NẤY (lưu tất cả các dòng từ Zappy API)
          if (order.sales && order.sales.length > 0) {
            for (const saleItem of order.sales) {
              try {
                // Lấy productType từ Loyalty API
                const productType = productTypeMap.get(saleItem.itemCode || '');
                // Lấy dvt từ saleItem hoặc từ Loyalty API, không gán mặc định
                const dvt = saleItem.dvt || productDvtMap.get(saleItem.itemCode || '') || undefined;
                
                // Bỏ qua sale item không có dvt - không lưu vào database
                if (!dvt || dvt.trim() === '') {
                  this.logger.warn(`Bỏ qua sale item ${order.docCode}/${saleItem.itemCode} vì không có đơn vị tính (dvt)`);
                  continue;
                }

                // Tính VIP type nếu có chiết khấu VIP
                let muaHangCkVip: string | undefined = undefined;
                if (saleItem.grade_discamt && saleItem.grade_discamt > 0) {
                  const materialCode = productMaterialCodeMap.get(saleItem.itemCode || '');
                  const trackInventory = productTrackInventoryMap.get(saleItem.itemCode || '');
                  const trackSerial = productTrackSerialMap.get(saleItem.itemCode || '');
                  
                  // Tính VIP type dựa trên quy tắc
                  if (productType === 'DIVU') {
                    muaHangCkVip = 'VIP DV MAT';
                  } else if (productType === 'VOUC') {
                    // Nếu productType == "VOUC" → "VIP VC MP"
                    muaHangCkVip = 'VIP VC MP';
                  } else {
                    const materialCodeStr = materialCode || '';
                    const codeStr = saleItem.itemCode || '';
                    // Kiểm tra "VC" trong materialCode, code, hoặc itemCode (không phân biệt hoa thường)
                    const hasVC = 
                      materialCodeStr.toUpperCase().includes('VC') ||
                      codeStr.toUpperCase().includes('VC');
                    
                    if (
                      materialCodeStr.startsWith('E.') ||
                      hasVC ||
                      (trackInventory === false && trackSerial === true)
                    ) {
                      muaHangCkVip = 'VIP VC MP';
                    } else {
                      muaHangCkVip = 'VIP MP';
                    }
                  }
                }

                // Enrich voucher/ECOIN data từ get_daily_cash
                let voucherRefno: string | undefined;
                let voucherAmount: number | undefined;
                if (voucherData.length > 0) {
                  // Lấy voucher đầu tiên (có thể có nhiều voucher)
                  const firstVoucher = voucherData[0];
                  voucherRefno = firstVoucher.refno;
                  voucherAmount = firstVoucher.total_in || 0;
                }
                
                // Kiểm tra ECOIN: Nếu có ECOIN trong cashio → v_paid là ECOIN, không phải voucher
                const hasEcoin = ecoinData.length > 0;
                let ecoinAmount: number | undefined = undefined;
                if (hasEcoin) {
                  // Lấy ECOIN đầu tiên (có thể có nhiều ECOIN)
                  const firstEcoin = ecoinData[0];
                  ecoinAmount = firstEcoin.total_in || 0;
                }
                
                const pkgCode = saleItem.pkg_code || saleItem.pkgCode || null;
                const promCode = saleItem.promCode || saleItem.prom_code || null;
                const vPaid = saleItem.paid_by_voucher_ecode_ecoin_bp || 0;
                const soSource = saleItem.order_source || saleItem.so_source || null;
                
                // Lưu vào đúng trường
                let paidByVoucherChinh: number | undefined = undefined;
                let chietKhauVoucherDp1: number | undefined = undefined;
                let voucherDp1Code: string | undefined = undefined;
                let chietKhauThanhToanTkTienAo: number | undefined = undefined;
                
                // Nếu có ECOIN → lưu vào chietKhauThanhToanTkTienAo, không lưu vào voucher
                if (hasEcoin && vPaid > 0) {
                  chietKhauThanhToanTkTienAo = ecoinAmount && ecoinAmount > 0 ? ecoinAmount : vPaid;
                } else if (vPaid > 0) {
                  // Không có ECOIN → xử lý voucher như bình thường
                  // Phân biệt voucher chính và voucher dự phòng
                  // Quy tắc:
                  // 1. Nếu pkg_code có giá trị → voucher chính
                  // 2. Nếu so_source = "SHOPEE" → voucher dự phòng
                  // 3. Nếu prom_code có giá trị và pkg_code trống → voucher dự phòng
                  // 4. Các trường hợp khác → voucher chính
                  
                  // Kiểm tra điều kiện voucher dự phòng
                  const isShopee = soSource && String(soSource).toUpperCase() === 'SHOPEE';
                  const hasPkgCode = pkgCode && pkgCode.trim() !== '';
                  const hasPromCode = promCode && promCode.trim() !== '';
                  
                  // Voucher dự phòng nếu:
                  // - Là SHOPEE, HOẶC
                  // - Có prom_code và không có pkg_code
                  const isVoucherDuPhong = isShopee || (hasPromCode && !hasPkgCode);
                  
                  // Voucher chính nếu không phải voucher dự phòng
                  const isVoucherChinh = !isVoucherDuPhong;
                  
                  if (isVoucherChinh && vPaid > 0) {
                    // Voucher chính: lưu vào paid_by_voucher_ecode_ecoin_bp
                    paidByVoucherChinh = vPaid;
                  } else if (isVoucherDuPhong && vPaid > 0) {
                    // Voucher dự phòng: lưu vào chietKhauVoucherDp1 và voucherDp1
                    chietKhauVoucherDp1 = vPaid;
                    voucherDp1Code = promCode; // Lưu prom_code vào voucherDp1
                  } else if (vPaid > 0) {
                    // Trường hợp khác: giữ nguyên logic cũ (lưu vào paid_by_voucher_ecode_ecoin_bp)
                    paidByVoucherChinh = vPaid;
                  }
                }
                
                // Luôn tạo sale mới - TRUYỀN MẤY LƯU NẤY (không check duplicate, lưu tất cả)
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
                    dvt: dvt, // Dvt từ saleItem hoặc Loyalty API, không gán mặc định
                    disc_amt: saleItem.disc_amt,
                    grade_discamt: saleItem.grade_discamt,
                    other_discamt: saleItem.other_discamt,
                    chietKhauMuaHangGiamGia: saleItem.chietKhauMuaHangGiamGia,
                    // Tính VIP type dựa trên quy tắc từ Loyalty API
                    muaHangCkVip: muaHangCkVip,
                    chietKhauMuaHangCkVip: saleItem.grade_discamt && saleItem.grade_discamt > 0 ? saleItem.grade_discamt : undefined,
                    // Phân biệt voucher chính và voucher dự phòng
                    paid_by_voucher_ecode_ecoin_bp: paidByVoucherChinh,
                    chietKhauVoucherDp1: chietKhauVoucherDp1,
                    voucherDp1: voucherDp1Code || voucherRefno, // Ưu tiên prom_code, nếu không có thì dùng voucherRefno từ get_daily_cash
                    // Thanh toán TK tiền ảo (ECOIN)
                    chietKhauThanhToanTkTienAo: chietKhauThanhToanTkTienAo,
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
                    // Enrich voucher data từ get_daily_cash (nếu chưa có voucherDp1)
                    thanhToanVoucher: voucherAmount && voucherAmount > 0 ? voucherAmount : undefined,
                    customer: customer,
                    isProcessed: false,
                  } as Partial<Sale>);
                  const savedSale = await this.saleRepository.save(newSale);
                  salesCount++;
              } catch (saleError: any) {
                const errorMsg = `Lỗi khi lưu sale ${order.docCode}/${saleItem.itemCode} (promCode: ${saleItem.promCode || 'null'}, serial: ${saleItem.serial || 'null'}): ${saleError?.message || saleError}`;
                this.logger.error(errorMsg);
                if (saleError?.stack) {
                  this.logger.error(`Stack trace: ${saleError.stack}`);
                }
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

      
      for (const docCode of docCodes) {
          try {
          const result = await this.salesService.createInvoiceViaFastApi(docCode);
          if (result.success) {
            invoiceSuccessCount++;
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
        } else {
          salesSkipped++;
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
    }
  }
}

