import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { Customer } from '../entities/customer.entity';
import { Sale } from '../entities/sale.entity';
import { DailyCashio } from '../entities/daily-cashio.entity';
import { CheckFaceId } from '../entities/check-face-id.entity';
import { StockTransfer } from '../entities/stock-transfer.entity';
import { ShiftEndCash } from '../entities/shift-end-cash.entity';
import { ShiftEndCashLine } from '../entities/shift-end-cash-line.entity';
import { RepackFormula } from '../entities/repack-formula.entity';
import { RepackFormulaItem } from '../entities/repack-formula-item.entity';
import { Promotion } from '../entities/promotion.entity';
import { PromotionLine } from '../entities/promotion-line.entity';
import { VoucherIssue } from '../entities/voucher-issue.entity';
import { VoucherIssueDetail } from '../entities/voucher-issue-detail.entity';
import { ZappyApiService } from './zappy-api.service';
import { LoyaltyService } from './loyalty.service';
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
    @InjectRepository(CheckFaceId)
    private checkFaceIdRepository: Repository<CheckFaceId>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
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
    @InjectRepository(VoucherIssueDetail)
    private voucherIssueDetailRepository: Repository<VoucherIssueDetail>,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private loyaltyService: LoyaltyService,
    @Inject(forwardRef(() => SalesService))
    private salesService: SalesService,
  ) {}

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

      // Batch query tất cả customers trước để tránh N+1 query
      const customerCodes = Array.from(new Set(orders.map((o) => o.customer.code).filter((code): code is string => !!code)));
      const existingCustomers = await this.customerRepository.find({
        where: customerCodes.map((code) => ({ code })),
      });
      const customerMap = new Map<string, Customer>();
      existingCustomers.forEach((c) => customerMap.set(c.code, c));

      // Xử lý từng order
      for (const order of orders) {
        try {
          // Lấy brand từ department.company
          const department = departmentMap.get(order.branchCode);
          const brandFromDepartment = department?.company
            ? mapCompanyToBrand(department.company)
            : order.customer.brand || '';

          // Tìm customer từ map (đã query batch trước)
          let customer = customerMap.get(order.customer.code);

          if (!customer) {
            const newCustomer = this.customerRepository.create({
              code: order.customer.code,
              name: order.customer.name,
              brand: brandFromDepartment,
              mobile: order.customer.mobile,
              phone: order.customer.mobile, // Set phone = mobile khi tạo mới
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
            customerMap.set(customer.code, customer); // Thêm vào map để dùng lại
            customersCount++;
          } else {
            // Cập nhật thông tin customer nếu cần
            let hasUpdate = false;
            
            if (order.customer.name && order.customer.name !== customer.name) {
              customer.name = order.customer.name;
              hasUpdate = true;
            }
            
            // Update mobile nếu có partner_mobile mới từ API (ưu tiên giá trị mới)
            if (order.customer.mobile && order.customer.mobile.trim() && order.customer.mobile !== customer.mobile) {
              customer.mobile = order.customer.mobile.trim();
              // Nếu phone chưa có hoặc bằng mobile cũ, cập nhật phone = mobile mới
              if (!customer.phone || customer.phone === customer.mobile) {
                customer.phone = order.customer.mobile.trim();
              }
              hasUpdate = true;
            }
            
            // Update phone nếu chưa có
            if (!customer.phone && customer.mobile) {
              customer.phone = customer.mobile;
              hasUpdate = true;
            }
            
            if (order.customer.grade_name && order.customer.grade_name !== customer.grade_name) {
              customer.grade_name = order.customer.grade_name;
              hasUpdate = true;
            }
            
            // Cập nhật brand từ department nếu có
            if (brandFromDepartment && brandFromDepartment !== customer.brand) {
              customer.brand = brandFromDepartment;
              hasUpdate = true;
            }
            
            // Chỉ save nếu có thay đổi
            if (hasUpdate) {
              customer = await this.customerRepository.save(customer);
            }
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
          
          // Collect tất cả itemCodes từ order để fetch products từ Loyalty API (đã trim)
          const orderItemCodes = Array.from(
            new Set(
              (order.sales || [])
                .map((s) => s.itemCode?.trim())
                .filter((code): code is string => !!code && code !== '')
            )
          );

          // Fetch products từ Loyalty API để kiểm tra dvt, productType, materialCode, trackInventory, trackSerial (song song)
          const productDvtMap = new Map<string, string>();
          const productTypeMap = new Map<string, string>();
          const productMaterialCodeMap = new Map<string, string>();
          const productTrackInventoryMap = new Map<string, boolean>();
          const productTrackSerialMap = new Map<string, boolean>();
          // Track các itemCodes không tồn tại (404) để bỏ qua khi lưu sale items
          const notFoundItemCodes = new Set<string>();
          
          if (orderItemCodes.length > 0) {
            await Promise.all(
              orderItemCodes.map(async (itemCode) => {
                const trimmedItemCode = itemCode?.trim();
                if (!trimmedItemCode) return;
                
                // Fetch product từ Loyalty API sử dụng LoyaltyService
                const loyaltyProduct = await this.loyaltyService.checkProduct(trimmedItemCode);
                
                // Nếu không tìm thấy, đánh dấu not found
                if (!loyaltyProduct) {
                  notFoundItemCodes.add(trimmedItemCode);
                  this.logger.log(`[Sync] Đã thêm ${trimmedItemCode} vào danh sách bỏ qua (notFoundItemCodes size: ${notFoundItemCodes.size})`);
                  return;
                }
                
                // Nếu có dữ liệu từ Loyalty API, lưu vào maps
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
              }),
            );
          }
          
          // Tạo tất cả compositeKeys trước để batch query check duplicate
          const compositeKeysToCheck: string[] = [];
          const saleItemDataMap = new Map<string, any>(); // Map compositeKey -> saleItem data
          
          if (order.sales && order.sales.length > 0) {
            for (let index = 0; index < order.sales.length; index++) {
              const saleItem = order.sales[index];
              // Parse api_id từ saleItem.id
              let apiId: number | undefined = undefined;
              if (saleItem.id !== undefined && saleItem.id !== null && saleItem.id !== '') {
                const parsedId = typeof saleItem.id === 'string' ? parseInt(saleItem.id, 10) : Number(saleItem.id);
                if (!isNaN(parsedId) && parsedId > 0) {
                  apiId = parsedId;
                }
              }
              
              const giaBanValue = saleItem.giaBan || saleItem.price || 0;
              // Thêm index vào compositeKey để phân biệt các items giống nhau trong cùng order
              const compositeKey = [
                order.docCode || '',
                saleItem.itemCode || '',
                (saleItem.qty || 0).toString(),
                giaBanValue.toString(),
                (saleItem.disc_amt || 0).toString(),
                (saleItem.grade_discamt || 0).toString(),
                (saleItem.other_discamt || 0).toString(),
                (saleItem.revenue || 0).toString(),
                saleItem.promCode || 'null',
                saleItem.serial || 'null',
                customer.id || '',
                apiId ? apiId.toString() : 'null',
                index.toString(), // Thêm index để phân biệt items giống nhau
              ].join('|');
              
              compositeKeysToCheck.push(compositeKey);
              saleItemDataMap.set(compositeKey, { saleItem, apiId, index });
            }
          }
          
          // Batch query tất cả existingSales dựa trên compositeKeys
          const existingSalesMap = new Map<string, Sale>();
          if (compositeKeysToCheck.length > 0) {
            const existingSales = await this.saleRepository.find({
              where: compositeKeysToCheck.map((key) => ({ compositeKey: key })),
            });
            existingSales.forEach((sale) => {
              if (sale.compositeKey) {
                existingSalesMap.set(sale.compositeKey, sale);
              }
            });
          }
          
          // Xử lý từng sale trong order - LƯU TẤT CẢ, đánh dấu statusAsys = false nếu sản phẩm không tồn tại (404)
          if (order.sales && order.sales.length > 0) {
            for (let index = 0; index < order.sales.length; index++) {
              const saleItem = order.sales[index];
              try {
                // Bỏ qua các item có itemcode = "TRUTONKEEP"
                const itemCode = saleItem.itemCode?.trim();
                if (itemCode && itemCode.toUpperCase() === 'TRUTONKEEP') {
                  this.logger.log(`[Sync] Bỏ qua sale item ${itemCode} (${saleItem.itemName || 'N/A'}) trong order ${order.docCode} - itemcode = TRUTONKEEP`);
                  continue;
                }
                
                // Kiểm tra xem sản phẩm có tồn tại trong Loyalty API không
                const isNotFound = itemCode && notFoundItemCodes.has(itemCode);
                // Set statusAsys: false nếu không tồn tại (404), true nếu tồn tại
                const statusAsys = !isNotFound;
                
                if (isNotFound) {
                  this.logger.warn(`[Sync] Sale item ${itemCode} (${saleItem.itemName || 'N/A'}) trong order ${order.docCode} - Sản phẩm không tồn tại trong Loyalty API (404), sẽ lưu với statusAsys = false`);
                }
                
                // Parse api_id từ saleItem.id
                let apiId: number | undefined = undefined;
                if (saleItem.id !== undefined && saleItem.id !== null && saleItem.id !== '') {
                  const parsedId = typeof saleItem.id === 'string' ? parseInt(saleItem.id, 10) : Number(saleItem.id);
                  if (!isNaN(parsedId) && parsedId > 0) {
                    apiId = parsedId;
                  }
                }
                
                // Lấy productType: Ưu tiên từ Zappy API (producttype), nếu không có thì lấy từ Loyalty API
                const productTypeFromZappy = saleItem.producttype || saleItem.productType || null;
                const productTypeFromLoyalty = productTypeMap.get(saleItem.itemCode || '');
                const productType = productTypeFromZappy || productTypeFromLoyalty || null;
                
                // Debug log để kiểm tra productType
                if (productTypeFromZappy) {
                  this.logger.debug(`[Sync] Sale ${order.docCode}/${saleItem.itemCode}: productType từ Zappy API = "${productTypeFromZappy}"`);
                } else if (productTypeFromLoyalty) {
                  this.logger.debug(`[Sync] Sale ${order.docCode}/${saleItem.itemCode}: productType từ Loyalty API = "${productTypeFromLoyalty}"`);
                } else {
                  this.logger.debug(`[Sync] Sale ${order.docCode}/${saleItem.itemCode}: Không có productType từ cả Zappy và Loyalty API`);
                }
                
                // Lấy dvt từ saleItem hoặc từ Loyalty API, nếu không có thì mặc định là "cái"
                const dvt = saleItem.dvt || productDvtMap.get(saleItem.itemCode || '') || 'cái';

                // Tính VIP type nếu có chiết khấu VIP
                let muaHangCkVip: string | undefined = undefined;
                if (saleItem.grade_discamt && saleItem.grade_discamt > 0) {
                  // Logic VIP khác nhau cho từng brand
                  const brandLower = brand?.toLowerCase() || '';
                  
                  if (brandLower === 'f3') {
                    // Logic cũ cho f3: DIVU → "FBV CKVIP DV", còn lại → "FBV CKVIP SP"
                    if (productType === 'DIVU') {
                      muaHangCkVip = 'FBV CKVIP DV';
                    } else {
                      muaHangCkVip = 'FBV CKVIP SP';
                    }
                  } else {
                    // Logic mới cho các brand khác (menard, labhair, yaman)
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
                  
                  // Kiểm tra brand để áp dụng logic khác nhau
                  const brandLower = brand?.toLowerCase() || '';
                  const isShopee = soSource && String(soSource).toUpperCase() === 'SHOPEE';
                  const hasPkgCode = pkgCode && pkgCode.trim() !== '';
                  const hasPromCode = promCode && promCode.trim() !== '';
                  
                  let isVoucherDuPhong = false;
                  
                  if (brandLower === 'f3') {
                    // Logic cho F3 (Facialbar):
                    // - Chỉ khi so_source = "SHOPEE" → voucher dự phòng
                    // - Tất cả các trường hợp khác (kể cả có prom_code và không có pkg_code) → voucher chính
                    isVoucherDuPhong = isShopee;
                  } else {
                    // Logic cho các brand khác (menard, labhair, yaman):
                    // - Nếu so_source = "SHOPEE" → voucher dự phòng
                    // - Nếu có prom_code và không có pkg_code → voucher dự phòng
                    // - Các trường hợp khác → voucher chính
                    isVoucherDuPhong = isShopee || (hasPromCode && !hasPkgCode);
                  }
                  
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
                
                // Set cục thuế mặc định cho F3
                let cucThue: string | undefined = undefined;
                const brandLowerForCucThue = (brand || '').toLowerCase().trim();
                if (brandLowerForCucThue === 'f3') {
                  cucThue = 'FBV';
                }
                
                // Tạo composite key từ TẤT CẢ các trường dữ liệu có khả năng khác nhau
                // Composite key: docCode|itemCode|qty|giaBan|disc_amt|grade_discamt|other_discamt|revenue|promCode|serial|customerId|api_id|index
                // Thêm index để phân biệt các items giống nhau trong cùng order
                const giaBanValue = saleItem.giaBan || saleItem.price || 0;
                const compositeKey = [
                  order.docCode || '',
                  saleItem.itemCode || '',
                  (saleItem.qty || 0).toString(),
                  giaBanValue.toString(),
                  (saleItem.disc_amt || 0).toString(),
                  (saleItem.grade_discamt || 0).toString(),
                  (saleItem.other_discamt || 0).toString(),
                  (saleItem.revenue || 0).toString(),
                  saleItem.promCode || 'null',
                  saleItem.serial || 'null',
                  customer.id || '',
                  apiId ? apiId.toString() : 'null',
                  index.toString(), // Thêm index để phân biệt items giống nhau
                ].join('|');
                
                // Check duplicate dựa trên compositeKey (đã query batch trước)
                const existingSale = existingSalesMap.get(compositeKey);
                
                // Nếu đã có sale với api_id + itemCode này, update; nếu chưa có, tạo mới
                if (existingSale) {
                  // Update sale đã tồn tại
                  existingSale.docCode = order.docCode;
                  existingSale.docDate = new Date(order.docDate);
                  existingSale.branchCode = order.branchCode;
                  existingSale.docSourceType = order.docSourceType;
                  if (saleItem.ordertype !== undefined) existingSale.ordertype = saleItem.ordertype;
                  if (saleItem.ordertype_name !== undefined) existingSale.ordertypeName = saleItem.ordertype_name;
                  if (saleItem.description !== undefined) existingSale.description = saleItem.description;
                  if (saleItem.partnerCode !== undefined) existingSale.partnerCode = saleItem.partnerCode;
                  // Update mobile từ customer tại thời điểm sync
                  if (order.customer.mobile) {
                    existingSale.mobile = order.customer.mobile;
                  }
                  existingSale.itemCode = saleItem.itemCode || '';
                  existingSale.itemName = saleItem.itemName || '';
                  existingSale.qty = saleItem.qty || 0;
                  existingSale.revenue = saleItem.revenue || 0;
                  existingSale.linetotal = saleItem.linetotal || saleItem.revenue || 0;
                  // Luôn update productType, kể cả khi là null (để cập nhật từ Zappy API)
                  // Nếu productType là empty string, set thành null
                  const finalProductType = productType && productType.trim() !== '' ? productType.trim() : null;
                  if (existingSale.productType !== finalProductType) {
                    this.logger.debug(
                      `[Sync] Update productType cho sale ${order.docCode}/${saleItem.itemCode}: ` +
                      `"${existingSale.productType}" → "${finalProductType}"`
                    );
                  }
                  existingSale.productType = finalProductType;
                  existingSale.tienHang = saleItem.tienHang || saleItem.linetotal || saleItem.revenue || 0;
                  existingSale.giaBan = saleItem.giaBan || 0;
                  if (saleItem.promCode !== undefined) existingSale.promCode = saleItem.promCode;
                  existingSale.serial = saleItem.serial;
                  existingSale.soSerial = saleItem.serial;
                  existingSale.dvt = dvt;
                  existingSale.disc_amt = saleItem.disc_amt;
                  existingSale.grade_discamt = saleItem.grade_discamt;
                  existingSale.other_discamt = saleItem.other_discamt;
                  existingSale.chietKhauMuaHangGiamGia = saleItem.chietKhauMuaHangGiamGia;
                  existingSale.muaHangCkVip = muaHangCkVip;
                  existingSale.chietKhauMuaHangCkVip = saleItem.grade_discamt && saleItem.grade_discamt > 0 ? saleItem.grade_discamt : undefined;
                  existingSale.paid_by_voucher_ecode_ecoin_bp = paidByVoucherChinh;
                  existingSale.chietKhauVoucherDp1 = chietKhauVoucherDp1;
                  existingSale.voucherDp1 = voucherDp1Code || voucherRefno;
                  existingSale.chietKhauThanhToanTkTienAo = chietKhauThanhToanTkTienAo;
                  existingSale.cucThue = cucThue;
                  existingSale.maCa = saleItem.shift_code;
                  existingSale.saleperson_id = this.validateInteger(saleItem.saleperson_id);
                  existingSale.partner_name = saleItem.partner_name;
                  existingSale.order_source = saleItem.order_source;
                  existingSale.maThe = saleItem.mvc_serial;
                  existingSale.cat1 = saleItem.cat1;
                  existingSale.cat2 = saleItem.cat2;
                  existingSale.cat3 = saleItem.cat3;
                  existingSale.catcode1 = saleItem.catcode1;
                  existingSale.catcode2 = saleItem.catcode2;
                  existingSale.catcode3 = saleItem.catcode3;
                  existingSale.thanhToanVoucher = voucherAmount && voucherAmount > 0 ? voucherAmount : undefined;
                  existingSale.customer = customer;
                  existingSale.compositeKey = compositeKey; // Update compositeKey
                  
                  await this.saleRepository.save(existingSale);
                  salesCount++;
                  continue; // Skip tạo mới, đã update rồi
                }
                
                // Tạo sale mới (chỉ khi chưa có existingSale)
                const newSale = this.saleRepository.create({
                    api_id: apiId || null, // Lưu id từ Zappy API
                    compositeKey: compositeKey, // Lưu compositeKey để check duplicate
                    docCode: order.docCode,
                    docDate: new Date(order.docDate),
                    branchCode: order.branchCode,
                    docSourceType: order.docSourceType,
                    ordertype: saleItem.ordertype,
                    ordertypeName: saleItem.ordertype_name,
                    description: saleItem.description,
                    partnerCode: saleItem.partnerCode,
                    mobile: order.customer.mobile || undefined, // Lưu mobile từ customer tại thời điểm bán
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
                    dvt: dvt, // Dvt từ saleItem hoặc Loyalty API, mặc định là "cái" nếu không có
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
                    // Cục thuế mặc định cho F3
                    cucThue: cucThue,
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
                    statusAsys: statusAsys, // Set statusAsys: true nếu sản phẩm tồn tại, false nếu 404
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


      // Tạm thời comment: Tự động tạo hóa đơn cho tất cả các đơn hàng vừa đồng bộ (chạy ngầm ở background)
      // Chỉ tạo invoice cho các đơn hàng trong ngày sync (từ orders vừa sync)
      // Lấy docCodes từ các orders vừa sync, sau đó kiểm tra xem có sales được lưu không
      // const orderDocCodes = [...new Set(orders.map(order => order.docCode).filter((code: string) => code))];
      
      // Kiểm tra xem các đơn hàng này có sales được lưu trong database không
      // (để tránh lỗi khi order không có sales nào được lưu do filter dvt)
      // const savedDocCodes = await this.saleRepository
      //   .createQueryBuilder('sale')
      //   .select('DISTINCT sale.docCode', 'docCode')
      //   .where('sale.docCode IN (:...docCodes)', { docCodes: orderDocCodes })
      //   .andWhere('sale.isProcessed = :isProcessed', { isProcessed: false })
      //   .getRawMany();
      
      // const docCodes = savedDocCodes.map((item: any) => item.docCode).filter((code: string) => code);
      
      // Tạo invoice ở background (không await) để trả về response ngay
      // if (docCodes.length > 0) {
      //   this.logger.log(`Bắt đầu tạo hóa đơn ngầm cho ${docCodes.length} đơn hàng...`);
      //   
      //   // Chạy ở background, không await
      //   this.createInvoicesInBackground(docCodes, date).catch((error) => {
      //     this.logger.error(`Lỗi khi tạo hóa đơn ngầm: ${error?.message || error}`);
      //   });
      // }

      // Sync checkFaceID data từ API
      try {
        await this.syncCheckFaceId(date, branchCodes);
      } catch (checkFaceIdError: any) {
        this.logger.warn(`Failed to sync checkFaceID data: ${checkFaceIdError?.message || checkFaceIdError}`);
        // Không throw error, chỉ log warning vì đây là tính năng bổ sung
      }

      // Trả về response ngay sau khi đồng bộ sale xong
      return {
        success: errors.length === 0,
        message: `Đồng bộ thành công ${orders.length} đơn hàng cho ngày ${date}`,
        ordersCount: orders.length,
        salesCount,
        customersCount,
        errors: errors.length > 0 ? errors : undefined,
      } as any;
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ từ Zappy API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo hóa đơn ở background (không block response)
   */
  private async createInvoicesInBackground(docCodes: string[], date: string): Promise<void> {
    let invoiceSuccessCount = 0;
    let invoiceFailureCount = 0;
    const invoiceErrors: string[] = [];

    // Tạo invoice song song (parallel) thay vì tuần tự để tăng tốc độ
    // Giới hạn số lượng concurrent requests để tránh quá tải (batch size = 5)
    const batchSize = 5;
    const totalBatches = Math.ceil(docCodes.length / batchSize);
    
    for (let i = 0; i < docCodes.length; i += batchSize) {
      const batch = docCodes.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      this.logger.log(`[Background] Đang tạo hóa đơn batch ${batchNumber}/${totalBatches} (${batch.length} đơn hàng)...`);
      
      await Promise.all(
        batch.map(async (docCode) => {
          try {
            const result = await this.salesService.createInvoiceViaFastApi(docCode);
            if (result.success) {
              invoiceSuccessCount++;
            } else {
              invoiceFailureCount++;
              const errorMsg = `Tạo hóa đơn thất bại cho ${docCode}: ${result.message || 'Unknown error'}`;
              invoiceErrors.push(errorMsg);
              this.logger.warn(`[Background] ✗ ${errorMsg}`);
            }
          } catch (error: any) {
            invoiceFailureCount++;
            const errorMsg = `Lỗi khi tạo hóa đơn cho đơn hàng ${docCode}: ${error?.message || error}`;
            invoiceErrors.push(errorMsg);
            this.logger.error(`[Background] ✗ ${errorMsg}`);
          }
        }),
      );
      
      this.logger.log(`[Background] Hoàn thành batch ${batchNumber}/${totalBatches}`);
    }

    this.logger.log(`[Background] Hoàn thành tạo hóa đơn: ${invoiceSuccessCount} thành công, ${invoiceFailureCount} thất bại cho ngày ${date}`);
    if (invoiceErrors.length > 0) {
      this.logger.warn(`[Background] Danh sách lỗi tạo hóa đơn:`, invoiceErrors);
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
            mobile: personalInfo.mobile || customer.mobile || undefined, // Lưu mobile từ customer tại thời điểm bán
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
          if (saleData.ordertype_name) saleDataToCreate.ordertypeName = saleData.ordertype_name;
          
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

  /**
   * Sync checkFaceID data từ API
   * @param date - Date format: DDMMMYYYY (ví dụ: 10DEC2025)
   * @param shopCodes - Array of shop codes (branch codes)
   */
  private async syncCheckFaceId(date: string, shopCodes: string[]): Promise<void> {
    if (!shopCodes || shopCodes.length === 0) {
      return;
    }

    // Parse date từ DDMMMYYYY sang YYYY-MM-DD
    // Ví dụ: 10DEC2025 -> 10-12-2025
    const parseDate = (dateStr: string): string => {
      // Format: DDMMMYYYY (ví dụ: 10DEC2025)
      const day = dateStr.substring(0, 2);
      const monthStr = dateStr.substring(2, 5).toUpperCase();
      const year = dateStr.substring(5, 9);
      
      const monthMap: Record<string, string> = {
        'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
        'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
        'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12',
      };
      
      const month = monthMap[monthStr] || '01';
      return `${day}-${month}-${year}`; // Format: 10-12-2025
    };

    const dateFormatted = parseDate(date);
    let savedCount = 0;
    let updatedCount = 0;

    // Gọi API checkFaceID cho mỗi shop_code
    for (const shopCode of shopCodes) {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://vmt.ipchello.com/api/inout-customer/`,
          {
            params: {
              shop_code: shopCode,
              fromDate: dateFormatted,
              toDate: dateFormatted,
            },
            headers: { accept: 'application/json' },
            timeout: 10000, // 10 seconds timeout
          },
        );

        const checkFaceIdData = response?.data?.data || [];
        if (!Array.isArray(checkFaceIdData) || checkFaceIdData.length === 0) {
          continue;
        }

        // Parse date string sang Date object
        const parseDateTime = (dateTimeStr: string): Date | null => {
          if (!dateTimeStr) return null;
          // Format: "10-12-2025 09:13:19"
          try {
            const [datePart, timePart] = dateTimeStr.split(' ');
            const [day, month, year] = datePart.split('-');
            const [hour, minute, second] = timePart ? timePart.split(':') : ['00', '00', '00'];
            return new Date(parseInt(year), parseInt(month) - 1, parseInt(day), parseInt(hour), parseInt(minute), parseInt(second || '0'));
          } catch (error) {
            return null;
          }
        };

        // Lưu từng checkFaceID record vào database
        for (const item of checkFaceIdData) {
          try {
            // Parse dates
            const startTime = item.start_time ? parseDateTime(item.start_time) : null;
            const checking = item.checking ? parseDateTime(item.checking) : null;
            const monthMapForDate: Record<string, string> = {
              'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
              'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
              'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12',
            };
            const dateObj = startTime || (date ? (() => {
              const day = parseInt(date.substring(0, 2));
              const monthStr = date.substring(2, 5).toUpperCase();
              const month = parseInt(monthMapForDate[monthStr] || '01');
              const year = parseInt(date.substring(5, 9));
              return new Date(year, month - 1, day);
            })() : new Date());

            // Kiểm tra xem item.code có tồn tại trong Customer không để tránh foreign key constraint violation
            let partnerCode: string | null = null;
            if (item.code && item.code.trim()) {
              const existingCustomer = await this.customerRepository.findOne({
                where: { code: item.code.trim() },
              });
              if (existingCustomer) {
                partnerCode = item.code.trim();
              }
            }

            // Đảm bảo mobile được lưu đúng (convert sang string và trim)
            const mobileValue = item.mobile ? String(item.mobile).trim() : undefined;

            // Kiểm tra xem đã có record với apiId chưa - nếu có thì update, không thì insert mới
            let existingCheckFaceId: CheckFaceId | null = null;
            if (item.id) {
              existingCheckFaceId = await this.checkFaceIdRepository.findOne({
                where: { apiId: item.id },
              });
            }

            if (existingCheckFaceId) {
              // Update record đã tồn tại (đặc biệt là mobile có thể đã bị lưu sai)
              // Luôn update các field từ API để đảm bảo dữ liệu mới nhất
              if (startTime) existingCheckFaceId.startTime = startTime;
              if (checking) existingCheckFaceId.checking = checking;
              existingCheckFaceId.isFirstInDay = item.is_first_in_day === true || item.is_first_in_day === 1;
              if (item.image) existingCheckFaceId.image = item.image;
              if (partnerCode) existingCheckFaceId.partnerCode = partnerCode;
              if (item.name) existingCheckFaceId.name = item.name;
              // Luôn update mobile nếu có giá trị từ API (để sửa lỗi bị cắt) - đây là field quan trọng nhất
              if (mobileValue) existingCheckFaceId.mobile = mobileValue;
              if (item.is_nv !== undefined) existingCheckFaceId.isNv = item.is_nv;
              if (item.shop_code) existingCheckFaceId.shopCode = item.shop_code;
              if (item.shop_name) existingCheckFaceId.shopName = item.shop_name;
              if (item.cam_id) existingCheckFaceId.camId = item.cam_id;
              if (dateObj) existingCheckFaceId.date = dateObj;
              
              await this.checkFaceIdRepository.save(existingCheckFaceId);
              updatedCount++;
            } else {
              // Insert mới
              const checkFaceIdDataToSave: Partial<CheckFaceId> = {
                apiId: item.id || undefined,
                startTime: startTime || undefined,
                checking: checking || undefined,
                isFirstInDay: item.is_first_in_day === true || item.is_first_in_day === 1,
                image: item.image || undefined,
                partnerCode: partnerCode || undefined,
                name: item.name || undefined,
                mobile: mobileValue, // Đảm bảo lưu đúng mobile từ API
                isNv: item.is_nv || undefined,
                shopCode: item.shop_code || shopCode,
                shopName: item.shop_name || undefined,
                camId: item.cam_id || undefined,
                date: dateObj || new Date(),
              };

              const newCheckFaceId = this.checkFaceIdRepository.create(checkFaceIdDataToSave);
              await this.checkFaceIdRepository.save(newCheckFaceId);
              savedCount++;
            }
          } catch (itemError: any) {
            this.logger.warn(`Failed to save checkFaceID record ${item.id || 'unknown'}: ${itemError?.message || itemError}`);
          }
        }
      } catch (error: any) {
        this.logger.warn(`Failed to fetch checkFaceID data for shop_code ${shopCode}: ${error?.message || error}`);
      }
    }

    this.logger.log(`[Sync] Đã lưu ${savedCount} checkFaceID records mới, cập nhật ${updatedCount} records đã tồn tại`);
  }

  /**
   * Sync checkFaceID data từ API theo ngày (public method)
   * @param date - Date format: DDMMMYYYY (ví dụ: 13DEC2025)
   * @param shopCodes - Optional array of shop codes. Nếu không có, sẽ gọi API không có shop_code để lấy tất cả
   */
  async syncFaceIdByDate(date: string, shopCodes?: string[]): Promise<{
    success: boolean;
    message: string;
    savedCount: number;
    updatedCount: number;
    errors?: string[];
  }> {
    try {
      // Parse date từ DDMMMYYYY sang DD-MM-YYYY
      const parseDate = (dateStr: string): string => {
        const day = dateStr.substring(0, 2);
        const monthStr = dateStr.substring(2, 5).toUpperCase();
        const year = dateStr.substring(5, 9);
        
        const monthMap: Record<string, string> = {
          'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
          'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
          'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12',
        };
        
        const month = monthMap[monthStr] || '01';
        return `${day}-${month}-${year}`; // Format: 13-12-2025
      };

      const dateFormatted = parseDate(date);
      let savedCount = 0;
      let updatedCount = 0;
      const errors: string[] = [];

      // Parse date string sang Date object
      const parseDateTime = (dateTimeStr: string): Date | null => {
        if (!dateTimeStr) return null;
        // Format: "13-12-2025 13:42:59"
        try {
          const [datePart, timePart] = dateTimeStr.split(' ');
          const [day, month, year] = datePart.split('-');
          const [hour, minute, second] = timePart ? timePart.split(':') : ['00', '00', '00'];
          return new Date(parseInt(year), parseInt(month) - 1, parseInt(day), parseInt(hour), parseInt(minute), parseInt(second || '0'));
        } catch (error) {
          return null;
        }
      };

      const monthMapForDate: Record<string, string> = {
        'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
        'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
        'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12',
      };
      const dateObj = (() => {
        const day = parseInt(date.substring(0, 2));
        const monthStr = date.substring(2, 5).toUpperCase();
        const month = parseInt(monthMapForDate[monthStr] || '01');
        const year = parseInt(date.substring(5, 9));
        return new Date(year, month - 1, day);
      })();

      // Nếu có shopCodes, gọi API cho từng shop_code
      // Nếu không có, gọi API một lần không có shop_code để lấy tất cả
      const shopCodesToProcess = shopCodes && shopCodes.length > 0 ? shopCodes : [null]; // null = không có shop_code

      for (const shopCode of shopCodesToProcess) {
        try {
          const params: any = {
            fromDate: dateFormatted,
            toDate: dateFormatted,
          };
          
          // Chỉ thêm shop_code vào params nếu có
          if (shopCode) {
            params.shop_code = shopCode;
          }

          const response = await this.httpService.axiosRef.get(
            `https://vmt.ipchello.com/api/inout-customer/`,
            {
              params,
              headers: { accept: 'application/json' },
              timeout: 30000, // 30 seconds timeout
            },
          );

          const checkFaceIdData = response?.data?.data || [];
          if (!Array.isArray(checkFaceIdData) || checkFaceIdData.length === 0) {
            continue;
          }

          // Lưu từng checkFaceID record vào database
          for (const item of checkFaceIdData) {
            try {
              // Parse dates
              const startTime = item.start_time ? parseDateTime(item.start_time) : null;
              const checking = item.checking ? parseDateTime(item.checking) : null;

              // Kiểm tra xem item.code có tồn tại trong Customer không để tránh foreign key constraint violation
              let partnerCode: string | null = null;
              if (item.code && item.code.trim()) {
                const existingCustomer = await this.customerRepository.findOne({
                  where: { code: item.code.trim() },
                });
                if (existingCustomer) {
                  partnerCode = item.code.trim();
                }
              }

              // Đảm bảo mobile được lưu đúng (convert sang string và trim)
              const mobileValue = item.mobile ? String(item.mobile).trim() : undefined;

              // Kiểm tra xem đã có record với apiId chưa - nếu có thì update, không thì insert mới
              let existingCheckFaceId: CheckFaceId | null = null;
              if (item.id) {
                existingCheckFaceId = await this.checkFaceIdRepository.findOne({
                  where: { apiId: item.id },
                });
              }

              if (existingCheckFaceId) {
                // Update record đã tồn tại (đặc biệt là mobile có thể đã bị lưu sai)
                // Luôn update các field từ API để đảm bảo dữ liệu mới nhất
                if (startTime) existingCheckFaceId.startTime = startTime;
                if (checking) existingCheckFaceId.checking = checking;
                existingCheckFaceId.isFirstInDay = item.is_first_in_day === true || item.is_first_in_day === 1;
                if (item.image) existingCheckFaceId.image = item.image;
                if (partnerCode) existingCheckFaceId.partnerCode = partnerCode;
                if (item.name) existingCheckFaceId.name = item.name;
                // Luôn update mobile nếu có giá trị từ API (để sửa lỗi bị cắt) - đây là field quan trọng nhất
                if (mobileValue) existingCheckFaceId.mobile = mobileValue;
                if (item.is_nv !== undefined) existingCheckFaceId.isNv = item.is_nv;
                if (item.shop_code) existingCheckFaceId.shopCode = item.shop_code;
                if (item.shop_name) existingCheckFaceId.shopName = item.shop_name;
                if (item.cam_id) existingCheckFaceId.camId = item.cam_id;
                if (startTime) {
                  existingCheckFaceId.date = new Date(startTime.getFullYear(), startTime.getMonth(), startTime.getDate());
                } else if (dateObj) {
                  existingCheckFaceId.date = dateObj;
                }
                
                await this.checkFaceIdRepository.save(existingCheckFaceId);
                updatedCount++;
              } else {
                // Insert mới
                const checkFaceIdDataToSave: Partial<CheckFaceId> = {
                  apiId: item.id || undefined,
                  startTime: startTime || undefined,
                  checking: checking || undefined,
                  isFirstInDay: item.is_first_in_day === true || item.is_first_in_day === 1,
                  image: item.image || undefined,
                  partnerCode: partnerCode || undefined,
                  name: item.name || undefined,
                  mobile: mobileValue, // Đảm bảo lưu đúng mobile từ API
                  isNv: item.is_nv || undefined,
                  shopCode: item.shop_code || shopCode || undefined,
                  shopName: item.shop_name || undefined,
                  camId: item.cam_id || undefined,
                  date: startTime ? new Date(startTime.getFullYear(), startTime.getMonth(), startTime.getDate()) : dateObj,
                };

                const newCheckFaceId = this.checkFaceIdRepository.create(checkFaceIdDataToSave);
                await this.checkFaceIdRepository.save(newCheckFaceId);
                savedCount++;
              }
            } catch (itemError: any) {
              const errorMsg = `Failed to save checkFaceID record ${item.id || 'unknown'}: ${itemError?.message || itemError}`;
              this.logger.warn(errorMsg);
              errors.push(errorMsg);
            }
          }
        } catch (error: any) {
          const errorMsg = `Failed to fetch checkFaceID data${shopCode ? ` for shop_code ${shopCode}` : ''}: ${error?.message || error}`;
          this.logger.warn(errorMsg);
          errors.push(errorMsg);
        }
      }

      this.logger.log(`[Sync FaceID] Đã lưu ${savedCount} checkFaceID records mới, cập nhật ${updatedCount} records đã tồn tại cho ngày ${date}`);

      return {
        success: errors.length === 0,
        message: `Đồng bộ FaceID thành công cho ngày ${date}. Đã lưu ${savedCount} records mới, cập nhật ${updatedCount} records đã tồn tại.`,
        savedCount,
        updatedCount,
        errors: errors.length > 0 ? errors : undefined,
      };
    } catch (error: any) {
      const errorMsg = `Lỗi khi đồng bộ FaceID cho ngày ${date}: ${error?.message || error}`;
      this.logger.error(errorMsg);
      throw new Error(errorMsg);
    }
  }

  /**
   * Đồng bộ dữ liệu xuất kho từ Zappy API
   * @param date - Date format: DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard)
   */
  async syncStockTransfer(date: string, brand: string): Promise<{
    success: boolean;
    message: string;
    recordsCount: number;
    savedCount: number;
    updatedCount: number;
    errors?: string[];
  }> {
    try {
      this.logger.log(`[Stock Transfer] Bắt đầu đồng bộ dữ liệu xuất kho cho brand ${brand} ngày ${date}`);
      
      // Gọi API với P_PART=1,2,3 tuần tự để tránh quá tải
      const parts = [1, 2, 3];
      const allStockTransData: any[] = [];
      
      for (const part of parts) {
        try {
          this.logger.log(`[Stock Transfer] Đang lấy dữ liệu part ${part} cho brand ${brand} ngày ${date}`);
          const partData = await this.zappyApiService.getDailyStockTrans(date, brand, part);
          if (partData && partData.length > 0) {
            allStockTransData.push(...partData);
            this.logger.log(`[Stock Transfer] Nhận được ${partData.length} records từ part ${part} cho brand ${brand} ngày ${date}`);
          } else {
            this.logger.log(`[Stock Transfer] Không có dữ liệu từ part ${part} cho brand ${brand} ngày ${date}`);
          }
        } catch (error: any) {
          this.logger.error(`[Stock Transfer] Lỗi khi lấy dữ liệu part ${part} cho brand ${brand} ngày ${date}: ${error?.message || error}`);
          // Tiếp tục với part tiếp theo, không throw error
        }
      }
      
      if (!allStockTransData || allStockTransData.length === 0) {
        this.logger.log(`[Stock Transfer] Không có dữ liệu xuất kho cho brand ${brand} ngày ${date}`);
        return {
          success: true,
          message: `Không có dữ liệu xuất kho cho brand ${brand} ngày ${date}`,
          recordsCount: 0,
          savedCount: 0,
          updatedCount: 0,
        };
      }

      this.logger.log(`[Stock Transfer] Tổng cộng nhận được ${allStockTransData.length} records xuất kho cho brand ${brand} ngày ${date}`);
      
      // KHÔNG deduplicate - giữ lại tất cả records để lưu vào database
      // Mỗi record sẽ có compositeKey unique với timestamp khi lưu
      const stockTransData = allStockTransData;
      
      this.logger.log(`[Stock Transfer] Giữ lại tất cả ${stockTransData.length} records (không deduplicate)`);

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
            return new Date(
              parseInt(year),
              parseInt(month) - 1,
              parseInt(day),
            );
          }
        } catch (error) {
          this.logger.warn(`Failed to parse transDate: ${dateStr}, using current date`);
          return new Date();
        }
      };

      // Fetch materialCode từ Loyalty API cho tất cả itemCode
      const uniqueItemCodes = Array.from(
        new Set(
          stockTransData
            .map((item) => item.item_code)
            .filter((code): code is string => !!code && code.trim() !== '')
        )
      );

      const materialCodeMap = new Map<string, string>();
      if (uniqueItemCodes.length > 0) {
        try {
          // fetchProducts sẽ tự động thử cả /code/ và /old-code/ cho mỗi itemCode
          const loyaltyProducts = await this.loyaltyService.fetchProducts(uniqueItemCodes);
          loyaltyProducts.forEach((product, itemCode) => {
            if (product?.materialCode) {
              materialCodeMap.set(itemCode, product.materialCode);
            }
          });
        } catch (error: any) {
          this.logger.warn(`[Stock Transfer] Lỗi khi fetch materialCode từ Loyalty API: ${error?.message || error}`);
        }
      }

      let savedCount = 0;
      const errors: string[] = [];

      // Xử lý từng record - chỉ insert mới, không update
      // Mỗi record sẽ có compositeKey unique với timestamp + index để đảm bảo lưu tất cả
      for (let index = 0; index < stockTransData.length; index++) {
        const item = stockTransData[index];
        try {
          // Tạo compositeKey với timestamp + index để đảm bảo unique cho mỗi record
          // Ngay cả khi có nhiều records giống nhau, mỗi record vẫn có unique key
          const timestamp = Date.now();
          const uniqueId = `${timestamp}_${index}_${Math.random().toString(36).substring(2, 9)}`;
          const compositeKey = [
            item.doccode || '',
            item.item_code || '',
            (item.qty || 0).toString(),
            item.stock_code || '',
            item.so_code || 'null',
            uniqueId, // Timestamp + index + random để đảm bảo unique
          ].join('|');

          // Lấy materialCode từ Loyalty API
          const materialCode = item.item_code ? materialCodeMap.get(item.item_code) : undefined;

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

          // Chỉ insert mới, không check duplicate
          const newStockTransfer = this.stockTransferRepository.create(stockTransferData);
          await this.stockTransferRepository.save(newStockTransfer);
          savedCount++;
        } catch (itemError: any) {
          const errorMsg = `Lỗi khi lưu stock transfer ${item.doccode}/${item.item_code}: ${itemError?.message || itemError}`;
          this.logger.error(`[Stock Transfer] ${errorMsg}`);
          errors.push(errorMsg);
        }
      }

      this.logger.log(`[Stock Transfer] Đã lưu ${savedCount} records mới cho brand ${brand} ngày ${date}`);

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
      this.logger.log(`[Stock Transfer Range] Bắt đầu đồng bộ dữ liệu xuất kho từ ${dateFrom} đến ${dateTo}${brand ? ` cho brand ${brand}` : ' cho tất cả brands'}`);

      // Parse dates từ DDMMMYYYY sang Date object
      const parseDate = (dateStr: string): Date => {
        const day = parseInt(dateStr.substring(0, 2));
        const monthStr = dateStr.substring(2, 5).toUpperCase();
        const year = parseInt(dateStr.substring(5, 9));
        
        const monthMap: Record<string, number> = {
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
        };
        
        const month = monthMap[monthStr] || 0;
        return new Date(year, month, day);
      };

      const formatToDDMMMYYYY = (d: Date): string => {
        const day = d.getDate().toString().padStart(2, '0');
        const monthIdx = d.getMonth();
        const year = d.getFullYear();
        const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
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
      let currentDate = new Date(startDate.getTime());
      while (currentDate <= endDate) {
        const dateStr = formatToDDMMMYYYY(currentDate);
        
        // Đồng bộ cho từng brand
        for (const brandItem of brands) {
          try {
            this.logger.log(`[Stock Transfer Range] Đang đồng bộ brand ${brandItem} cho ngày ${dateStr}`);
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

            this.logger.log(`[Stock Transfer Range] Hoàn thành đồng bộ brand ${brandItem} cho ngày ${dateStr}`);
          } catch (error: any) {
            const errorMsg = `Lỗi khi đồng bộ stock transfer cho brand ${brandItem} ngày ${dateStr}: ${error?.message || error}`;
            this.logger.error(`[Stock Transfer Range] ${errorMsg}`);
            errors.push(errorMsg);
          }
        }

        // Tăng 1 ngày
        currentDate.setDate(currentDate.getDate() + 1);
      }

      this.logger.log(`[Stock Transfer Range] Hoàn thành đồng bộ dữ liệu xuất kho từ ${dateFrom} đến ${dateTo}. Tổng: ${totalRecordsCount} records, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`);

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

      const queryBuilder = this.stockTransferRepository.createQueryBuilder('st');

      // Apply filters
      if (params.brand) {
        queryBuilder.andWhere('st.brand = :brand', { brand: params.brand });
      }
      if (params.branchCode) {
        queryBuilder.andWhere('st.branchCode = :branchCode', { branchCode: params.branchCode });
      }
      if (params.itemCode) {
        queryBuilder.andWhere('st.itemCode LIKE :itemCode', { itemCode: `%${params.itemCode}%` });
      }
      if (params.soCode) {
        queryBuilder.andWhere('st.soCode = :soCode', { soCode: params.soCode });
      }
      if (params.dateFrom) {
        // Parse DDMMMYYYY to Date
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
          };
          const month = monthMap[monthStr] || 0;
          return new Date(year, month, day);
        };
        const fromDate = parseDate(params.dateFrom);
        queryBuilder.andWhere('st.transDate >= :dateFrom', { dateFrom: fromDate });
      }
      if (params.dateTo) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
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
      this.logger.error(`Error getting stock transfers: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Đồng bộ báo cáo nộp quỹ cuối ca từ ERP API
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard). Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
   */
  async syncShiftEndCash(date: string, brand?: string): Promise<{
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
          this.logger.log(`[ShiftEndCash] Đang đồng bộ ${brandName} cho ngày ${date}`);
          
          // Lấy dữ liệu từ API
          const shiftEndCashData = await this.zappyApiService.getShiftEndCash(date, brandName);
          
          if (!shiftEndCashData || shiftEndCashData.length === 0) {
            this.logger.log(`[ShiftEndCash] Không có dữ liệu cho ${brandName} - ngày ${date}`);
            continue;
          }

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Parse date string sang Date object
          const parseDateString = (dateStr: string | null | undefined): Date | null => {
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

              // Parse dates
              const openat = record.openat ? new Date(record.openat) : null;
              const closedat = record.closedat ? new Date(record.closedat) : null;
              const docdate = parseDateString(record.docdate);
              const gl_date = parseDateString(record.gl_date);
              const enteredat = parseDateString(record.enteredat);

              if (existingRecord) {
                // Update existing record
                existingRecord.draw_code = record.draw_code || existingRecord.draw_code;
                existingRecord.status = record.status || existingRecord.status;
                existingRecord.teller_code = record.teller_code || existingRecord.teller_code;
                existingRecord.openat = openat || existingRecord.openat;
                existingRecord.closedat = closedat || existingRecord.closedat;
                existingRecord.shift_status = record.shift_status || existingRecord.shift_status;
                existingRecord.docdate = docdate || existingRecord.docdate;
                existingRecord.gl_date = gl_date || existingRecord.gl_date;
                existingRecord.description = record.description || existingRecord.description;
                existingRecord.total = record.total ? Number(record.total) : existingRecord.total;
                existingRecord.enteredat = enteredat || existingRecord.enteredat;
                existingRecord.enteredby = record.enteredby || existingRecord.enteredby;
                existingRecord.sync_date = date;

                // Xóa các lines cũ và tạo mới
                if (record.lines && Array.isArray(record.lines)) {
                  await this.shiftEndCashLineRepository.delete({ shiftEndCashId: existingRecord.id });
                  
                  const linesToCreate = record.lines.map((line: any) => ({
                    shiftEndCashId: existingRecord.id,
                    fop_code: line.fop_code || undefined,
                    fop_name: line.fop_name || undefined,
                    system_amt: line.system_amt ? Number(line.system_amt) : 0,
                    sys_acct_code: line.sys_acct_code || undefined,
                    actual_amt: line.actual_amt ? Number(line.actual_amt) : 0,
                    actual_acct_code: line.actual_acct_code || undefined,
                    diff_amount: line.diff_amount ? Number(line.diff_amount) : 0,
                    diff_acct_code: line.diff_acct_code || undefined,
                    template_id: line.template_id ? Number(line.template_id) : undefined,
                  }));
                  const lines = this.shiftEndCashLineRepository.create(linesToCreate);
                  await this.shiftEndCashLineRepository.save(lines);
                }

                await this.shiftEndCashRepository.save(existingRecord);
                brandUpdatedCount++;
              } else {
                // Tạo record mới
                const newRecord = this.shiftEndCashRepository.create({
                  api_id: record.id,
                  draw_code: record.draw_code || '',
                  status: record.status || null,
                  teller_code: record.teller_code || null,
                  openat: openat,
                  closedat: closedat,
                  shift_status: record.shift_status || null,
                  docdate: docdate,
                  gl_date: gl_date,
                  description: record.description || null,
                  total: record.total ? Number(record.total) : 0,
                  enteredat: enteredat,
                  enteredby: record.enteredby || null,
                  sync_date: date,
                  brand: brandName,
                } as any);

                const savedRecord = (await this.shiftEndCashRepository.save(newRecord)) as unknown as ShiftEndCash;

                // Tạo lines
                if (record.lines && Array.isArray(record.lines)) {
                  const linesToCreate = record.lines.map((line: any) => ({
                    shiftEndCashId: savedRecord.id,
                    fop_code: line.fop_code || undefined,
                    fop_name: line.fop_name || undefined,
                    system_amt: line.system_amt ? Number(line.system_amt) : 0,
                    sys_acct_code: line.sys_acct_code || undefined,
                    actual_amt: line.actual_amt ? Number(line.actual_amt) : 0,
                    actual_acct_code: line.actual_acct_code || undefined,
                    diff_amount: line.diff_amount ? Number(line.diff_amount) : 0,
                    diff_acct_code: line.diff_acct_code || undefined,
                    template_id: line.template_id ? Number(line.template_id) : undefined,
                  }));
                  const lines = this.shiftEndCashLineRepository.create(linesToCreate as any);
                  await this.shiftEndCashLineRepository.save(lines);
                }

                brandSavedCount++;
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

          this.logger.log(`[ShiftEndCash] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`);
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
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ shift end cash: ${error?.message || error}`);
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

      const queryBuilder = this.shiftEndCashRepository
        .createQueryBuilder('sec')
        .leftJoinAndSelect('sec.lines', 'lines')
        .orderBy('sec.docdate', 'DESC')
        .addOrderBy('sec.draw_code', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('sec.brand = :brand', { brand: params.brand });
      }

      // Filter by branchCode (extract from draw_code)
      if (params.branchCode) {
        queryBuilder.andWhere('sec.draw_code LIKE :branchCode', { branchCode: `${params.branchCode}%` });
      }

      // Filter by drawCode
      if (params.drawCode) {
        queryBuilder.andWhere('sec.draw_code = :drawCode', { drawCode: params.drawCode });
      }

      // Filter by dateFrom
      if (params.dateFrom) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
          };
          const month = monthMap[monthStr] || 0;
          return new Date(year, month, day);
        };
        const fromDate = parseDate(params.dateFrom);
        queryBuilder.andWhere('sec.docdate >= :dateFrom', { dateFrom: fromDate });
      }

      // Filter by dateTo
      if (params.dateTo) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
          };
          const month = monthMap[monthStr] || 0;
          return new Date(year, month, day, 23, 59, 59);
        };
        const toDate = parseDate(params.dateTo);
        queryBuilder.andWhere('sec.docdate <= :dateTo', { dateTo: toDate });
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
      this.logger.error(`Error getting shift end cash: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Đồng bộ FaceID theo khoảng thời gian
   * @param startDate - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01OCT2025)
   * @param endDate - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 31OCT2025)
   * @param shopCodes - Optional array of shop codes
   * @returns Kết quả đồng bộ
   */
  async syncFaceIdByDateRange(
    startDate: string,
    endDate: string,
    shopCodes?: string[],
  ): Promise<{
    success: boolean;
    message: string;
    totalSavedCount: number;
    totalUpdatedCount: number;
    errors?: string[];
  }> {
    try {
      // Parse dates
      const parseDate = (dateStr: string): Date => {
        const day = parseInt(dateStr.substring(0, 2));
        const monthStr = dateStr.substring(2, 5).toUpperCase();
        const year = parseInt(dateStr.substring(5, 9));
        const monthMap: Record<string, number> = {
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
        };
        const month = monthMap[monthStr] || 0;
        return new Date(year, month, day);
      };

      const formatDate = (date: Date): string => {
        const day = date.getDate().toString().padStart(2, '0');
        const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
        const month = months[date.getMonth()];
        const year = date.getFullYear();
        return `${day}${month}${year}`;
      };

      const start = parseDate(startDate);
      const end = parseDate(endDate);

      if (start > end) {
        throw new Error('Ngày bắt đầu phải nhỏ hơn hoặc bằng ngày kết thúc');
      }

      let totalSavedCount = 0;
      let totalUpdatedCount = 0;
      const allErrors: string[] = [];

      // Lặp qua từng ngày trong khoảng thời gian
      const currentDate = new Date(start);
      while (currentDate <= end) {
        const dateStr = formatDate(currentDate);
        try {
          this.logger.log(`[syncFaceIdByDateRange] Đồng bộ FaceID - ngày ${dateStr}`);
          const result = await this.syncFaceIdByDate(dateStr, shopCodes);
          
          totalSavedCount += result.savedCount;
          totalUpdatedCount += result.updatedCount;
          
          if (result.errors && result.errors.length > 0) {
            allErrors.push(...result.errors.map(err => `[${dateStr}] ${err}`));
          }
        } catch (error: any) {
          const errorMsg = `Lỗi khi đồng bộ FaceID ngày ${dateStr}: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }

        // Tăng ngày lên 1
        currentDate.setDate(currentDate.getDate() + 1);
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ FaceID thành công từ ${startDate} đến ${endDate}: ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        totalSavedCount,
        totalUpdatedCount,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ FaceID theo khoảng thời gian: ${error?.message || error}`);
      throw error;
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
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
        };
        const month = monthMap[monthStr] || 0;
        return new Date(year, month, day);
      };

      const formatDate = (date: Date): string => {
        const day = date.getDate().toString().padStart(2, '0');
        const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
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
          this.logger.log(`[syncShiftEndCashByDateRange] Bắt đầu đồng bộ brand: ${brandName}`);
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Lặp qua từng ngày trong khoảng thời gian
          const currentDate = new Date(start);
          while (currentDate <= end) {
            const dateStr = formatDate(currentDate);
            try {
              this.logger.log(`[syncShiftEndCashByDateRange] Đồng bộ ${brandName} - ngày ${dateStr}`);
              const result = await this.syncShiftEndCash(dateStr, brandName);
              
              brandRecordsCount += result.recordsCount;
              brandSavedCount += result.savedCount;
              brandUpdatedCount += result.updatedCount;
              
              if (result.errors && result.errors.length > 0) {
                brandErrors.push(...result.errors.map(err => `[${dateStr}] ${err}`));
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

          this.logger.log(`[syncShiftEndCashByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} báo cáo`);
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
      this.logger.error(`Lỗi khi đồng bộ shift end cash theo khoảng thời gian: ${error?.message || error}`);
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
          this.logger.log(`[RepackFormula] Đang đồng bộ ${brandName} cho khoảng ${dateFrom} - ${dateTo}`);
          
          // Lấy dữ liệu từ API
          const repackFormulaData = await this.zappyApiService.getRepackFormula(dateFrom, dateTo, brandName);
          
          if (!repackFormulaData || repackFormulaData.length === 0) {
            this.logger.log(`[RepackFormula] Không có dữ liệu cho ${brandName} - khoảng ${dateFrom} - ${dateTo}`);
            continue;
          }

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Parse date string sang Date object
          const parseDateString = (dateStr: string | null | undefined): Date | null => {
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
              const existingRecord = await this.repackFormulaRepository.findOne({
                where: {
                  api_id: formulaData.id,
                  brand: brandName,
                },
              });

              const validFromdate = parseDateString(formulaData.valid_fromdate);
              const validTodate = parseDateString(formulaData.valid_todate);
              const enteredat = parseDateString(formulaData.enteredat);

              if (existingRecord) {
                // Cập nhật record đã tồn tại
                existingRecord.name = formulaData.name || existingRecord.name;
                existingRecord.check_qty_constraint = formulaData.check_qty_constraint || existingRecord.check_qty_constraint;
                existingRecord.depr_pct = formulaData.depr_pct !== undefined ? formulaData.depr_pct : existingRecord.depr_pct;
                existingRecord.branch_codes = formulaData.branch_codes !== undefined ? formulaData.branch_codes : existingRecord.branch_codes;
                existingRecord.repack_cat_name = formulaData.repack_cat_name || existingRecord.repack_cat_name;
                existingRecord.valid_fromdate = validFromdate || existingRecord.valid_fromdate;
                existingRecord.valid_todate = validTodate !== null ? validTodate : existingRecord.valid_todate;
                existingRecord.enteredby = formulaData.enteredby || existingRecord.enteredby;
                existingRecord.enteredat = enteredat || existingRecord.enteredat;
                existingRecord.locked = formulaData.locked || existingRecord.locked;
                existingRecord.sync_date_from = dateFrom;
                existingRecord.sync_date_to = dateTo;

                // Xóa các items cũ
                await this.repackFormulaItemRepository.delete({ repackFormulaId: existingRecord.id });

                // Tạo lại items từ from_items và to_items
                const items: RepackFormulaItem[] = [];
                if (formulaData.from_items && Array.isArray(formulaData.from_items)) {
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
                if (formulaData.to_items && Array.isArray(formulaData.to_items)) {
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
                  check_qty_constraint: formulaData.check_qty_constraint || undefined,
                  depr_pct: formulaData.depr_pct !== undefined ? formulaData.depr_pct : 0,
                  branch_codes: formulaData.branch_codes !== undefined ? formulaData.branch_codes : undefined,
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

                const savedRecord = (await this.repackFormulaRepository.save(newRecord)) as unknown as RepackFormula;

                // Tạo items từ from_items và to_items
                const items: RepackFormulaItem[] = [];
                if (formulaData.from_items && Array.isArray(formulaData.from_items)) {
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
                if (formulaData.to_items && Array.isArray(formulaData.to_items)) {
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

          this.logger.log(`[RepackFormula] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`);
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
      this.logger.error(`Lỗi khi đồng bộ repack formula: ${error?.message || error}`);
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
          'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
          'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
          'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
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
          this.logger.log(`[syncRepackFormulaByDateRange] Bắt đầu đồng bộ brand: ${brandName}`);
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          try {
            this.logger.log(`[syncRepackFormulaByDateRange] Đồng bộ ${brandName} - khoảng ${startDate} - ${endDate}`);
            const result = await this.syncRepackFormula(startDate, endDate, brandName);
            
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

          this.logger.log(`[syncRepackFormulaByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} công thức`);
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
      this.logger.error(`Lỗi khi đồng bộ repack formula theo khoảng thời gian: ${error?.message || error}`);
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
        queryBuilder.andWhere('rf.repack_cat_name = :repackCatName', { repackCatName: params.repackCatName });
      }

      // Filter by itemcode (trong items)
      if (params.itemcode) {
        queryBuilder.andWhere('items.itemcode = :itemcode', { itemcode: params.itemcode });
      }

      // Filter by dateFrom
      if (params.dateFrom) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
          };
          const month = monthMap[monthStr] || 0;
          return new Date(year, month, day);
        };
        const fromDate = parseDate(params.dateFrom);
        queryBuilder.andWhere('rf.valid_fromdate >= :dateFrom', { dateFrom: fromDate });
      }

      // Filter by dateTo
      if (params.dateTo) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
          };
          const month = monthMap[monthStr] || 0;
          return new Date(year, month, day, 23, 59, 59);
        };
        const toDate = parseDate(params.dateTo);
        queryBuilder.andWhere('rf.valid_fromdate <= :dateTo', { dateTo: toDate });
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
      this.logger.error(`Error getting repack formula: ${error?.message || error}`);
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
          this.logger.log(`[Promotion] Đang đồng bộ ${brandName} cho khoảng ${dateFrom} - ${dateTo}`);
          
          // Lấy danh sách promotion từ API
          const promotionData = await this.zappyApiService.getPromotion(dateFrom, dateTo, brandName);
          
          if (!promotionData || promotionData.length === 0) {
            this.logger.log(`[Promotion] Không có dữ liệu cho ${brandName} - khoảng ${dateFrom} - ${dateTo}`);
            continue;
          }

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Parse date string sang Date object
          const parseDateString = (dateStr: string | null | undefined): Date | null => {
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
                this.logger.warn(`Invalid date values: ${dateStr} (year: ${year}, month: ${month}, day: ${day})`);
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
                this.logger.warn(`Invalid Date object created from: ${dateStr}`);
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
            const apiId = typeof promoData.id === 'number' ? promoData.id : parseInt(String(promoData.id), 10);
            
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
                this.logger.log(`[Promotion] Cập nhật promotion ${apiId} (${promoData.code || 'N/A'}) cho brand ${brandName}`);
                // Cập nhật record đã tồn tại
                existingRecord.code = promoData.code || existingRecord.code;
                existingRecord.seq = promoData.seq !== undefined ? promoData.seq : existingRecord.seq;
                existingRecord.name = promoData.name || existingRecord.name;
                existingRecord.fromdate = fromdate || existingRecord.fromdate;
                existingRecord.todate = todate !== null ? todate : existingRecord.todate;
                existingRecord.ptype = promoData.ptype || existingRecord.ptype;
                existingRecord.pricetype = promoData.pricetype || existingRecord.pricetype;
                existingRecord.brand_code = promoData.brand_code || existingRecord.brand_code;
                existingRecord.locked = promoData.locked || existingRecord.locked;
                existingRecord.status = promoData.status || existingRecord.status;
                existingRecord.enteredby = promoData.enteredby || existingRecord.enteredby;
                existingRecord.enteredat = enteredat || existingRecord.enteredat;
                existingRecord.sync_date_from = dateFrom;
                existingRecord.sync_date_to = dateTo;

                // Xóa các lines cũ
                await this.promotionLineRepository.delete({ promotionId: existingRecord.id });

                // Lấy chi tiết lines từ API
                let lines: PromotionLine[] = [];
                try {
                  const lineData = await this.zappyApiService.getPromotionLine(promoData.id, brandName);
                  
                  // Tạo lại lines từ i_lines và v_lines
                  lines = [];
                  if (lineData.i_lines && Array.isArray(lineData.i_lines)) {
                    for (const iLine of lineData.i_lines) {
                      const line = this.promotionLineRepository.create({
                        promotion: existingRecord,
                        line_type: 'i_lines',
                        seq: iLine.seq !== undefined ? iLine.seq : undefined,
                        buy_items: iLine.buy_items || undefined,
                        buy_qty: iLine.buy_qty !== undefined ? iLine.buy_qty : 0,
                        buy_type: iLine.buy_type || undefined,
                        buy_combined_qty: iLine.buy_combined_qty !== undefined ? iLine.buy_combined_qty : undefined,
                        prom_group: iLine.prom_group || undefined,
                        card_pattern: iLine.card_pattern || undefined,
                        get_items: iLine.get_items || undefined,
                        get_item_price: iLine.get_item_price !== undefined ? iLine.get_item_price : undefined,
                        get_qty: iLine.get_qty !== undefined ? iLine.get_qty : 0,
                        get_discamt: iLine.get_discamt !== undefined ? iLine.get_discamt : undefined,
                        get_max_discamt: iLine.get_max_discamt !== undefined ? iLine.get_max_discamt : 0,
                        get_discpct: iLine.get_discpct !== undefined ? iLine.get_discpct : undefined,
                        get_item_option: iLine.get_item_option || undefined,
                        svc_card_months: iLine.svc_card_months !== undefined ? iLine.svc_card_months : undefined,
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
                        buy_qty: vLine.buy_qty !== undefined ? vLine.buy_qty : 0,
                        buy_type: vLine.buy_type || undefined,
                        buy_combined_qty: vLine.buy_combined_qty !== undefined ? vLine.buy_combined_qty : undefined,
                        prom_group: vLine.prom_group || undefined,
                        card_pattern: vLine.card_pattern || undefined,
                        get_items: vLine.get_items || undefined,
                        get_item_price: vLine.get_item_price !== undefined ? vLine.get_item_price : undefined,
                        get_qty: vLine.get_qty !== undefined ? vLine.get_qty : 0,
                        get_discamt: vLine.get_discamt !== undefined ? vLine.get_discamt : undefined,
                        get_max_discamt: vLine.get_max_discamt !== undefined ? vLine.get_max_discamt : 0,
                        get_discpct: vLine.get_discpct !== undefined ? vLine.get_discpct : undefined,
                        get_item_option: vLine.get_item_option || undefined,
                        svc_card_months: vLine.svc_card_months !== undefined ? vLine.svc_card_months : undefined,
                        guideline: vLine.guideline || undefined,
                      });
                      lines.push(line);
                    }
                  }

                  if (lines.length > 0) {
                    await this.promotionLineRepository.save(lines);
                  }
                } catch (lineError: any) {
                  this.logger.warn(`Không thể lấy lines cho promotion ${apiId}: ${lineError?.message || lineError}`);
                  // Vẫn tiếp tục, không throw error
                }

                existingRecord.lines = lines;
                await this.promotionRepository.save(existingRecord);
                brandUpdatedCount++;
              } else {
                // Tạo record mới
                this.logger.log(`[Promotion] Tạo mới promotion ${apiId} (${promoData.code || 'N/A'}) cho brand ${brandName}`);
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

                const savedRecord = (await this.promotionRepository.save(newRecord)) as unknown as Promotion;

                // Lấy chi tiết lines từ API
                try {
                  const lineData = await this.zappyApiService.getPromotionLine(apiId, brandName);
                  
                  // Tạo lines từ i_lines và v_lines
                  const lines: PromotionLine[] = [];
                  if (lineData.i_lines && Array.isArray(lineData.i_lines)) {
                    for (const iLine of lineData.i_lines) {
                      const line = this.promotionLineRepository.create({
                        promotion: savedRecord,
                        line_type: 'i_lines',
                        seq: iLine.seq !== undefined ? iLine.seq : undefined,
                        buy_items: iLine.buy_items || undefined,
                        buy_qty: iLine.buy_qty !== undefined ? iLine.buy_qty : 0,
                        buy_type: iLine.buy_type || undefined,
                        buy_combined_qty: iLine.buy_combined_qty !== undefined ? iLine.buy_combined_qty : undefined,
                        prom_group: iLine.prom_group || undefined,
                        card_pattern: iLine.card_pattern || undefined,
                        get_items: iLine.get_items || undefined,
                        get_item_price: iLine.get_item_price !== undefined ? iLine.get_item_price : undefined,
                        get_qty: iLine.get_qty !== undefined ? iLine.get_qty : 0,
                        get_discamt: iLine.get_discamt !== undefined ? iLine.get_discamt : undefined,
                        get_max_discamt: iLine.get_max_discamt !== undefined ? iLine.get_max_discamt : 0,
                        get_discpct: iLine.get_discpct !== undefined ? iLine.get_discpct : undefined,
                        get_item_option: iLine.get_item_option || undefined,
                        svc_card_months: iLine.svc_card_months !== undefined ? iLine.svc_card_months : undefined,
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
                        buy_qty: vLine.buy_qty !== undefined ? vLine.buy_qty : 0,
                        buy_type: vLine.buy_type || undefined,
                        buy_combined_qty: vLine.buy_combined_qty !== undefined ? vLine.buy_combined_qty : undefined,
                        prom_group: vLine.prom_group || undefined,
                        card_pattern: vLine.card_pattern || undefined,
                        get_items: vLine.get_items || undefined,
                        get_item_price: vLine.get_item_price !== undefined ? vLine.get_item_price : undefined,
                        get_qty: vLine.get_qty !== undefined ? vLine.get_qty : 0,
                        get_discamt: vLine.get_discamt !== undefined ? vLine.get_discamt : undefined,
                        get_max_discamt: vLine.get_max_discamt !== undefined ? vLine.get_max_discamt : 0,
                        get_discpct: vLine.get_discpct !== undefined ? vLine.get_discpct : undefined,
                        get_item_option: vLine.get_item_option || undefined,
                        svc_card_months: vLine.svc_card_months !== undefined ? vLine.svc_card_months : undefined,
                        guideline: vLine.guideline || undefined,
                      });
                      lines.push(line);
                    }
                  }

                  if (lines.length > 0) {
                    await this.promotionLineRepository.save(lines);
                  }
                } catch (lineError: any) {
                  this.logger.warn(`Không thể lấy lines cho promotion ${apiId}: ${lineError?.message || lineError}`);
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

          this.logger.log(`[Promotion] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`);
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
      this.logger.error(`Lỗi khi đồng bộ promotion: ${error?.message || error}`);
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
          this.logger.log(`[syncPromotionByDateRange] Bắt đầu đồng bộ brand: ${brandName}`);
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          try {
            this.logger.log(`[syncPromotionByDateRange] Đồng bộ ${brandName} - khoảng ${startDate} - ${endDate}`);
            const result = await this.syncPromotion(startDate, endDate, brandName);
            
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

          this.logger.log(`[syncPromotionByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} chương trình`);
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
      this.logger.error(`Lỗi khi đồng bộ promotion theo khoảng thời gian: ${error?.message || error}`);
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
        queryBuilder.andWhere('p.code LIKE :code', { code: `%${params.code}%` });
      }

      // Filter by dateFrom
      if (params.dateFrom) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
          };
          const month = monthMap[monthStr] || 0;
          return new Date(year, month, day);
        };
        const fromDate = parseDate(params.dateFrom);
        queryBuilder.andWhere('p.fromdate >= :dateFrom', { dateFrom: fromDate });
      }

      // Filter by dateTo
      if (params.dateTo) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
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
   * Đồng bộ dữ liệu danh sách Voucher Issue từ API
   * @param dateFrom - Ngày bắt đầu (format: DDMMMYYYY, ví dụ: 01NOV2025)
   * @param dateTo - Ngày kết thúc (format: DDMMMYYYY, ví dụ: 30NOV2025)
   * @param brand - Optional brand name. Nếu không có thì đồng bộ tất cả brands
   * @returns Kết quả đồng bộ
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
          this.logger.log(`[VoucherIssue] Đang đồng bộ ${brandName} cho khoảng ${dateFrom} - ${dateTo}`);
          
          // Lấy danh sách voucher issue từ API
          const voucherIssueData = await this.zappyApiService.getVoucherIssue(dateFrom, dateTo, brandName);
          
          if (!voucherIssueData || voucherIssueData.length === 0) {
            this.logger.log(`[VoucherIssue] Không có dữ liệu cho ${brandName} - khoảng ${dateFrom} - ${dateTo}`);
            continue;
          }

          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          // Parse date string sang Date object (sử dụng hàm parseDateString từ syncPromotion)
          const parseDateString = (dateStr: string | null | undefined): Date | null => {
            if (!dateStr || typeof dateStr !== 'string') return null;
            try {
              const trimmed = dateStr.trim();
              if (!trimmed) return null;

              if (trimmed.includes('T') || trimmed.includes('Z')) {
                const isoDate = new Date(trimmed);
                if (isNaN(isoDate.getTime())) {
                  this.logger.warn(`Invalid ISO date: ${dateStr}`);
                  return null;
                }
                return isoDate;
              }
              
              const parts = trimmed.split(' ');
              const datePart = parts[0];
              const timePart = parts[1] || '00:00';
              
              if (!datePart || !datePart.includes('/')) {
                this.logger.warn(`Invalid date format (no /): ${dateStr}`);
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
                this.logger.warn(`Invalid date values: ${dateStr}`);
                return null;
              }
              
              if (monthNum < 1 || monthNum > 12 || dayNum < 1 || dayNum > 31) {
                this.logger.warn(`Invalid month/day: ${dateStr}`);
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
                this.logger.warn(`Invalid Date object created from: ${dateStr}`);
                return null;
              }
              
              return date;
            } catch (error) {
              this.logger.warn(`Không thể parse date: ${dateStr} - ${error}`);
              return null;
            }
          };

          for (const voucherData of voucherIssueData) {
            // Đảm bảo api_id là number
            const apiId = typeof voucherData.id === 'number' ? voucherData.id : parseInt(String(voucherData.id), 10);
            
            if (isNaN(apiId)) {
              this.logger.warn(`Invalid api_id for voucher issue: ${voucherData.id}`);
              continue;
            }
            
            try {
              // Kiểm tra xem đã tồn tại chưa (dựa trên api_id và brand)
              const existingRecord = await this.voucherIssueRepository.findOne({
                where: {
                  api_id: apiId,
                  brand: brandName,
                },
              });

              const docdate = parseDateString(voucherData.docdate);
              const validFromdate = parseDateString(voucherData.valid_fromdate);
              const validTodate = parseDateString(voucherData.valid_todate);
              const enteredat = parseDateString(voucherData.enteredat);

              if (existingRecord) {
                this.logger.log(`[VoucherIssue] Cập nhật voucher issue ${apiId} (${voucherData.code || 'N/A'}) cho brand ${brandName}`);
                
                // Cập nhật record đã tồn tại
                existingRecord.code = voucherData.code || existingRecord.code;
                existingRecord.status_lov = voucherData.status_lov || existingRecord.status_lov;
                existingRecord.docdate = docdate || existingRecord.docdate;
                existingRecord.description = voucherData.description || existingRecord.description;
                existingRecord.brand_code = voucherData.brand_code || existingRecord.brand_code;
                existingRecord.apply_for_branch_types = voucherData.apply_for_branch_types || existingRecord.apply_for_branch_types;
                existingRecord.val = voucherData.val !== undefined && voucherData.val !== null ? voucherData.val : existingRecord.val;
                existingRecord.percent = voucherData.percent !== undefined && voucherData.percent !== null ? voucherData.percent : (existingRecord.percent ?? 0);
                existingRecord.max_value = voucherData.max_value !== undefined && voucherData.max_value !== null ? voucherData.max_value : existingRecord.max_value;
                existingRecord.saletype = voucherData.saletype || existingRecord.saletype;
                existingRecord.enable_precost = voucherData.enable_precost || existingRecord.enable_precost;
                existingRecord.supplier_support_fee = voucherData.supplier_support_fee !== undefined && voucherData.supplier_support_fee !== null ? voucherData.supplier_support_fee : existingRecord.supplier_support_fee;
                existingRecord.valid_fromdate = validFromdate || existingRecord.valid_fromdate;
                existingRecord.valid_todate = validTodate !== null ? validTodate : existingRecord.valid_todate;
                existingRecord.valid_days_from_so = voucherData.valid_days_from_so !== undefined && voucherData.valid_days_from_so !== null ? voucherData.valid_days_from_so : existingRecord.valid_days_from_so;
                existingRecord.check_ownership = voucherData.check_ownership || existingRecord.check_ownership;
                existingRecord.allow_cashback = voucherData.allow_cashback || existingRecord.allow_cashback;
                existingRecord.prom_for_employee = voucherData.prom_for_employee || existingRecord.prom_for_employee;
                existingRecord.bonus_for_sale_employee = voucherData.bonus_for_sale_employee || existingRecord.bonus_for_sale_employee;
                existingRecord.so_percent = voucherData.so_percent !== undefined && voucherData.so_percent !== null ? voucherData.so_percent : existingRecord.so_percent;
                existingRecord.r_total_scope = voucherData.r_total_scope || existingRecord.r_total_scope;
                existingRecord.ecode_item_code = voucherData.ecode_item_code !== undefined ? voucherData.ecode_item_code : existingRecord.ecode_item_code;
                existingRecord.voucher_item_code = voucherData.voucher_item_code || existingRecord.voucher_item_code;
                existingRecord.voucher_item_name = voucherData.voucher_item_name || existingRecord.voucher_item_name;
                existingRecord.cost_for_gl = voucherData.cost_for_gl !== undefined && voucherData.cost_for_gl !== null ? voucherData.cost_for_gl : existingRecord.cost_for_gl;
                existingRecord.buy_items_by_date_range = voucherData.buy_items_by_date_range || existingRecord.buy_items_by_date_range;
                existingRecord.buy_items_option_name = voucherData.buy_items_option_name || existingRecord.buy_items_option_name;
                existingRecord.disable_bonus_point_for_sale = voucherData.disable_bonus_point_for_sale || existingRecord.disable_bonus_point_for_sale;
                existingRecord.disable_bonus_point = voucherData.disable_bonus_point || existingRecord.disable_bonus_point;
                existingRecord.for_mkt_kol = voucherData.for_mkt_kol || existingRecord.for_mkt_kol;
                existingRecord.for_mkt_prom = voucherData.for_mkt_prom || existingRecord.for_mkt_prom;
                existingRecord.allow_apply_for_promoted_so = voucherData.allow_apply_for_promoted_so || existingRecord.allow_apply_for_promoted_so;
                existingRecord.campaign_code = voucherData.campaign_code !== undefined ? voucherData.campaign_code : existingRecord.campaign_code;
                existingRecord.sl_max_sudung_cho_1_kh = voucherData.sl_max_sudung_cho_1_kh !== undefined ? voucherData.sl_max_sudung_cho_1_kh : existingRecord.sl_max_sudung_cho_1_kh;
                existingRecord.is_locked = voucherData.is_locked || existingRecord.is_locked;
                existingRecord.enteredat = enteredat || existingRecord.enteredat;
                existingRecord.enteredby = voucherData.enteredby || existingRecord.enteredby;
                existingRecord.material_type = voucherData.material_type || existingRecord.material_type;
                existingRecord.applyfor_wso = voucherData.applyfor_wso !== undefined ? voucherData.applyfor_wso : existingRecord.applyfor_wso;
                existingRecord.sync_date_from = dateFrom;
                existingRecord.sync_date_to = dateTo;

                // Xóa các details cũ
                await this.voucherIssueDetailRepository.delete({ voucherIssueId: existingRecord.id });

                // Lấy chi tiết từ API
                let details: VoucherIssueDetail[] = [];
                try {
                  const detailData = await this.zappyApiService.getVoucherIssueDetail(apiId, brandName);
                  
                  // Lưu toàn bộ detail_data dạng JSON
                  if (detailData) {
                    const detail = this.voucherIssueDetailRepository.create({
                      voucherIssue: existingRecord,
                      detail_data: detailData,
                    });
                    details.push(detail);
                  }
                  
                  if (details.length > 0) {
                    await this.voucherIssueDetailRepository.save(details);
                  }
                } catch (detailError: any) {
                  this.logger.warn(`Không thể lấy details cho voucher issue ${apiId}: ${detailError?.message || detailError}`);
                  // Vẫn tiếp tục, không throw error
                }

                existingRecord.details = details;
                await this.voucherIssueRepository.save(existingRecord);
                brandUpdatedCount++;
              } else {
                // Tạo record mới
                this.logger.log(`[VoucherIssue] Tạo mới voucher issue ${apiId} (${voucherData.code || 'N/A'}) cho brand ${brandName}`);
                const newRecord = this.voucherIssueRepository.create({
                  api_id: apiId,
                  code: voucherData.code || undefined,
                  status_lov: voucherData.status_lov || undefined,
                  docdate: docdate || undefined,
                  description: voucherData.description || undefined,
                  brand_code: voucherData.brand_code || undefined,
                  apply_for_branch_types: voucherData.apply_for_branch_types || undefined,
                  val: voucherData.val !== undefined && voucherData.val !== null ? voucherData.val : 0,
                  percent: voucherData.percent !== undefined && voucherData.percent !== null ? voucherData.percent : 0,
                  max_value: voucherData.max_value !== undefined && voucherData.max_value !== null ? voucherData.max_value : 0,
                  saletype: voucherData.saletype || undefined,
                  enable_precost: voucherData.enable_precost || undefined,
                  supplier_support_fee: voucherData.supplier_support_fee !== undefined && voucherData.supplier_support_fee !== null ? voucherData.supplier_support_fee : 0,
                  valid_fromdate: validFromdate || undefined,
                  valid_todate: validTodate || undefined,
                  valid_days_from_so: voucherData.valid_days_from_so !== undefined && voucherData.valid_days_from_so !== null ? voucherData.valid_days_from_so : 0,
                  check_ownership: voucherData.check_ownership || undefined,
                  allow_cashback: voucherData.allow_cashback || undefined,
                  prom_for_employee: voucherData.prom_for_employee || undefined,
                  bonus_for_sale_employee: voucherData.bonus_for_sale_employee || undefined,
                  so_percent: voucherData.so_percent !== undefined && voucherData.so_percent !== null ? voucherData.so_percent : undefined,
                  r_total_scope: voucherData.r_total_scope || undefined,
                  ecode_item_code: voucherData.ecode_item_code !== undefined ? voucherData.ecode_item_code : undefined,
                  voucher_item_code: voucherData.voucher_item_code || undefined,
                  voucher_item_name: voucherData.voucher_item_name || undefined,
                  cost_for_gl: voucherData.cost_for_gl !== undefined && voucherData.cost_for_gl !== null ? voucherData.cost_for_gl : 0,
                  buy_items_by_date_range: voucherData.buy_items_by_date_range || undefined,
                  buy_items_option_name: voucherData.buy_items_option_name || undefined,
                  disable_bonus_point_for_sale: voucherData.disable_bonus_point_for_sale || undefined,
                  disable_bonus_point: voucherData.disable_bonus_point || undefined,
                  for_mkt_kol: voucherData.for_mkt_kol || undefined,
                  for_mkt_prom: voucherData.for_mkt_prom || undefined,
                  allow_apply_for_promoted_so: voucherData.allow_apply_for_promoted_so || undefined,
                  campaign_code: voucherData.campaign_code !== undefined ? voucherData.campaign_code : undefined,
                  sl_max_sudung_cho_1_kh: voucherData.sl_max_sudung_cho_1_kh !== undefined && voucherData.sl_max_sudung_cho_1_kh !== null ? voucherData.sl_max_sudung_cho_1_kh : 0,
                  is_locked: voucherData.is_locked || undefined,
                  enteredat: enteredat || undefined,
                  enteredby: voucherData.enteredby || undefined,
                  material_type: voucherData.material_type || undefined,
                  applyfor_wso: voucherData.applyfor_wso !== undefined ? voucherData.applyfor_wso : undefined,
                  sync_date_from: dateFrom,
                  sync_date_to: dateTo,
                  brand: brandName,
                });

                const savedRecord = (await this.voucherIssueRepository.save(newRecord)) as unknown as VoucherIssue;

                // Lấy chi tiết từ API
                try {
                  const detailData = await this.zappyApiService.getVoucherIssueDetail(apiId, brandName);
                  
                  // Lưu toàn bộ detail_data dạng JSON
                  if (detailData) {
                    const detail = this.voucherIssueDetailRepository.create({
                      voucherIssue: savedRecord,
                      detail_data: detailData,
                    });
                    await this.voucherIssueDetailRepository.save(detail);
                  }
                } catch (detailError: any) {
                  this.logger.warn(`Không thể lấy details cho voucher issue ${apiId}: ${detailError?.message || detailError}`);
                  // Vẫn tiếp tục, không throw error
                }

                brandSavedCount++;
              }
              totalRecordsCount++;
            } catch (error: any) {
              const errorMsg = `Lỗi khi xử lý voucher issue id ${apiId}: ${error?.message || error}`;
              this.logger.error(errorMsg);
              brandErrors.push(errorMsg);
            }
          }

          totalSavedCount += brandSavedCount;
          totalUpdatedCount += brandUpdatedCount;
          allErrors.push(...brandErrors);

          this.logger.log(`[VoucherIssue] Hoàn thành đồng bộ ${brandName}: ${brandSavedCount} mới, ${brandUpdatedCount} cập nhật`);
        } catch (error: any) {
          const errorMsg = `[${brandName}] Lỗi khi đồng bộ voucher issue: ${error?.message || error}`;
          this.logger.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      return {
        success: allErrors.length === 0,
        message: `Đồng bộ danh sách Voucher Issue thành công: ${totalRecordsCount} voucher, ${totalSavedCount} mới, ${totalUpdatedCount} cập nhật`,
        recordsCount: totalRecordsCount,
        savedCount: totalSavedCount,
        updatedCount: totalUpdatedCount,
        errors: allErrors.length > 0 ? allErrors : undefined,
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ voucher issue: ${error?.message || error}`);
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
          this.logger.log(`[syncVoucherIssueByDateRange] Bắt đầu đồng bộ brand: ${brandName}`);
          let brandRecordsCount = 0;
          let brandSavedCount = 0;
          let brandUpdatedCount = 0;
          const brandErrors: string[] = [];

          try {
            this.logger.log(`[syncVoucherIssueByDateRange] Đồng bộ ${brandName} - khoảng ${startDate} - ${endDate}`);
            const result = await this.syncVoucherIssue(startDate, endDate, brandName);
            
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

          this.logger.log(`[syncVoucherIssueByDateRange] Hoàn thành đồng bộ brand: ${brandName} - ${brandRecordsCount} voucher`);
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
      this.logger.error(`Lỗi khi đồng bộ voucher issue theo khoảng thời gian: ${error?.message || error}`);
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
    code?: string;
    materialType?: string;
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
        .leftJoinAndSelect('vi.details', 'details')
        .orderBy('vi.docdate', 'DESC')
        .addOrderBy('vi.code', 'ASC');

      // Filter by brand
      if (params.brand) {
        queryBuilder.andWhere('vi.brand = :brand', { brand: params.brand });
      }

      // Filter by status
      if (params.status) {
        queryBuilder.andWhere('vi.status_lov = :status', { status: params.status });
      }

      // Filter by code
      if (params.code) {
        queryBuilder.andWhere('vi.code LIKE :code', { code: `%${params.code}%` });
      }

      // Filter by material_type
      if (params.materialType) {
        queryBuilder.andWhere('vi.material_type = :materialType', { materialType: params.materialType });
      }

      // Filter by dateFrom
      if (params.dateFrom) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
          };
          const month = monthMap[monthStr] || 0;
          return new Date(year, month, day);
        };
        const fromDate = parseDate(params.dateFrom);
        queryBuilder.andWhere('vi.docdate >= :dateFrom', { dateFrom: fromDate });
      }

      // Filter by dateTo
      if (params.dateTo) {
        const parseDate = (dateStr: string): Date => {
          const day = parseInt(dateStr.substring(0, 2));
          const monthStr = dateStr.substring(2, 5).toUpperCase();
          const year = parseInt(dateStr.substring(5, 9));
          const monthMap: Record<string, number> = {
            'JAN': 0, 'FEB': 1, 'MAR': 2, 'APR': 3,
            'MAY': 4, 'JUN': 5, 'JUL': 6, 'AUG': 7,
            'SEP': 8, 'OCT': 9, 'NOV': 10, 'DEC': 11,
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
      this.logger.error(`Error getting voucher issue: ${error?.message || error}`);
      throw error;
    }
  }
}

