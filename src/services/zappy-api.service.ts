import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { Order, OrderCustomer, SaleItem } from '../types/order.types';

/**
 * Service để gọi API từ Zappy và transform dữ liệu
 */
@Injectable()
export class ZappyApiService {
  private readonly logger = new Logger(ZappyApiService.name);
  private readonly DEFAULT_ZAPPY_API_BASE_URL = process.env.ZAPPY_API_BASE_URL || 'https://zappy.io.vn/ords/vmt/api';

  // Map brand name to Zappy API base URL
  private readonly brandApiUrls: Record<string, string> = {
    'f3': 'https://zappy.io.vn/ords/vmt/api',
    'labhair': 'https://zappy.io.vn/ords/labhair/api',
    'yaman': 'https://zappy.io.vn/ords/yaman/api',
    'menard': 'https://vmterp.com/ords/erp/retail/api',
  };

  constructor(private readonly httpService: HttpService) {}

  /**
   * Lấy base URL cho brand
   */
  private getBaseUrlForBrand(brand?: string): string {
    if (brand && this.brandApiUrls[brand.toLowerCase()]) {
      return this.brandApiUrls[brand.toLowerCase()];
    }
    return this.DEFAULT_ZAPPY_API_BASE_URL;
  }

  /**
   * Lấy dữ liệu đơn hàng từ Zappy API
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 04DEC2025)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Array of Order objects
   */
  async getDailySales(date: string, brand?: string): Promise<Order[]> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      const url = `${baseUrl}/get_daily_sale?P_DATE=${date}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      const rawData = response?.data?.data || [];
      if (!Array.isArray(rawData) || rawData.length === 0) {
        this.logger.warn(`No sales data found for date ${date}`);
        return [];
      }

      // Chỉ lấy các dòng có doctype là "SALE_ORDER", bỏ qua các loại khác (SALE_RETURN, etc.)
      const saleOrdersOnly = rawData.filter((item) => item.doctype === 'SALE_ORDER');
      
      if (saleOrdersOnly.length === 0) {
        this.logger.warn(`No SALE_ORDER data found for date ${date} (filtered from ${rawData.length} total items)`);
        return [];
      }

      // Transform dữ liệu từ Zappy format sang Order format
      return this.transformZappySalesToOrders(saleOrdersOnly);
    } catch (error: any) {
      this.logger.error(`Error fetching daily sales from Zappy API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lấy dữ liệu thanh toán từ Zappy API (voucher payments)
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Array of cash payment records
   */
  async getDailyCash(date: string, brand?: string): Promise<any[]> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      // labhair, yaman, menard dùng get_daily_cashio, f3 và default dùng get_daily_cash
      const brandLower = brand?.toLowerCase();
      const endpoint = ['labhair', 'yaman', 'menard'].includes(brandLower || '') 
        ? 'get_daily_cashio' 
        : 'get_daily_cash';
      const url = `${baseUrl}/${endpoint}?P_DATE=${date}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      return response?.data?.data || [];
    } catch (error: any) {
      this.logger.error(`Error fetching daily cash from Zappy API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Transform dữ liệu từ Zappy format sang Order format
   */
  private transformZappySalesToOrders(zappySales: any[]): Order[] {
    // Group sales by docCode (code field)
    const ordersMap = new Map<string, Order>();

    zappySales.forEach((zappySale) => {
      const docCode = zappySale.code;
      if (!docCode) return;

      // Map customer info
      const customer: OrderCustomer = {
        code: zappySale.partner_code || '',
        name: zappySale.partner_name || '',
        brand: '', // Không có trong Zappy API, để rỗng
        mobile: undefined,
        sexual: undefined,
        idnumber: undefined,
        enteredat: undefined,
        crm_lead_source: undefined,
        address: undefined,
        province_name: undefined,
        birthday: undefined,
        grade_name: zappySale.partner_grade || undefined,
        branch_code: zappySale.branch_code || undefined,
      };

      // Map sale item
      const saleItem: SaleItem = {
        id: zappySale.id?.toString(),
        promCode: zappySale.prom_code || undefined,
        itemCode: zappySale.itemcode || undefined,
        itemName: zappySale.itemname || undefined,
        description: zappySale.description || undefined,
        partnerCode: zappySale.partner_code || undefined,
        ordertype: zappySale.ordertype_name || undefined, // Lưu ordertype_name trực tiếp
        branchCode: zappySale.branch_code || undefined,
        serial: zappySale.serial || undefined,
        qty: zappySale.qty || 0,
        revenue: zappySale.revenue || 0,
        linetotal: zappySale.mn_linetotal || zappySale.revenue || 0,
        tienHang: zappySale.mn_linetotal || zappySale.revenue || 0,
        giaBan: zappySale.price || 0,
        disc_amt: zappySale.discamt ?? 0,
        grade_discamt: zappySale.grade_discamt ?? 0,
        other_discamt: zappySale.other_discamt ?? 0,
        chietKhauMuaHangGiamGia: zappySale.other_discamt ?? 0, // Chiết khấu mua hàng giảm giá = other_discamt
        paid_by_voucher_ecode_ecoin_bp: zappySale.v_paid ?? 0,
        shift_code: zappySale.shift_code || undefined,
        saleperson_id: zappySale.saleperson_code ? parseInt(zappySale.saleperson_code) : undefined,
        order_source: zappySale.so_source || undefined,
        partner_name: zappySale.partner_name || undefined,
        // Các trường khác
        producttype: zappySale.producttype || undefined,
        pkg_code: zappySale.pkg_code || undefined,
        social_page_id: zappySale.social_page_id || undefined,
        sp_email: zappySale.sp_email || undefined,
        mvc_serial: zappySale.mvc_serial || undefined,
        vc_promotion_code: zappySale.vc_promotion_code || undefined,
        // Category fields
        cat1: zappySale.cat1 || undefined,
        cat2: zappySale.cat2 || undefined,
        cat3: zappySale.cat3 || undefined,
        catcode1: zappySale.catcode1 || undefined,
        catcode2: zappySale.catcode2 || undefined,
        catcode3: zappySale.catcode3 || undefined,
      };

      // Parse docDate từ format "DD-MM-YYYY HH:mm" sang ISO string
      const docDate = this.parseZappyDate(zappySale.docdate);

      // Nếu đã có order với docCode này, thêm sale vào
      if (ordersMap.has(docCode)) {
        const existingOrder = ordersMap.get(docCode)!;
        existingOrder.sales = existingOrder.sales || [];
        existingOrder.sales.push(saleItem);
        existingOrder.totalQty += saleItem.qty || 0;
        existingOrder.totalRevenue += saleItem.revenue || 0;
        existingOrder.totalItems += 1;
      } else {
        // Tạo order mới
        const newOrder: Order = {
          docCode,
          docDate,
          branchCode: zappySale.branch_code || '',
          docSourceType: zappySale.doctype || 'SALE_ORDER',
          customer,
          totalRevenue: saleItem.revenue || 0,
          totalQty: saleItem.qty || 0,
          totalItems: 1,
          isProcessed: false,
          sales: [saleItem],
        };
        ordersMap.set(docCode, newOrder);
      }
    });

    return Array.from(ordersMap.values());
  }

  /**
   * Map order type name từ Zappy sang code (deprecated - giữ lại để tương thích)
   * Bây giờ lưu ordertype_name trực tiếp
   */
  private mapOrderTypeNameToCode(ordertypeName: string): string | undefined {
    if (!ordertypeName) return undefined;
    // Trả về ordertype_name trực tiếp thay vì map sang code
    return ordertypeName;
  }

  /**
   * Parse date từ format Zappy "DD-MM-YYYY HH:mm" sang ISO string
   */
  private parseZappyDate(dateStr: string): string {
    if (!dateStr) return new Date().toISOString();

    try {
      // Format: "04-12-2025 11:33"
      const [datePart, timePart] = dateStr.split(' ');
      const [day, month, year] = datePart.split('-');
      
      if (timePart) {
        const [hours, minutes] = timePart.split(':');
        return new Date(
          parseInt(year),
          parseInt(month) - 1,
          parseInt(day),
          parseInt(hours),
          parseInt(minutes),
        ).toISOString();
      } else {
        return new Date(
          parseInt(year),
          parseInt(month) - 1,
          parseInt(day),
        ).toISOString();
      }
    } catch (error) {
      this.logger.warn(`Failed to parse date: ${dateStr}, using current date`);
      return new Date().toISOString();
    }
  }
}

