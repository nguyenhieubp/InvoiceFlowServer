import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { Order, OrderCustomer, SaleItem } from '../types/order.types';
import axios from 'axios';
import { convertDate, convertOrderToOrderLineFormat } from 'src/utils/convert.utils';

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

  constructor(private readonly httpService: HttpService) { }

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
    const formattedDate = convertDate(date);
    try {
      if (brand === 'chando') {
        const url = 'https://ecs.vmt.vn/api/sale-orders';
        const response = await axios.post(url, {
          params: {
            token: 'chHIqq7u8bhm5rFD68be',
            date_from: formattedDate,
            date_to: formattedDate
          },
        }, {
          headers: {
            'Content-Type': 'application/json',
          },
        });

        const orderChando = response?.data?.result?.data || [];

        const orders = orderChando.flatMap((order: any) =>
          convertOrderToOrderLineFormat(order)
        );

        // orders là MẢNG PHẲNG [{...}, {...}, ...]
        return this.transformZappySalesToOrders(orders)
      } else {
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

        // Lấy tất cả các doctype (SALE_ORDER, SALE_RETURN, etc.), nhưng bỏ qua itemCode = "TRUTONKEEP"
        const filteredData = rawData.filter((item) => {
          const itemCode = item.itemcode || item.itemCode || '';
          const normalizedItemCode = String(itemCode).trim().toUpperCase();
          // Bỏ qua các item có itemcode = "TRUTONKEEP"
          if (normalizedItemCode === 'TRUTONKEEP') {
            return false;
          }
          return true;
        });

        if (filteredData.length === 0) {
          this.logger.warn(`No sales data found for date ${date} after filtering (filtered from ${rawData.length} total items)`);
          return [];
        }

        // Log số lượng đã filter
        const trutonkeepCount = rawData.length - filteredData.length;
        if (trutonkeepCount > 0) {
          this.logger.log(`Filtered out ${trutonkeepCount} items with itemCode = "TRUTONKEEP"`);
        }

        // Transform dữ liệu từ Zappy format sang Order format
        return this.transformZappySalesToOrders(filteredData);
      }
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
   * Lấy dữ liệu báo cáo nộp quỹ cuối ca từ ERP API
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Array of shift end cash records
   */
  async getShiftEndCash(date: string, brand?: string): Promise<any[]> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      const url = `${baseUrl}/get_shift_end_cash?P_DATE=${date}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      const rawData = response?.data?.data || [];
      if (!Array.isArray(rawData)) {
        this.logger.warn(`No shift end cash data found for date ${date}`);
        return [];
      }

      return rawData;
    } catch (error: any) {
      this.logger.error(`Error fetching shift end cash from API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lấy dữ liệu danh sách CTKM (Promotion) từ API
   * @param dateFrom - Ngày bắt đầu theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param dateTo - Ngày kết thúc theo format DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Array of promotion records
   */
  async getPromotion(dateFrom: string, dateTo: string, brand?: string): Promise<any[]> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      const url = `${baseUrl}/get_promotion?P_FDATE=${dateFrom}&P_TDATE=${dateTo}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      const rawData = response?.data?.data || [];
      if (!Array.isArray(rawData)) {
        this.logger.warn(`No promotion data found for date range ${dateFrom} - ${dateTo}`);
        return [];
      }

      return rawData;
    } catch (error: any) {
      this.logger.error(`Error fetching promotion from API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lấy chi tiết lines của một promotion
   * @param promotionId - ID của promotion (ví dụ: 609741)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Object chứa i_lines và v_lines
   */
  async getPromotionLine(promotionId: number, brand?: string): Promise<any> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      const url = `${baseUrl}/get_1promotion_line?P_ID=${promotionId}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      // Menard có cấu trúc: response.data.items[0].data[0]
      // Các brand khác có cấu trúc: response.data.data[0]
      let rawData: any = null;

      if (response?.data?.items && Array.isArray(response.data.items) && response.data.items.length > 0) {
        // Cấu trúc menard: items[0].data[0]
        const firstItem = response.data.items[0];
        if (firstItem?.data && Array.isArray(firstItem.data) && firstItem.data.length > 0) {
          rawData = firstItem.data[0];
        }
      } else if (response?.data?.data && Array.isArray(response.data.data) && response.data.data.length > 0) {
        // Cấu trúc các brand khác: data[0]
        rawData = response.data.data[0];
      }

      if (!rawData) {
        this.logger.warn(`No promotion line data found for promotionId ${promotionId} (brand: ${brand})`);
        return {};
      }

      return rawData;
    } catch (error: any) {
      this.logger.error(`Error fetching promotion line from API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lấy dữ liệu danh sách Voucher Issue từ API
   * @param dateFrom - Ngày bắt đầu theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param dateTo - Ngày kết thúc theo format DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Array of voucher issue records
   */
  async getVoucherIssue(dateFrom: string, dateTo: string, brand?: string): Promise<any[]> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      const url = `${baseUrl}/get_voucher_issue?P_FDATE=${dateFrom}&P_TDATE=${dateTo}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      const rawData = response?.data?.data || [];
      if (!Array.isArray(rawData)) {
        this.logger.warn(`No voucher issue data found for date range ${dateFrom} - ${dateTo}`);
        return [];
      }

      return rawData;
    } catch (error: any) {
      this.logger.error(`Error fetching voucher issue from API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lấy chi tiết của một voucher issue
   * @param voucherIssueId - ID của voucher issue (ví dụ: 10206)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Object chứa chi tiết voucher issue
   */
  async getVoucherIssueDetail(voucherIssueId: number, brand?: string): Promise<any> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      const url = `${baseUrl}/get_1voucher_issue?P_ID=${voucherIssueId}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      const rawData = response?.data?.data?.[0] || {};
      return rawData;
    } catch (error: any) {
      this.logger.error(`Error fetching voucher issue detail from API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lấy dữ liệu tách gộp BOM (Repack Formula) từ ERP API
   * @param dateFrom - Ngày bắt đầu theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param dateTo - Ngày kết thúc theo format DDMMMYYYY (ví dụ: 30NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @returns Array of repack formula records
   */
  async getRepackFormula(dateFrom: string, dateTo: string, brand?: string): Promise<any[]> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      const url = `${baseUrl}/get_repack_formula?P_FDATE=${dateFrom}&P_TDATE=${dateTo}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      const rawData = response?.data?.data || [];
      if (!Array.isArray(rawData)) {
        this.logger.warn(`No repack formula data found for date range ${dateFrom} - ${dateTo}`);
        return [];
      }

      return rawData;
    } catch (error: any) {
      this.logger.error(`Error fetching repack formula from API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lấy dữ liệu xuất kho từ Zappy API
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 01NOV2025)
   * @param brand - Brand name (f3, labhair, yaman, menard, ...). Nếu không có thì dùng default
   * @param part - Phần dữ liệu cần lấy (1, 2, 3). Nếu không có thì lấy tất cả
   * @returns Array of stock transfer records
   */
  async getDailyStockTrans(date: string, brand?: string, part?: number): Promise<any[]> {
    try {
      const baseUrl = this.getBaseUrlForBrand(brand);
      let url = `${baseUrl}/get_daily_stock_trans?P_DATE=${date}`;

      // Thêm P_PART nếu có
      if (part !== undefined && part !== null) {
        url += `&P_PART=${part}`;
      }

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: { accept: 'application/json' },
        }),
      );

      const rawData = response?.data?.data || [];
      if (!Array.isArray(rawData)) {
        this.logger.warn(`No stock transfer data found for date ${date}${part ? ` part ${part}` : ''}`);
        return [];
      }

      return rawData;
    } catch (error: any) {
      this.logger.error(`Error fetching daily stock transfer from Zappy API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Transform dữ liệu từ Zappy format sang Order format
   */
  private transformZappySalesToOrders(zappySales: any[]): Order[] {
    return zappySales.map((zappySale) => {
      const docCode = zappySale.code || '';

      // Map customer info
      const customer: OrderCustomer = {
        code: zappySale.partner_code || '',
        name: zappySale.partner_name || '',
        brand: '',
        mobile: zappySale.partner_mobile || undefined,
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

      // Map sale item (1 line)
      const saleItem: SaleItem = {
        id: zappySale.id?.toString(),
        promCode: zappySale.prom_code || undefined,
        itemCode: zappySale.itemcode || undefined,
        itemName: zappySale.itemname || undefined,
        description: zappySale.description || undefined,
        partnerCode: zappySale.partner_code || undefined,
        ordertype: zappySale.ordertype || '01.Thường' || undefined,
        ordertype_name: zappySale.ordertype_name || '01.Thường' || undefined,
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
        chietKhauMuaHangGiamGia: zappySale.other_discamt ?? 0,
        paid_by_voucher_ecode_ecoin_bp: zappySale.v_paid ?? 0,
        shift_code: zappySale.shift_code || undefined,
        saleperson_id: zappySale.saleperson_code
          ? parseInt(zappySale.saleperson_code)
          : undefined,
        order_source: zappySale.so_source || undefined,
        partner_name: zappySale.partner_name || undefined,
        producttype:
          zappySale.producttype !== undefined && zappySale.producttype !== null
            ? zappySale.producttype
            : undefined,
        pkg_code: zappySale.pkg_code || undefined,
        social_page_id: zappySale.social_page_id || undefined,
        sp_email: zappySale.sp_email || undefined,
        mvc_serial: zappySale.mvc_serial || undefined,
        vc_promotion_code: zappySale.vc_promotion_code || undefined,
        cat1: zappySale.cat1 || undefined,
        cat2: zappySale.cat2 || undefined,
        cat3: zappySale.cat3 || undefined,
        catcode1: zappySale.catcode1 || undefined,
        catcode2: zappySale.catcode2 || undefined,
        catcode3: zappySale.catcode3 || undefined,
      };

      const docDate = this.parseZappyDate(zappySale.docdate);

      // 👉 MỖI LINE = 1 ORDER
      const order: Order = {
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

      return order;
    });
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

