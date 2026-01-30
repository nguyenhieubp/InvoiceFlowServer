import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { WarehouseProcessed } from '../entities/warehouse-processed.entity';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';

@Injectable()
export class FastApiClientService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(FastApiClientService.name);
  private readonly baseUrl = 'http://103.145.79.169:6688/Fast';
  private readonly credentials = {
    UserName: 'F3',
    Password: 'F3@$^2024!#',
  };

  private tokenData: { token: string; expiresAt: number } | null = null;
  private refreshInterval: NodeJS.Timeout | null = null;
  private readonly TOKEN_REFRESH_INTERVAL = 5 * 60 * 1000; // 5 phút
  private readonly TOKEN_EXPIRY_BUFFER = 10 * 60 * 1000; // Refresh trước 10 phút khi hết hạn

  constructor(
    private readonly httpService: HttpService,
    @InjectRepository(WarehouseProcessed)
    private readonly warehouseProcessedRepository: Repository<WarehouseProcessed>,
  ) {}

  /**
   * Đăng nhập và lấy token
   */
  async login(): Promise<string | null> {
    try {
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/Login`, this.credentials),
      );

      const token = response.data?.token || null;
      const expiresMinutes = response.data?.expires_minute || 180; // Mặc định 180 phút

      if (token) {
        // Tính thời gian hết hạn (trừ đi buffer để refresh sớm)
        const expiresAt =
          Date.now() + expiresMinutes * 60 * 1000 - this.TOKEN_EXPIRY_BUFFER;

        this.tokenData = {
          token,
          expiresAt,
        };

        this.logger.log(
          `Token saved, expires at: ${new Date(expiresAt).toLocaleString()}`,
        );
      }

      return token;
    } catch (error: any) {
      this.logger.error(`Error logging in: ${error?.message || error}`);
      return null;
    }
  }

  /**
   * Lấy token hiện tại, tự động refresh nếu cần
   */
  async getToken(): Promise<string | null> {
    const now = Date.now();

    // Kiểm tra token còn hợp lệ không
    if (this.tokenData && this.tokenData.expiresAt > now) {
      return this.tokenData.token;
    }

    // Token hết hạn hoặc chưa có, đăng nhập lại
    this.logger.log('Token expired or not found, refreshing...');
    return await this.login();
  }

  /**
   * Refresh token nếu cần
   */
  async refreshTokenIfNeeded(): Promise<void> {
    const now = Date.now();

    if (!this.tokenData || this.tokenData.expiresAt <= now) {
      this.logger.log('Refreshing token automatically...');
      await this.login();
    }
  }

  /**
   * Bắt đầu tự động refresh token
   */
  startAutoRefresh(): void {
    // Clear interval cũ nếu có
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }

    // Refresh token ngay lập tức nếu chưa có
    this.refreshTokenIfNeeded();

    // Refresh token định kỳ
    this.refreshInterval = setInterval(() => {
      this.refreshTokenIfNeeded();
    }, this.TOKEN_REFRESH_INTERVAL);

    this.logger.log('Auto-refresh started');
  }

  /**
   * Dừng tự động refresh token
   */
  stopAutoRefresh(): void {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
      this.logger.log('Auto-refresh stopped');
    }
  }

  /**
   * Gọi API salesOrder (đơn hàng bán)
   */
  async submitSalesOrder(orderData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API salesOrder với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/salesOrder`, orderData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('Sales order submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting sales order: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(
          `Response data: ${JSON.stringify(error.response.data)}`,
        );
        // Log thêm thông tin chi tiết về request
        if (error.config) {
          this.logger.error(`Request URL: ${error.config.url}`);
          this.logger.error(
            `Request payload: ${JSON.stringify(error.config.data)}`,
          );
        }
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log(
          'Token expired, refreshing and retrying sales order...',
        );
        const newToken = await this.login();
        if (newToken) {
          try {
            this.logger.debug(
              `Retry sales order payload: ${JSON.stringify(orderData, null, 2)}`,
            );
            const retryResponse = await firstValueFrom(
              this.httpService.post(`${this.baseUrl}/salesOrder`, orderData, {
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${newToken}`,
                },
              }),
            );
            this.logger.log('Sales order submitted successfully (after retry)');
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(
              `Retry sales order API failed: ${retryError?.message || retryError}`,
            );
            if (retryError?.response) {
              this.logger.error(
                `Retry response status: ${retryError.response.status}`,
              );
              this.logger.error(
                `Retry response data: ${JSON.stringify(retryError.response.data)}`,
              );
            }
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  //* Call Promotion
  async callPromotion(promotionData: {
    ma_ctkm: string;
    ten_ctkm: string;
    ma_phi: string;
    tk_cpkm: string;
    tk_ck: string;
  }): Promise<any> {
    try {
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      const result = Object.fromEntries(
        Object.entries(promotionData).filter(
          ([_, value]) => value !== undefined && value !== null,
        ),
      );

      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/Promotions`, result, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error calling promotion: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Gọi API salesInvoice
   */
  async submitSalesInvoice(invoiceData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API salesInvoice với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/salesInvoice`, invoiceData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('Sales invoice submitted successfully');
      return response.data;
    } catch (error: any) {
      // Log chi tiết error response (chỉ log message, không log toàn bộ payload)
      if (error?.response?.data) {
        const errorData = error.response.data;
        if (Array.isArray(errorData) && errorData.length > 0) {
          this.logger.error(
            `Error submitting sales invoice: ${errorData[0]?.message || error.message || error}`,
          );
        } else if (typeof errorData === 'object' && errorData.message) {
          this.logger.error(
            `Error submitting sales invoice: ${errorData.message}`,
          );
        } else if (typeof errorData === 'string') {
          this.logger.error(`Error submitting sales invoice: ${errorData}`);
        } else {
          this.logger.error(
            `Error submitting sales invoice: ${error?.message || error}`,
          );
        }
      } else {
        this.logger.error(
          `Error submitting sales invoice: ${error?.message || error}`,
        );
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying...');
        const newToken = await this.login();
        if (newToken) {
          // Retry với token mới
          try {
            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/salesInvoice`,
                invoiceData,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log(
              'Sales invoice submitted successfully (after retry)',
            );
            return retryResponse.data;
          } catch (retryError: any) {
            if (retryError?.response?.data) {
              const errorData = retryError.response.data;
              if (Array.isArray(errorData) && errorData.length > 0) {
                this.logger.error(
                  `Error submitting sales invoice (retry): ${errorData[0]?.message || retryError.message || retryError}`,
                );
              } else if (typeof errorData === 'object' && errorData.message) {
                this.logger.error(
                  `Error submitting sales invoice (retry): ${errorData.message}`,
                );
              } else {
                this.logger.error(
                  `Error submitting sales invoice (retry): ${retryError?.message || retryError}`,
                );
              }
            } else {
              this.logger.error(
                `Error submitting sales invoice (retry): ${retryError?.message || retryError}`,
              );
            }
            this.logger.error(`Retry failed: ${retryError}`);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Tạo hoặc cập nhật khách hàng trong Fast API
   * @param customerData - Thông tin khách hàng (chỉ cần ma_kh và ten_kh là required)
   */
  async createOrUpdateCustomer(customerData: {
    code: string;
    name: string;
    address?: string;
    birthDate?: string;
    cccd?: string;
    email?: string;
    gioi_tinh?: string;
    tel?: string;
  }): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Chỉ gửi các field có giá trị
      const payload: any = {
        ma_kh: customerData.code,
        ten_kh: customerData.name,
        dia_chi: customerData.address,
        ngay_sinh: customerData.birthDate,
        so_cccd: customerData.cccd,
        e_mail: customerData.email,
        gioi_tinh: customerData.gioi_tinh,
      };

      // Gọi API Customer với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/Customer`, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log(
        `Customer ${customerData.code} created/updated successfully`,
      );
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error creating/updating customer ${customerData.code}: ${error?.message || error}`,
      );

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log(
          'Token expired, refreshing and retrying customer API...',
        );
        const newToken = await this.login();
        if (newToken) {
          try {
            const payload: any = {
              ma_kh: customerData.code,
              ten_kh: customerData.name,
            };
            if (customerData.address) payload.dia_chi = customerData.address;
            if (customerData.birthDate)
              payload.ngay_sinh = customerData.birthDate;
            if (customerData.cccd) payload.so_cccd = customerData.cccd;
            if (customerData.email) payload.e_mail = customerData.email;
            if (customerData.gioi_tinh)
              payload.gioi_tinh = customerData.gioi_tinh;
            if (customerData.tel) payload.dien_thoai = customerData.tel;

            const retryResponse = await firstValueFrom(
              this.httpService.post(`${this.baseUrl}/Customer`, payload, {
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${newToken}`,
                },
              }),
            );
            this.logger.log(
              `Customer ${customerData.code} created/updated successfully (after retry)`,
            );
            return retryResponse.data;
          } catch (retryError) {
            this.logger.error(`Retry customer API failed: ${retryError}`);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Tạo hoặc cập nhật vật tư trong Fast API
   * 2.2/ Danh mục vật tư
   * @param itemData - Thông tin vật tư (ma_vt và ten_vt là required)
   */
  async createOrUpdateItem(itemData: {
    ma_vt: string;
    ten_vt: string;
    ten_vt2?: string;
    dvt?: string;
    lo_yn?: number;
    nhieu_dvt?: number;
    loai_hh_dv?: string;
  }): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Chỉ gửi các field có giá trị
      const payload: any = {
        ma_vt: itemData.ma_vt,
        ten_vt: itemData.ten_vt,
      };

      if (itemData.ten_vt2) payload.ten_vt2 = itemData.ten_vt2;
      if (itemData.dvt) payload.dvt = itemData.dvt;
      if (itemData.lo_yn !== undefined) payload.lo_yn = itemData.lo_yn;
      if (itemData.nhieu_dvt !== undefined)
        payload.nhieu_dvt = itemData.nhieu_dvt;
      if (itemData.loai_hh_dv) payload.loai_hh_dv = itemData.loai_hh_dv;

      // Gọi API Item với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/Item`, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log(`Item ${itemData.ma_vt} created/updated successfully`);
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error creating/updating item ${itemData.ma_vt}: ${error?.message || error}`,
      );

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying item API...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const payload: any = {
              ma_vt: itemData.ma_vt,
              ten_vt: itemData.ten_vt,
            };
            if (itemData.ten_vt2) payload.ten_vt2 = itemData.ten_vt2;
            if (itemData.dvt) payload.dvt = itemData.dvt;
            if (itemData.lo_yn !== undefined) payload.lo_yn = itemData.lo_yn;
            if (itemData.nhieu_dvt !== undefined)
              payload.nhieu_dvt = itemData.nhieu_dvt;
            if (itemData.loai_hh_dv) payload.loai_hh_dv = itemData.loai_hh_dv;

            const retryResponse = await firstValueFrom(
              this.httpService.post(`${this.baseUrl}/Item`, payload, {
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${newToken}`,
                },
              }),
            );
            this.logger.log(
              `Item ${itemData.ma_vt} created/updated successfully (after retry)`,
            );
            return retryResponse.data;
          } catch (retryError) {
            this.logger.error(`Retry item API failed: ${retryError}`);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Tạo hoặc cập nhật lô trong Fast API
   * 2.14/ Danh mục lô
   * @param lotData - Thông tin lô (ma_vt, ma_lo, ten_lo là required)
   */
  async createOrUpdateLot(lotData: {
    ma_vt: string;
    ma_lo: string;
    ten_lo: string;
    ngay_nhap?: string | Date;
    ten_lo2?: string;
    ngay_sx?: string | Date;
    ngay_hhsd?: string | Date;
    ngay_hhbh?: string | Date;
    ghi_chu?: string;
    ma_phu?: string;
    active?: string;
    action?: string;
  }): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Format dates to ISO string
      const formatDate = (
        date: string | Date | undefined,
      ): string | undefined => {
        if (!date) return undefined;
        const d = typeof date === 'string' ? new Date(date) : date;
        if (isNaN(d.getTime())) return undefined;
        return d.toISOString();
      };

      // Chỉ gửi các field có giá trị
      const payload: any = {
        ma_vt: lotData.ma_vt,
        ma_lo: lotData.ma_lo,
        ten_lo: lotData.ten_lo,
        action: lotData.action || '0',
      };

      if (lotData.ngay_nhap) payload.ngay_nhap = formatDate(lotData.ngay_nhap);
      if (lotData.ten_lo2) payload.ten_lo2 = lotData.ten_lo2;
      if (lotData.ngay_sx) payload.ngay_sx = formatDate(lotData.ngay_sx);
      if (lotData.ngay_hhsd) payload.ngay_hhsd = formatDate(lotData.ngay_hhsd);
      if (lotData.ngay_hhbh) payload.ngay_hhbh = formatDate(lotData.ngay_hhbh);
      if (lotData.ghi_chu) payload.ghi_chu = lotData.ghi_chu;
      if (lotData.ma_phu) payload.ma_phu = lotData.ma_phu;
      if (lotData.active !== undefined) payload.active = lotData.active;

      // Gọi API Lot với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/Lot`, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log(
        `Lot ${lotData.ma_lo} for item ${lotData.ma_vt} created/updated successfully`,
      );
      // this.logger.log(`==================Lot API Response: ${JSON.stringify(response.data)}`);
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error creating/updating lot ${lotData.ma_lo} for item ${lotData.ma_vt}: ${error?.message || error}`,
      );

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying lot API...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const formatDate = (
              date: string | Date | undefined,
            ): string | undefined => {
              if (!date) return undefined;
              const d = typeof date === 'string' ? new Date(date) : date;
              if (isNaN(d.getTime())) return undefined;
              return d.toISOString();
            };

            const payload: any = {
              ma_vt: lotData.ma_vt,
              ma_lo: lotData.ma_lo,
              ten_lo: lotData.ten_lo,
              action: lotData.action || '0',
            };

            if (lotData.ngay_nhap)
              payload.ngay_nhap = formatDate(lotData.ngay_nhap);
            if (lotData.ten_lo2) payload.ten_lo2 = lotData.ten_lo2;
            if (lotData.ngay_sx) payload.ngay_sx = formatDate(lotData.ngay_sx);
            if (lotData.ngay_hhsd)
              payload.ngay_hhsd = formatDate(lotData.ngay_hhsd);
            if (lotData.ngay_hhbh)
              payload.ngay_hhbh = formatDate(lotData.ngay_hhbh);
            if (lotData.ghi_chu) payload.ghi_chu = lotData.ghi_chu;
            if (lotData.ma_phu) payload.ma_phu = lotData.ma_phu;
            if (lotData.active !== undefined) payload.active = lotData.active;

            const retryResponse = await firstValueFrom(
              this.httpService.post(`${this.baseUrl}/Lot`, payload, {
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${newToken}`,
                },
              }),
            );
            this.logger.log(
              `Lot ${lotData.ma_lo} for item ${lotData.ma_vt} created/updated successfully (after retry)`,
            );
            this.logger.log(
              `Lot API Response (after retry): ${JSON.stringify(retryResponse.data)}`,
            );
            return retryResponse.data;
          } catch (retryError) {
            this.logger.error(`Retry lot API failed: ${retryError}`);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Tạo hoặc cập nhật kho (Site) trong Fast API
   * 2.12/ Danh mục kho
   * @param siteData - Thông tin kho (ma_dvcs, ma_kho, ten_kho là required)
   */
  async createOrUpdateSite(siteData: {
    ma_dvcs: string;
    ma_kho: string;
    ten_kho: string;
    ma_bp?: string;
  }): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Chỉ gửi các field có giá trị
      const payload: any = {
        ma_dvcs: siteData.ma_dvcs,
        ma_kho: siteData.ma_kho,
        ten_kho: siteData.ten_kho,
      };

      if (siteData.ma_bp) payload.ma_bp = siteData.ma_bp;

      // Gọi API Site với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/Site`, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log(
        `Site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs} created/updated successfully`,
      );
      // this.logger.log(`==================Site API Response: ${JSON.stringify(response.data)}`);
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error creating/updating site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs}: ${error?.message || error}`,
      );

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying site API...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const payload: any = {
              ma_dvcs: siteData.ma_dvcs,
              ma_kho: siteData.ma_kho,
              ten_kho: siteData.ten_kho,
            };
            if (siteData.ma_bp) payload.ma_bp = siteData.ma_bp;

            const retryResponse = await firstValueFrom(
              this.httpService.post(`${this.baseUrl}/Site`, payload, {
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${newToken}`,
                },
              }),
            );
            this.logger.log(
              `Site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs} created/updated successfully (after retry)`,
            );
            // this.logger.log(`==================Site API Response (after retry): ${JSON.stringify(retryResponse.data)}`);
            return retryResponse.data;
          } catch (retryError) {
            this.logger.error(`Retry site API failed: ${retryError}`);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Kiểm tra token còn hợp lệ không
   */
  isTokenValid(): boolean {
    if (!this.tokenData) return false;
    return this.tokenData.expiresAt > Date.now();
  }

  /**
   * Tự động khởi động khi module được load
   */
  onModuleInit() {
    this.startAutoRefresh();
  }

  /**
   * Gọi API stockTransfer (phiếu xuất/nhập kho)
   */
  async submitStockTransfer(stockTransferData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Log payload gửi lên API (đã tắt để giảm log)
      // this.logger.log('==================Stock Transfer API Request Payload:');
      // this.logger.log(JSON.stringify(stockTransferData, null, 2));
      // this.logger.log('==================End of Stock Transfer API Request Payload');

      // Gọi API warehouseRelease với token
      const endpoint = `${this.baseUrl}/warehouseRelease`;
      this.logger.log(`Calling FastAPI endpoint: ${endpoint}`);

      const response = await firstValueFrom(
        this.httpService.post(endpoint, stockTransferData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('Stock transfer submitted successfully');
      // this.logger.log('==================Stock Transfer API Response:');
      // this.logger.log(JSON.stringify(response.data, null, 2));
      // this.logger.log('==================End of Stock Transfer API Response');
      return response.data;
    } catch (error: any) {
      // Log chi tiết lỗi
      this.logger.error(
        `Error submitting stock transfer: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(
          `Response data: ${JSON.stringify(error.response.data)}`,
        );
        this.logger.error(
          `Response headers: ${JSON.stringify(error.response.headers)}`,
        );
      }
      if (error?.config) {
        this.logger.error(`Request URL: ${error.config.url}`);
        this.logger.error(`Request method: ${error.config.method}`);
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying...');
        const newToken = await this.login();
        if (newToken) {
          // Retry với token mới
          try {
            const endpoint = `${this.baseUrl}/warehouseRelease`;
            const retryResponse = await firstValueFrom(
              this.httpService.post(endpoint, stockTransferData, {
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${newToken}`,
                },
              }),
            );
            this.logger.log(
              'Stock transfer submitted successfully after token refresh',
            );
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(
              `Error submitting stock transfer after retry: ${retryError?.message || retryError}`,
            );
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Gọi API cashReceipt (Phiếu thu tiền mặt)
   * @param cashReceiptData - Dữ liệu phiếu thu tiền mặt
   */
  async submitCashReceipt(cashReceiptData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API cashReceipt với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/cashReceipt`, cashReceiptData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('Cash receipt submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting cash receipt: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `Cash receipt error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Cash receipt error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Gọi API creditAdvice (Giấy báo có)
   * @param creditAdviceData - Dữ liệu giấy báo có
   */
  async submitCreditAdvice(creditAdviceData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API creditAdvice với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/creditAdvice`,
          creditAdviceData,
          {
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Credit advice submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting credit advice: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `Credit advice error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Credit advice error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Gọi API payment (Phiếu chi tiền mặt)
   * @param paymentData - Dữ liệu phiếu chi tiền mặt
   */
  async submitPayment(paymentData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API payment với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/payment`, paymentData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('Payment submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error submitting payment: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(
          `Payment error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Payment error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Gọi API debitAdvice (Giấy báo nợ)
   * @param debitAdviceData - Dữ liệu giấy báo nợ
   */
  async submitDebitAdvice(debitAdviceData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API debitAdvice với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/debitAdvice`, debitAdviceData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('Debit advice submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting debit advice: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `Debit advice error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Debit advice error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Gọi API warehouseReceipt (Phiếu nhập kho)
   * @param warehouseReceiptData - Dữ liệu phiếu nhập kho
   */
  async submitWarehouseReceipt(warehouseReceiptData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API warehouseReceipt với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/warehouseReceipt`,
          warehouseReceiptData,
          {
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Warehouse receipt submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting warehouse receipt: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `Warehouse receipt error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Warehouse receipt error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Gọi API warehouseRelease (Phiếu xuất kho)
   * @param warehouseReleaseData - Dữ liệu phiếu xuất kho
   */
  async submitWarehouseRelease(warehouseReleaseData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API warehouseRelease với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/warehouseRelease`,
          warehouseReleaseData,
          {
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Warehouse release submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting warehouse release: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `Warehouse release error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Warehouse release error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Gọi API warehouseTransfer (Phiếu điều chuyển kho)
   * @param warehouseTransferData - Dữ liệu phiếu điều chuyển kho
   */
  async submitWarehouseTransfer(warehouseTransferData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API warehouseTransfer với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/warehouseTransfer`,
          warehouseTransferData,
          {
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Warehouse transfer submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting warehouse transfer: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(
          `Warehouse transfer error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Warehouse transfer error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  /**
   * Gọi API gxtInvoice (Phiếu tạo gộp – xuất tách)
   * @param gxtInvoiceData - Dữ liệu phiếu tạo gộp/xuất tách
   */
  async submitSalesReturn(salesReturnData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API salesReturn với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/salesReturn`, salesReturnData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('Sales return submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting sales return: ${error?.message || error}`,
      );

      // Log chi tiết error response để debug
      if (error?.response) {
        this.logger.error(
          `Sales return error response status: ${error.response.status}`,
        );
        this.logger.error(
          `Sales return error response data: ${JSON.stringify(error.response.data)}`,
        );
      }
      if (error?.config) {
        this.logger.error(`Sales return request URL: ${error.config.url}`);
        this.logger.error(
          `Sales return request payload: ${JSON.stringify(error.config.data)}`,
        );
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying...');
        const newToken = await this.login();
        if (newToken) {
          // Retry với token mới
          try {
            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/salesReturn`,
                salesReturnData,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log(
              'Sales return submitted successfully after token refresh',
            );
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(
              `Error submitting sales return after retry: ${retryError?.message || retryError}`,
            );
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  async submitGxtInvoice(gxtInvoiceData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API gxtInvoice với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/gxtInvoice`, gxtInvoiceData, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log('GxtInvoice submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting gxtInvoice: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(
          `Response data: ${JSON.stringify(error.response.data)}`,
        );
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying gxtInvoice...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/gxtInvoice`,
                gxtInvoiceData,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log('GxtInvoice submitted successfully (after retry)');
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(
              `Retry gxtInvoice API failed: ${retryError?.message || retryError}`,
            );
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Gọi API Hình thức thanh toán
   */
  async submitPaymentMethod(paymentMethodData: any): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Gọi API paymentMethod với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/paymentMethod`,
          paymentMethodData,
          {
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Payment method submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error submitting payment method: ${error?.message || error}`,
      );
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(
          `Response data: ${JSON.stringify(error.response.data)}`,
        );
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log(
          'Token expired, refreshing and retrying payment method...',
        );
        const newToken = await this.login();
        if (newToken) {
          try {
            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/paymentMethod`,
                paymentMethodData,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log(
              'Payment method submitted successfully (after retry)',
            );
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(
              `Retry payment method API failed: ${retryError?.message || retryError}`,
            );
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Tạo hoặc cập nhật mã serial trong Fast API
   * @param serialData - Thông tin serial (ma_vt, ma_serial, ten_serial là required)
   */
  async createOrUpdateSerial(serialData: {
    ma_vt: string;
    ma_serial: string;
    ten_serial: string;
    ghi_chu?: string;
    active?: string;
    action?: string;
  }): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Chỉ gửi các field có giá trị
      const payload: any = {
        ma_vt: serialData.ma_vt,
        ma_serial: serialData.ma_serial,
        ten_serial: serialData.ten_serial,
        action: serialData.action || '0',
      };

      if (serialData.ghi_chu) payload.ghi_chu = serialData.ghi_chu;
      if (serialData.active !== undefined) payload.active = serialData.active;

      // Gọi API Serial với token
      const response = await firstValueFrom(
        this.httpService.post(`${this.baseUrl}/Serial`, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`,
          },
        }),
      );

      this.logger.log(
        `Serial ${serialData.ma_serial} for item ${serialData.ma_vt} created/updated successfully`,
      );
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error creating/updating serial ${serialData.ma_serial} for item ${serialData.ma_vt}: ${error?.message || error}`,
      );

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying serial API...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const payload: any = {
              ma_vt: serialData.ma_vt,
              ma_serial: serialData.ma_serial,
              ten_serial: serialData.ten_serial,
              action: serialData.action || '0',
            };
            if (serialData.ghi_chu) payload.ghi_chu = serialData.ghi_chu;
            if (serialData.active !== undefined)
              payload.active = serialData.active;

            const retryResponse = await firstValueFrom(
              this.httpService.post(`${this.baseUrl}/Serial`, payload, {
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${newToken}`,
                },
              }),
            );
            this.logger.log(
              `Serial ${serialData.ma_serial} for item ${serialData.ma_vt} created/updated successfully (after retry)`,
            );
            return retryResponse.data;
          } catch (retryError) {
            this.logger.error(`Retry serial API failed: ${retryError}`);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Cleanup khi module bị destroy
   */
  onModuleDestroy() {
    this.stopAutoRefresh();
  }
}
