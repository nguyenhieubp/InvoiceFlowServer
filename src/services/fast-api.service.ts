import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class FastApiService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(FastApiService.name);
  private readonly baseUrl = 'http://103.145.79.169:6688/Fast';
  private readonly credentials = {
    UserName: 'F3',
    Password: 'F3@$^2024!#',
  };

  private tokenData: { token: string; expiresAt: number } | null = null;
  private refreshInterval: NodeJS.Timeout | null = null;
  private readonly TOKEN_REFRESH_INTERVAL = 5 * 60 * 1000; // 5 phút
  private readonly TOKEN_EXPIRY_BUFFER = 10 * 60 * 1000; // Refresh trước 10 phút khi hết hạn

  constructor(private readonly httpService: HttpService) {}

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
        const expiresAt = Date.now() + (expiresMinutes * 60 * 1000) - this.TOKEN_EXPIRY_BUFFER;

        this.tokenData = {
          token,
          expiresAt,
        };

        this.logger.log(`Token saved, expires at: ${new Date(expiresAt).toLocaleString()}`);
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

      // Log payload để debug
      this.logger.debug(`Sales order payload: ${JSON.stringify(orderData, null, 2)}`);

      // Gọi API salesOrder với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/salesOrder`,
          orderData,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Sales order submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error submitting sales order: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(`Response data: ${JSON.stringify(error.response.data)}`);
        // Log thêm thông tin chi tiết về request
        if (error.config) {
          this.logger.error(`Request URL: ${error.config.url}`);
          this.logger.error(`Request payload: ${JSON.stringify(error.config.data)}`);
        }
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying sales order...');
        const newToken = await this.login();
        if (newToken) {
          try {
            this.logger.debug(`Retry sales order payload: ${JSON.stringify(orderData, null, 2)}`);
            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/salesOrder`,
                orderData,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log('Sales order submitted successfully (after retry)');
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(`Retry sales order API failed: ${retryError?.message || retryError}`);
            if (retryError?.response) {
              this.logger.error(`Retry response status: ${retryError.response.status}`);
              this.logger.error(`Retry response data: ${JSON.stringify(retryError.response.data)}`);
            }
            throw retryError;
          }
        }
      }

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

      // Log payload gửi lên API (đã tắt để giảm log)
      // this.logger.log('==================Sales Invoice API Request Payload:');
      // this.logger.log(JSON.stringify(invoiceData, null, 2));
      // this.logger.log('==================End of Sales Invoice API Request Payload');

      // Gọi API salesInvoice với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/salesInvoice`,
          invoiceData,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Sales invoice submitted successfully');
      // this.logger.log('==================Sales Invoice API Response:');
      // this.logger.log(JSON.stringify(response.data, null, 2));
      // this.logger.log('==================End of Sales Invoice API Response');
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error submitting sales invoice: ${error?.message || error}`);

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
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log('Sales invoice submitted successfully (after retry)');
            // this.logger.log('==================Sales Invoice API Response (after retry):');
            // this.logger.log(JSON.stringify(retryResponse.data, null, 2));
            // this.logger.log('==================End of Sales Invoice API Response (after retry)');
            return retryResponse.data;
          } catch (retryError) {
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
    ma_kh: string;
    ten_kh: string;
    dia_chi?: string;
    ngay_sinh?: string;
    so_cccd?: string;
    e_mail?: string;
    gioi_tinh?: string;
    dien_thoai?: string;
  }): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Chỉ gửi các field có giá trị
      const payload: any = {
        ma_kh: customerData.ma_kh,
        ten_kh: customerData.ten_kh,
      };

      if (customerData.dia_chi) payload.dia_chi = customerData.dia_chi;
      if (customerData.ngay_sinh) payload.ngay_sinh = customerData.ngay_sinh;
      if (customerData.so_cccd) payload.so_cccd = customerData.so_cccd;
      if (customerData.e_mail) payload.e_mail = customerData.e_mail;
      if (customerData.gioi_tinh) payload.gioi_tinh = customerData.gioi_tinh;
      if (customerData.dien_thoai) payload.dien_thoai = customerData.dien_thoai;

      // Gọi API Customer với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/Customer`,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log(`Customer ${customerData.ma_kh} created/updated successfully`);
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error creating/updating customer ${customerData.ma_kh}: ${error?.message || error}`);
      
      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying customer API...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const payload: any = {
              ma_kh: customerData.ma_kh,
              ten_kh: customerData.ten_kh,
            };
            if (customerData.dia_chi) payload.dia_chi = customerData.dia_chi;
            if (customerData.ngay_sinh) payload.ngay_sinh = customerData.ngay_sinh;
            if (customerData.so_cccd) payload.so_cccd = customerData.so_cccd;
            if (customerData.e_mail) payload.e_mail = customerData.e_mail;
            if (customerData.gioi_tinh) payload.gioi_tinh = customerData.gioi_tinh;
            if (customerData.dien_thoai) payload.dien_thoai = customerData.dien_thoai;

            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/Customer`,
                payload,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log(`Customer ${customerData.ma_kh} created/updated successfully (after retry)`);
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
      if (itemData.nhieu_dvt !== undefined) payload.nhieu_dvt = itemData.nhieu_dvt;
      if (itemData.loai_hh_dv) payload.loai_hh_dv = itemData.loai_hh_dv;

      // Gọi API Item với token
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/Item`,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log(`Item ${itemData.ma_vt} created/updated successfully`);
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error creating/updating item ${itemData.ma_vt}: ${error?.message || error}`);
      
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
            if (itemData.nhieu_dvt !== undefined) payload.nhieu_dvt = itemData.nhieu_dvt;
            if (itemData.loai_hh_dv) payload.loai_hh_dv = itemData.loai_hh_dv;

            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/Item`,
                payload,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log(`Item ${itemData.ma_vt} created/updated successfully (after retry)`);
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
      const formatDate = (date: string | Date | undefined): string | undefined => {
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
        this.httpService.post(
          `${this.baseUrl}/Lot`,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log(`Lot ${lotData.ma_lo} for item ${lotData.ma_vt} created/updated successfully`);
      // this.logger.log(`==================Lot API Response: ${JSON.stringify(response.data)}`);
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error creating/updating lot ${lotData.ma_lo} for item ${lotData.ma_vt}: ${error?.message || error}`);
      
      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying lot API...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const formatDate = (date: string | Date | undefined): string | undefined => {
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

            if (lotData.ngay_nhap) payload.ngay_nhap = formatDate(lotData.ngay_nhap);
            if (lotData.ten_lo2) payload.ten_lo2 = lotData.ten_lo2;
            if (lotData.ngay_sx) payload.ngay_sx = formatDate(lotData.ngay_sx);
            if (lotData.ngay_hhsd) payload.ngay_hhsd = formatDate(lotData.ngay_hhsd);
            if (lotData.ngay_hhbh) payload.ngay_hhbh = formatDate(lotData.ngay_hhbh);
            if (lotData.ghi_chu) payload.ghi_chu = lotData.ghi_chu;
            if (lotData.ma_phu) payload.ma_phu = lotData.ma_phu;
            if (lotData.active !== undefined) payload.active = lotData.active;

            const retryResponse = await firstValueFrom(
              this.httpService.post(
                `${this.baseUrl}/Lot`,
                payload,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log(`Lot ${lotData.ma_lo} for item ${lotData.ma_vt} created/updated successfully (after retry)`);
            this.logger.log(`Lot API Response (after retry): ${JSON.stringify(retryResponse.data)}`);
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
        this.httpService.post(
          `${this.baseUrl}/Site`,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log(`Site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs} created/updated successfully`);
      // this.logger.log(`==================Site API Response: ${JSON.stringify(response.data)}`);
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error creating/updating site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs}: ${error?.message || error}`);
      
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
              this.httpService.post(
                `${this.baseUrl}/Site`,
                payload,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log(`Site ${siteData.ma_kho} for ma_dvcs ${siteData.ma_dvcs} created/updated successfully (after retry)`);
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
        this.httpService.post(
          endpoint,
          stockTransferData,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Stock transfer submitted successfully');
      // this.logger.log('==================Stock Transfer API Response:');
      // this.logger.log(JSON.stringify(response.data, null, 2));
      // this.logger.log('==================End of Stock Transfer API Response');
      return response.data;
    } catch (error: any) {
      // Log chi tiết lỗi
      this.logger.error(`Error submitting stock transfer: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(`Response data: ${JSON.stringify(error.response.data)}`);
        this.logger.error(`Response headers: ${JSON.stringify(error.response.headers)}`);
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
              this.httpService.post(
                endpoint,
                stockTransferData,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log('Stock transfer submitted successfully after token refresh');
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
   * Gọi API warehouseRelease (xuất kho) với ioType: O
   */
  async submitWarehouseRelease(warehouseData: any, ioType: string = 'O'): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Thêm ioType vào payload
      const payload = {
        ...warehouseData,
        ioType,
      };

      // Gọi API warehouseRelease với token
      const endpoint = `${this.baseUrl}/warehouseRelease`;
      this.logger.log(`Calling FastAPI endpoint: ${endpoint} with ioType: ${ioType}`);
      
      const response = await firstValueFrom(
        this.httpService.post(
          endpoint,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Warehouse release submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error submitting warehouse release: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(`Response data: ${JSON.stringify(error.response.data)}`);
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying warehouse release...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const endpoint = `${this.baseUrl}/warehouseRelease`;
            const payload = {
              ...warehouseData,
              ioType,
            };
            const retryResponse = await firstValueFrom(
              this.httpService.post(
                endpoint,
                payload,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log('Warehouse release submitted successfully after token refresh');
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(`Error submitting warehouse release after retry: ${retryError?.message || retryError}`);
            throw retryError;
          }
        }
      }

      throw error;
    }
  }

  /**
   * Gọi API warehouseReceipt (nhập kho) với ioType: I
   */
  async submitWarehouseReceipt(warehouseData: any, ioType: string = 'I'): Promise<any> {
    try {
      // Lấy token (tự động refresh nếu cần)
      const token = await this.getToken();
      if (!token) {
        throw new Error('Không thể lấy token đăng nhập');
      }

      // Thêm ioType vào payload
      const payload = {
        ...warehouseData,
        ioType,
      };

      // Gọi API warehouseReceipt với token
      const endpoint = `${this.baseUrl}/warehouseReceipt`;
      this.logger.log(`Calling FastAPI endpoint: ${endpoint} with ioType: ${ioType}`);
      
      const response = await firstValueFrom(
        this.httpService.post(
          endpoint,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`,
            },
          },
        ),
      );

      this.logger.log('Warehouse receipt submitted successfully');
      return response.data;
    } catch (error: any) {
      this.logger.error(`Error submitting warehouse receipt: ${error?.message || error}`);
      if (error?.response) {
        this.logger.error(`Response status: ${error.response.status}`);
        this.logger.error(`Response data: ${JSON.stringify(error.response.data)}`);
      }

      // Nếu lỗi 401 (Unauthorized), refresh token và retry
      if (error?.response?.status === 401) {
        this.logger.log('Token expired, refreshing and retrying warehouse receipt...');
        const newToken = await this.login();
        if (newToken) {
          try {
            const endpoint = `${this.baseUrl}/warehouseReceipt`;
            const payload = {
              ...warehouseData,
              ioType,
            };
            const retryResponse = await firstValueFrom(
              this.httpService.post(
                endpoint,
                payload,
                {
                  headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${newToken}`,
                  },
                },
              ),
            );
            this.logger.log('Warehouse receipt submitted successfully after token refresh');
            return retryResponse.data;
          } catch (retryError: any) {
            this.logger.error(`Error submitting warehouse receipt after retry: ${retryError?.message || retryError}`);
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

