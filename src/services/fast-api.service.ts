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
   * Cleanup khi module bị destroy
   */
  onModuleDestroy() {
    this.stopAutoRefresh();
  }
}

