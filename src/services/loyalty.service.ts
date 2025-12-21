import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';

@Injectable()
export class LoyaltyService {
  private readonly logger = new Logger(LoyaltyService.name);
  private readonly LOYALTY_API_BASE_URL = 'https://loyaltyapi.vmt.vn';
  private readonly REQUEST_TIMEOUT = 5000; // 5 seconds

  constructor(private readonly httpService: HttpService) {}

  /**
   * Kiểm tra và fetch product từ Loyalty API
   * Thử endpoint /material-catalogs/code/ trước, nếu không có thì thử /material-catalogs/old-code/
   * @param itemCode - Mã sản phẩm cần kiểm tra
   * @returns Product object nếu tìm thấy, null nếu không tìm thấy
   */
  async checkProduct(itemCode: string): Promise<any> {
    if (!itemCode) {
      return null;
    }

    const trimmedItemCode = itemCode.trim();
    if (!trimmedItemCode) {
      return null;
    }

    // Thử endpoint /material-catalogs/code/ trước
    try {
      const response = await this.httpService.axiosRef.get(
        `${this.LOYALTY_API_BASE_URL}/material-catalogs/code/${encodeURIComponent(trimmedItemCode)}`,
        {
          headers: { accept: 'application/json' },
          timeout: this.REQUEST_TIMEOUT,
        },
      );

      // Parse response: endpoint /material-catalogs/code/ trả về data.item
      const product = response?.data?.data?.item || response?.data?.data || response?.data;
      
      if (product && (product.id || product.code)) {
        this.logger.debug(`[LoyaltyService] Tìm thấy sản phẩm ${trimmedItemCode} tại /material-catalogs/code/`);
        return product;
      }

      // Response 200 nhưng không có data hợp lệ → thử fallback
      this.logger.warn(`[LoyaltyService] Sản phẩm ${trimmedItemCode} không có data hợp lệ tại /material-catalogs/code/, thử /material-catalogs/old-code/...`);
    } catch (error: any) {
      // Nếu 404, thử fallback
      if (error?.response?.status === 404) {
        this.logger.debug(`[LoyaltyService] Sản phẩm không tìm thấy tại /material-catalogs/code/: ${trimmedItemCode} (404), thử /material-catalogs/old-code/...`);
      } else {
        // Lỗi khác 404 - log warning nhưng vẫn thử fallback
        this.logger.warn(`[LoyaltyService] Lỗi khi fetch product ${trimmedItemCode} từ /material-catalogs/code/: ${error?.message || error?.response?.status || 'Unknown error'}`);
      }
    }

    // Nếu /material-catalogs/code/ không tìm thấy, thử fallback /material-catalogs/old-code/
    try {
      const fallbackResponse = await this.httpService.axiosRef.get(
        `${this.LOYALTY_API_BASE_URL}/material-catalogs/old-code/${encodeURIComponent(trimmedItemCode)}`,
        {
          headers: { accept: 'application/json' },
          timeout: this.REQUEST_TIMEOUT,
        },
      );

      // Parse response: endpoint /material-catalogs/old-code/ trả về trực tiếp object
      const product = fallbackResponse?.data;
      if (product && (product.id || product.code)) {
        this.logger.debug(`[LoyaltyService] Tìm thấy sản phẩm ${trimmedItemCode} tại /material-catalogs/old-code/`);
        return product;
      }
    } catch (fallbackError: any) {
      // Cả 2 endpoint đều không tìm thấy
      if (fallbackError?.response?.status === 404) {
        this.logger.debug(`[LoyaltyService] Sản phẩm không tìm thấy tại /material-catalogs/old-code/: ${trimmedItemCode} (404)`);
      } else {
        this.logger.warn(`[LoyaltyService] Lỗi khi fetch product ${trimmedItemCode} từ /material-catalogs/old-code/: ${fallbackError?.message || fallbackError?.response?.status || 'Unknown error'}`);
      }
    }

    return null;
  }

  /**
   * Alias cho checkProduct - fetch một product từ Loyalty API
   * @param itemCode - Mã sản phẩm cần fetch
   * @returns Product object nếu tìm thấy, null nếu không tìm thấy
   */
  async fetchProduct(itemCode: string): Promise<any> {
    return this.checkProduct(itemCode);
  }

  /**
   * Fetch nhiều products từ Loyalty API song song
   * @param itemCodes - Mảng các mã sản phẩm cần fetch
   * @returns Map<string, any> với key là itemCode và value là product object (nếu tìm thấy)
   */
  async fetchProducts(itemCodes: string[]): Promise<Map<string, any>> {
    const productMap = new Map<string, any>();
    
    if (itemCodes.length === 0) {
      return productMap;
    }

    // Fetch tất cả products song song
    const productPromises = itemCodes.map(async (itemCode) => {
      try {
        const loyaltyProduct = await this.checkProduct(itemCode);
        return { itemCode, loyaltyProduct };
      } catch (error) {
        this.logger.warn(`[LoyaltyService] Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
        return { itemCode, loyaltyProduct: null };
      }
    });

    const results = await Promise.all(productPromises);
    
    results.forEach(({ itemCode, loyaltyProduct }) => {
      if (loyaltyProduct) {
        productMap.set(itemCode, loyaltyProduct);
      }
    });

    return productMap;
  }

  /**
   * Kiểm tra product có tồn tại trong Loyalty API không
   * @param itemCode - Mã sản phẩm cần kiểm tra
   * @returns true nếu product tồn tại, false nếu không
   */
  async productExists(itemCode: string): Promise<boolean> {
    const product = await this.checkProduct(itemCode);
    return product !== null && (product.id || product.code);
  }
}

