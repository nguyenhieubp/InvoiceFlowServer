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
   * Thử endpoint /material-catalogs/code/ trước, nếu không có thì thử /material-catalogs/old-code/,
   * cuối cùng thử /material-catalogs/material-code/
   * @param itemCode - Mã sản phẩm cần kiểm tra
   * @returns Product object nếu tìm thấy, null nếu không tìm thấy
   */
  async checkProduct(itemCode: string): Promise<any> {
    if (!itemCode) return null;
    const trimmedItemCode = itemCode.trim();
    if (!trimmedItemCode) return null;

    // 1. Thử endpoint /material-catalogs/code/
    try {
      this.logger.debug(`[LoyaltyService] Check 1: /code/${trimmedItemCode}`);
      const response = await this.httpService.axiosRef.get(
        `${this.LOYALTY_API_BASE_URL}/material-catalogs/code/${encodeURIComponent(trimmedItemCode)}`,
        {
          headers: { accept: 'application/json' },
          timeout: this.REQUEST_TIMEOUT,
        },
      );

      const product =
        response?.data?.data?.item || response?.data?.data || response?.data;

      if (product && (product.id || product.code)) {
        return product;
      }
      this.logger.warn(
        `[LoyaltyService] /code/ returned 200 but no valid product. Fallback...`,
      );
    } catch (error: any) {
      if (error?.response?.status !== 404) {
        this.logger.warn(`[LoyaltyService] /code/ error: ${error?.message}`);
      }
      // Continue to fallback
    }

    // 2. Thử endpoint /material-catalogs/old-code/
    try {
      this.logger.debug(
        `[LoyaltyService] Check 2: /old-code/${trimmedItemCode}`,
      );
      const response = await this.httpService.axiosRef.get(
        `${this.LOYALTY_API_BASE_URL}/material-catalogs/old-code/${encodeURIComponent(trimmedItemCode)}`,
        {
          headers: { accept: 'application/json' },
          timeout: this.REQUEST_TIMEOUT,
        },
      );

      const product = response?.data;
      if (product && (product.id || product.code)) {
        return product;
      }
    } catch (error: any) {
      // Continue to next fallback
    }

    // 3. Thử endpoint /material-catalogs/material-code/
    try {
      this.logger.debug(
        `[LoyaltyService] Check 3: /material-code/${trimmedItemCode}`,
      );
      const response = await this.httpService.axiosRef.get(
        `${this.LOYALTY_API_BASE_URL}/material-catalogs/material-code/${encodeURIComponent(trimmedItemCode)}`,
        {
          headers: { accept: 'application/json' },
          timeout: this.REQUEST_TIMEOUT,
        },
      );

      const product = response?.data;
      if (product && (product.id || product.code)) {
        return product;
      }
    } catch (error: any) {
      // All failed
    }

    this.logger.debug(
      `[LoyaltyService] Product ${trimmedItemCode} NOT FOUND in all endpoints.`,
    );
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
        this.logger.warn(
          `[LoyaltyService] Failed to fetch product ${itemCode} from Loyalty API: ${error}`,
        );
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

  /**
   * Fetch departments từ Loyalty API
   * @param branchCodes - Mảng các mã chi nhánh
   * @returns Map<string, any> với key là branchCode và value là department object
   */
  async fetchLoyaltyDepartments(
    branchCodes: string[],
  ): Promise<Map<string, any>> {
    const departmentMap = new Map<string, any>();
    if (branchCodes.length === 0) return departmentMap;

    const departmentPromises = branchCodes.map(async (branchCode) => {
      try {
        const response = await this.httpService.axiosRef.get(
          `${this.LOYALTY_API_BASE_URL}/departments?page=1&limit=25&branchcode=${branchCode}`,
          {
            headers: { accept: 'application/json' },
            timeout: this.REQUEST_TIMEOUT,
          },
        );
        const department = response?.data?.data?.items?.[0];
        return { branchCode, department };
      } catch (error) {
        this.logger.warn(
          `Failed to fetch department for branchCode ${branchCode}: ${error}`,
        );
        return { branchCode, department: null };
      }
    });

    const results = await Promise.all(departmentPromises);
    results.forEach(({ branchCode, department }) => {
      if (department) {
        departmentMap.set(branchCode, department);
      }
    });

    return departmentMap;
  }

  /**
   * Fetch single ma_dvcs from Loyalty API by branch code
   * Wrapper around fetchLoyaltyDepartments or direct call
   */
  async fetchMaDvcs(branchCode: string): Promise<string> {
    if (!branchCode) return '';
    try {
      const url = `${this.LOYALTY_API_BASE_URL}/departments?page=1&limit=25&ma_bp=${branchCode}`;
      const response = await this.httpService.axiosRef.get(url, {
        headers: { accept: 'application/json' },
        timeout: this.REQUEST_TIMEOUT,
      });
      const data = response?.data?.data?.items || [];
      if (Array.isArray(data) && data.length > 0) {
        return data[0].ma_dvcs || '';
      }
      return '';
    } catch (error) {
      this.logger.warn(
        `[LoyaltyService] Failed to fetch ma_dvcs for branch ${branchCode}: ${error}`,
      );
      return '';
    }
  }

  /**
   * Lấy materialCode từ svcCode (Mã dịch vụ)
   * Thử endpoint /material-catalogs/code/ trước, sau đó là /material-catalogs/old-code/
   * @param svcCode - Mã dịch vụ cần tra cứu
   * @returns materialCode nếu tìm thấy, null nếu không
   */
  async getMaterialCodeBySvcCode(svcCode: string): Promise<string | null> {
    if (!svcCode) return null;
    const trimmedCode = svcCode.trim();
    if (!trimmedCode) return null;

    // 1. Thử endpoint chính: /material-catalogs/code/:code
    try {
      const url = `${this.LOYALTY_API_BASE_URL}/material-catalogs/code/${encodeURIComponent(trimmedCode)}`;
      const response = await this.httpService.axiosRef.get(url, {
        headers: { accept: 'application/json' },
        timeout: this.REQUEST_TIMEOUT,
      });

      // Response format: { data: { item: { materialCode: "..." } } }
      const item = response?.data?.data?.item;
      if (item && item.materialCode) {
        return item.materialCode;
      }
    } catch (error: any) {
      // 404 is expected if not found, ignore
      if (error?.response?.status !== 404) {
        this.logger.warn(
          `[LoyaltyService] Error fetching materialCode for svcCode ${trimmedCode} (primary): ${error?.message || error}`,
        );
      }
    }

    // 2. Thử endpoint fallback: /material-catalogs/old-code/:code
    try {
      const url = `${this.LOYALTY_API_BASE_URL}/material-catalogs/old-code/${encodeURIComponent(trimmedCode)}`;
      const response = await this.httpService.axiosRef.get(url, {
        headers: { accept: 'application/json' },
        timeout: this.REQUEST_TIMEOUT,
      });

      // Response format: { data: { item: { materialCode: "..." } } }
      const item = response?.data?.data?.item;
      if (item && item.materialCode) {
        return item.materialCode;
      }
    } catch (error: any) {
      // 404 is expected if not found, ignore
      if (error?.response?.status !== 404) {
        this.logger.warn(
          `[LoyaltyService] Error fetching materialCode for svcCode ${trimmedCode} (fallback): ${error?.message || error}`,
        );
      }
    }

    return null;
  }
}
