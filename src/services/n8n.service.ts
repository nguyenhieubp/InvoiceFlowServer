import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';

/**
 * N8N Service
 * Service để gọi các API của N8N workflows
 */
@Injectable()
export class N8nService {
  private readonly logger = new Logger(N8nService.name);
  private readonly baseUrl = 'https://n8n.vmt.vn/webhook/vmt';

  constructor(private readonly httpService: HttpService) {}

  /**
   * Fetch card data từ N8N get_card webhook
   * @param docCode - Mã đơn hàng
   * @returns Card data array
   */
  async fetchCardData(docCode: string): Promise<any[]> {
    const apiUrl = `${this.baseUrl}/get_card`;
    const requestBody = { doccode: docCode };

    try {
      const response = await this.httpService.axiosRef.request({
        method: 'GET',
        url: apiUrl,
        headers: {
          'Content-Type': 'application/json',
        },
        data: requestBody,
      });
      return response.data;
    } catch (error: any) {
      this.logger.error(
        `Error fetching card data for ${docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  // In-memory cache for card data
  private readonly cardCache = new Map<string, any>();

  /**
   * Fetch card data với retry logic (GET -> POST fallback)
   * Dùng cho đơn "08. Tách thẻ" để lấy issue_partner_code
   * @param docCode - Mã đơn hàng
   * @returns Card response data
   */
  async fetchCardDataWithRetry(docCode: string): Promise<any> {
    // Check cache
    if (this.cardCache.has(docCode)) {
      // this.logger.debug(`[N8nService] Cache hit for ${docCode}`);
      return this.cardCache.get(docCode);
    }

    const apiUrl = `${this.baseUrl}/get_card`;
    const requestBody = { doccode: docCode };

    // Try GET first
    try {
      const response = await this.httpService.axiosRef.request({
        method: 'GET',
        url: apiUrl,
        headers: {
          'Content-Type': 'application/json',
        },
        data: requestBody,
        timeout: 10000, // Reduced from 30s to 10s to fail faster if stuck
      });
      const data = response.data;
      if (data) this.cardCache.set(docCode, data);
      return data;
    } catch (getError: any) {
      // Nếu GET fail với 404 hoặc 405, thử POST
      if (
        getError?.response?.status === 404 ||
        getError?.response?.status === 405
      ) {
        try {
          const response = await this.httpService.axiosRef.post(
            apiUrl,
            requestBody,
            {
              headers: {
                'Content-Type': 'application/json',
              },
              timeout: 10000, // Reduced from 30s to 10s
            },
          );
          const data = response.data;
          if (data) this.cardCache.set(docCode, data);
          return data;
        } catch (postError: any) {
          this.logger.error(
            `Error fetching card data (POST fallback) for ${docCode}: ${postError?.message || postError}`,
          );
          return null;
        }
      } else {
        this.logger.error(
          `Error fetching card data (GET) for ${docCode}: ${getError?.message || getError}`,
        );
        return null;
      }
    }
  }

  /**
   * Parse card data từ N8N response
   * @param cardResponse - Response từ N8N API
   * @returns Array of card data
   */
  parseCardData(cardResponse: any): any[] {
    if (
      !cardResponse ||
      !Array.isArray(cardResponse) ||
      cardResponse.length === 0
    ) {
      return [];
    }

    const firstItem = cardResponse[0];
    if (firstItem?.data && Array.isArray(firstItem.data)) {
      return firstItem.data;
    }

    return [];
  }

  /**
   * Map issue_partner_code từ card data vào sales
   * @param sales - Danh sách sales
   * @param cardData - Card data từ N8N
   */
  mapIssuePartnerCodeToSales(sales: any[], cardData: any[]): void {
    if (cardData.length === 0) {
      return;
    }

    sales.forEach((sale: any) => {
      const saleQty = Number(sale.qty || 0);

      if (saleQty < 0) {
        const negativeItem = cardData.find(
          (item: any) => Number(item.qty || 0) < 0,
        );
        if (negativeItem?.issue_partner_code) {
          sale.issuePartnerCode = negativeItem.issue_partner_code;
        }
      } else if (saleQty > 0) {
        const positiveItem = cardData.find(
          (item: any) => Number(item.qty || 0) > 0 && item.action === 'ADJUST',
        );
        if (positiveItem?.issue_partner_code) {
          sale.issuePartnerCode = positiveItem.issue_partner_code;
        } else {
          // Fallback: Tìm item có qty > 0 (không cần action = "ADJUST")
          const positiveItemFallback = cardData.find(
            (item: any) => Number(item.qty || 0) > 0,
          );
          if (positiveItemFallback?.issue_partner_code) {
            sale.issuePartnerCode = positiveItemFallback.issue_partner_code;
          }
        }
      }
    });
  }

  // In-memory cache for employee check
  private readonly employeeCache = new Map<string, boolean>();

  /**
   * Check if a customer is an Employee by calling VMT webhook
   * Employee is determined by group_code === "206"
   * @param partnerCode - Mã khách hàng (e.g., NV6466)
   * @param sourceCompany - Brand/source company (e.g., f3, menard)
   * @returns Promise<boolean> - true if employee (group_code === "206")
   */
  async checkCustomerIsEmployee(
    partnerCode: string,
    sourceCompany: string,
  ): Promise<boolean> {
    if (!partnerCode || !sourceCompany) {
      return false;
    }

    const cacheKey = `${partnerCode}_${sourceCompany}`;

    // Check cache first
    if (this.employeeCache.has(cacheKey)) {
      return this.employeeCache.get(cacheKey)!;
    }

    const apiUrl = `${this.baseUrl}/check_customer`;

    try {
      const response = await this.httpService.axiosRef.request({
        method: 'GET',
        url: apiUrl,
        headers: {
          'Content-Type': 'application/json',
        },
        data: {
          partner_code: partnerCode,
          source_company: sourceCompany,
        },
        timeout: 5000, // 5s timeout
      });

      // Parse response: [{ data: [{ group_code: "206", ... }] }]
      const responseData = response.data;
      let isEmployee = false;

      if (Array.isArray(responseData) && responseData.length > 0) {
        const firstItem = responseData[0];
        if (
          firstItem?.data &&
          Array.isArray(firstItem.data) &&
          firstItem.data.length > 0
        ) {
          const customerData = firstItem.data[0];
          isEmployee = customerData?.group_code === '206';
        }
      }

      // Cache result
      this.employeeCache.set(cacheKey, isEmployee);

      return isEmployee;
    } catch (error: any) {
      this.logger.warn(
        `[checkCustomerIsEmployee] Error checking ${partnerCode}/${sourceCompany}: ${error?.message || error}`,
      );
      // On error, cache as false to avoid repeated failed calls
      this.employeeCache.set(cacheKey, false);
      return false;
    }
  }

  /**
   * Batch check multiple customers for employee status
   * @param customers - Array of { partnerCode, sourceCompany }
   * @returns Map<string, boolean> keyed by partnerCode
   */
  async checkCustomersIsEmployee(
    customers: Array<{ partnerCode: string; sourceCompany: string }>,
  ): Promise<Map<string, boolean>> {
    const result = new Map<string, boolean>();

    // Filter unique and non-empty
    const uniqueCustomers = customers.filter(
      (c, i, arr) =>
        c.partnerCode &&
        c.sourceCompany &&
        arr.findIndex(
          (x) =>
            x.partnerCode === c.partnerCode &&
            x.sourceCompany === c.sourceCompany,
        ) === i,
    );

    // Process in parallel with concurrency limit
    const MAX_CONCURRENT = 5;
    for (let i = 0; i < uniqueCustomers.length; i += MAX_CONCURRENT) {
      const batch = uniqueCustomers.slice(i, i + MAX_CONCURRENT);
      const promises = batch.map(async (c) => {
        const isEmployee = await this.checkCustomerIsEmployee(
          c.partnerCode,
          c.sourceCompany,
        );
        result.set(c.partnerCode, isEmployee);
      });
      await Promise.all(promises);
    }

    return result;
  }
}
