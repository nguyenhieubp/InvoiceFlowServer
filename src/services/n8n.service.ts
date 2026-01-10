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
      this.logger.error(`Error fetching card data for ${docCode}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Fetch card data với retry logic (GET -> POST fallback)
   * Dùng cho đơn "08. Tách thẻ" để lấy issue_partner_code
   * @param docCode - Mã đơn hàng
   * @returns Card response data
   */
  async fetchCardDataWithRetry(docCode: string): Promise<any> {
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
        timeout: 30000,
      });
      return response.data;
    } catch (getError: any) {
      // Nếu GET fail với 404 hoặc 405, thử POST
      if (getError?.response?.status === 404 || getError?.response?.status === 405) {
        try {
          const response = await this.httpService.axiosRef.post(
            apiUrl,
            requestBody,
            {
              headers: {
                'Content-Type': 'application/json',
              },
              timeout: 30000,
            }
          );
          return response.data;
        } catch (postError: any) {
          this.logger.error(`Error fetching card data (POST fallback) for ${docCode}: ${postError?.message || postError}`);
          return null;
        }
      } else {
        this.logger.error(`Error fetching card data (GET) for ${docCode}: ${getError?.message || getError}`);
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
    if (!cardResponse || !Array.isArray(cardResponse) || cardResponse.length === 0) {
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
        // Tìm item có qty < 0
        const negativeItem = cardData.find((item: any) => Number(item.qty || 0) < 0);
        if (negativeItem?.issue_partner_code) {
          sale.issuePartnerCode = negativeItem.issue_partner_code;
        }
      } else if (saleQty > 0) {
        // Tìm item có qty > 0 và action = "ADJUST"
        const positiveItem = cardData.find(
          (item: any) => Number(item.qty || 0) > 0 && item.action === 'ADJUST'
        );
        if (positiveItem?.issue_partner_code) {
          sale.issuePartnerCode = positiveItem.issue_partner_code;
        } else {
          // Fallback: Tìm item có qty > 0 (không cần action = "ADJUST")
          const positiveItemFallback = cardData.find((item: any) => Number(item.qty || 0) > 0);
          if (positiveItemFallback?.issue_partner_code) {
            sale.issuePartnerCode = positiveItemFallback.issue_partner_code;
          }
        }
      }
    });
  }
}