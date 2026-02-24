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

  constructor(private readonly httpService: HttpService) { }



  /**
   * Map issue_partner_code từ svc_serial vào sales (thay thế N8N get_card flow)
   * @param sales - Danh sách sales (đã có svc_serial / maThe)
   * @param partnerMap - Map<svc_serial, { partner_code, partner_name, ... }> từ Zappy
   */
  mapSvcSerialToSales(sales: any[], partnerMap: Map<string, any>): void {
    if (!partnerMap || partnerMap.size === 0) return;

    sales.forEach((sale: any) => {
      const svcSerial = sale.svc_serial || sale.maThe;
      if (!svcSerial) return;

      const partner = partnerMap.get(svcSerial);
      if (!partner) return;

      // Gán issue_partner_code từ API response
      if (partner.partner_code) {
        sale.issuePartnerCode = partner.partner_code;
      }
      if (partner.partner_name) {
        sale.issuePartnerName = partner.partner_name;
      }

      // maThe đã có sẵn từ svc_serial (không cần gán lại)
      // Nhưng đảm bảo soSerial nhất quán
      if (svcSerial && !sale.soSerial) {
        sale.soSerial = svcSerial;
      }
    });
  }

  /**
   * Check generic customer info via N8n webhook
   * @param partnerCode - Mã khách hàng
   * @param sourceCompany - Brand
   */
  async checkCustomer(
    partnerCode: string,
    sourceCompany: string,
  ): Promise<any> {
    const apiUrl = `${this.baseUrl}/check_customer`;
    try {
      this.logger.log(
        `[N8n] Checking customer ${partnerCode} (${sourceCompany})...`,
      );
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
        timeout: 10000,
      });

      const responseData = response.data;
      if (
        Array.isArray(responseData) &&
        responseData.length > 0 &&
        responseData[0].data &&
        Array.isArray(responseData[0].data) &&
        responseData[0].data.length > 0
      ) {
        return responseData[0].data[0];
      }
      return null;
    } catch (error: any) {
      this.logger.error(
        `[N8n] Failed to check customer ${partnerCode}: ${error?.message || error}`,
      );
      return null;
    }
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
