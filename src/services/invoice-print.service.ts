import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { Invoice } from '../entities/invoice.entity';

@Injectable()
export class InvoicePrintService {
  private readonly logger = new Logger(InvoicePrintService.name);
  private readonly printApiUrl =
    'https://tcservice.fast.com.vn/api/Command/Executecommand';
  private readonly clientCode = '002598';
  private readonly proxyCode = '002051';
  private readonly user = 'chuthihoa@gmail.com';
  private readonly unitCode = 'CDO';
  private readonly checkSum = '4903a64e69c68ad105db3dc6776e2290';

  constructor(private httpService: HttpService) {}

  async printInvoice(invoice: Invoice): Promise<any> {
    this.logger.log(`Đang in hóa đơn: ${invoice.key}`);

    const payload = this.buildPrintPayload(invoice);

    try {
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.printApiUrl}?action=0&method=7312&clientCode=${this.clientCode}&proxyCode=${this.proxyCode}`,
          payload,
          {
            headers: {
              'Content-Type': 'text/plain',
              'user': this.user,
              'unitCode': this.unitCode,
              'checkSum': this.checkSum,
            },
          },
        ),
      );

      // Kiểm tra nếu API trả về Success: 0 thì coi là lỗi
      const responseData = response.data;
      if (responseData && typeof responseData === 'object') {
        if (responseData.Success === 0 || responseData.success === 0) {
          const errorMessage =
            responseData.Message ||
            responseData.message ||
            'Lỗi không xác định từ API in hóa đơn';
          this.logger.error(
            `API in hóa đơn trả về lỗi cho ${invoice.key}: ${errorMessage}`,
          );
          throw new Error(errorMessage);
        }
      }

      this.logger.log(`In hóa đơn thành công: ${invoice.key}`);
      return response.data;
    } catch (error) {
      this.logger.error(
        `Lỗi khi in hóa đơn ${invoice.key}: ${error.message}`,
      );
      throw error;
    }
  }

  private buildPrintPayload(invoice: Invoice): any {
    // Format ngày: DD/MM/YYYY
    const invoiceDate = this.formatDate(invoice.invoiceDate);

    // Master data theo thứ tự structure
    const master = [
      invoice.key, // Key
      invoiceDate, // InvoiceDate
      invoice.customerCode, // CustomerCode
      invoice.customerName, // CustomerName
      invoice.customerTaxCode || '', // CustomerTaxCode
      invoice.address || '.', // Address
      invoice.phoneNumber || '', // PhoneNumber
      invoice.idCardNo || '', // IDCardNo
      invoice.currency, // Currency
      invoice.exchangeRate, // ExchangeRate
      invoice.amount, // Amount
      invoice.discountAmount, // DiscountAmount
      invoice.taxAmount, // TaxAmount
      invoice.totalAmount, // TotalAmount
      invoice.amountInWords, // AmountInWords
      invoice.humanName || 'SYSTEM', // HumanName
    ];

    // Detail data
    const detail = invoice.items.map((item) => [
      item.processType, // ProcessType
      item.itemCode, // ItemCode
      item.itemName, // ItemName
      item.uom, // UOM
      item.quantity, // Quantity
      item.price, // Price
      item.amount, // Amount
      item.taxRate, // TaxRate
      item.taxAmount, // TaxAmount
      item.discountRate, // DiscountRate
      item.discountAmount, // DiscountAmount
    ]);

    return {
      voucherBook: invoice.voucherBook,
      data: {
        structure: {
          master: [
            'Key',
            'InvoiceDate',
            'CustomerCode',
            'CustomerName',
            'CustomerTaxCode',
            'Address',
            'PhoneNumber',
            'IDCardNo',
            'Currency',
            'ExchangeRate',
            'Amount',
            'DiscountAmount',
            'TaxAmount',
            'TotalAmount',
            'AmountInWords',
            'HumanName',
          ],
          detail: [
            'ProcessType',
            'ItemCode',
            'ItemName',
            'UOM',
            'Quantity',
            'Price',
            'Amount',
            'TaxRate',
            'TaxAmount',
            'DiscountRate',
            'DiscountAmount',
          ],
        },
        invoices: [
          {
            master,
            detail,
          },
        ],
      },
    };
  }

  async printInvoiceFromOrder(orderData: any): Promise<any> {
    this.logger.log(`Đang in hóa đơn từ order: ${orderData.docCode}`);

    const payload = this.buildPrintPayloadFromOrder(orderData);

    try {
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.printApiUrl}?action=0&method=7312&clientCode=${this.clientCode}&proxyCode=${this.proxyCode}`,
          payload,
          {
            headers: {
              'Content-Type': 'text/plain',
              'user': this.user,
              'unitCode': this.unitCode,
              'checkSum': this.checkSum,
            },
          },
        ),
      );

      // Kiểm tra nếu API trả về Success: 0 thì coi là lỗi
      const responseData = response.data;
      if (responseData && typeof responseData === 'object') {
        if (responseData.Success === 0 || responseData.success === 0) {
          const errorMessage = responseData.Message || responseData.message || 'Lỗi không xác định từ API in hóa đơn';
          this.logger.error(`API in hóa đơn trả về lỗi cho ${orderData.docCode}: ${errorMessage}`);
          throw new Error(errorMessage);
        }
      }

      this.logger.log(`In hóa đơn thành công: ${orderData.docCode}`);
      return response.data;
    } catch (error) {
      this.logger.error(
        `Lỗi khi in hóa đơn ${orderData.docCode}: ${error.message}`,
      );
      throw error;
    }
  }

  async downloadInvoicePdf(keySearch: string): Promise<string> {
    const payload = {
      keySearch,
    };

    try {
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.printApiUrl}?action=0&method=7380&clientCode=${this.clientCode}&proxyCode=${this.proxyCode}`,
          payload,
          {
            headers: {
              'Content-Type': 'text/plain',
              user: this.user,
              unitCode: this.unitCode,
              checkSum: this.checkSum,
            },
          },
        ),
      );
      return response.data;
    } catch (error) {
      this.logger.error(
        `Lỗi khi tải hóa đơn PDF với keySearch ${keySearch}: ${error.message}`,
      );
      throw error;
    }
  }

  async checkInvoiceStatus(
    key: string,
    invoiceDate: Date | string,
  ): Promise<{ taxStatus: number; record: any; raw?: any }> {
    const formattedDate =
      typeof invoiceDate === 'string'
        ? this.normalizeDateString(invoiceDate)
        : this.formatDate(invoiceDate);

    const payload = {
      data: [
        {
          key,
          invoiceDate: formattedDate,
        },
      ],
    };

    try {
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.printApiUrl}?action=0&method=7370&clientCode=${this.clientCode}&proxyCode=${this.proxyCode}`,
          payload,
          {
            headers: {
              'Content-Type': 'text/plain',
              user: this.user,
              unitCode: this.unitCode,
              checkSum: this.checkSum,
            },
          },
        ),
      );

      const responseData = response.data;
      if (!(responseData?.Success === 1 || responseData?.success === 1)) {
        const errorMessage =
          responseData?.Message ||
          responseData?.message ||
          'FAST trả về lỗi khi kiểm tra trạng thái hóa đơn';
        throw new Error(errorMessage);
      }

      const records = this.extractRecordsFromFastResponse(responseData);
      if (!records || records.length === 0) {
        throw new Error('FAST không trả về dữ liệu hóa đơn');
      }

      const record = records[0];
      if (
        record.taxStatus === undefined ||
        record.taxStatus === null ||
        record.taxStatus === ''
      ) {
        throw new Error('FAST không trả về taxStatus cho hóa đơn');
      }

      return {
        taxStatus: Number(record.taxStatus),
        record,
        raw: responseData,
      };
    } catch (error) {
      this.logger.error(
        `Lỗi khi kiểm tra trạng thái hóa đơn ${key}: ${error.message}`,
      );
      throw error;
    }
  }

  private buildPrintPayloadFromOrder(orderData: any): any {
    // Format ngày: DD/MM/YYYY
    // Đảm bảo parse date đúng từ string ISO
    let docDate: Date;
    if (orderData.docDate instanceof Date) {
      docDate = orderData.docDate;
    } else if (typeof orderData.docDate === 'string') {
      const dateStr = orderData.docDate.split('.')[0];
      docDate = new Date(dateStr);
      if (isNaN(docDate.getTime())) {
        this.logger.warn(
          `Không thể parse date: ${orderData.docDate}, sử dụng ngày hiện tại`,
        );
        docDate = new Date();
      }
    } else {
      docDate = new Date();
    }
    const invoiceDate = this.formatDate(docDate);
    
    // Tính toán các giá trị
    const totalAmount = orderData.totalRevenue || 0;
    const taxAmount = Math.round(totalAmount * 0.08); // 8% VAT
    const amountBeforeTax = totalAmount - taxAmount;
    const discountAmount = 0;
    
    // Chuyển đổi số thành chữ (cần implement hoặc dùng thư viện)
    const amountInWords = this.numberToWords(totalAmount);

    // Master data theo thứ tự structure
    const master = [
      orderData.docCode, // Key
      invoiceDate, // InvoiceDate
      orderData.customer?.code || '', // CustomerCode
      orderData.customer?.name || '', // CustomerName
      '', // CustomerTaxCode
      orderData.customer?.street || orderData.customer?.address || '', // Address
      orderData.customer?.phone || orderData.customer?.mobile || '', // PhoneNumber
      orderData.customer?.idnumber || '', // IDCardNo
      'VND', // Currency
      1.00, // ExchangeRate
      amountBeforeTax, // Amount
      discountAmount, // DiscountAmount
      taxAmount, // TaxAmount
      totalAmount, // TotalAmount
      amountInWords, // AmountInWords
      'SYSTEM', // HumanName
    ];

    // Detail data từ sales
    const detail = orderData.sales.map((sale: any) => {
      const qty = Number(sale.qty);
      const revenue = Number(sale.revenue);
      const price = qty > 0 ? revenue / qty : 0;
      const taxRate = 8.0; // 8% VAT
      const taxAmount = Math.round(revenue * taxRate / 100);
      const amountBeforeTax = revenue - taxAmount;

      return [
        '1', // ProcessType
        sale.itemCode || '', // ItemCode
        sale.itemName || '', // ItemName
        'Pcs', // UOM
        qty, // Quantity
        price, // Price
        amountBeforeTax, // Amount
        taxRate, // TaxRate
        taxAmount, // TaxAmount
        0.00, // DiscountRate
        0.00, // DiscountAmount
      ];
    });

    return {
      voucherBook: '1C25MCD', // Có thể config sau
      data: {
        structure: {
          master: [
            'Key',
            'InvoiceDate',
            'CustomerCode',
            'CustomerName',
            'CustomerTaxCode',
            'Address',
            'PhoneNumber',
            'IDCardNo',
            'Currency',
            'ExchangeRate',
            'Amount',
            'DiscountAmount',
            'TaxAmount',
            'TotalAmount',
            'AmountInWords',
            'HumanName',
          ],
          detail: [
            'ProcessType',
            'ItemCode',
            'ItemName',
            'UOM',
            'Quantity',
            'Price',
            'Amount',
            'TaxRate',
            'TaxAmount',
            'DiscountRate',
            'DiscountAmount',
          ],
        },
        invoices: [
          {
            master,
            detail,
          },
        ],
      },
    };
  }

  private numberToWords(num: number): string {
    // Simple implementation - có thể cải thiện sau
    const ones = ['', 'một', 'hai', 'ba', 'bốn', 'năm', 'sáu', 'bảy', 'tám', 'chín'];
    const tens = ['', '', 'hai mươi', 'ba mươi', 'bốn mươi', 'năm mươi', 'sáu mươi', 'bảy mươi', 'tám mươi', 'chín mươi'];
    const hundreds = ['', 'một trăm', 'hai trăm', 'ba trăm', 'bốn trăm', 'năm trăm', 'sáu trăm', 'bảy trăm', 'tám trăm', 'chín trăm'];
    
    if (num === 0) return 'không đồng';
    
    const numStr = Math.floor(num).toString();
    const parts: string[] = [];
    
    // Xử lý hàng triệu
    if (numStr.length > 6) {
      const millions = parseInt(numStr.slice(0, -6));
      if (millions > 0) {
        parts.push(this.convertNumber(millions, ones, tens, hundreds) + ' triệu');
      }
    }
    
    // Xử lý hàng nghìn
    if (numStr.length > 3) {
      const thousands = parseInt(numStr.slice(-6, -3) || '0');
      if (thousands > 0) {
        parts.push(this.convertNumber(thousands, ones, tens, hundreds) + ' nghìn');
      }
    }
    
    // Xử lý hàng đơn vị
    const units = parseInt(numStr.slice(-3) || '0');
    if (units > 0) {
      parts.push(this.convertNumber(units, ones, tens, hundreds));
    }
    
    return parts.join(' ') + ' đồng chẵn';
  }

  private convertNumber(num: number, ones: string[], tens: string[], hundreds: string[]): string {
    if (num === 0) return '';
    if (num < 10) return ones[num];
    if (num < 20) {
      if (num === 10) return 'mười';
      if (num === 11) return 'mười một';
      return 'mười ' + ones[num % 10];
    }
    if (num < 100) {
      const ten = Math.floor(num / 10);
      const one = num % 10;
      if (one === 0) return tens[ten];
      if (one === 1) return tens[ten] + ' mốt';
      if (one === 5) return tens[ten] + ' lăm';
      return tens[ten] + ' ' + ones[one];
    }
    const hundred = Math.floor(num / 100);
    const remainder = num % 100;
    if (remainder === 0) return hundreds[hundred];
    return hundreds[hundred] + ' ' + this.convertNumber(remainder, ones, tens, hundreds);
  }

  private formatDate(date: Date): string {
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const year = date.getFullYear();
    return `${day}/${month}/${year}`;
  }

  private normalizeDateString(dateStr: string): string {
    if (dateStr.includes('/')) {
      return dateStr;
    }
    const parsed = new Date(dateStr);
    if (isNaN(parsed.getTime())) {
      return this.formatDate(new Date());
    }
    return this.formatDate(parsed);
  }

  private extractRecordsFromFastResponse(responseData: any): any[] | null {
    if (responseData?.Data && Array.isArray(responseData.Data)) {
      return responseData.Data;
    }
    const message = responseData?.Message || responseData?.message;
    if (typeof message === 'string') {
      try {
        const parsed = JSON.parse(message);
        if (Array.isArray(parsed)) {
          return parsed;
        }
      } catch (error) {
        this.logger.warn(`Không thể parse Message từ FAST: ${error}`);
      }
    }
    return null;
  }
}

