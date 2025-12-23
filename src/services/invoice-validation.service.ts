import { Injectable, Logger } from '@nestjs/common';

/**
 * Service để validate điều kiện tạo hóa đơn
 * Có thể dễ dàng mở rộng để thêm các điều kiện khác trong tương lai
 */
@Injectable()
export class InvoiceValidationService {
  private readonly logger = new Logger(InvoiceValidationService.name);

  /**
   * Danh sách các order type được phép tạo hóa đơn
   * Có thể mở rộng thêm các loại khác trong tương lai
   */
  private readonly ALLOWED_ORDER_TYPES = [
    '01.Thường',
    '01. Thường',
  ];

  /**
   * Validate xem đơn hàng có được phép tạo hóa đơn không
   * @param orderData - Dữ liệu đơn hàng
   * @returns Validation result với success = true nếu hợp lệ, false nếu không hợp lệ
   */
  validateOrderForInvoice(orderData: {
    docCode: string;
    sales?: Array<{
      ordertypeName?: string | null;
      ordertype?: string | null;
    }>;
  }): {
    success: boolean;
    message?: string;
    orderType?: string;
  } {
    // Kiểm tra orderData có hợp lệ không
    if (!orderData || !orderData.sales || orderData.sales.length === 0) {
      return {
        success: false,
        message: `Đơn hàng ${orderData?.docCode || 'N/A'} không có dữ liệu sales`,
      };
    }

    // Lấy order type từ sale đầu tiên
    const firstSale = orderData.sales[0];
    const ordertypeName = firstSale?.ordertypeName || firstSale?.ordertype || '';
    const normalizedOrderType = String(ordertypeName).trim();

    // Kiểm tra xem order type có trong danh sách được phép không
    const isAllowed = this.ALLOWED_ORDER_TYPES.includes(normalizedOrderType);

    if (!isAllowed) {
      const allowedTypesStr = this.ALLOWED_ORDER_TYPES.join(', ');
      const errorMessage = `Chỉ cho phép tạo hóa đơn cho đơn hàng có Loại thuộc: [${allowedTypesStr}]. Đơn hàng ${orderData.docCode} có Loại = "${ordertypeName}"`;
      
      this.logger.warn(`[InvoiceValidation] ${errorMessage}`);
      
      return {
        success: false,
        message: errorMessage,
        orderType: ordertypeName,
      };
    }

    this.logger.debug(`[InvoiceValidation] Đơn hàng ${orderData.docCode} với Loại = "${ordertypeName}" được phép tạo hóa đơn`);
    
    return {
      success: true,
      orderType: ordertypeName,
    };
  }

  /**
   * Thêm order type vào danh sách được phép (để mở rộng sau này)
   * @param orderType - Order type cần thêm (ví dụ: "02. Làm dịch vụ")
   */
  addAllowedOrderType(orderType: string): void {
    const normalized = String(orderType).trim();
    if (normalized && !this.ALLOWED_ORDER_TYPES.includes(normalized)) {
      this.ALLOWED_ORDER_TYPES.push(normalized);
      this.logger.log(`[InvoiceValidation] Đã thêm order type "${normalized}" vào danh sách được phép`);
    }
  }

  /**
   * Xóa order type khỏi danh sách được phép
   * @param orderType - Order type cần xóa
   */
  removeAllowedOrderType(orderType: string): void {
    const normalized = String(orderType).trim();
    const index = this.ALLOWED_ORDER_TYPES.indexOf(normalized);
    if (index > -1) {
      this.ALLOWED_ORDER_TYPES.splice(index, 1);
      this.logger.log(`[InvoiceValidation] Đã xóa order type "${normalized}" khỏi danh sách được phép`);
    }
  }

  /**
   * Lấy danh sách các order type được phép
   * @returns Array of allowed order types
   */
  getAllowedOrderTypes(): string[] {
    return [...this.ALLOWED_ORDER_TYPES];
  }
}

