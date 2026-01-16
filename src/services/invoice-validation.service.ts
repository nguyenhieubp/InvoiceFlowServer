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
   * Tất cả các loại đơn này đều được xử lý qua 2 API: salesOrder và salesInvoice
   * Mỗi loại có thể có nhiều biến thể (có/không có khoảng trắng)
   */
  private readonly ALLOWED_ORDER_TYPES = [
    // 01. Thường
    '01.Thường',
    '01. Thường',
    // 02. Làm dịch vụ
    '02. Làm dịch vụ',
    '02.Làm dịch vụ',
    '02.  Làm dịch vụ',
    // 03. Đổi điểm
    '03. Đổi điểm',
    '03.Đổi điểm',
    '03.  Đổi điểm',
    // 04. Đổi DV
    '04. Đổi DV',
    '04.Đổi DV',
    '04.  Đổi DV',
    // 05. Tặng sinh nhật
    '05. Tặng sinh nhật',
    '05.Tặng sinh nhật',
    '05.  Tặng sinh nhật',
    // 06. Đầu tư
    '06. Đầu tư',
    '06.Đầu tư',
    '06.  Đầu tư',
    // 07. Bán tài khoản
    '07. Bán tài khoản',
    '07.Bán tài khoản',
    '07.  Bán tài khoản',
    // Đổi vỏ
    'Đổi vỏ',
  ];

  /**
   * Lấy danh sách các order type chính (không có biến thể) để hiển thị trong message
   * Tự động extract từ ALLOWED_ORDER_TYPES
   */
  private getAllowedOrderTypesDisplay(): string {
    // Lấy các order type chính (loại bỏ biến thể trùng lặp)
    const uniqueTypes = new Set<string>();
    for (const type of this.ALLOWED_ORDER_TYPES) {
      // Lấy pattern chính (ví dụ: "01.Thường" hoặc "01. Thường" -> "01.Thường")
      const normalized = type.replace(/\s+/g, ' ').trim();
      // Tìm số thứ tự (01, 03, 04, 05, 06, 07) và phần mô tả
      const match = normalized.match(/^(\d+\.)\s*(.+)$/);
      if (match) {
        const [, prefix, description] = match;
        // Sử dụng format chuẩn: "01. Thường" (có khoảng trắng sau dấu chấm)
        uniqueTypes.add(`${prefix} ${description.trim()}`);
      }
    }
    return Array.from(uniqueTypes).sort().join(', ');
  }

  /**
   * Kiểm tra xem order type có thuộc danh sách được phép không
   * Sử dụng includes để xử lý các biến thể (có/không có khoảng trắng)
   */
  private isOrderTypeAllowed(ordertypeValue: string): boolean {
    return this.ALLOWED_ORDER_TYPES.some((allowedType) => {
      return ordertypeValue.includes(allowedType);
    });
  }

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

    // Kiểm tra TẤT CẢ các sales trong đơn hàng
    // Tất cả các sales phải có order type thuộc danh sách được phép
    const allowedTypesStr = this.getAllowedOrderTypesDisplay();

    for (const sale of orderData.sales) {
      const ordertypeName = sale?.ordertypeName || sale?.ordertype || '';
      const ordertypeValue = String(ordertypeName).trim();

      // Kiểm tra xem order type có thuộc các loại được phép không
      // Sử dụng helper method để xử lý các biến thể (có/không có khoảng trắng)
      const isAllowed = this.isOrderTypeAllowed(ordertypeValue);

      // Kiểm tra ngoại lệ: WHOLESALE và ordertypeName chứa "Bán buôn kênh Đại lý"
      const typeSale = (sale as any)?.type_sale?.toUpperCase()?.trim();
      if (
        typeSale === 'WHOLESALE' &&
        ordertypeValue.includes('Bán buôn kênh Đại lý')
      ) {
        // Cho phép, bỏ qua check isAllowed
        continue;
      }

      if (!isAllowed) {
        const errorMessage = `Chỉ cho phép tạo hóa đơn cho đơn hàng có Loại thuộc: [${allowedTypesStr}]. Đơn hàng ${orderData.docCode} có Loại = "${ordertypeName}"`;

        this.logger.warn(`[InvoiceValidation] ${errorMessage}`);

        return {
          success: false,
          message: errorMessage,
          orderType: ordertypeName,
        };
      }
    }

    // Tất cả các sales đều hợp lệ
    const firstSale = orderData.sales[0];
    const ordertypeName =
      firstSale?.ordertypeName || firstSale?.ordertype || '';
    this.logger.debug(
      `[InvoiceValidation] Đơn hàng ${orderData.docCode} với Loại = "${ordertypeName}" được phép tạo hóa đơn`,
    );

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
      this.logger.log(
        `[InvoiceValidation] Đã thêm order type "${normalized}" vào danh sách được phép`,
      );
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
      this.logger.log(
        `[InvoiceValidation] Đã xóa order type "${normalized}" khỏi danh sách được phép`,
      );
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
