import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { Invoice } from '../../entities/invoice.entity';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { InvoicePrintService } from '../../services/invoice-print.service';
import { InvoiceService } from '../../services/invoice.service';
import { ZappyApiService } from '../../services/zappy-api.service';
import { FastApiService } from '../../services/fast-api.service';
import { FastApiInvoiceFlowService } from '../../services/fast-api-invoice-flow.service';
import { Order } from '../../types/order.types';
import { CreateStockTransferDto, StockTransferItem } from '../../dto/create-stock-transfer.dto';

@Injectable()
export class SalesService {
  private readonly logger = new Logger(SalesService.name);

  /**
   * Xử lý promotion code: cắt phần sau dấu "-" để lấy code hiển thị
   */
  private getPromotionDisplayCode(promCode: string | null | undefined): string | null {
    if (!promCode) return null;
    const parts = promCode.split('-');
    return parts[0] || promCode;
  }

  /**
   * Lấy prefix từ ordertype để tính mã kho
   * - "L" cho: "02. Làm dịch vụ", "04. Đổi DV", "08. Tách thẻ", "Đổi thẻ KEEP->Thẻ DV"
   * - "B" cho: "01.Thường", "03. Đổi điểm", "05. Tặng sinh nhật", "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT", "Đổi vỏ"
   */
  private getOrderTypePrefix(ordertypeName: string | null | undefined): string | null {
    if (!ordertypeName) return null;

    const normalized = String(ordertypeName).trim();

    // Kho hàng làm (prefix L)
    const orderTypeLNames = [
      '02. Làm dịch vụ',
      '04. Đổi DV',
      '08. Tách thẻ',
      'Đổi thẻ KEEP->Thẻ DV',
      'LAM_DV',
      'DOI_VO_LAY_DV',
      'KEEP_TO_SVC',
      'LAM_THE_DV',
      'SUA_THE_DV',
      'DOI_THE_DV',
      'LAM_DV_LE',
      'LAM_THE_KEEP',
      'NOI_THE_KEEP',
      'RENAME_CARD',
    ];

    // Kho hàng bán (prefix B)
    const orderTypeBNames = [
      '01.Thường',
      '01. Thường',
      '03. Đổi điểm',
      '05. Tặng sinh nhật',
      '06. Đầu tư',
      '07. Bán tài khoản',
      '9. Sàn TMDT',
      'Đổi vỏ',
      'NORMAL',
      'KM_TRA_DL',
      'BIRTHDAY_PROM',
      'BP_TO_ITEM',
      'BAN_ECOIN',
      'SAN_TMDT',
      'SO_DL',
      'SO_HTDT_HB',
      'SO_HTDT_HK',
      'SO_HTDT_HL_CB',
      'SO_HTDT_HL_HB',
      'SO_HTDT_HL_KM',
      'SO_HTDT_HT',
      'ZERO_CTY',
      'ZERO_SHOP',
    ];

    if (orderTypeLNames.includes(normalized)) {
      return 'L';
    }

    if (orderTypeBNames.includes(normalized)) {
      return 'B';
    }

    return null;
  }

  /**
   * Tính mã kho từ ordertype + ma_bp (bộ phận)
   * Format: prefix + ma_bp (ví dụ: "L" + "MH10" = "LMH10", "B" + "MH10" = "BMH10")
   */
  private calculateMaKho(
    ordertype: string | null | undefined,
    maBp: string | null | undefined
  ): string | null {
    const prefix = this.getOrderTypePrefix(ordertype);
    if (!prefix || !maBp) {
      return null;
    }
    return prefix + maBp;
  }

  /**
   * Xác định nên dùng ma_lo hay so_serial dựa trên productType từ Loyalty API
   * VOUC → dùng so_serial
   * SKIN, TPCN, GIFT → dùng ma_lo
   */
  private shouldUseBatchFromProductType(productType: string | null | undefined): boolean {
    if (!productType) {
      return false; // Mặc định dùng serial nếu không có productType
    }
    const type = String(productType).toUpperCase().trim();
    // SKIN, TPCN, GIFT → dùng ma_lo
    return type === 'SKIN' || type === 'TPCN' || type === 'GIFT';
  }

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    private invoicePrintService: InvoicePrintService,
    private invoiceService: InvoiceService,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private fastApiService: FastApiService,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
  ) {}

  async findAll(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
  }) {
    const { brand, isProcessed, page = 1, limit = 50 } = options;

    const query = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer')
      .orderBy('sale.docDate', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (brand) {
      query.andWhere('customer.brand = :brand', { brand });
    }

    if (isProcessed !== undefined) {
      query.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }

    const [data, total] = await query.getManyAndCount();

    // Thêm promotionDisplayCode vào mỗi sale
    const enrichedData = data.map((sale) => ({
      ...sale,
      promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
    }));

    return {
      data: enrichedData,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findAllOrders(options: {
    brand?: string;
    isProcessed?: boolean;
    page?: number;
    limit?: number;
    date?: string; // Format: DDMMMYYYY (ví dụ: 04DEC2025)
  }) {
    const { brand, isProcessed, page = 1, limit = 50, date } = options;

    // Nếu có date parameter, lấy dữ liệu từ Zappy API
    if (date) {
      try {
        const orders = await this.zappyApiService.getDailySales(date);
        
        // Filter by brand nếu có
        let filteredOrders = orders;
        if (brand) {
          filteredOrders = orders.filter(
            (order) => order.customer.brand?.toLowerCase() === brand.toLowerCase()
          );
        }

        // Fetch departments để tính maKho
        const branchCodes = Array.from(
          new Set(
            filteredOrders
              .flatMap((order) => order.sales || [])
              .map((sale) => sale.branchCode)
              .filter((code): code is string => !!code && code.trim() !== '')
          )
        );

        const departmentMap = new Map<string, any>();
        for (const branchCode of branchCodes) {
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
              { headers: { accept: 'application/json' } },
            );
            const department = response?.data?.data?.items?.[0];
            if (department) {
              departmentMap.set(branchCode, department);
            }
          } catch (error) {
            this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
          }
        }

        // Fetch products từ Loyalty API để lấy producttype
        const itemCodes = Array.from(
          new Set(
            filteredOrders
              .flatMap((order) => order.sales || [])
              .map((sale) => sale.itemCode)
              .filter((code): code is string => !!code && code.trim() !== '')
          )
        );

        const loyaltyProductMap = new Map<string, any>();
        for (const itemCode of itemCodes) {
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
              { headers: { accept: 'application/json' } },
            );
            const loyaltyProduct = response?.data?.data?.item || response?.data;
            if (loyaltyProduct) {
              loyaltyProductMap.set(itemCode, loyaltyProduct);
            }
          } catch (error) {
            this.logger.warn(`Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
          }
        }

        // Thêm promotionDisplayCode, maKho và producttype vào các sales items
        const enrichedOrders = filteredOrders.map((order) => ({
          ...order,
          sales: order.sales?.map((sale) => {
            const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
            const maBp = department?.ma_bp || sale.branchCode || null;
            const calculatedMaKho = this.calculateMaKho(sale.ordertype, maBp);
            const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
            
            return {
              ...sale,
              promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
              department: department,
              maKho: calculatedMaKho || sale.maKho || sale.branchCode || null,
              // Lấy producttype từ Loyalty API (không còn trong database)
              producttype: loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
              product: loyaltyProduct ? {
                ...loyaltyProduct,
                producttype: loyaltyProduct.producttype || loyaltyProduct.productType || null,
                // Đảm bảo productType từ Loyalty API được giữ lại
                productType: loyaltyProduct.productType || loyaltyProduct.producttype || null,
              } : (sale.product || null),
            };
          }) || [],
        }));

        // Phân trang
        const total = enrichedOrders.length;
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedOrders = enrichedOrders.slice(startIndex, endIndex);

        return {
          data: paginatedOrders,
          total,
          page,
          limit,
          totalPages: Math.ceil(total / limit),
        };
      } catch (error: any) {
        this.logger.error(`Error fetching orders from Zappy API: ${error?.message || error}`);
        // Fallback to database if Zappy API fails
        this.logger.warn('Falling back to database query');
      }
    }

    // Lấy tất cả sales với filter
    let query = this.saleRepository
      .createQueryBuilder('sale')
      .leftJoinAndSelect('sale.customer', 'customer')
      .orderBy('sale.docDate', 'DESC')
      .addOrderBy('sale.docCode', 'ASC');

    if (brand) {
      query = query.andWhere('customer.brand = :brand', { brand });
    }

    if (isProcessed !== undefined) {
      query = query.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }

    const allSales = await query.getMany();
    
    // Lấy tất cả itemCode unique từ sales
    const itemCodes = Array.from(
      new Set(
        allSales
          .map((sale) => sale.itemCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );
    
    // Load tất cả products một lần
    const products = itemCodes.length > 0
      ? await this.productItemRepository.find({
          where: { maERP: In(itemCodes) },
        })
      : [];
    
    // Tạo map để lookup nhanh
    const productMap = new Map<string, ProductItem>();
    products.forEach((product) => {
      if (product.maERP) {
        productMap.set(product.maERP, product);
      }
    });
    
    // Fetch products từ Loyalty API để lấy producttype
    const loyaltyProductMap = new Map<string, any>();
    for (const itemCode of itemCodes) {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
          { headers: { accept: 'application/json' } },
        );
        const loyaltyProduct = response?.data?.data?.item || response?.data;
        if (loyaltyProduct) {
          loyaltyProductMap.set(itemCode, loyaltyProduct);
        }
      } catch (error) {
        this.logger.warn(`Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
      }
    }

    // Enrich sales với product information, promotionDisplayCode và producttype
    const enrichedSales = allSales.map((sale) => {
      const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
      const existingProduct = sale.itemCode ? productMap.get(sale.itemCode) || null : null;
      
      return {
        ...sale,
        // Lấy producttype từ Loyalty API (không còn trong database)
        producttype: loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
        product: loyaltyProduct ? {
          ...existingProduct,
          ...loyaltyProduct,
          producttype: loyaltyProduct.producttype || loyaltyProduct.productType || null,
          // Đảm bảo productType từ Loyalty API được giữ lại
          productType: loyaltyProduct.productType || loyaltyProduct.producttype || null,
        } : (existingProduct ? {
          ...existingProduct,
          producttype: null,
        } : null),
        promotionDisplayCode: this.getPromotionDisplayCode(sale.promCode),
      };
    });

    // Fetch departments để lấy ma_bp và tính maKho
    const branchCodes = Array.from(
      new Set(
        allSales
          .map((sale) => sale.branchCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );

    const departmentMap = new Map<string, any>();
    for (const branchCode of branchCodes) {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
          { headers: { accept: 'application/json' } },
        );
        const department = response?.data?.data?.items?.[0];
        if (department) {
          departmentMap.set(branchCode, department);
        }
      } catch (error) {
        this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
      }
    }

    // Enrich sales với department information và tính maKho
    const enrichedSalesWithDepartment = enrichedSales.map((sale) => {
      const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
      const maBp = department?.ma_bp || sale.branchCode || null;
      const calculatedMaKho = this.calculateMaKho(sale.ordertype, maBp);
      
      return {
        ...sale,
        department: department,
        maKho: calculatedMaKho || sale.maKho || sale.branchCode || null,
      };
    });

    // Gộp theo docCode
    const orderMap = new Map<string, {
      docCode: string;
      docDate: Date;
      branchCode: string;
      docSourceType: string;
      customer: any;
      totalRevenue: number;
      totalQty: number;
      totalItems: number;
      isProcessed: boolean;
      sales: any[];
    }>();

    for (const sale of enrichedSalesWithDepartment) {
      const docCode = sale.docCode;
      
      if (!orderMap.has(docCode)) {
        orderMap.set(docCode, {
          docCode: sale.docCode,
          docDate: sale.docDate,
          branchCode: sale.branchCode,
          docSourceType: sale.docSourceType,
          customer: sale.customer,
          totalRevenue: 0,
          totalQty: 0,
          totalItems: 0,
          isProcessed: sale.isProcessed,
          sales: [],
        });
      }

      const order = orderMap.get(docCode)!;
      order.totalRevenue += Number(sale.revenue);
      order.totalQty += Number(sale.qty);
      order.totalItems += 1;
      order.sales.push(sale);
      
      // Nếu có ít nhất 1 sale chưa xử lý thì đơn hàng chưa xử lý
      if (!sale.isProcessed) {
        order.isProcessed = false;
      }
    }

    // Chuyển Map thành Array và sắp xếp
    const orders = Array.from(orderMap.values()).sort((a, b) => {
      return new Date(b.docDate).getTime() - new Date(a.docDate).getTime();
    });

    // Phân trang
    const total = orders.length;
    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    const paginatedOrders = orders.slice(startIndex, endIndex);

    return {
      data: paginatedOrders,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOne(id: string) {
    const sale = await this.saleRepository.findOne({
      where: { id },
      relations: ['customer'],
    });

    if (!sale) {
      throw new NotFoundException(`Sale with ID ${id} not found`);
    }

    return sale;
  }

  async findByOrderCode(docCode: string) {
    // Lấy tất cả sales có cùng docCode (cùng đơn hàng)
    const sales = await this.saleRepository.find({
      where: { docCode },
      relations: ['customer'],
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    if (sales.length === 0) {
      throw new NotFoundException(`Order with docCode ${docCode} not found`);
    }

    // Lấy tất cả itemCode unique từ sales
    const itemCodes = Array.from(
      new Set(
        sales
          .map((sale) => sale.itemCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );
    
    // Load tất cả products một lần
    const products = itemCodes.length > 0
      ? await this.productItemRepository.find({
          where: { maERP: In(itemCodes) },
        })
      : [];
    
    // Tạo map để lookup nhanh
    const productMap = new Map<string, ProductItem>();
    products.forEach((product) => {
      if (product.maERP) {
        productMap.set(product.maERP, product);
      }
    });
    
    // Enrich sales với product information từ database
    const enrichedSales = sales.map((sale) => ({
      ...sale,
      product: sale.itemCode ? productMap.get(sale.itemCode) || null : null,
    }));

    // Fetch products từ Loyalty API cho các itemCode không có trong database hoặc không có dvt
    const loyaltyProductMap = new Map<string, any>();
    for (const itemCode of itemCodes) {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
          { headers: { accept: 'application/json' } },
        );
        const loyaltyProduct = response?.data?.data?.item || response?.data;
        if (loyaltyProduct) {
          loyaltyProductMap.set(itemCode, loyaltyProduct);
        }
      } catch (error) {
        this.logger.warn(`Failed to fetch product ${itemCode} from Loyalty API: ${error}`);
      }
    }

    // Enrich sales với product từ Loyalty API (thêm dvt từ unit)
    const enrichedSalesWithLoyalty = enrichedSales.map((sale) => {
      const loyaltyProduct = sale.itemCode ? loyaltyProductMap.get(sale.itemCode) : null;
      const existingProduct = sale.product;
      
      // Nếu có product từ Loyalty API, merge thông tin (ưu tiên dvt từ Loyalty API)
      if (loyaltyProduct) {
        return {
          ...sale,
          // Lấy producttype từ Loyalty API (không còn trong database)
          producttype: loyaltyProduct?.producttype || loyaltyProduct?.productType || null,
          product: {
            ...existingProduct,
            ...loyaltyProduct,
            // Map unit từ Loyalty API thành dvt
            dvt: loyaltyProduct.unit || existingProduct?.dvt || null,
            // Giữ lại các field từ database nếu có, chỉ dùng materialCode từ Loyalty API
            maVatTu: existingProduct?.maVatTu || loyaltyProduct.materialCode || sale.itemCode,
            maERP: existingProduct?.maERP || loyaltyProduct.materialCode || sale.itemCode,
            // Đảm bảo productType từ Loyalty API được giữ lại (ưu tiên productType, sau đó producttype)
            productType: loyaltyProduct.productType || loyaltyProduct.producttype || (existingProduct as any)?.productType || null,
            // Lấy producttype từ Loyalty API
            producttype: loyaltyProduct.producttype || loyaltyProduct.productType || (existingProduct as any)?.producttype || null,
          },
        };
      }
      
      return sale;
    });

    // Fetch departments để lấy ma_dvcs
    const branchCodes = Array.from(
      new Set(
        sales
          .map((sale) => sale.branchCode)
          .filter((code): code is string => !!code && code.trim() !== '')
      )
    );

    const departmentMap = new Map<string, any>();
    for (const branchCode of branchCodes) {
      try {
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
          { headers: { accept: 'application/json' } },
        );
        const department = response?.data?.data?.items?.[0];
        if (department) {
          departmentMap.set(branchCode, department);
        }
      } catch (error) {
        this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
      }
    }

    // Enrich sales với department information và tính maKho
    const enrichedSalesWithDepartment = enrichedSalesWithLoyalty.map((sale) => {
      const department = sale.branchCode ? departmentMap.get(sale.branchCode) || null : null;
      const maBp = department?.ma_bp || sale.branchCode || null;
      const calculatedMaKho = this.calculateMaKho(sale.ordertype, maBp);
      
      return {
        ...sale,
        department: department,
        maKho: calculatedMaKho || sale.maKho || sale.branchCode || null,
      };
    });

    // Tính tổng doanh thu của đơn hàng
    const totalRevenue = sales.reduce((sum, sale) => sum + Number(sale.revenue), 0);
    const totalQty = sales.reduce((sum, sale) => sum + Number(sale.qty), 0);

    // Lấy thông tin chung từ sale đầu tiên
    const firstSale = sales[0];

    // Lấy thông tin khuyến mại từ Loyalty API cho các promCode trong đơn hàng
    const promotionsByCode: Record<string, any> = {};
    const uniquePromCodes = Array.from(
      new Set(
        sales
          .map((s) => s.promCode)
          .filter((code): code is string => !!code && code.trim() !== ''),
      ),
    );

    for (const promCode of uniquePromCodes) {
      try {
        // Gọi Loyalty API theo externalCode = promCode
        const response = await this.httpService.axiosRef.get(
          `https://loyaltyapi.vmt.vn/promotions/item/external/${promCode}`,
          {
            headers: { accept: 'application/json' },
          },
        );

        const data = response?.data;
        // Lưu cả response gốc và promotion data
        promotionsByCode[promCode] = {
          raw: data,
          main: data || null,
        };
      } catch (error) {
        this.logger.error(
          `Lỗi khi lấy promotion cho promCode ${promCode}: ${
            (error as any)?.message || error
          }`,
        );
        // Nếu không tìm thấy promotion, lưu null để không ảnh hưởng đến flow
        promotionsByCode[promCode] = {
          raw: null,
          main: null,
        };
      }
    }

    // Gắn promotion tương ứng vào từng dòng sale (chỉ để trả ra API, không lưu DB)
    const enrichedSalesWithPromotion = enrichedSalesWithDepartment.map((sale) => {
      const promCode = sale.promCode;
      const promotion =
        promCode && promotionsByCode[promCode]
          ? promotionsByCode[promCode]
          : null;

      return {
        ...sale,
        promotion,
        promotionDisplayCode: this.getPromotionDisplayCode(promCode),
      };
    });

    return {
      docCode: firstSale.docCode,
      docDate: firstSale.docDate,
      branchCode: firstSale.branchCode,
      docSourceType: firstSale.docSourceType,
      customer: firstSale.customer,
      totalRevenue,
      totalQty,
      totalItems: sales.length,
      sales: enrichedSalesWithPromotion,
      promotions: promotionsByCode,
    };
  }

  async printOrder(docCode: string): Promise<any> {
    const orderData = await this.findByOrderCode(docCode);
    
    // In hóa đơn
    const printResult = await this.invoicePrintService.printInvoiceFromOrder(orderData);
    
    // Tạo và lưu invoice vào database
    const invoice = await this.createInvoiceFromOrder(orderData, printResult);
    
    // Đánh dấu tất cả các sale trong đơn hàng là đã xử lý
    // Đảm bảo luôn được gọi ngay cả khi có lỗi ở trên
    try {
      await this.markOrderAsProcessed(docCode);
    } catch (error) {
      // Log lỗi nhưng không throw để không ảnh hưởng đến response
      console.error(`Lỗi khi đánh dấu đơn hàng ${docCode} là đã xử lý:`, error);
    }
    
    return {
      success: true,
      message: `In hóa đơn ${docCode} thành công`,
      invoice,
      printResult,
    };
  }

  async printMultipleOrders(docCodes: string[]): Promise<any> {
    const results: Array<{
      docCode: string;
      success: boolean;
      message: string;
      invoice?: Invoice;
      error?: string;
    }> = [];

    for (const docCode of docCodes) {
      try {
        const result = await this.printOrder(docCode);
        results.push({
          docCode,
          success: true,
          message: result.message,
          invoice: result.invoice,
        });
      } catch (error: any) {
        this.logger.error(`Lỗi khi in đơn hàng ${docCode}: ${error?.message || error}`);
        results.push({
          docCode,
          success: false,
          message: `In hóa đơn ${docCode} thất bại`,
          error: error?.response?.data?.message || error?.message || 'Unknown error',
        });
      }
    }

    const successCount = results.filter((r) => r.success).length;
    const failureCount = results.length - successCount;

    return {
      total: results.length,
      successCount,
      failureCount,
      results,
    };
  }

  /**
   * Lưu hóa đơn vào bảng kê hóa đơn (FastApiInvoice)
   */
  private async saveFastApiInvoice(data: {
    docCode: string;
    maDvcs?: string;
    maKh?: string;
    tenKh?: string;
    ngayCt?: Date;
    status: number;
    message?: string;
    guid?: string | null;
    fastApiResponse?: string;
  }): Promise<FastApiInvoice> {
    try {
      // Kiểm tra xem đã có chưa
      const existing = await this.fastApiInvoiceRepository.findOne({
        where: { docCode: data.docCode },
      });

      if (existing) {
        // Cập nhật record hiện có
        existing.status = data.status;
        existing.message = data.message || existing.message;
        existing.guid = data.guid || existing.guid;
        existing.fastApiResponse = data.fastApiResponse || existing.fastApiResponse;
        if (data.maDvcs) existing.maDvcs = data.maDvcs;
        if (data.maKh) existing.maKh = data.maKh;
        if (data.tenKh) existing.tenKh = data.tenKh;
        if (data.ngayCt) existing.ngayCt = data.ngayCt;
        
        const saved = await this.fastApiInvoiceRepository.save(existing);
        return Array.isArray(saved) ? saved[0] : saved;
      } else {
        // Tạo mới
        const fastApiInvoice = this.fastApiInvoiceRepository.create({
          docCode: data.docCode,
          maDvcs: data.maDvcs ?? null,
          maKh: data.maKh ?? null,
          tenKh: data.tenKh ?? null,
          ngayCt: data.ngayCt ?? new Date(),
          status: data.status,
          message: data.message ?? null,
          guid: data.guid ?? null,
          fastApiResponse: data.fastApiResponse ?? null,
        } as Partial<FastApiInvoice>);
        
        const saved = await this.fastApiInvoiceRepository.save(fastApiInvoice);
        return Array.isArray(saved) ? saved[0] : saved;
      }
    } catch (error: any) {
      this.logger.error(`Error saving FastApiInvoice for ${data.docCode}: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Lưu phiếu xuất kho vào bảng warehouse_releases
   */

  private async markOrderAsProcessed(docCode: string): Promise<void> {
    // Tìm tất cả các sale có cùng docCode
    const sales = await this.saleRepository.find({
      where: { docCode },
    });

    // Cập nhật isProcessed = true cho tất cả các sale
    if (sales.length > 0) {
      await this.saleRepository.update(
        { docCode },
        { isProcessed: true },
      );
    }
  }

  /**
   * Đánh dấu lại các đơn hàng đã có invoice là đã xử lý
   * Method này dùng để xử lý các invoice đã được tạo trước đó
   */
  async markProcessedOrdersFromInvoices(): Promise<{ updated: number; message: string }> {
    // Tìm tất cả các invoice đã được in (isPrinted = true)
    const invoices = await this.invoiceRepository.find({
      where: { isPrinted: true },
    });

    let updatedCount = 0;
    const processedDocCodes = new Set<string>();

    // Duyệt qua các invoice và tìm docCode từ key
    // Key có thể là docCode hoặc có format INV_xxx_xxx
    for (const invoice of invoices) {
      let docCode: string | null = null;

      // Thử 1: Key chính là docCode (cho các invoice mới)
      const salesByKey = await this.saleRepository.find({
        where: { docCode: invoice.key },
        take: 1,
      });
      if (salesByKey.length > 0) {
        docCode = invoice.key;
      } else {
        // Thử 2: Tìm trong printResponse xem có docCode không
        try {
          if (invoice.printResponse) {
            const printResponse = JSON.parse(invoice.printResponse);
            
            // Tìm trong Message (là JSON string chứa array)
            if (printResponse.Message) {
              try {
                const messageData = JSON.parse(printResponse.Message);
                if (Array.isArray(messageData) && messageData.length > 0) {
                  const data = messageData[0];
                  if (data.key) {
                    // Extract docCode từ key (format: SO52.00005808_X -> SO52.00005808)
                    const keyParts = data.key.split('_');
                    if (keyParts.length > 0) {
                      const potentialDocCode = keyParts[0];
                      const salesByPotentialKey = await this.saleRepository.find({
                        where: { docCode: potentialDocCode },
                        take: 1,
                      });
                      if (salesByPotentialKey.length > 0) {
                        docCode = potentialDocCode;
                      }
                    }
                  }
                }
              } catch (msgError) {
                // Message không phải JSON string, bỏ qua
              }
            }
            
            // Thử tìm trong Data nếu có
            if (!docCode && printResponse.Data && Array.isArray(printResponse.Data) && printResponse.Data.length > 0) {
              const data = printResponse.Data[0];
              if (data.key) {
                const keyParts = data.key.split('_');
                if (keyParts.length > 0) {
                  const potentialDocCode = keyParts[0];
                  const salesByPotentialKey = await this.saleRepository.find({
                    where: { docCode: potentialDocCode },
                    take: 1,
                  });
                  if (salesByPotentialKey.length > 0) {
                    docCode = potentialDocCode;
                  }
                }
              }
            }
          }
        } catch (error) {
          // Ignore parse errors
        }
      }

      // Nếu tìm thấy docCode, đánh dấu các sale là đã xử lý
      if (docCode && !processedDocCodes.has(docCode)) {
        const updateResult = await this.saleRepository.update(
          { docCode },
          { isProcessed: true },
        );
        if (updateResult.affected && updateResult.affected > 0) {
          updatedCount += updateResult.affected;
          processedDocCodes.add(docCode);
        }
      }
    }

    return {
      updated: updatedCount,
      message: `Đã đánh dấu ${processedDocCodes.size} đơn hàng là đã xử lý (${updatedCount} sale records)`,
    };
  }

  private async createInvoiceFromOrder(orderData: any, printResult: any): Promise<any> {
    // Kiểm tra xem invoice đã tồn tại chưa (dựa trên key = docCode)
    const existingInvoice = await this.invoiceRepository.findOne({
      where: { key: orderData.docCode },
      relations: ['items'],
    });
    
    if (existingInvoice) {
      // Cập nhật invoice đã tồn tại
      existingInvoice.isPrinted = true;
      existingInvoice.printResponse = JSON.stringify(printResult);
      await this.invoiceRepository.save(existingInvoice);
      return existingInvoice;
    }

    // Tính toán các giá trị
    const totalAmount = orderData.totalRevenue || 0;
    const taxAmount = Math.round(totalAmount * 0.08); // 8% VAT
    const amountBeforeTax = totalAmount - taxAmount;
    const discountAmount = 0;

    // Format ngày - đảm bảo parse đúng
    let invoiceDate: Date;
    if (orderData.docDate instanceof Date) {
      invoiceDate = orderData.docDate;
    } else if (typeof orderData.docDate === 'string') {
      // Thử parse ISO string trước
      invoiceDate = new Date(orderData.docDate);
      // Kiểm tra nếu date không hợp lệ
      if (isNaN(invoiceDate.getTime())) {
        // Thử parse format khác hoặc fallback
        invoiceDate = new Date(); // Fallback to current date
      }
    } else {
      invoiceDate = new Date(); // Fallback to current date
    }

    // Tạo invoice items từ sales
    const items = orderData.sales.map((sale: any) => {
      const qty = Number(sale.qty);
      const revenue = Number(sale.revenue);
      const price = qty > 0 ? revenue / qty : 0;
      const taxRate = 8.0; // 8% VAT
      const itemTaxAmount = Math.round(revenue * taxRate / 100);
      const itemAmountBeforeTax = revenue - itemTaxAmount;

      return {
        processType: '1',
        itemCode: sale.itemCode || '',
        itemName: sale.itemName || '',
        uom: 'Pcs',
        quantity: qty,
        price: price,
        amount: itemAmountBeforeTax,
        taxRate: taxRate,
        taxAmount: itemTaxAmount,
        discountRate: 0.00,
        discountAmount: 0.00,
      };
    });

    // Format date cho DTO - InvoiceService.parseDate() expect DD/MM/YYYY
    const day = invoiceDate.getDate().toString().padStart(2, '0');
    const month = (invoiceDate.getMonth() + 1).toString().padStart(2, '0');
    const year = invoiceDate.getFullYear();
    const invoiceDateStr = `${day}/${month}/${year}`;
    
    // Tạo invoice DTO
    const invoiceDto = {
      key: orderData.docCode, // Sử dụng docCode làm key
      invoiceDate: invoiceDateStr,
      customerCode: orderData.customer?.code || '',
      customerName: orderData.customer?.name || '',
      customerTaxCode: '',
      address: orderData.customer?.street || orderData.customer?.address || '',
      phoneNumber: orderData.customer?.phone || orderData.customer?.mobile || '',
      idCardNo: orderData.customer?.idnumber || '',
      voucherBook: '1C25MCD',
      items: items,
    };

    // Tạo invoice
    const invoice = await this.invoiceService.createInvoice(invoiceDto);

    // Cập nhật trạng thái đã in và lưu response
    invoice.isPrinted = true;
    invoice.printResponse = JSON.stringify(printResult);
    await this.invoiceRepository.save(invoice);

    return invoice;
  }

  /**
   * Đồng bộ dữ liệu từ Zappy API và lưu vào database
   * @param date - Ngày theo format DDMMMYYYY (ví dụ: 04DEC2025)
   * @returns Kết quả đồng bộ
   */
  async syncFromZappy(date: string): Promise<{
    success: boolean;
    message: string;
    ordersCount: number;
    salesCount: number;
    customersCount: number;
    errors?: string[];
  }> {
    this.logger.log(`Bắt đầu đồng bộ dữ liệu từ Zappy API cho ngày ${date}`);

    try {
      // Lấy dữ liệu từ Zappy API
      const orders = await this.zappyApiService.getDailySales(date);
      
      // Lấy dữ liệu cash/voucher từ get_daily_cash để enrich
      let cashData: any[] = [];
      try {
        cashData = await this.zappyApiService.getDailyCash(date);
        this.logger.log(`Fetched ${cashData.length} cash records for date ${date}`);
      } catch (error) {
        this.logger.warn(`Failed to fetch daily cash data: ${error}`);
      }

      // Tạo map cash data theo so_code để dễ lookup
      const cashMapBySoCode = new Map<string, any[]>();
      cashData.forEach((cash) => {
        const soCode = cash.so_code || cash.master_code;
        if (soCode) {
          if (!cashMapBySoCode.has(soCode)) {
            cashMapBySoCode.set(soCode, []);
          }
          cashMapBySoCode.get(soCode)!.push(cash);
        }
      });

      if (orders.length === 0) {
        return {
          success: true,
          message: `Không có dữ liệu để đồng bộ cho ngày ${date}`,
          ordersCount: 0,
          salesCount: 0,
          customersCount: 0,
        };
      }

      let salesCount = 0;
      let customersCount = 0;
      const errors: string[] = [];

      // Collect tất cả branchCodes để fetch departments
      const branchCodes = Array.from(
        new Set(
          orders
            .map((o) => o.branchCode)
            .filter((code): code is string => !!code && code.trim() !== '')
        )
      );

      // Fetch departments để lấy company và map sang brand
      const departmentMap = new Map<string, { company?: string }>();
      for (const branchCode of branchCodes) {
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchCode}`,
            { headers: { accept: 'application/json' } },
          );
          const department = response?.data?.data?.items?.[0];
          if (department?.company) {
            departmentMap.set(branchCode, { company: department.company });
          }
        } catch (error) {
          this.logger.warn(`Failed to fetch department for branchCode ${branchCode}: ${error}`);
        }
      }

      // Map company sang brand
      const mapCompanyToBrand = (company: string | null | undefined): string => {
        if (!company) return '';
        const companyUpper = company.toUpperCase();
        const brandMap: Record<string, string> = {
          'F3': 'f3',
          'FACIALBAR': 'f3',
          'MENARD': 'menard',
          'CHANDO': 'chando',
          'LABHAIR': 'labhair',
          'YAMAN': 'yaman',
        };
        return brandMap[companyUpper] || company.toLowerCase();
      };

      // Xử lý từng order
      for (const order of orders) {
        try {
          // Lấy brand từ department.company
          const department = departmentMap.get(order.branchCode);
          const brandFromDepartment = department?.company
            ? mapCompanyToBrand(department.company)
            : order.customer.brand || '';

          // Tìm hoặc tạo customer
          let customer = await this.customerRepository.findOne({
            where: { code: order.customer.code },
          });

          if (!customer) {
            const newCustomer = this.customerRepository.create({
              code: order.customer.code,
              name: order.customer.name,
              brand: brandFromDepartment,
              mobile: order.customer.mobile,
              sexual: order.customer.sexual,
              idnumber: order.customer.idnumber,
              enteredat: order.customer.enteredat ? new Date(order.customer.enteredat) : null,
              crm_lead_source: order.customer.crm_lead_source,
              address: order.customer.address,
              province_name: order.customer.province_name,
              birthday: order.customer.birthday ? new Date(order.customer.birthday) : null,
              grade_name: order.customer.grade_name,
              branch_code: order.customer.branch_code,
            } as Partial<Customer>);
            customer = await this.customerRepository.save(newCustomer);
            customersCount++;
          } else {
            // Cập nhật thông tin customer nếu cần
            customer.name = order.customer.name || customer.name;
            customer.mobile = order.customer.mobile || customer.mobile;
            customer.grade_name = order.customer.grade_name || customer.grade_name;
            // Cập nhật brand từ department nếu có
            if (brandFromDepartment) {
              customer.brand = brandFromDepartment;
            }
            customer = await this.customerRepository.save(customer);
          }

          // Đảm bảo customer không null
          if (!customer) {
            const errorMsg = `Không thể tạo hoặc tìm customer với code ${order.customer.code}`;
            this.logger.error(errorMsg);
            errors.push(errorMsg);
            continue;
          }

          // Lấy cash/voucher data cho order này
          const orderCashData = cashMapBySoCode.get(order.docCode) || [];
          const voucherData = orderCashData.filter((cash) => cash.fop_syscode === 'VOUCHER');
          
          // Xử lý từng sale trong order
          if (order.sales && order.sales.length > 0) {
            for (const saleItem of order.sales) {
              try {
                // Kiểm tra xem sale đã tồn tại chưa (dựa trên docCode, itemCode)
                const existingSale = await this.saleRepository.findOne({
                  where: {
                    docCode: order.docCode,
                    itemCode: saleItem.itemCode,
                    customer: { id: customer.id },
                  },
                });
                
                // Enrich voucher data từ get_daily_cash
                let voucherRefno: string | undefined;
                let voucherAmount: number | undefined;
                if (voucherData.length > 0) {
                  // Lấy voucher đầu tiên (có thể có nhiều voucher)
                  const firstVoucher = voucherData[0];
                  voucherRefno = firstVoucher.refno;
                  voucherAmount = firstVoucher.total_in || 0;
                }

                if (existingSale) {
                  // Cập nhật sale đã tồn tại
                  existingSale.qty = saleItem.qty || existingSale.qty;
                  existingSale.revenue = saleItem.revenue || existingSale.revenue;
                  existingSale.linetotal = saleItem.linetotal || existingSale.linetotal;
                  existingSale.tienHang = saleItem.tienHang || existingSale.tienHang;
                  existingSale.giaBan = saleItem.giaBan || existingSale.giaBan;
                  existingSale.itemName = saleItem.itemName || existingSale.itemName;
                  existingSale.ordertype = saleItem.ordertype || existingSale.ordertype;
                  existingSale.branchCode = saleItem.branchCode || existingSale.branchCode;
                  existingSale.promCode = saleItem.promCode || existingSale.promCode;
                  existingSale.serial = saleItem.serial !== undefined ? saleItem.serial : existingSale.serial;
                  existingSale.soSerial = saleItem.serial !== undefined ? saleItem.serial : existingSale.soSerial;
                  existingSale.disc_amt = saleItem.disc_amt || existingSale.disc_amt;
                  existingSale.grade_discamt = saleItem.grade_discamt || existingSale.grade_discamt;
                  existingSale.other_discamt = saleItem.other_discamt !== undefined ? saleItem.other_discamt : existingSale.other_discamt;
                  existingSale.chietKhauMuaHangGiamGia = saleItem.chietKhauMuaHangGiamGia !== undefined ? saleItem.chietKhauMuaHangGiamGia : existingSale.chietKhauMuaHangGiamGia;
                  existingSale.paid_by_voucher_ecode_ecoin_bp = saleItem.paid_by_voucher_ecode_ecoin_bp || existingSale.paid_by_voucher_ecode_ecoin_bp;
                  existingSale.maCa = saleItem.shift_code || existingSale.maCa;
                  existingSale.saleperson_id = saleItem.saleperson_id || existingSale.saleperson_id;
                  existingSale.partnerCode = saleItem.partnerCode || existingSale.partnerCode;
                  existingSale.partner_name = saleItem.partner_name || existingSale.partner_name;
                  existingSale.order_source = saleItem.order_source || existingSale.order_source;
                  // Lưu mvc_serial vào maThe
                  existingSale.maThe = saleItem.mvc_serial !== undefined ? saleItem.mvc_serial : existingSale.maThe;
                  // Category fields
                  existingSale.cat1 = saleItem.cat1 !== undefined ? saleItem.cat1 : existingSale.cat1;
                  existingSale.cat2 = saleItem.cat2 !== undefined ? saleItem.cat2 : existingSale.cat2;
                  existingSale.cat3 = saleItem.cat3 !== undefined ? saleItem.cat3 : existingSale.cat3;
                  existingSale.catcode1 = saleItem.catcode1 !== undefined ? saleItem.catcode1 : existingSale.catcode1;
                  existingSale.catcode2 = saleItem.catcode2 !== undefined ? saleItem.catcode2 : existingSale.catcode2;
                  existingSale.catcode3 = saleItem.catcode3 !== undefined ? saleItem.catcode3 : existingSale.catcode3;
                  // Enrich voucher data
                  if (voucherRefno) {
                    existingSale.voucherDp1 = voucherRefno;
                  }
                  if (voucherAmount !== undefined && voucherAmount > 0) {
                    existingSale.thanhToanVoucher = voucherAmount;
                  }
                  await this.saleRepository.save(existingSale);
                } else {
                  // Tạo sale mới
                  const newSale = this.saleRepository.create({
                    docCode: order.docCode,
                    docDate: new Date(order.docDate),
                    branchCode: order.branchCode,
                    docSourceType: order.docSourceType,
                    ordertype: saleItem.ordertype,
                    description: saleItem.description,
                    partnerCode: saleItem.partnerCode,
                    itemCode: saleItem.itemCode || '',
                    itemName: saleItem.itemName || '',
                    qty: saleItem.qty || 0,
                    revenue: saleItem.revenue || 0,
                    linetotal: saleItem.linetotal || saleItem.revenue || 0,
                    tienHang: saleItem.tienHang || saleItem.linetotal || saleItem.revenue || 0,
                    giaBan: saleItem.giaBan || 0,
                    promCode: saleItem.promCode,
                    serial: saleItem.serial,
                    soSerial: saleItem.serial,
                    disc_amt: saleItem.disc_amt,
                    grade_discamt: saleItem.grade_discamt,
                    other_discamt: saleItem.other_discamt,
                    chietKhauMuaHangGiamGia: saleItem.chietKhauMuaHangGiamGia,
                    paid_by_voucher_ecode_ecoin_bp: saleItem.paid_by_voucher_ecode_ecoin_bp,
                    maCa: saleItem.shift_code,
                    saleperson_id: saleItem.saleperson_id,
                    partner_name: saleItem.partner_name,
                    order_source: saleItem.order_source,
                    // Lưu mvc_serial vào maThe
                    maThe: saleItem.mvc_serial,
                    // Category fields
                    cat1: saleItem.cat1,
                    cat2: saleItem.cat2,
                    cat3: saleItem.cat3,
                    catcode1: saleItem.catcode1,
                    catcode2: saleItem.catcode2,
                    catcode3: saleItem.catcode3,
                    // Enrich voucher data từ get_daily_cash
                    voucherDp1: voucherRefno,
                    thanhToanVoucher: voucherAmount && voucherAmount > 0 ? voucherAmount : undefined,
                    customer: customer,
                    isProcessed: false,
                  } as Partial<Sale>);
                  await this.saleRepository.save(newSale);
                  salesCount++;
                }
              } catch (saleError: any) {
                const errorMsg = `Lỗi khi lưu sale ${order.docCode}/${saleItem.itemCode}: ${saleError?.message || saleError}`;
                this.logger.error(errorMsg);
                errors.push(errorMsg);
              }
            }
          }
        } catch (orderError: any) {
          const errorMsg = `Lỗi khi xử lý order ${order.docCode}: ${orderError?.message || orderError}`;
          this.logger.error(errorMsg);
          errors.push(errorMsg);
        }
      }

      this.logger.log(
        `Hoàn thành đồng bộ: ${orders.length} orders, ${salesCount} sales mới, ${customersCount} customers mới`,
      );

      return {
        success: errors.length === 0,
        message: `Đồng bộ thành công ${orders.length} đơn hàng cho ngày ${date}`,
        ordersCount: orders.length,
        salesCount,
        customersCount,
        errors: errors.length > 0 ? errors : undefined,
      };
    } catch (error: any) {
      this.logger.error(`Lỗi khi đồng bộ từ Zappy API: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Tạo hóa đơn qua Fast API từ đơn hàng
   */
  async createInvoiceViaFastApi(docCode: string, forceRetry: boolean = false): Promise<any> {
    this.logger.log(`Creating invoice via Fast API for order ${docCode}${forceRetry ? ' (force retry)' : ''}`);

    try {
      // Kiểm tra xem đơn hàng đã có trong bảng kê hóa đơn chưa (đã tạo thành công)
      // Nếu forceRetry = true, bỏ qua check này để cho phép retry
      if (!forceRetry) {
        const existingInvoice = await this.fastApiInvoiceRepository.findOne({
          where: { docCode },
        });

        if (existingInvoice && existingInvoice.status === 1) {
          this.logger.log(`Order ${docCode} already exists in invoice list with success status, skipping creation`);
          return {
            success: true,
            message: `Đơn hàng ${docCode} đã được tạo hóa đơn thành công trước đó`,
            result: existingInvoice.fastApiResponse ? JSON.parse(existingInvoice.fastApiResponse) : null,
            alreadyExists: true,
          };
        }
      }

      // Lấy thông tin đơn hàng
      const orderData = await this.findByOrderCode(docCode);

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(`Order ${docCode} not found or has no sales`);
      }

      this.logger.debug(`Order data retrieved: ${JSON.stringify({
        docCode: orderData.docCode,
        docDate: orderData.docDate,
        salesCount: orderData.sales?.length,
        firstSaleBranchCode: orderData.sales[0]?.branchCode,
        firstSaleDepartment: orderData.sales[0]?.department ? 'exists' : 'missing',
      })}`);

      // Build invoice data
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      this.logger.debug(`Invoice data built: ${JSON.stringify({
        ma_dvcs: invoiceData.ma_dvcs,
        ma_kh: invoiceData.ma_kh,
        so_ct: invoiceData.so_ct,
        detailCount: invoiceData.detail?.length,
      })}`);

      // Gọi API tạo đơn hàng
      let result: any;
      try {
        result = await this.fastApiInvoiceFlowService.executeFullInvoiceFlow({
          ...invoiceData,
          customer: orderData.customer,
          ten_kh: orderData.customer?.name || invoiceData.ong_ba || '',
        });
      } catch (error: any) {
        // Lấy thông báo lỗi chính xác từ Fast API response
        let errorMessage = 'Tạo hóa đơn thất bại';
        
        if (error?.response?.data) {
          // Fast API trả về lỗi trong response.data
          const errorData = error.response.data;
          if (Array.isArray(errorData) && errorData.length > 0) {
            errorMessage = errorData[0].message || errorData[0].error || errorMessage;
          } else if (errorData.message) {
            errorMessage = errorData.message;
          } else if (errorData.error) {
            errorMessage = errorData.error;
          } else if (typeof errorData === 'string') {
            errorMessage = errorData;
          }
        } else if (error?.message) {
          errorMessage = error.message;
        }
        
        // Lưu vào bảng kê hóa đơn với status = 0 (thất bại)
        await this.saveFastApiInvoice({
          docCode,
          maDvcs: invoiceData.ma_dvcs,
          maKh: invoiceData.ma_kh,
          tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
          ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
          status: 0,
          message: errorMessage,
          guid: null,
          fastApiResponse: JSON.stringify(error?.response?.data || error),
        });
        
        this.logger.error(`Invoice creation failed for order ${docCode}: ${errorMessage}`);
        
        return {
          success: false,
          message: errorMessage,
          result: error?.response?.data || error,
        };
      }

      // Check response từ Fast API - nếu status === 0 thì coi là lỗi
      const isSuccess = Array.isArray(result) 
        ? result.every((item: any) => item.status !== 0)
        : (result?.status !== 0 && result?.status !== undefined);

      // Lấy thông tin từ response
      const responseStatus = Array.isArray(result) && result.length > 0 
        ? result[0].status 
        : result?.status ?? 0;
      const responseMessage = Array.isArray(result) && result.length > 0
        ? result[0].message || result[0].error || 'Tạo hóa đơn thất bại'
        : result?.message || result?.error || 'Tạo hóa đơn thất bại';
      const responseGuid = Array.isArray(result) && result.length > 0
        ? (Array.isArray(result[0].guid) ? result[0].guid[0] : result[0].guid)
        : result?.guid;

      // Lưu vào bảng kê hóa đơn (cả thành công và thất bại)
      await this.saveFastApiInvoice({
        docCode,
        maDvcs: invoiceData.ma_dvcs,
        maKh: invoiceData.ma_kh,
        tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
        ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
        status: responseStatus,
        message: responseMessage,
        guid: responseGuid || null,
        fastApiResponse: JSON.stringify(result),
      });

      if (!isSuccess) {
        // Có lỗi từ Fast API
        this.logger.error(`Invoice creation failed for order ${docCode}: ${responseMessage}`);
        
        // Kiểm tra nếu là lỗi duplicate key - có thể đơn hàng đã tồn tại trong Fast API
        const isDuplicateError = responseMessage && (
          responseMessage.toLowerCase().includes('duplicate') ||
          responseMessage.toLowerCase().includes('primary key constraint') ||
          responseMessage.toLowerCase().includes('pk_d81')
        );
        
        if (isDuplicateError) {
          this.logger.warn(`Order ${docCode} appears to already exist in Fast API (duplicate key error). Treating as already processed.`);
          // Cập nhật status thành 1 (thành công) vì có thể đã tồn tại trong Fast API
          await this.saveFastApiInvoice({
            docCode,
            maDvcs: invoiceData.ma_dvcs,
            maKh: invoiceData.ma_kh,
            tenKh: orderData.customer?.name || invoiceData.ong_ba || '',
            ngayCt: invoiceData.ngay_ct ? new Date(invoiceData.ngay_ct) : new Date(),
            status: 1, // Coi như thành công vì đã tồn tại
            message: `Đơn hàng đã tồn tại trong Fast API: ${responseMessage}`,
            guid: responseGuid || null,
            fastApiResponse: JSON.stringify(result),
          });
          
          return {
            success: true,
            message: `Đơn hàng ${docCode} đã tồn tại trong Fast API (có thể đã được tạo trước đó)`,
            result,
            alreadyExists: true,
          };
        }
        
        return {
          success: false,
          message: responseMessage,
          result,
        };
      }

      // Đánh dấu đơn hàng là đã xử lý
      await this.markOrderAsProcessed(docCode);

      this.logger.log(`Invoice created successfully for order ${docCode}`);

      return {
        success: true,
        message: `Tạo hóa đơn ${docCode} thành công`,
        result,
      };
    } catch (error: any) {
      this.logger.error(`Error creating invoice for order ${docCode}: ${error?.message || error}`);
      this.logger.error(`Error stack: ${error?.stack || 'No stack trace'}`);
      
      throw error;
    }
  }

  /**
   * Build invoice data cho Fast API (format mới)
   */
  private async buildFastApiInvoiceData(orderData: any): Promise<any> {
    try {
      const toNumber = (value: any, defaultValue: number = 0): number => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        const num = Number(value);
        return isNaN(num) ? defaultValue : num;
      };

      // Format ngày theo ISO 8601 với milliseconds và Z
      const formatDateISO = (date: Date): string => {
        if (isNaN(date.getTime())) {
          throw new Error('Invalid date');
        }
        return date.toISOString();
      };

      // Format ngày
      let docDate: Date;
      if (orderData.docDate instanceof Date) {
        docDate = orderData.docDate;
      } else if (typeof orderData.docDate === 'string') {
        docDate = new Date(orderData.docDate);
        if (isNaN(docDate.getTime())) {
          this.logger.warn(`Invalid docDate string: ${orderData.docDate}, using current date`);
          docDate = new Date();
        }
      } else {
        this.logger.warn(`docDate is missing or invalid, using current date`);
        docDate = new Date();
      }

      const minDate = new Date('1753-01-01T00:00:00');
      const maxDate = new Date('9999-12-31T23:59:59');
      if (docDate < minDate || docDate > maxDate) {
        throw new Error(`Date out of range for SQL Server: ${docDate.toISOString()}`);
      }

      const ngayCt = formatDateISO(docDate);
      const ngayLct = formatDateISO(docDate);

    // Lọc bỏ các sale item không có dvt trước khi xử lý
    const salesWithDvt = (orderData.sales || []).filter((sale: any) => {
      const dvt = sale.dvt || sale.product?.dvt || sale.product?.unit;
      return dvt && String(dvt).trim() !== '';
    });

    // Nếu không còn sale item nào có dvt, throw error để bỏ qua order này
    if (salesWithDvt.length === 0) {
      throw new Error(`Đơn hàng ${orderData.docCode} không có sale item nào có đơn vị tính (dvt), bỏ qua không đồng bộ`);
    }

    // Xử lý từng sale với index để tính dong
    const detail = await Promise.all(salesWithDvt.map(async (sale: any, index: number) => {
      const tienHang = toNumber(sale.tienHang || sale.linetotal || sale.revenue, 0);
      const qty = toNumber(sale.qty, 0);
      let giaBan = toNumber(sale.giaBan, 0);
      if (tienHang > 0 && qty > 0) {
        giaBan = tienHang / qty;
      }

      // Tính toán các chiết khấu
      const ck01_nt = toNumber(sale.other_discamt || sale.chietKhauMuaHangGiamGia, 0);
      const ck02_nt = toNumber(sale.chietKhauCkTheoChinhSach, 0);
      const ck03_nt = toNumber(sale.chietKhauMuaHangCkVip || sale.grade_discamt, 0);
      // ma_ck04: Thanh toán coupon
      const ck04_nt = toNumber(sale.chietKhauThanhToanCoupon || sale.chietKhau09, 0);
      // ma_ck05: Thanh toán voucher
      const ck05_nt = toNumber(sale.chietKhauThanhToanVoucher || sale.paid_by_voucher_ecode_ecoin_bp, 0);
      const ck06_nt = toNumber(sale.chietKhauVoucherDp1, 0);
      const ck07_nt = toNumber(sale.chietKhauVoucherDp2, 0);
      const ck08_nt = toNumber(sale.chietKhauVoucherDp3, 0);
      // Các chiết khấu từ 09-22 mặc định là 0
      const ck09_nt = toNumber(sale.chietKhau09, 0);
      const ck10_nt = toNumber(sale.chietKhau10, 0);
      // ck11_nt: Thanh toán TK tiền ảo
      const ck11_nt = toNumber(sale.chietKhauThanhToanTkTienAo || sale.chietKhau11, 0);
      const ck12_nt = toNumber(sale.chietKhau12, 0);
      const ck13_nt = toNumber(sale.chietKhau13, 0);
      const ck14_nt = toNumber(sale.chietKhau14, 0);
      const ck15_nt = toNumber(sale.chietKhau15, 0);
      const ck16_nt = toNumber(sale.chietKhau16, 0);
      const ck17_nt = toNumber(sale.chietKhau17, 0);
      const ck18_nt = toNumber(sale.chietKhau18, 0);
      const ck19_nt = toNumber(sale.chietKhau19, 0);
      const ck20_nt = toNumber(sale.chietKhau20, 0);
      const ck21_nt = toNumber(sale.chietKhau21, 0);
      const ck22_nt = toNumber(sale.chietKhau22, 0);

      // Helper function để đảm bảo giá trị luôn là string, không phải null/undefined
      const toString = (value: any, defaultValue: string = ''): string => {
        if (value === null || value === undefined || value === '') {
          return defaultValue;
        }
        return String(value);
      };

      // Mỗi sale item xử lý riêng, không dùng giá trị mặc định chung
      // Lấy dvt từ chính sale item hoặc từ product của nó (đã được fetch từ Loyalty API với unit)
      // Nếu không có thì dùng 'Cái' làm mặc định (Fast API yêu cầu field này phải có giá trị)
      const dvt = toString(sale.dvt || sale.product?.dvt || sale.product?.unit, 'Cái');
      
      // Tính mã kho từ ordertype + ma_bp (bộ phận)
      // Nếu không tính được thì fallback về sale.maKho hoặc branchCode
      const maBpForMaKho = sale.department?.ma_bp || sale.branchCode || orderData.branchCode || '';
      const calculatedMaKho = this.calculateMaKho(sale.ordertype, maBpForMaKho);
      const maKho = toString(calculatedMaKho || sale.maKho || sale.branchCode, '');
      
      // Debug: Log maLo value từ sale
      if (index === 0) {
        this.logger.debug(`[BuildInvoice] sale.maLo for item ${sale.itemCode}: "${sale.maLo || ''}"`);
      }
      
      // Lấy productType từ database trước (đã được lưu khi sync), nếu không có thì lấy từ product object
      // Nếu ma_vt (materialCode) khác itemCode, cần fetch lại product bằng materialCode để lấy đúng productType
      let productTypeFromLoyalty = sale.productType || sale.product?.productType || sale.product?.producttype;
      const materialCode = sale.product?.maVatTu || sale.product?.materialCode || sale.itemCode;
      
      // Nếu materialCode khác itemCode, luôn fetch lại bằng materialCode để lấy đúng productType
      // (vì productType của materialCode có thể khác với productType của itemCode)
      if (materialCode && materialCode !== sale.itemCode) {
        try {
          const response = await this.httpService.axiosRef.get(
            `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(materialCode)}`,
            { headers: { accept: 'application/json' } },
          );
          const loyaltyProductByMaterialCode = response?.data?.data?.item || response?.data;
          if (loyaltyProductByMaterialCode) {
            // Ưu tiên productType từ materialCode (vì đây là mã vật tư thực tế được gửi lên)
            productTypeFromLoyalty = loyaltyProductByMaterialCode.productType || loyaltyProductByMaterialCode.producttype || productTypeFromLoyalty;
            // Update sale.product với productType từ materialCode
            if (sale.product) {
              sale.product.productType = productTypeFromLoyalty;
              sale.product.producttype = productTypeFromLoyalty;
            }
            this.logger.log(`[BuildInvoice] Fetched productType from materialCode ${materialCode}: ${productTypeFromLoyalty} (itemCode was: ${sale.itemCode})`);
          }
        } catch (error) {
          this.logger.warn(`Failed to fetch product by materialCode ${materialCode} from Loyalty API: ${error}`);
        }
      }
      
      const productTypeUpper = productTypeFromLoyalty ? String(productTypeFromLoyalty).toUpperCase().trim() : null;
      
      // Lấy giá trị serial từ sale (tất cả đều lấy từ field "serial")
      const serialValue = toString(sale.serial || '', '');
      
      // Debug: Log productType và serial để kiểm tra
      this.logger.log(`[BuildInvoice] Item ${sale.itemCode} (materialCode: ${materialCode}): serial="${serialValue}", productTypeFromLoyalty="${productTypeFromLoyalty || 'N/A'}", productTypeUpper="${productTypeUpper || 'N/A'}", sale.product=${JSON.stringify(sale.product ? { productType: sale.product.productType, producttype: sale.product.producttype, materialCode: sale.product.materialCode || sale.product.maVatTu } : 'null')}`);
      
      // Xác định có dùng ma_lo hay so_serial dựa trên productType từ Loyalty API
      // VOUC → dùng so_serial (không dùng ma_lo)
      // SKIN, TPCN → dùng ma_lo
      const useBatch = this.shouldUseBatchFromProductType(productTypeFromLoyalty);
      
      // Xác định ma_lo và so_serial dựa trên productType
      let maLo: string | null = null;
      let soSerial: string | null = null;
      
      if (useBatch) {
        // SKIN, TPCN, GIFT → dùng ma_lo với giá trị serial
        if (serialValue && serialValue.trim() !== '') {
          if (productTypeUpper === 'TPCN') {
            // Nếu productType là "TPCN", cắt lấy 8 ký tự cuối
            maLo = serialValue.length >= 8 ? serialValue.slice(-8) : serialValue;
            this.logger.log(`[BuildInvoice] Item ${sale.itemCode}: TPCN → cắt 8 ký tự cuối: "${serialValue}" → "${maLo}"`);
          } else if (productTypeUpper === 'SKIN' || productTypeUpper === 'GIFT') {
            // Nếu productType là "SKIN" hoặc "GIFT", cắt lấy 4 ký tự cuối
            maLo = serialValue.length >= 4 ? serialValue.slice(-4) : serialValue;
            this.logger.log(`[BuildInvoice] Item ${sale.itemCode}: ${productTypeUpper} → cắt 4 ký tự cuối: "${serialValue}" → "${maLo}"`);
          } else {
            // Các trường hợp khác (nếu có) → giữ nguyên toàn bộ serial
            maLo = serialValue;
            this.logger.log(`[BuildInvoice] Item ${sale.itemCode}: productType=${productTypeUpper} (không phải SKIN/TPCN/GIFT) → giữ nguyên: "${maLo}"`);
          }
        } else {
          maLo = null;
          this.logger.warn(`[BuildInvoice] Item ${sale.itemCode}: ${productTypeUpper} (SKIN/TPCN/GIFT) nhưng không có serial, ma_lo=null`);
        }
        soSerial = null;
      } else {
        // VOUC hoặc không có productType → dùng so_serial với giá trị serial
        maLo = null;
        soSerial = serialValue && serialValue.trim() !== '' ? serialValue : null;
        this.logger.log(`[BuildInvoice] Item ${sale.itemCode}: productType=${productTypeFromLoyalty || 'N/A'} → VOUC/Other → ma_lo=null, so_serial="${soSerial || ''}"`);
      }
      
      // Log kết quả cuối cùng
      this.logger.log(`[BuildInvoice] Item ${sale.itemCode} (materialCode: ${materialCode}): FINAL → productType="${productTypeFromLoyalty || 'N/A'}", serial="${serialValue || 'N/A'}", ma_lo=${maLo ? `"${maLo}"` : 'null'}, so_serial=${soSerial ? `"${soSerial}"` : 'null'}, useBatch=${useBatch}`);
      
      // Cảnh báo nếu không có serial nhưng productType yêu cầu
      if (!serialValue || serialValue.trim() === '') {
        if (useBatch) {
          this.logger.warn(`[BuildInvoice] Item ${sale.itemCode}: productType=${productTypeFromLoyalty} (SKIN/TPCN) nhưng không có serial → không gửi ma_lo`);
        } else if (productTypeFromLoyalty === 'VOUC') {
          this.logger.warn(`[BuildInvoice] Item ${sale.itemCode}: productType=VOUC nhưng không có serial → không gửi so_serial`);
        }
      }
      
      const maThe = toString(sale.maThe || sale.mvc_serial, '');
      
      // Extract chỉ phần số từ ordertype (ví dụ: "01.Thường" -> "01", "02. Làm dịch vụ" -> "02")
      let loaiGd = '01';
      if (sale.ordertype) {
        const match = String(sale.ordertype).match(/^(\d+)/);
        loaiGd = match ? match[1] : '01';
      }
      
      const loai = toString(sale.loai || sale.cat1, '');

      // Lấy ma_bp - bắt buộc phải có giá trị
      const maBp = toString(
        sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
        ''
      );
      
      // Validate ma_bp - nếu vẫn empty thì log warning
      if (!maBp || maBp.trim() === '') {
        this.logger.warn(
          `[BuildInvoice] Warning: ma_bp is empty for sale ${sale.itemCode} in order ${orderData.docCode}. ` +
          `department.ma_bp: ${sale.department?.ma_bp || 'N/A'}, ` +
          `sale.branchCode: ${sale.branchCode || 'N/A'}, ` +
          `orderData.branchCode: ${orderData.branchCode || 'N/A'}`
        );
      }

      return {
        ma_vt: toString(sale.product?.maVatTu || ''),
        dvt: dvt,
        loai: loai,
        ma_ctkm_th: toString(sale.maCtkmTangHang, ''),
        ma_kho: maKho,
        so_luong: Number(qty),
        gia_ban: Number(giaBan),
        tien_hang: Number(tienHang),
        is_reward_line: sale.isRewardLine ? 1 : 0,
        is_bundle_reward_line: sale.isBundleRewardLine ? 1 : 0,
        km_yn: sale.promCode ? 1 : 0,
        dong_thuoc_goi: toString(sale.dongThuocGoi, ''),
        trang_thai: toString(sale.trangThai, ''),
        barcode: toString(sale.barcode, ''),
        ma_ck01: sale.promCode ? sale.promCode : '',
        ck01_nt: Number(ck01_nt),
        ma_ck02: toString(sale.ckTheoChinhSach, ''),
        ck02_nt: Number(ck02_nt),
        ma_ck03: toString(sale.muaHangCkVip, ''),
        ck03_nt: Number(ck03_nt),
        // ma_ck04: Thanh toán coupon
        ma_ck04: (ck04_nt > 0 || sale.thanhToanCoupon) ? toString(sale.maCk04 || 'COUPON', '') : '',
        ck04_nt: Number(ck04_nt),
        // ma_ck05: Thanh toán voucher
        ma_ck05: (ck05_nt > 0 || sale.thanhToanVoucher) ? toString(sale.maCk05 || 'VOUCHER', '') : '',
        ck05_nt: Number(ck05_nt),
        ma_ck06: sale.voucherDp1 ? 'VOUCHER_DP1' : '',
        ck06_nt: Number(ck06_nt),
        ma_ck07: sale.voucherDp2 ? 'VOUCHER_DP2' : '',
        ck07_nt: Number(ck07_nt),
        ma_ck08: sale.voucherDp3 ? 'VOUCHER_DP3' : '',
        ck08_nt: Number(ck08_nt),
        // ma_ck09: Chiết khấu hãng
        ma_ck09: toString(sale.maCk09, ''),
        ck09_nt: Number(ck09_nt),
        // ma_ck10: Thưởng bằng hàng
        ma_ck10: toString(sale.maCk10, ''),
        ck10_nt: Number(ck10_nt),
        // ma_ck11: Thanh toán TK tiền ảo
        ma_ck11: (ck11_nt > 0 || sale.thanhToanTkTienAo) ? toString(sale.maCk11 || 'TK_TIEN_AO', '') : '',
        ck11_nt: Number(ck11_nt),
        ma_ck12: toString(sale.maCk12, ''),
        ck12_nt: Number(ck12_nt),
        ma_ck13: toString(sale.maCk13, ''),
        ck13_nt: Number(ck13_nt),
        ma_ck14: toString(sale.maCk14, ''),
        ck14_nt: Number(ck14_nt),
        ma_ck15: toString(sale.maCk15, ''),
        ck15_nt: Number(ck15_nt),
        ma_ck16: toString(sale.maCk16, ''),
        ck16_nt: Number(ck16_nt),
        ma_ck17: toString(sale.maCk17, ''),
        ck17_nt: Number(ck17_nt),
        ma_ck18: toString(sale.maCk18, ''),
        ck18_nt: Number(ck18_nt),
        ma_ck19: toString(sale.maCk19, ''),
        ck19_nt: Number(ck19_nt),
        ma_ck20: toString(sale.maCk20, ''),
        ck20_nt: Number(ck20_nt),
        ma_ck21: toString(sale.maCk21, ''),
        ck21_nt: Number(ck21_nt),
        ma_ck22: toString(sale.maCk22, ''),
        ck22_nt: Number(ck22_nt),
        dt_tg_nt: Number(toNumber(sale.dtTgNt, 0)),
        ma_thue: toString(sale.maThue, '10'),
        thue_suat: Number(toNumber(sale.thueSuat, 0)),
        tien_thue: Number(toNumber(sale.tienThue, 0)),
        tk_thue: toString(sale.tkThueCo, ''),
        tk_cpbh: toString(sale.tkCpbh, ''),
        // ma_bp là bắt buộc - đã được validate ở trên
        ma_bp: maBp,
        ma_the: maThe,
        // Chỉ thêm ma_lo hoặc so_serial vào payload (không gửi cả hai, và chỉ gửi khi có giá trị)
        // Logic: Dựa trên productType từ Loyalty API
        // - VOUC → dùng so_serial (nếu có serial)
        // - SKIN, TPCN → dùng ma_lo (nếu có serial, cắt theo productType)
        // - Không có serial → không gửi cả hai
        ...(soSerial && soSerial.trim() !== '' 
          ? { so_serial: soSerial } 
          : (maLo && maLo.trim() !== '' ? { ma_lo: maLo } : {})),
        loai_gd: loaiGd,
        ma_combo: toString(sale.maCombo, ''),
        id_goc: toString(sale.idGoc, ''),
        id_goc_ct: toString(sale.idGocCt, ''),
        id_goc_so: Number(toNumber(sale.idGocSo, 0)),
        dong: index + 1, // Số thứ tự dòng
        id_goc_ngay: sale.idGocNgay ? formatDateISO(new Date(sale.idGocNgay)) : formatDateISO(new Date()),
        id_goc_dv: sale.idGocDv || null,
      };
    }));
    
    // Debug: Log payload của detail item đầu tiên để kiểm tra ma_lo
    if (detail.length > 0) {
      const firstDetail = detail[0];
      this.logger.log(`[BuildInvoice] First detail item payload: ma_lo=${firstDetail.ma_lo ? `"${firstDetail.ma_lo}"` : 'undefined'}, so_serial=${firstDetail.so_serial ? `"${firstDetail.so_serial}"` : 'undefined'}, ma_vt="${firstDetail.ma_vt || ''}"`);
    }

      // Validate sales array
      if (!orderData.sales || orderData.sales.length === 0) {
        throw new Error('Order has no sales items');
      }

      // Build cbdetail từ detail (tổng hợp thông tin sản phẩm)
      const cbdetail = detail.map((item: any) => {
        // Tính tổng chiết khấu từ tất cả các loại chiết khấu
        const tongChietKhau = 
          Number(item.ck01_nt || 0) +
          Number(item.ck02_nt || 0) +
          Number(item.ck03_nt || 0) +
          Number(item.ck04_nt || 0) +
          Number(item.ck05_nt || 0) +
          Number(item.ck06_nt || 0) +
          Number(item.ck07_nt || 0) +
          Number(item.ck08_nt || 0) +
          Number(item.ck09_nt || 0) +
          Number(item.ck10_nt || 0) +
          Number(item.ck11_nt || 0) +
          Number(item.ck12_nt || 0) +
          Number(item.ck13_nt || 0) +
          Number(item.ck14_nt || 0) +
          Number(item.ck15_nt || 0) +
          Number(item.ck16_nt || 0) +
          Number(item.ck17_nt || 0) +
          Number(item.ck18_nt || 0) +
          Number(item.ck19_nt || 0) +
          Number(item.ck20_nt || 0) +
          Number(item.ck21_nt || 0) +
          Number(item.ck22_nt || 0);

        return {
          ma_vt: item.ma_vt || '',
          dvt: item.dvt || '',
          so_luong: Number(item.so_luong || 0),
          ck_nt: Number(tongChietKhau),
          gia_nt: Number(item.gia_ban || 0),
          tien_nt: Number(item.tien_hang || 0),
        };
      });

      const firstSale = orderData.sales[0];
      const maKenh = 'ONLINE'; // Fix mã kênh là ONLINE
      const soSeri = firstSale?.kyHieu || firstSale?.branchCode || orderData.branchCode || 'DEFAULT';
      
      // Extract chỉ phần số từ ordertype (ví dụ: "01.Thường" -> "01")
      let loaiGd = '01';
      if (firstSale?.ordertype) {
        const match = String(firstSale.ordertype).match(/^(\d+)/);
        loaiGd = match ? match[1] : '01';
      }

      // Lấy ma_dvcs từ department API (ưu tiên), nếu không có thì fallback
      const maDvcs = firstSale?.department?.ma_dvcs 
        || firstSale?.department?.ma_dvcs_ht
        || orderData.customer?.brand 
        || orderData.branchCode 
        || '';

      return {
        action: 0,
        ma_dvcs: maDvcs,
        ma_kh: orderData.customer?.code || '',
        ong_ba: orderData.customer?.name || null,
        ma_gd: '2',
        ma_tt: null,
        ma_ca: firstSale?.maCa || null,
        hinh_thuc: '0',
        dien_giai: orderData.docCode || null,
        ngay_lct: ngayLct,
        ngay_ct: ngayCt,
        so_ct: orderData.docCode || '',
        so_seri: soSeri,
        ma_nt: 'VND',
        ty_gia: 1.0,
        ma_bp: firstSale?.department?.ma_bp || firstSale?.branchCode || '',
        tk_thue_no: '131111',
        ma_kenh: maKenh,
        loai_gd: loaiGd,
        detail,
        cbdetail,
      };
    } catch (error: any) {
      this.logger.error(`Error building Fast API invoice data: ${error?.message || error}`);
      this.logger.error(`Error stack: ${error?.stack || 'No stack trace'}`);
      this.logger.error(`Order data: ${JSON.stringify({
        docCode: orderData?.docCode,
        docDate: orderData?.docDate,
        salesCount: orderData?.sales?.length,
        customer: orderData?.customer ? { code: orderData.customer.code, name: orderData.customer.name } : null,
      })}`);
      throw new Error(`Failed to build invoice data: ${error?.message || error}`);
    }
  }

  /**
   * Tạo phiếu xuất kho từ STOCK_TRANSFER data
   */
  async createStockTransfer(createDto: CreateStockTransferDto): Promise<any> {
    try {
      // Group theo doccode để xử lý từng phiếu
      const transferMap = new Map<string, StockTransferItem[]>();
      
      for (const item of createDto.data) {
        if (!transferMap.has(item.doccode)) {
          transferMap.set(item.doccode, []);
        }
        transferMap.get(item.doccode)!.push(item);
      }

      const results: Array<{
        doccode: string;
        success: boolean;
        result?: any;
        error?: string;
      }> = [];

      for (const [doccode, items] of transferMap.entries()) {
        try {
          // Lấy item đầu tiên để lấy thông tin chung
          const firstItem = items[0];
          
          // Join với order nếu có so_code
          let orderData: any = null;
          if (firstItem.so_code) {
            try {
              orderData = await this.findByOrderCode(firstItem.so_code);
            } catch (error) {
              this.logger.warn(
                `Không tìm thấy order với docCode ${firstItem.so_code} cho stock transfer ${doccode}`,
              );
            }
          }

          // Build FastAPI stock transfer data
          const stockTransferData = await this.buildStockTransferData(items, orderData);

          // Submit to FastAPI
          const result = await this.fastApiService.submitStockTransfer(stockTransferData);

          results.push({
            doccode,
            success: true,
            result,
          });
        } catch (error: any) {
          this.logger.error(
            `Error creating stock transfer for ${doccode}: ${error?.message || error}`,
          );
          results.push({
            doccode,
            success: false,
            error: error?.message || 'Unknown error',
          });
        }
      }

      return {
        success: true,
        results,
        total: results.length,
        successCount: results.filter((r) => r.success).length,
        failedCount: results.filter((r) => !r.success).length,
      };
    } catch (error: any) {
      this.logger.error(`Error creating stock transfers: ${error?.message || error}`);
      throw error;
    }
  }

  /**
   * Build FastAPI stock transfer data từ STOCK_TRANSFER items
   */
  private async buildStockTransferData(
    items: StockTransferItem[],
    orderData: any,
  ): Promise<any> {
    const firstItem = items[0];

    // Lấy ma_dvcs từ order hoặc branch_code
    let maDvcs = '';
    if (orderData) {
      const firstSale = orderData.sales?.[0];
      maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';
    }
    if (!maDvcs) {
      maDvcs = firstItem.branch_code || '';
    }

    // Lấy ma_kh từ order
    const maKh = orderData?.customer?.code || '';

    // Map iotype sang ma_nx (mã nhập xuất)
    // iotype: 'O' = xuất, 'I' = nhập
    // ma_nx: có thể là '1111' cho xuất, '1112' cho nhập (cần xác nhận với FastAPI)
    const getMaNx = (iotype: string): string => {
      if (iotype === 'O') {
        return '1111'; // Xuất nội bộ
      } else if (iotype === 'I') {
        return '1112'; // Nhập nội bộ
      }
      return '1111'; // Default
    };

    // Build detail items
    const detail = await Promise.all(
      items.map(async (item, index) => {
        // Lookup product để lấy dvt và producttype
        // Lấy producttype và productType từ Loyalty API (stockTransData không có producttype)
        let dvt = 'Cái'; // Default
        let producttype: string | null = null;
        let productTypeFromLoyalty: string | null = null;
        
        try {
          const product = await this.productItemRepository.findOne({
            where: { maERP: item.item_code },
          });
          if (product?.dvt) {
            dvt = product.dvt;
          }
          // Try to get from Loyalty API để lấy dvt, producttype và productType
          try {
            const response = await this.httpService.axiosRef.get(
              `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(item.item_code)}`,
              { headers: { accept: 'application/json' } },
            );
            const loyaltyProduct = response?.data?.data?.item || response?.data;
            if (loyaltyProduct?.unit) {
              dvt = loyaltyProduct.unit;
            }
            // Lấy producttype và productType từ Loyalty API
            producttype = loyaltyProduct?.producttype || loyaltyProduct?.productType || producttype;
            productTypeFromLoyalty = loyaltyProduct?.productType || loyaltyProduct?.producttype || null;
          } catch (error) {
            this.logger.warn(
              `Không lấy được dvt từ Loyalty API cho item ${item.item_code}`,
            );
          }
        } catch (error) {
          this.logger.warn(`Không tìm thấy product cho item ${item.item_code}`);
        }

        // Xác định ma_lo và so_serial dựa trên productType từ Loyalty API (bỏ logic cũ dựa trên producttype I/B/M/V/S)
        const productTypeUpper = productTypeFromLoyalty ? String(productTypeFromLoyalty).toUpperCase().trim() : null;
        
        // Debug log để kiểm tra productType
        if (index === 0) {
          this.logger.debug(`[BuildStockTransfer] Item ${item.item_code}: productTypeFromLoyalty=${productTypeFromLoyalty || 'N/A'}, productTypeUpper=${productTypeUpper || 'N/A'}`);
        }
        
        // Xác định có dùng ma_lo hay so_serial dựa trên productType từ Loyalty API
        // VOUC → dùng so_serial (không dùng ma_lo)
        // SKIN, TPCN → dùng ma_lo
        const useBatch = this.shouldUseBatchFromProductType(productTypeFromLoyalty);
        
        let maLo: string | null = null;
        let soSerial: string | null = null;
        
        if (useBatch) {
          // SKIN, TPCN, GIFT → dùng ma_lo với giá trị batchserial
          const batchSerial = item.batchserial || null;
          if (batchSerial) {
            if (productTypeUpper === 'TPCN') {
              // Nếu productType là "TPCN", cắt lấy 8 ký tự cuối
              maLo = batchSerial.length >= 8 ? batchSerial.slice(-8) : batchSerial;
            } else if (productTypeUpper === 'SKIN' || productTypeUpper === 'GIFT') {
              // Nếu productType là "SKIN" hoặc "GIFT", cắt lấy 4 ký tự cuối
              maLo = batchSerial.length >= 4 ? batchSerial.slice(-4) : batchSerial;
            } else {
              // Các trường hợp khác (nếu có) → giữ nguyên toàn bộ
              maLo = batchSerial;
            }
          } else {
            maLo = null;
          }
          soSerial = null;
        } else {
          // VOUC hoặc không có productType → dùng so_serial, không set ma_lo
          maLo = null;
          soSerial = item.batchserial || null;
        }

        return {
          ma_vt: item.item_code,
          dvt: dvt,
          so_serial: soSerial,
          ma_kho: item.stock_code,
          so_luong: Math.abs(item.qty), // Lấy giá trị tuyệt đối
          gia_nt: 0, // Stock transfer thường không có giá
          tien_nt: 0, // Stock transfer thường không có tiền
          ma_lo: maLo,
          px_gia_dd: 0, // Mặc định 0
          ma_nx: getMaNx(item.iotype),
          ma_vv: null,
          ma_bp: orderData?.sales?.[0]?.department?.ma_bp || item.branch_code || null,
          so_lsx: null,
          ma_sp: null,
          ma_hd: null,
          ma_phi: null,
          ma_ku: null,
          ma_phi_hh: null,
          ma_phi_ttlk: null,
          tien_hh_nt: 0,
          tien_ttlk_nt: 0,
        };
      }),
    );

    // Format date
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      return d.toISOString();
    };

    const transDate = new Date(firstItem.transdate);
    const ngayCt = formatDateISO(transDate);
    const ngayLct = formatDateISO(transDate);

    // Lấy ma_nx từ item đầu tiên (tất cả items trong cùng 1 phiếu nên có cùng iotype)
    const maNx = getMaNx(firstItem.iotype);

    return {
      action: 0, // Thêm action field giống như salesInvoice
      ma_dvcs: maDvcs,
      ma_kh: maKh,
      ong_ba: orderData?.customer?.name || null,
      ma_gd: '2', // Mã giao dịch: 2 - Xuất nội bộ
      ma_nx: maNx, // Thêm ma_nx vào header
      ngay_ct: ngayCt,
      so_ct: firstItem.doccode,
      ma_nt: 'VND',
      ty_gia: 1.0,
      dien_giai: firstItem.doc_desc || null,
      detail: detail,
    };
  }

  /**
   * Build warehouse release data cho Fast API
   */
}

