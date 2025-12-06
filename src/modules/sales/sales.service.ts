import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { Sale } from '../../entities/sale.entity';
import { Customer } from '../../entities/customer.entity';
import { ProductItem } from '../../entities/product-item.entity';
import { Invoice } from '../../entities/invoice.entity';
import { InvoicePrintService } from '../../services/invoice-print.service';
import { InvoiceService } from '../../services/invoice.service';
import { ZappyApiService } from '../../services/zappy-api.service';
import { FastApiService } from '../../services/fast-api.service';
import { Order } from '../../types/order.types';

@Injectable()
export class SalesService {
  private readonly logger = new Logger(SalesService.name);

  constructor(
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    private invoicePrintService: InvoicePrintService,
    private invoiceService: InvoiceService,
    private httpService: HttpService,
    private zappyApiService: ZappyApiService,
    private fastApiService: FastApiService,
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

    return {
      data,
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

        // Phân trang
        const total = filteredOrders.length;
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedOrders = filteredOrders.slice(startIndex, endIndex);

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
    
    // Enrich sales với product information
    const enrichedSales = allSales.map((sale) => ({
      ...sale,
      product: sale.itemCode ? productMap.get(sale.itemCode) || null : null,
    }));

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

    for (const sale of enrichedSales) {
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
    
    // Enrich sales với product information
    const enrichedSales = sales.map((sale) => ({
      ...sale,
      product: sale.itemCode ? productMap.get(sale.itemCode) || null : null,
    }));

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

    // Enrich sales với department information
    const enrichedSalesWithDepartment = enrichedSales.map((sale) => ({
      ...sale,
      department: sale.branchCode ? departmentMap.get(sale.branchCode) || null : null,
    }));

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
  async createInvoiceViaFastApi(docCode: string): Promise<any> {
    this.logger.log(`Creating invoice via Fast API for order ${docCode}`);

    try {
      // Lấy thông tin đơn hàng
      const orderData = await this.findByOrderCode(docCode);

      if (!orderData || !orderData.sales || orderData.sales.length === 0) {
        throw new NotFoundException(`Order ${docCode} not found or has no sales`);
      }

      // Build invoice data
      const invoiceData = this.buildFastApiInvoiceData(orderData);

      // Gọi Fast API
      const result = await this.fastApiService.submitSalesInvoice(invoiceData);

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
      throw error;
    }
  }

  /**
   * Build invoice data cho Fast API (format mới)
   */
  private buildFastApiInvoiceData(orderData: any): any {
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
        docDate = new Date();
      }
    } else {
      docDate = new Date();
    }

    const minDate = new Date('1753-01-01T00:00:00');
    const maxDate = new Date('9999-12-31T23:59:59');
    if (docDate < minDate || docDate > maxDate) {
      throw new Error('Date out of range for SQL Server');
    }

    const ngayCt = formatDateISO(docDate);
    const ngayLct = formatDateISO(docDate);

    // Xử lý từng sale với index để tính dong
    const detail = orderData.sales.map((sale: any, index: number) => {
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
      const ck04_nt = toNumber(sale.chietKhauThanhToanVoucher, 0);
      const ck05_nt = toNumber(sale.chietKhauThanhToanTkTienAo, 0);
      const ck06_nt = toNumber(sale.chietKhauVoucherDp1, 0);
      const ck07_nt = toNumber(sale.chietKhauVoucherDp2, 0);
      const ck08_nt = toNumber(sale.chietKhauVoucherDp3, 0);
      // Các chiết khấu từ 09-22 mặc định là 0
      const ck09_nt = toNumber(sale.chietKhau09, 0);
      const ck10_nt = toNumber(sale.chietKhau10, 0);
      const ck11_nt = toNumber(sale.chietKhau11, 0);
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

      // Tính maKho, maLo
      const maKho = sale.maKho || '';
      const maLo = sale.maLo || '';
      const maThe = sale.maThe || sale.mvc_serial || '';
      const soSerial = sale.serial || sale.soSerial || '';
      const loaiGd = sale.ordertype || '01';
      const loai = sale.loai || sale.cat1 || '';

      return {
        ma_vt: sale.itemCode || sale.product?.maVatTu || '',
        dvt: sale.dvt || sale.product?.dvt || '',
        so_serial: soSerial,
        loai: loai,
        ma_ctkm_th: sale.maCtkmTangHang || '',
        ma_kho: maKho,
        so_luong: qty,
        gia_ban: giaBan,
        tien_hang: tienHang,
        is_reward_line: sale.isRewardLine ? 1 : 0,
        is_bundle_reward_line: sale.isBundleRewardLine ? 1 : 0,
        km_yn: sale.promCode ? 1 : 0,
        dong_thuoc_goi: sale.dongThuocGoi || '',
        trang_thai: sale.trangThai || '',
        barcode: sale.barcode || '',
        ma_ck01: sale.muaHangGiamGia ? 'MUA_HANG_GIAM_GIA' : '',
        ck01_nt: ck01_nt,
        ma_ck02: sale.ckTheoChinhSach || '',
        ck02_nt: ck02_nt,
        ma_ck03: sale.muaHangCkVip || '',
        ck03_nt: ck03_nt,
        ma_ck04: sale.thanhToanVoucher ? 'VOUCHER' : '',
        ck04_nt: ck04_nt,
        ma_ck05: sale.thanhToanTkTienAo ? 'TK_TIEN_AO' : '',
        ck05_nt: ck05_nt,
        ma_ck06: sale.voucherDp1 ? 'VOUCHER_DP1' : '',
        ck06_nt: ck06_nt,
        ma_ck07: sale.voucherDp2 ? 'VOUCHER_DP2' : '',
        ck07_nt: ck07_nt,
        ma_ck08: sale.voucherDp3 ? 'VOUCHER_DP3' : '',
        ck08_nt: ck08_nt,
        ma_ck09: sale.maCk09 || '',
        ck09_nt: ck09_nt,
        ma_ck10: sale.maCk10 || '',
        ck10_nt: ck10_nt,
        ma_ck11: sale.maCk11 || '',
        ck11_nt: ck11_nt,
        ma_ck12: sale.maCk12 || '',
        ck12_nt: ck12_nt,
        ma_ck13: sale.maCk13 || '',
        ck13_nt: ck13_nt,
        ma_ck14: sale.maCk14 || '',
        ck14_nt: ck14_nt,
        ma_ck15: sale.maCk15 || '',
        ck15_nt: ck15_nt,
        ma_ck16: sale.maCk16 || '',
        ck16_nt: ck16_nt,
        ma_ck17: sale.maCk17 || '',
        ck17_nt: ck17_nt,
        ma_ck18: sale.maCk18 || '',
        ck18_nt: ck18_nt,
        ma_ck19: sale.maCk19 || '',
        ck19_nt: ck19_nt,
        ma_ck20: sale.maCk20 || '',
        ck20_nt: ck20_nt,
        ma_ck21: sale.maCk21 || '',
        ck21_nt: ck21_nt,
        ma_ck22: sale.maCk22 || '',
        ck22_nt: ck22_nt,
        dt_tg_nt: toNumber(sale.dtTgNt, 0),
        ma_thue: sale.maThue || '10',
        thue_suat: toNumber(sale.thueSuat, 0),
        tien_thue: toNumber(sale.tienThue, 0),
        tk_thue: sale.tkThueCo || '',
        tk_cpbh: sale.tkCpbh || '',
        ma_bp: sale.department?.ma_bp || sale.branchCode || '',
        ma_the: maThe,
        ma_lo: maLo,
        loai_gd: loaiGd,
        ma_combo: sale.maCombo || '',
        id_goc: sale.idGoc || '',
        id_goc_ct: sale.idGocCt || '',
        id_goc_so: toNumber(sale.idGocSo, 0),
        dong: index + 1, // Số thứ tự dòng
        id_goc_ngay: sale.idGocNgay ? formatDateISO(new Date(sale.idGocNgay)) : formatDateISO(new Date()),
        id_goc_dv: sale.idGocDv || null,
      };
    });

    // cbdetail có thể là null theo format mới
    const cbdetail = null;

    const maKenh = orderData.sales[0]?.kenh || orderData.sales[0]?.branchCode || orderData.branchCode || '';
    const soSeri = orderData.sales[0]?.kyHieu || orderData.sales[0]?.branchCode || orderData.branchCode || 'DEFAULT';
    const loaiGd = orderData.sales[0]?.ordertype || '01';

    // Lấy ma_dvcs từ department API (ưu tiên), nếu không có thì fallback
    const maDvcs = orderData.sales[0]?.department?.ma_dvcs 
      || orderData.sales[0]?.department?.ma_dvcs_ht
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
      ma_ca: orderData.sales[0]?.maCa || null,
      hinh_thuc: '0',
      dien_giai: null,
      ngay_lct: ngayLct,
      ngay_ct: ngayCt,
      so_ct: orderData.docCode || '',
      so_seri: soSeri,
      ma_nt: 'VND',
      ty_gia: 1.0,
      ma_bp: orderData.sales[0]?.department?.ma_bp || orderData.sales[0]?.branchCode || '',
      ma_nvbh: orderData.sales[0]?.saleperson_id?.toString() || orderData.sales[0]?.tenNhanVienBan || '',
      tk_thue_no: '131111',
      ma_kenh: maKenh,
      loai_gd: loaiGd,
      detail,
      cbdetail,
    };
  }
}

