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
    const enrichedSalesWithPromotion = enrichedSales.map((sale) => {
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
                  existingSale.serial = saleItem.serial || existingSale.serial;
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
                    disc_amt: saleItem.disc_amt,
                    grade_discamt: saleItem.grade_discamt,
                    other_discamt: saleItem.other_discamt,
                    chietKhauMuaHangGiamGia: saleItem.chietKhauMuaHangGiamGia,
                    paid_by_voucher_ecode_ecoin_bp: saleItem.paid_by_voucher_ecode_ecoin_bp,
                    maCa: saleItem.shift_code,
                    saleperson_id: saleItem.saleperson_id,
                    partner_name: saleItem.partner_name,
                    order_source: saleItem.order_source,
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
}

