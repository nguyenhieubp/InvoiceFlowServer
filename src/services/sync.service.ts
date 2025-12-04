import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { Customer } from '../entities/customer.entity';
import { Sale } from '../entities/sale.entity';
import { SyncDataDto, SyncResponseDto, SyncApiResponse } from '../dto/sync-data.dto';

@Injectable()
export class SyncService {
  private readonly logger = new Logger(SyncService.name);
  private readonly n8nBaseUrl = 'https://n8n.vmt.vn/webhook';

  private readonly brands = [
    { name: 'chando', endpoint: 'kh_chando' },
    { name: 'f3', endpoint: 'kh_f3' },
    { name: 'labhair', endpoint: 'kh_labhair' },
    { name: 'yaman', endpoint: 'kh_yaman' },
    { name: 'menard', endpoint: 'kh_menard' },
  ];

  constructor(
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    private httpService: HttpService,
  ) {}

  async syncAllBrands(): Promise<void> {
    this.logger.log('Bắt đầu đồng bộ dữ liệu từ tất cả nhãn hàng...');

    for (const brand of this.brands) {
      try {
        await this.syncBrand(brand.name, brand.endpoint);
      } catch (error) {
        this.logger.error(`Lỗi khi đồng bộ ${brand.name}: ${error.message}`);
      }
    }

    this.logger.log('Hoàn thành đồng bộ dữ liệu');
  }

  async syncBrand(brandName: string, endpoint: string, useT8: boolean = false): Promise<void> {
    this.logger.log(`Đang đồng bộ dữ liệu từ ${brandName}...`);

    try {
      // Nếu useT8 = true, dùng endpoint /t8, ngược lại dùng /daily
      const url = useT8 
        ? `${this.n8nBaseUrl}/${endpoint}/t8`
        : `${this.n8nBaseUrl}/${endpoint}/daily`;
      const response = await firstValueFrom(
        this.httpService.get<SyncApiResponse>(url, {
          headers: { accept: 'application/json' },
        }),
      );

      // Xử lý cấu trúc dữ liệu: [{ data: [{ data_customer: {...} }, ...] }, ...]
      const responseBody: any = response.data;
      let allCustomers: SyncDataDto[] = [];
      
      // Response là mảng các object, mỗi object có key "data"
      if (Array.isArray(responseBody)) {
        // Kiểm tra xem phần tử đầu tiên có phải là object với key "data" không
        const firstItem = responseBody[0];
        if (firstItem && typeof firstItem === 'object' && 'data' in firstItem && Array.isArray(firstItem.data)) {
          // Cấu trúc mới: [{ data: [...] }, ...]
          for (const batch of responseBody) {
            if (batch && batch.data && Array.isArray(batch.data)) {
              allCustomers = allCustomers.concat(batch.data);
            }
          }
          this.logger.debug(`${brandName}: Nhận được ${allCustomers.length} customers từ ${responseBody.length} batch(es)`);
        } else {
          // Cấu trúc cũ: trực tiếp là mảng các data_customer
          allCustomers = responseBody as SyncDataDto[];
          this.logger.debug(`${brandName}: Nhận được ${allCustomers.length} items từ mảng trực tiếp`);
        }
      } 
      // Tương thích ngược: nếu response trực tiếp là { data: [...] }
      else if (responseBody && typeof responseBody === 'object' && 'data' in responseBody && Array.isArray(responseBody.data)) {
        allCustomers = responseBody.data;
        this.logger.debug(`${brandName}: Nhận được ${allCustomers.length} items từ cấu trúc { data: [...] }`);
      }
      else {
        this.logger.warn(`${brandName}: Dữ liệu không đúng định dạng. Response type: ${typeof responseBody}`);
        return;
      }

      if (allCustomers.length === 0) {
        this.logger.log(`${brandName}: Không có dữ liệu để đồng bộ`);
        return;
      }

      let processedCount = 0;
      let skippedCount = 0;
      
      for (const item of allCustomers) {
        if (item && item.data_customer) {
          try {
            await this.processCustomerData(item.data_customer, brandName);
            processedCount++;
          } catch (error) {
            this.logger.error(`${brandName}: Lỗi khi xử lý customer ${item.data_customer?.Personal_Info?.code}: ${error.message}`);
            skippedCount++;
          }
        } else {
          skippedCount++;
          this.logger.warn(`${brandName}: Item không có data_customer: ${JSON.stringify(item).substring(0, 100)}`);
        }
      }

      this.logger.log(
        `${brandName}: Đã xử lý ${processedCount} khách hàng${skippedCount > 0 ? `, bỏ qua ${skippedCount} items` : ''}`,
      );
    } catch (error) {
      this.logger.error(
        `Lỗi khi lấy dữ liệu từ ${brandName}: ${error.message}`,
      );
      throw error;
    }
  }

  private async processCustomerData(
    customerData: SyncDataDto['data_customer'],
    brand: string,
  ): Promise<void> {
    const personalInfo = customerData.Personal_Info;
    const sales = customerData.Sales || [];

    // Tìm hoặc tạo customer
    let customer = await this.customerRepository.findOne({
      where: { code: personalInfo.code, brand },
    });

    if (!customer) {
      const newCustomerData: Partial<Customer> = {
        code: personalInfo.code,
        name: personalInfo.name,
        brand,
      };
      
      // Xử lý địa chỉ: ưu tiên street, nếu không có thì dùng address
      if (personalInfo.street) {
        newCustomerData.street = personalInfo.street;
      } else if (personalInfo.address) {
        newCustomerData.street = personalInfo.address;
        newCustomerData.address = personalInfo.address;
      }
      
      if (personalInfo.birthday) newCustomerData.birthday = new Date(personalInfo.birthday);
      if (personalInfo.sexual) newCustomerData.sexual = personalInfo.sexual;
      
      // Xử lý số điện thoại: lưu cả phone và mobile
      if (personalInfo.phone) {
        newCustomerData.phone = personalInfo.phone;
      }
      if (personalInfo.mobile) {
        newCustomerData.mobile = personalInfo.mobile;
        // Nếu chưa có phone thì dùng mobile
        if (!newCustomerData.phone) {
          newCustomerData.phone = personalInfo.mobile;
        }
      }
      
      // Lưu các trường mới
      if (personalInfo.idnumber) newCustomerData.idnumber = personalInfo.idnumber;
      if (personalInfo.enteredat) newCustomerData.enteredat = new Date(personalInfo.enteredat);
      if (personalInfo.crm_lead_source) newCustomerData.crm_lead_source = personalInfo.crm_lead_source;
      if (personalInfo.province_name) newCustomerData.province_name = personalInfo.province_name;
      if (personalInfo.grade_name) newCustomerData.grade_name = personalInfo.grade_name;
      if (personalInfo.branch_code) newCustomerData.branch_code = personalInfo.branch_code;

      customer = this.customerRepository.create(newCustomerData);
      customer = await this.customerRepository.save(customer);
    } else {
      // Cập nhật thông tin nếu có thay đổi
      customer.name = personalInfo.name;
      
      // Xử lý địa chỉ
      if (personalInfo.street !== undefined) {
        customer.street = personalInfo.street;
      }
      if (personalInfo.address !== undefined) {
        customer.address = personalInfo.address;
        if (!customer.street) {
          customer.street = personalInfo.address;
        }
      }
      
      // Xử lý số điện thoại: lưu cả phone và mobile
      if (personalInfo.phone !== undefined) {
        customer.phone = personalInfo.phone;
      }
      if (personalInfo.mobile !== undefined) {
        customer.mobile = personalInfo.mobile;
        // Nếu chưa có phone thì dùng mobile
        if (!customer.phone) {
          customer.phone = personalInfo.mobile;
        }
      }
      
      if (personalInfo.birthday) {
        customer.birthday = new Date(personalInfo.birthday);
      }
      
      // Cập nhật các trường mới
      if (personalInfo.idnumber !== undefined) customer.idnumber = personalInfo.idnumber;
      if (personalInfo.enteredat) customer.enteredat = new Date(personalInfo.enteredat);
      if (personalInfo.crm_lead_source !== undefined) customer.crm_lead_source = personalInfo.crm_lead_source;
      if (personalInfo.province_name !== undefined) customer.province_name = personalInfo.province_name;
      if (personalInfo.grade_name !== undefined) customer.grade_name = personalInfo.grade_name;
      if (personalInfo.branch_code !== undefined) customer.branch_code = personalInfo.branch_code;
      
      await this.customerRepository.save(customer);
    }

    // Xử lý sales
    // LOGIC SALE:
    // Sale = Dòng bán hàng (KHÔNG phải đơn hàng)
    // Order = Đơn hàng (được nhận diện bởi docCode)
    // 
    // Mối quan hệ: 1 Order (docCode) = Nhiều Sale (nhiều sản phẩm)
    //
    // 1. Mỗi sale đại diện cho 1 dòng bán hàng (1 sản phẩm trong 1 đơn hàng)
    // 2. Sale được lưu trực tiếp từ API với các thông tin:
    //    - qty: Số lượng sản phẩm (từ API, không tính toán)
    //    - revenue: Doanh thu của dòng này (từ API, có thể = 0 nếu là hàng khuyến mãi/tặng)
    //    - docCode: Mã đơn hàng (Order) - cùng 1 docCode = cùng 1 đơn hàng
    //    - itemCode: Mã sản phẩm
    // 3. Tránh trùng lặp: Kiểm tra sale đã tồn tại dựa trên (docCode + itemCode + customerId)
    //    - Nếu đã tồn tại: Bỏ qua (không cập nhật)
    //    - Nếu chưa tồn tại: Tạo mới với isProcessed = false
    // 4. Lưu ý: 
    //    - 1 Order (docCode) có thể có nhiều Sale (nhiều sản phẩm trong đơn)
    //    - Cùng 1 docCode + itemCode có thể có nhiều sale nếu qty khác nhau (tách dòng)
    //    - revenue có thể = 0 (hàng tặng, khuyến mãi)
    //    - isProcessed: Đánh dấu sale đã được dùng để tạo invoice chưa
    
    let salesCreated = 0;
    let salesSkipped = 0;
    let salesError = 0;
    
    for (const saleData of sales) {
      try {
        // Kiểm tra xem sale đã tồn tại chưa
        // Kiểm tra dựa trên: docCode + itemCode + qty + revenue + customerId
        // Điều này cho phép lưu nhiều sale với cùng docCode + itemCode nếu qty hoặc revenue khác nhau
        const existingSale = await this.saleRepository.findOne({
          where: {
            docCode: saleData.doccode,
            itemCode: saleData.itemcode,
            qty: saleData.qty,
            revenue: saleData.revenue,
            customerId: customer.id,
          },
        });

        if (!existingSale) {
          // Tạo sale mới - lưu trực tiếp qty và revenue từ API
          // KHÔNG tính toán lại, chỉ lưu giá trị từ API
          const saleDataToCreate: Partial<Sale> = {
            branchCode: saleData.branch_code,
            docCode: saleData.doccode, // Mã đơn hàng
            docDate: new Date(saleData.docdate),
            docSourceType: saleData.docsourcetype || 'sale', // Mặc định 'sale' nếu không có
            partnerCode: saleData.partner_code || personalInfo.code, // Dùng customer code nếu không có partner_code
            itemCode: saleData.itemcode, // Mã sản phẩm
            itemName: saleData.itemname,
            qty: saleData.qty, // Số lượng - lưu trực tiếp từ API
            revenue: saleData.revenue || 0, // Doanh thu - lưu trực tiếp từ API (có thể = 0)
            customerId: customer.id,
            isProcessed: false, // Mặc định chưa xử lý (chưa tạo invoice)
          };
          
          // Các trường optional
          if (saleData.description) saleDataToCreate.description = saleData.description;
          if (saleData.kenh) saleDataToCreate.kenh = saleData.kenh;
          if (saleData.prom_code) saleDataToCreate.promCode = saleData.prom_code;
          if (saleData.ordertype) saleDataToCreate.ordertype = saleData.ordertype;
          
          // Các trường bổ sung từ API
          if (saleData.cat1 !== undefined) saleDataToCreate.cat1 = saleData.cat1;
          if (saleData.cat2 !== undefined) saleDataToCreate.cat2 = saleData.cat2;
          if (saleData.cat3 !== undefined) saleDataToCreate.cat3 = saleData.cat3;
          if (saleData.catcode1 !== undefined) saleDataToCreate.catcode1 = saleData.catcode1;
          if (saleData.catcode2 !== undefined) saleDataToCreate.catcode2 = saleData.catcode2;
          if (saleData.catcode3 !== undefined) saleDataToCreate.catcode3 = saleData.catcode3;
          if (saleData.ck_tm !== undefined && saleData.ck_tm !== null) saleDataToCreate.ck_tm = saleData.ck_tm;
          if (saleData.ck_dly !== undefined && saleData.ck_dly !== null) saleDataToCreate.ck_dly = saleData.ck_dly;
          if (saleData.docid !== undefined) saleDataToCreate.docid = saleData.docid;
          if (saleData.serial !== undefined && saleData.serial !== null) saleDataToCreate.serial = saleData.serial;
          if (saleData.cm_code !== undefined && saleData.cm_code !== null) saleDataToCreate.cm_code = saleData.cm_code;
          if (saleData.line_id !== undefined) saleDataToCreate.line_id = saleData.line_id;
          if (saleData.disc_amt !== undefined) saleDataToCreate.disc_amt = saleData.disc_amt;
          if (saleData.docmonth !== undefined) saleDataToCreate.docmonth = saleData.docmonth;
          if (saleData.itemcost !== undefined) saleDataToCreate.itemcost = saleData.itemcost;
          if (saleData.linetotal !== undefined) saleDataToCreate.linetotal = saleData.linetotal;
          if (saleData.totalcost !== undefined) saleDataToCreate.totalcost = saleData.totalcost;
          if (saleData.crm_emp_id !== undefined) saleDataToCreate.crm_emp_id = saleData.crm_emp_id;
          if (saleData.doctype_name !== undefined) saleDataToCreate.doctype_name = saleData.doctype_name;
          if (saleData.order_source !== undefined && saleData.order_source !== null) saleDataToCreate.order_source = saleData.order_source;
          if (saleData.partner_name !== undefined) saleDataToCreate.partner_name = saleData.partner_name;
          if (saleData.crm_branch_id !== undefined) saleDataToCreate.crm_branch_id = saleData.crm_branch_id;
          if (saleData.grade_discamt !== undefined) saleDataToCreate.grade_discamt = saleData.grade_discamt;
          if (saleData.revenue_wsale !== undefined) saleDataToCreate.revenue_wsale = saleData.revenue_wsale;
          if (saleData.saleperson_id !== undefined) saleDataToCreate.saleperson_id = saleData.saleperson_id;
          if (saleData.revenue_retail !== undefined) saleDataToCreate.revenue_retail = saleData.revenue_retail;
          if (saleData.paid_by_voucher_ecode_ecoin_bp !== undefined) saleDataToCreate.paid_by_voucher_ecode_ecoin_bp = saleData.paid_by_voucher_ecode_ecoin_bp;

          const sale = this.saleRepository.create(saleDataToCreate);
          await this.saleRepository.save(sale);
          salesCreated++;
          this.logger.debug(
            `Đã tạo sale: ${saleData.doccode}/${saleData.itemcode} - qty: ${saleData.qty}, revenue: ${saleData.revenue}`,
          );
        } else {
          salesSkipped++;
          this.logger.debug(
            `Bỏ qua sale (đã tồn tại): ${saleData.doccode}/${saleData.itemcode} - qty: ${saleData.qty}, revenue: ${saleData.revenue}`,
          );
        }
      } catch (error) {
        salesError++;
        this.logger.error(
          `Lỗi khi lưu sale ${saleData.doccode}/${saleData.itemcode} (qty: ${saleData.qty}, revenue: ${saleData.revenue}): ${error.message}`,
        );
        // Tiếp tục xử lý sale tiếp theo, không dừng toàn bộ
      }
    }
    
    if (sales.length > 0) {
      this.logger.log(
        `Customer ${personalInfo.code}: Tổng ${sales.length} sales - Đã tạo: ${salesCreated}, Bỏ qua: ${salesSkipped}, Lỗi: ${salesError}`,
      );
    }
  }
}

