import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { DailyCashio } from '../../entities/daily-cashio.entity';
import { Sale } from '../../entities/sale.entity';
import { PaymentSyncLog } from '../../entities/payment-sync-log.entity';
import { LoyaltyService } from 'src/services/loyalty.service';
import { CategoriesService } from '../categories/categories.service';
import { getSupplierCode } from '../../utils/payment-supplier.util';
import { FastApiInvoiceFlowService } from 'src/services/fast-api-invoice-flow.service';

export interface PaymentData {
  // From daily_cashio (ds)
  fop_syscode: string; // Mã hình thức thanh toán
  docdate: Date; // Ngày (from daily_cashio)
  total_in: number; // Tiền thu
  so_code: string; // Mã đơn hàng
  branch_code_cashio: string; // Mã chi nhánh (from daily_cashio)
  ma_dvcs_cashio: string; // Mã ĐVCS từ cashio branch
  refno: string; // Mã tham chiếu
  bank_code: string; // Ngân hàng
  period_code: string; // Kỳ hạn
  ma_doi_tac_payment?: string; // Mã đối tác từ PaymentMethod
  company?: string; // Nhãn hàng/Company

  // From sales (s)
  docDate: Date; // Ngày hóa đơn (from sales)
  revenue: number; // Doanh thu
  branchCode: string; // Mã chi nhánh (from sales)
  boPhan: string; // Mã bộ phận (from sales)
  ma_dvcs_sale: string; // Mã ĐVCS từ sale branch
  maCa: string; // Mã ca
  partnerCode: string; // Mã đối tác
}

@Injectable()
export class PaymentService {
  private readonly logger = new Logger(PaymentService.name);

  constructor(
    @InjectRepository(DailyCashio)
    private dailyCashioRepository: Repository<DailyCashio>,
    private loyaltyService: LoyaltyService,
    private categoryService: CategoriesService,
    private fastApiInvoiceFlowService: FastApiInvoiceFlowService,
    @InjectRepository(PaymentSyncLog)
    private paymentSyncLogRepository: Repository<PaymentSyncLog>,
  ) {}

  async findAll(options: {
    page?: number;
    limit?: number;
    search?: string;
    dateFrom?: string;
    dateTo?: string;
    brand?: string;
    fopSyscode?: string;
  }) {
    const {
      page = 1,
      limit = 10,
      search,
      dateFrom,
      dateTo,
      brand,
      fopSyscode,
    } = options;

    const query = this.createBasePaymentQuery();

    // Filters
    if (search) {
      query.andWhere(
        '(ds.so_code ILIKE :search OR s.partnerCode ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    if (dateFrom) {
      query.andWhere('ds.docdate >= :dateFrom', { dateFrom });
    }

    if (dateTo) {
      query.andWhere('ds.docdate <= :dateTo', { dateTo });
    }

    if (brand) {
      query.andWhere('ds.brand ILIKE :brand', { brand: `%${brand}%` });
    }

    if (fopSyscode) {
      query.andWhere('ds.fop_syscode ILIKE :fopSyscode', {
        fopSyscode: `%${fopSyscode}%`,
      });
    }

    query.offset((page - 1) * limit).limit(limit);

    const results = await query.getRawMany();
    const enrichedResults = await this.enrichPaymentResults(results, false);

    // Get total count
    const countQuery = this.dailyCashioRepository
      .createQueryBuilder('ds')
      .innerJoin(Sale, 's', 'ds.so_code = s.docCode')
      .where('ds.fop_syscode != :voucherCode', { voucherCode: 'VOUCHER' });

    if (search) {
      countQuery.andWhere(
        '(ds.so_code ILIKE :search OR s.partnerCode ILIKE :search)',
        { search: `%${search}%` },
      );
    }
    if (dateFrom) {
      countQuery.andWhere('ds.docdate >= :dateFrom', { dateFrom });
    }
    if (dateTo) {
      countQuery.andWhere('ds.docdate <= :dateTo', { dateTo });
    }
    if (brand) {
      countQuery.andWhere('ds.brand ILIKE :brand', { brand: `%${brand}%` });
    }
    if (fopSyscode) {
      countQuery.andWhere('ds.fop_syscode ILIKE :fopSyscode', {
        fopSyscode: `%${fopSyscode}%`,
      });
    }

    const totalResult = await countQuery
      .select('COUNT(DISTINCT ds.id)', 'count')
      .getRawOne();
    const total = parseInt(totalResult?.count || 0);

    return {
      data: enrichedResults,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findPaymentByDocCode(docCode: string): Promise<PaymentData[]> {
    const query = this.createBasePaymentQuery();
    query.andWhere('s.docCode = :docCode', { docCode });

    const results = await query.getRawMany();
    return this.enrichPaymentResults(results, true);
  }

  async getStatistics(options: {
    dateFrom?: string;
    dateTo?: string;
    brand?: string;
  }) {
    const { dateFrom, dateTo, brand } = options;

    const query = this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .select('SUM(cashio.total_in)', 'totalRevenue')
      .addSelect('COUNT(DISTINCT cashio.so_code)', 'totalOrders')
      .addSelect('COUNT(*)', 'totalTransactions');

    if (dateFrom) {
      query.andWhere('cashio.docdate >= :dateFrom', { dateFrom });
    }

    if (dateTo) {
      query.andWhere('cashio.docdate <= :dateTo', { dateTo });
    }

    if (brand) {
      query.andWhere('cashio.brand = :brand', { brand });
    }

    const result = await query.getRawOne();

    return {
      totalRevenue: parseFloat(result.totalRevenue || 0),
      totalOrders: parseInt(result.totalOrders || 0),
      totalTransactions: parseInt(result.totalTransactions || 0),
    };
  }

  async getPaymentMethods(options: {
    dateFrom?: string;
    dateTo?: string;
    brand?: string;
  }) {
    const { dateFrom, dateTo, brand } = options;

    const query = this.dailyCashioRepository
      .createQueryBuilder('cashio')
      .select('cashio.fop_syscode', 'paymentMethod')
      .addSelect('cashio.fop_description', 'description')
      .addSelect('SUM(cashio.total_in)', 'totalAmount')
      .addSelect('COUNT(*)', 'count')
      .groupBy('cashio.fop_syscode')
      .addGroupBy('cashio.fop_description')
      .orderBy('SUM(cashio.total_in)', 'DESC');

    if (dateFrom) {
      query.andWhere('cashio.docdate >= :dateFrom', { dateFrom });
    }

    if (dateTo) {
      query.andWhere('cashio.docdate <= :dateTo', { dateTo });
    }

    if (brand) {
      query.andWhere('cashio.brand = :brand', { brand });
    }

    const results = await query.getRawMany();

    return results.map((r) => ({
      paymentMethod: r.paymentMethod,
      description: r.description,
      totalAmount: parseFloat(r.totalAmount || 0),
      count: parseInt(r.count || 0),
    }));
  }

  async getDailyPaymentDetails(date: string): Promise<PaymentData[]> {
    const query = this.createBasePaymentQuery();
    query.andWhere(
      "ds.docdate >= :date AND ds.docdate < (:date::date + interval '1 day')",
      { date },
    );

    const results = await query.getRawMany();
    return this.enrichPaymentResults(results, true);
  }

  async autoLogPaymentData() {
    this.logger.log('Starting daily payment method sync...');
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split('T')[0];

    try {
      // Use getDailyPaymentDetails to get detailed records for the day
      const paymentMethods = await this.getDailyPaymentDetails(dateStr);
      if (paymentMethods.length > 0) {
        await this.processFastPayment(paymentMethods);
      } else {
        this.logger.log(`No payment data found for ${dateStr}`);
      }
    } catch (error) {
      this.logger.error(`Daily payment method sync failed: ${error}`);
    }
  }

  async getAuditLogs(options: {
    page?: number;
    limit?: number;
    docCode?: string;
    status?: string;
  }) {
    const { page = 1, limit = 20, docCode, status } = options;
    const query = this.paymentSyncLogRepository.createQueryBuilder('log');

    if (docCode) {
      query.andWhere('log.docCode LIKE :docCode', { docCode: `%${docCode}%` });
    }
    if (status) {
      query.andWhere('log.status = :status', { status });
    }

    query.orderBy('log.createdAt', 'DESC');
    query.skip((page - 1) * limit).take(limit);

    const [items, total] = await query.getManyAndCount();

    return {
      items,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async retryPaymentSync(id: string) {
    const log = await this.paymentSyncLogRepository.findOne({ where: { id } });
    if (!log) {
      throw new Error('Audit log not found');
    }

    if (!log.requestPayload) {
      throw new Error('No payload to retry');
    }

    const payload = JSON.parse(log.requestPayload);

    // Update retry count
    log.retryCount += 1;
    await this.paymentSyncLogRepository.save(log);

    // Resubmit (this will create a NEW log entry via submitPaymentPayload)
    return this.fastApiInvoiceFlowService.submitPaymentPayload(payload);
  }

  async processFastPayment(data: any[]) {
    if (!Array.isArray(data)) {
      this.logger.warn('processFastPayment received invalid data (not array)');
      return;
    }

    this.logger.log(`Processing ${data.length} payment method records...`);

    for (const item of data) {
      try {
        await this.fastApiInvoiceFlowService.processCashioPayment(item);
      } catch (error) {
        this.logger.error(
          `Failed to sync payment method ${item.paymentMethod || item.fop_syscode}: ${error}`,
        );
        // Continue with next item
      }
    }
    this.logger.log('Finished processing payment methods.');
  }

  // Helper Methods

  private createBasePaymentQuery() {
    return this.dailyCashioRepository
      .createQueryBuilder('ds')
      .select([
        'ds.fop_syscode as fop_syscode',
        'ds.docdate as docdate',
        'ds.total_in as total_in',
        'ds.so_code as so_code',
        'ds.branch_code as branch_code_cashio',
        'ds.refno as refno',
        'ds.bank_code as bank_code',
        'ds.period_code as period_code',
        'MAX(s.docDate) as "docDate"',
        'SUM(s.revenue) as revenue',
        'MAX(s.branchCode) as "branchCode"',
        'MAX(s.branchCode) as "boPhan"',
        'MAX(s.maCa) as "maCa"',
        'MAX(s.partnerCode) as "partnerCode"',
      ])
      .innerJoin(Sale, 's', 'ds.so_code = s.docCode')
      .groupBy('ds.id')
      .addGroupBy('ds.fop_syscode')
      .addGroupBy('ds.docdate')
      .addGroupBy('ds.total_in')
      .addGroupBy('ds.so_code')
      .addGroupBy('ds.branch_code')
      .addGroupBy('ds.refno')
      .addGroupBy('ds.bank_code')
      .addGroupBy('ds.period_code')
      .where('ds.fop_syscode != :voucherCode', { voucherCode: 'VOUCHER' })
      .orderBy('ds.docdate', 'DESC');
  }

  private async enrichPaymentResults(
    results: any[],
    includeCash: boolean = false,
  ): Promise<PaymentData[]> {
    if (!results || results.length === 0) {
      return [];
    }

    // Fetch ma_dvcs for all branch codes
    const branchCodes = new Set<string>();
    results.forEach((row: any) => {
      if (row.branch_code_cashio) branchCodes.add(row.branch_code_cashio);
      if (row.branchCode) branchCodes.add(row.branchCode);
    });

    const departmentMap =
      branchCodes.size > 0
        ? await this.loyaltyService.fetchLoyaltyDepartments(
            Array.from(branchCodes),
          )
        : new Map();

    // Fetch payment methods
    const paymentTasks = new Map<
      string,
      { code: string; dvcs: string | null }
    >();

    results.forEach((row: any) => {
      if (!row.fop_syscode || row.fop_syscode === 'VOUCHER') return;

      const saleDept = departmentMap.get(row.branchCode);
      const dvcs = saleDept?.ma_dvcs || null;
      const key = `${row.fop_syscode}|${dvcs}`;
      if (!paymentTasks.has(key)) {
        paymentTasks.set(key, { code: row.fop_syscode, dvcs });
      }
    });

    const paymentMethodMap = new Map<string, any>();
    if (paymentTasks.size > 0) {
      await Promise.all(
        Array.from(paymentTasks.values()).map(async ({ code, dvcs }) => {
          const pm = await this.categoryService.findPaymentMethodByCode(
            code,
            dvcs || '',
          );

          if (pm && pm.documentType === 'Giấy báo có') {
            paymentMethodMap.set(`${code}|${dvcs}`, pm);
          }
        }),
      );
    }

    // Map ma_dvcs and payment info to results
    return results
      .map((row: any) => {
        const saleDept = departmentMap.get(row.branchCode);
        const dvcs = saleDept?.ma_dvcs || null;
        const key = `${row.fop_syscode}|${dvcs}`;
        const paymentMethod = paymentMethodMap.get(key);

        return { row, saleDept, dvcs, paymentMethod };
      })

      .map(({ row, saleDept, dvcs, paymentMethod }) => {
        // Rule: cắt từ dưới lên đén / thì dừng (e.g. VIETCOMBANK/6 -> 6)
        const periodCode = row.period_code
          ? row.period_code.split('/').pop()
          : null;

        return {
          ...row,
          period_code: periodCode,
          ma_dvcs_cashio: paymentMethod?.bankUnit || null,
          ma_dvcs_sale: dvcs,
          company: saleDept?.company || null,
          ma_doi_tac_payment: getSupplierCode(paymentMethod?.maDoiTac) || null,
        };
      });
  }
}
