import { Controller, Get, Query } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';
import { ShopeeFee } from '../../entities/shopee-fee.entity';
import { TikTokFee } from '../../entities/tiktok-fee.entity';
import { Between, ILike, In } from 'typeorm';
import { PlatformFeeImportShopee } from '../../entities/platform-fee-import-shopee.entity';
import { PlatformFeeImportTiktok } from '../../entities/platform-fee-import-tiktok.entity';
import { Sale } from '../../entities/sale.entity';

@Controller('order-fees')
export class OrderFeeController {
  constructor(
    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,

    @InjectRepository(ShopeeFee)
    private shopeeFeeRepository: Repository<ShopeeFee>,

    @InjectRepository(TikTokFee)
    private tiktokFeeRepository: Repository<TikTokFee>,

    @InjectRepository(PlatformFeeImportShopee)
    private platformFeeImportShopeeRepo: Repository<PlatformFeeImportShopee>,

    @InjectRepository(PlatformFeeImportTiktok)
    private platformFeeImportTiktokRepo: Repository<PlatformFeeImportTiktok>,

    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
  ) { }

  /**
   * GET /order-fees
   * Lấy danh sách order fees với pagination và filters
   */
  @Get()
  async findAll(
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 10,
    @Query('brand') brand?: string,
    @Query('search') search?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
    @Query('platform') platform?: string, // [NEW] Platform filter
  ) {
    // Default brand = 'menard'
    if (!brand) brand = 'menard';

    // Default date range = last 30 days
    if (!startDate || !endDate) {
      const now = new Date();
      if (!endDate) {
        endDate = now.toISOString().split('T')[0];
      }
      if (!startDate) {
        const thirtyDaysAgo = new Date(now);
        thirtyDaysAgo.setDate(now.getDate() - 30);
        startDate = thirtyDaysAgo.toISOString().split('T')[0];
      }
    }
    const skip = (page - 1) * limit;
    const queryBuilder = this.orderFeeRepository.createQueryBuilder('orderFee');

    // Filter by brand
    if (brand) {
      queryBuilder.andWhere('orderFee.brand = :brand', { brand });
    }

    // Filter by platform
    if (platform) {
      queryBuilder.andWhere('orderFee.platform = :platform', { platform });
    }

    // Search by ERP order code
    if (search) {
      queryBuilder.andWhere('orderFee.erpOrderCode ILIKE :search', {
        search: `%${search}%`,
      });
    }

    // Filter by Order Date
    if (startDate) {
      const start = new Date(startDate);
      start.setHours(0, 0, 0, 0); // Start of day (Local)
      queryBuilder.andWhere('orderFee.orderCreatedAt >= :start', {
        start,
      });
    }
    if (endDate) {
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999); // End of day (Local)
      queryBuilder.andWhere('orderFee.orderCreatedAt <= :end', {
        end,
      });
    }

    // Order by Order Date descending
    queryBuilder.orderBy('orderFee.orderCreatedAt', 'DESC');

    // Filter chỉ lấy đơn bên bán (is_customer_pay = false trong rawData)
    // queryBuilder.andWhere("orderFee.rawData ->> 'is_customer_pay' = 'false'");

    // Get data and total count in one go
    const [data, total] = await queryBuilder
      .skip(skip)
      .take(limit)
      .getManyAndCount();

    // Map data to extract fields
    // Map data to extract fields
    const mappedData = data.map((item) => {
      const raw = item.rawData || {};
      const details = raw.raw_data || {};
      const isTikTok = item.platform === 'tiktok';

      const result: any = { ...item };
      delete result.rawData; // Explicitly remove rawData

      // Mapping logic based on Platform
      if (isTikTok) {
        return {
          ...result,
          orderCode: raw.order_sn || raw.id, // User: order_sn (or id)
          orderCreatedAt: raw.create_time || raw.created_at,
          voucherShop: raw.payment?.sellerDiscount || 0, // User: payment.sellerDiscount
          commissionFee: 0, // User: Chưa có trong JSON này
          serviceFee: 0, // User: Chưa có trong JSON này
          paymentFee: 0, // User: Chưa có trong JSON này
        };
      }

      // Shopee (Default)
      return {
        ...result,
        orderCode: raw.order_sn,
        orderCreatedAt: raw.created_at,
        voucherShop: details.voucher_from_seller || 0,
        commissionFee: details.commission_fee || 0,
        serviceFee: details.service_fee || 0,
        paymentFee: details.credit_card_transaction_fee || 0,
      };
    });

    return {
      data: mappedData,
      meta: {
        total,
        page: Number(page),
        limit: Number(limit),
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  /**
   * GET /order-fees/shopee
   */
  @Get('shopee')
  async findShopeeFees(
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 10,
    @Query('brand') brand?: string,
    @Query('search') search?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    const skip = (page - 1) * limit;
    const baseWhere: any = {};

    if (brand) baseWhere.brand = brand;

    if (startDate && endDate) {
      const start = new Date(startDate);
      start.setHours(0, 0, 0, 0);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      baseWhere.orderCreatedAt = Between(start, end);
    }

    let where: any = baseWhere;
    if (search) {
      where = [
        { ...baseWhere, erpOrderCode: ILike(`%${search}%`) },
        { ...baseWhere, orderSn: ILike(`%${search}%`) },
      ];
    }

    const [data, total] = await this.shopeeFeeRepository.findAndCount({
      where,
      skip,
      take: limit,
      order: { orderCreatedAt: 'DESC' },
    });

    // Merge with Imported Data to get missing fees (shippingFeeSaver etc)
    const orderSns = data.map((i) => i.orderSn).filter(Boolean);
    let importedFees: PlatformFeeImportShopee[] = [];
    if (orderSns.length > 0) {
      importedFees = await this.platformFeeImportShopeeRepo.find({
        where: { maSan: In(orderSns) },
      });
    }

    const mergedData = data.map((item) => {
      const imported = importedFees.find((imp) => imp.maSan === item.orderSn);
      return {
        ...item,
        shippingFeeSaver: imported?.chiPhiDichVuShippingFeeSaver164010 || 0,
        marketingFee: imported?.phiPiShipDoMktDangKy164010 || 0,
        affiliateFee: imported?.phiHoaHongTiepThiLienKet21150050 || 0,
      };
    });

    // Valid ERP Order Codes
    const erpOrderCodes = mergedData.map((i) => i.erpOrderCode).filter(Boolean);

    // Fetch Invoice Date from Sales
    let salesMap = new Map<string, Date>();
    if (erpOrderCodes.length > 0) {
      const sales = await this.saleRepository
        .createQueryBuilder('sales')
        .select(['sales.docCode', 'sales.docDate'])
        .where('sales.docCode IN (:...code)', { code: erpOrderCodes })
        .distinctOn(['sales.docCode'])
        .getMany();

      sales.forEach((s) => {
        if (s.docCode && s.docDate) {
          salesMap.set(s.docCode, s.docDate);
        }
      });
    }

    const finalData = mergedData.map((item) => {
      return {
        ...item,
        invoiceDate: salesMap.get(item.erpOrderCode) || null,
      };
    });

    return {
      data: finalData,
      meta: {
        total,
        page: Number(page),
        limit: Number(limit),
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  /**
   * GET /order-fees/tiktok
   */
  @Get('tiktok')
  async findTikTokFees(
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 10,
    @Query('brand') brand?: string,
    @Query('search') search?: string,
    @Query('startDate') startDate?: string,
    @Query('endDate') endDate?: string,
  ) {
    const skip = (page - 1) * limit;
    const where: any = {};

    if (brand) where.brand = brand;
    if (search) where.erpOrderCode = ILike(`%${search}%`);

    if (startDate && endDate) {
      const start = new Date(startDate);
      start.setHours(0, 0, 0, 0);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      where.orderCreatedAt = Between(start, end);
    }

    const [data, total] = await this.tiktokFeeRepository.findAndCount({
      where,
      skip,
      take: limit,
      order: { orderCreatedAt: 'DESC' },
    });

    // Merge with Imported Data
    const erpOrderCodes = data.map((f) => f.erpOrderCode).filter(Boolean);
    let importFees: PlatformFeeImportTiktok[] = [];
    if (erpOrderCodes.length > 0) {
      importFees = await this.platformFeeImportTiktokRepo.find({
        where: { maNoiBoSp: In(erpOrderCodes) },
      });
      console.log(`[TikTokFees] Found ${importFees.length} import records for ${erpOrderCodes.length} orders`);
    }

    const mergedData = data.map((fee) => {
      const importFee = importFees.find(
        (imp) => imp.maNoiBoSp === fee.erpOrderCode,
      );

      // Explicitly construct object to ensure keys are present and use camelCase
      const mapped = {
        id: fee.id,
        brand: fee.brand,
        platform: fee.platform,
        erpOrderCode: fee.erpOrderCode,
        orderSn: fee.orderSn,
        orderStatus: fee.orderStatus,
        orderCreatedAt: fee.orderCreatedAt,
        syncedAt: fee.syncedAt,
        tax: Number(fee.tax),
        currency: fee.currency,
        subTotal: Number(fee.subTotal),
        shippingFee: Number(fee.shippingFee),
        totalAmount: Number(fee.totalAmount),
        sellerDiscount: Number(fee.sellerDiscount),
        platformDiscount: Number(fee.platformDiscount),
        originalShippingFee: Number(fee.originalShippingFee),
        originalTotalProductPrice: Number(fee.originalTotalProductPrice),
        shippingFeeSellerDiscount: Number(fee.shippingFeeSellerDiscount),
        shippingFeeCofundedDiscount: Number(fee.shippingFeeCofundedDiscount),
        shippingFeePlatformDiscount: Number(fee.shippingFeePlatformDiscount),
        createdAt: fee.createdAt,
        updatedAt: fee.updatedAt,

        // Fee fields from Import
        tiktokCommission: Number(importFee?.phiHoaHongTraChoTiktok454164020 || 0),
        transactionFee: Number(importFee?.phiGiaoDichTyLe5164020 || 0),
        sfpServiceFee: Number(importFee?.phiDichVuSfp6164020 || 0),
        affiliateCommission: Number(importFee?.phiHoaHongTiepThiLienKet150050 || 0),
      };
      return mapped;
    });

    return {
      data: mergedData,
      meta: {
        total,
        page: Number(page),
        limit: Number(limit),
        totalPages: Math.ceil(total / limit),
      },
    };
  }
}
