import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between, ILike, In } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';
import { ShopeeFee } from '../../entities/shopee-fee.entity';
import { TikTokFee } from '../../entities/tiktok-fee.entity';
import { PlatformFeeImportShopee } from '../../entities/platform-fee-import-shopee.entity';
import { PlatformFeeImportTiktok } from '../../entities/platform-fee-import-tiktok.entity';
import { Sale } from '../../entities/sale.entity';

@Injectable()
export class OrderFeeService {
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

  async findShopeeFees(
    page: number = 1,
    limit: number = 10,
    brand?: string,
    search?: string,
    startDate?: string,
    endDate?: string,
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
      total,
      page: Number(page),
      limit: Number(limit),
      totalPages: Math.ceil(total / limit),
    };
  }

  async findTikTokFees(
    page: number = 1,
    limit: number = 10,
    brand?: string,
    search?: string,
    startDate?: string,
    endDate?: string,
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
    }

    const mergedData = data.map((fee) => {
      const importFee = importFees.find(
        (imp) => imp.maNoiBoSp === fee.erpOrderCode,
      );

      return {
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
        tiktokCommission: Number(importFee?.phiHoaHongTraChoTiktok454164020 || 0),
        transactionFee: Number(importFee?.phiGiaoDichTyLe5164020 || 0),
        sfpServiceFee: Number(importFee?.phiDichVuSfp6164020 || 0),
        affiliateCommission: Number(importFee?.phiHoaHongTiepThiLienKet150050 || 0),
      };
    });

    return {
      data: mergedData,
      total,
      page: Number(page),
      limit: Number(limit),
      totalPages: Math.ceil(total / limit),
    };
  }
}
