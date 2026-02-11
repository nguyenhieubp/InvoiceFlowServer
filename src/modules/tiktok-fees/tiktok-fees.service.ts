import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between, Like } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';
import { PlatformFeeImportTiktok } from '../../entities/platform-fee-import-tiktok.entity';

@Injectable()
export class TikTokFeesService {
  private readonly logger = new Logger(TikTokFeesService.name);

  constructor(
    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,
    @InjectRepository(PlatformFeeImportTiktok)
    private tiktokImportRepository: Repository<PlatformFeeImportTiktok>,
  ) { }

  /**
   * Get all TikTok fees with pagination and filters
   */
  async findAll(params: {
    page?: number;
    limit?: number;
    brand?: string;
    search?: string;
    startDate?: string;
    endDate?: string;
  }) {
    const page = params.page || 1;
    const limit = params.limit || 10;
    const skip = (page - 1) * limit;

    const queryBuilder = this.orderFeeRepository
      .createQueryBuilder('orderFee')
      .where('orderFee.platform = :platform', { platform: 'tiktok' });

    // Brand filter
    if (params.brand) {
      queryBuilder.andWhere('orderFee.brand = :brand', {
        brand: params.brand,
      });
    }

    // Search filter
    if (params.search) {
      queryBuilder.andWhere('orderFee.erpOrderCode LIKE :search', {
        search: `%${params.search}%`,
      });
    }

    // Date range filter
    if (params.startDate && params.endDate) {
      queryBuilder.andWhere(
        'orderFee.orderCreatedAt BETWEEN :startDate AND :endDate',
        {
          startDate: params.startDate,
          endDate: params.endDate,
        },
      );
    }

    // Get total count
    const total = await queryBuilder.getCount();

    // Get paginated results
    const data = await queryBuilder
      .orderBy('orderFee.orderCreatedAt', 'DESC')
      .skip(skip)
      .take(limit)
      .getMany();

    // Extract fields from rawData for display (TikTok structure)
    const erpOrderCodes = data.map((f) => f.erpOrderCode).filter(Boolean);
    let importFees: any[] = [];
    if (erpOrderCodes.length > 0) {
      importFees = await this.tiktokImportRepository.find({
        where: {
          maNoiBoSp: Between(
            erpOrderCodes[0],
            erpOrderCodes[erpOrderCodes.length - 1],
          ), // Optimization: range query if sorted, else In()
          // For safety use In if valid
        },
      });
      // Better to use In() for exact matching
      importFees = await this.tiktokImportRepository
        .createQueryBuilder('imp')
        .where('imp.maNoiBoSp IN (:...codes)', { codes: erpOrderCodes })
        .getMany();
    }

    const formattedData = data.map((fee) => {
      const payment = fee.rawData?.payment || {};
      const importFee = importFees.find(
        (imp) => imp.maNoiBoSp === fee.erpOrderCode,
      );

      return {
        id: fee.id,
        brand: fee.brand,
        platform: fee.platform,
        erpOrderCode: fee.erpOrderCode,
        orderCode: fee.orderSn,
        orderCreatedAt: fee.orderCreatedAt,
        syncedAt: fee.syncedAt,
        // Payment fields from rawData.payment
        tax: payment.tax,
        currency: payment.currency,
        subTotal: payment.subTotal,
        shippingFee: payment.shippingFee,
        totalAmount: payment.totalAmount,
        sellerDiscount: payment.sellerDiscount,
        platformDiscount: payment.platformDiscount,
        originalShippingFee: payment.originalShippingFee,
        originalTotalProductPrice: payment.originalTotalProductPrice,
        shippingFeeSellerDiscount: payment.shippingFeeSellerDiscount,
        shippingFeeCofundedDiscount: payment.shippingFeeCofundedDiscount,
        shippingFeePlatformDiscount: payment.shippingFeePlatformDiscount,
        // Restoring missing fee fields for sync (Prefer Import Data)
        tiktokCommission: importFee?.phiHoaHongTraChoTiktok454164020 || payment.tiktokCommission || 0,
        transactionFee: importFee?.phiGiaoDichTyLe5164020 || payment.transactionFee || 0,
        sfpServiceFee: importFee?.phiDichVuSfp6164020 || payment.sfpServiceFee || 0,
        affiliateCommission: importFee?.phiHoaHongTiepThiLienKet150050 || payment.affiliateCommission || 0,
        rawData: fee.rawData, // TikTok has different structure, return full data
      };
    });

    return {
      data: formattedData,
      meta: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  /**
   * Get single TikTok fee by ERP code
   */
  async findByErpCode(erpCode: string) {
    return this.orderFeeRepository.findOne({
      where: {
        erpOrderCode: erpCode,
        platform: 'tiktok',
      },
    });
  }
}
