import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between, Like } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';

@Injectable()
export class TikTokFeesService {
  private readonly logger = new Logger(TikTokFeesService.name);

  constructor(
    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,
  ) {}

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
    const formattedData = data.map((fee) => {
      const payment = fee.rawData?.payment || {};
      return {
        id: fee.id,
        brand: fee.brand,
        platform: fee.platform,
        erpOrderCode: fee.erpOrderCode,
        orderCode: fee.rawData?.order_id || fee.rawData?.id,
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
