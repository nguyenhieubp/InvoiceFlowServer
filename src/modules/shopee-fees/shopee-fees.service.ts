import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between, Like } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';

@Injectable()
export class ShopeeFeesService {
  private readonly logger = new Logger(ShopeeFeesService.name);

  constructor(
    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,
  ) {}

  /**
   * Get all Shopee fees with pagination and filters
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
      .where('orderFee.platform = :platform', { platform: 'shopee' });

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

    // Extract fields from rawData for display
    const formattedData = data.map((fee) => ({
      id: fee.id,
      brand: fee.brand,
      platform: fee.platform,
      erpOrderCode: fee.erpOrderCode,
      orderCode: fee.rawData?.raw_data?.order_sn || fee.rawData?.order_sn,
      voucherShop: fee.rawData?.raw_data?.voucher_from_seller || 0,
      commissionFee: fee.rawData?.raw_data?.commission_fee || 0,
      serviceFee: fee.rawData?.raw_data?.service_fee || 0,
      paymentFee: fee.rawData?.raw_data?.payment_fee || 0,
      orderCreatedAt: fee.orderCreatedAt,
      syncedAt: fee.syncedAt,
    }));

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
   * Get single Shopee fee by ERP code
   */
  async findByErpCode(erpCode: string) {
    return this.orderFeeRepository.findOne({
      where: {
        erpOrderCode: erpCode,
        platform: 'shopee',
      },
    });
  }
}
