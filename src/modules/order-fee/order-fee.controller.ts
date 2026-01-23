import { Controller, Get, Query } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';

@Controller('order-fees')
export class OrderFeeController {
  constructor(
    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,
  ) {}

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
}
