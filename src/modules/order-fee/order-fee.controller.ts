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
  ) {
    const skip = (page - 1) * limit;
    const queryBuilder = this.orderFeeRepository.createQueryBuilder('orderFee');

    // Filter by brand
    if (brand) {
      queryBuilder.andWhere('orderFee.brand = :brand', { brand });
    }

    // Search by ERP order code
    if (search) {
      queryBuilder.andWhere('orderFee.erpOrderCode ILIKE :search', {
        search: `%${search}%`,
      });
    }

    // Filter by date range (syncedAt)
    if (startDate) {
      queryBuilder.andWhere('orderFee.syncedAt >= :startDate', { startDate });
    }
    if (endDate) {
      queryBuilder.andWhere('orderFee.syncedAt <= :endDate', { endDate });
    }

    // Order by syncedAt descending
    queryBuilder.orderBy('orderFee.syncedAt', 'DESC');

    // Get total count
    const total = await queryBuilder.getCount();

    // Get paginated data
    const data = await queryBuilder.skip(skip).take(limit).getMany();

    return {
      data,
      meta: {
        total,
        page: Number(page),
        limit: Number(limit),
        totalPages: Math.ceil(total / limit),
      },
    };
  }
}
