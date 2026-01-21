import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull, Not, Like, Equal } from 'typeorm';
import { StockTransfer } from '../../entities/stock-transfer.entity';

@Injectable()
export class StockTransferService {
  constructor(
    @InjectRepository(StockTransfer)
    private stockTransferRepo: Repository<StockTransfer>,
  ) {}

  async findMissingMaterial(
    page: number = 1,
    limit: number = 10,
    search?: string,
  ) {
    const queryBuilder = this.stockTransferRepo.createQueryBuilder('st');

    // Filter: soCode is not null/empty AND materialCode is null/empty
    queryBuilder
      .where('st.soCode IS NOT NULL')
      .andWhere("st.soCode != ''")
      .andWhere(
        '(st.materialCode IS NULL OR st.materialCode = :empty OR TRIM(st.materialCode) = :empty)',
        {
          empty: '',
        },
      )
      .andWhere('st.itemCode != :excludedItemCode', {
        excludedItemCode: 'TRUTONKEEP',
      });

    if (search) {
      const searchTerm = search.trim().toLowerCase();
      queryBuilder.andWhere('LOWER(st.soCode) LIKE :search', {
        search: `%${searchTerm}%`,
      });
    }

    // Default sorting
    queryBuilder.orderBy('st.transDate', 'DESC');

    // Debug logging
    // console.log('Query:', queryBuilder.getSql());
    // console.log('Params:', queryBuilder.getParameters());

    const skip = (page - 1) * limit;
    const [items, total] = await queryBuilder
      .skip(skip)
      .take(limit)
      .getManyAndCount();

    const totalPages = Math.ceil(total / limit);

    return {
      items,
      meta: {
        totalItems: total,
        itemCount: items.length,
        itemsPerPage: limit,
        totalPages,
        currentPage: page,
      },
    };
  }

  async updateMaterialCode(id: string, materialCode: string) {
    const transfer = await this.stockTransferRepo.findOne({ where: { id } });
    if (!transfer) {
      throw new NotFoundException(`Stock transfer with ID ${id} not found`);
    }

    transfer.materialCode = materialCode;
    return this.stockTransferRepo.save(transfer);
  }
}
