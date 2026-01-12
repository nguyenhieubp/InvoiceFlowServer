import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { PlatformFee } from '../../entities/platform-fee.entity';
import { CreatePlatformFeeDto } from './dto/create-platform-fee.dto';
import { UpdatePlatformFeeDto } from './dto/update-platform-fee.dto';

@Injectable()
export class PlatformFeeService {
  constructor(
    @InjectRepository(PlatformFee)
    private readonly platformFeeRepository: Repository<PlatformFee>,
  ) {}

  async create(createPlatformFeeDto: CreatePlatformFeeDto) {
    const platformFee = this.platformFeeRepository.create({
      ...createPlatformFeeDto,
      syncedAt: new Date(),
    });
    return this.platformFeeRepository.save(platformFee);
  }

  async findAll(query?: {
    brand?: string;
    page?: number;
    limit?: number;
    search?: string;
  }) {
    const page = query?.page || 1;
    const limit = query?.limit || 10;
    const skip = (page - 1) * limit;

    const where: any = {};
    if (query?.brand) {
      where.brand = query.brand;
    }

    if (query?.search) {
      const search = `%${query.search}%`;
      // Use raw query builder or find options for ILIKE
      // Since typeorm find options is simpler for basic conditions
      // But ILIKE is postgres specific. Let's use Raw or ILIKE operator if available
      // Or simply use Like if case insensitive collation is not guaranteed, but usually standard Like is case sensitive.
      // Nest TypeORM usually supports ILIKE operator from recent versions.
      // If ILIKE is not importable directly, we can use Raw.
      // To keep it simple and safe:
      /*  
          where.erpOrderCode = ILIKE(`%${query.search}%`) 
      */
      // However, ILIKE import might be needed. Let's use simpler approach with Raw if needed or just use typeorm ILIKE
      // Let's trying importing ILIKE first? No, easier to use Raw for safety across versions
      /* where.erpOrderCode = Raw(alias => `${alias} ILIKE '${search}'`) */
    }

    // Actually, let's use query builder for flexibility
    const qb = this.platformFeeRepository.createQueryBuilder('pf');

    if (query?.brand) {
      qb.andWhere('pf.brand = :brand', { brand: query.brand });
    }

    if (query?.search) {
      qb.andWhere('pf.erpOrderCode ILIKE :search', {
        search: `%${query.search}%`,
      });
    }

    qb.orderBy('pf.syncedAt', 'DESC');
    qb.skip(skip);
    qb.take(limit);

    const [data, total] = await qb.getManyAndCount();

    return {
      data,
      meta: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  async findOne(id: string) {
    const platformFee = await this.platformFeeRepository.findOne({
      where: { id },
    });
    if (!platformFee) {
      throw new NotFoundException(`PlatformFee with ID ${id} not found`);
    }
    return platformFee;
  }

  async update(id: string, updatePlatformFeeDto: UpdatePlatformFeeDto) {
    const platformFee = await this.findOne(id);
    this.platformFeeRepository.merge(platformFee, updatePlatformFeeDto);
    return this.platformFeeRepository.save(platformFee);
  }

  async remove(id: string) {
    const platformFee = await this.findOne(id);
    return this.platformFeeRepository.remove(platformFee);
  }
}
