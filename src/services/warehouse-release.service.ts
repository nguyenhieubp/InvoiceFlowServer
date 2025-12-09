import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Like, Between } from 'typeorm';
import { WarehouseRelease } from '../entities/warehouse-release.entity';

@Injectable()
export class WarehouseReleaseService {
  private readonly logger = new Logger(WarehouseReleaseService.name);

  constructor(
    @InjectRepository(WarehouseRelease)
    private warehouseReleaseRepository: Repository<WarehouseRelease>,
  ) {}

  async findAll(options: {
    page?: number;
    limit?: number;
    status?: number;
    docCode?: string;
    maKh?: string;
    tenKh?: string;
    maDvcs?: string;
    startDate?: Date;
    endDate?: Date;
  }) {
    const { page = 1, limit = 50, status, docCode, maKh, tenKh, maDvcs, startDate, endDate } = options;

    const query = this.warehouseReleaseRepository.createQueryBuilder('warehouseRelease');

    if (status !== undefined) {
      query.andWhere('warehouseRelease.status = :status', { status });
    }

    if (docCode) {
      query.andWhere('warehouseRelease.docCode LIKE :docCode', { docCode: `%${docCode}%` });
    }

    if (maKh) {
      query.andWhere('warehouseRelease.maKh LIKE :maKh', { maKh: `%${maKh}%` });
    }

    if (tenKh) {
      query.andWhere('warehouseRelease.tenKh LIKE :tenKh', { tenKh: `%${tenKh}%` });
    }

    if (maDvcs) {
      query.andWhere('warehouseRelease.maDvcs = :maDvcs', { maDvcs });
    }

    if (startDate && endDate) {
      query.andWhere('warehouseRelease.ngayCt BETWEEN :startDate AND :endDate', {
        startDate,
        endDate,
      });
    } else if (startDate) {
      query.andWhere('warehouseRelease.ngayCt >= :startDate', { startDate });
    } else if (endDate) {
      query.andWhere('warehouseRelease.ngayCt <= :endDate', { endDate });
    }

    query.orderBy('warehouseRelease.createdAt', 'DESC');
    query.skip((page - 1) * limit);
    query.take(limit);

    const [items, total] = await query.getManyAndCount();

    return {
      items,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
        hasNext: (page - 1) * limit + limit < total,
        hasPrev: page > 1,
      },
    };
  }

  async findOne(id: string) {
    return this.warehouseReleaseRepository.findOne({ where: { id } });
  }

  async findByDocCode(docCode: string) {
    return this.warehouseReleaseRepository.findOne({ where: { docCode } });
  }

  async getStatistics(options: {
    startDate?: Date;
    endDate?: Date;
    maDvcs?: string;
  }) {
    const { startDate, endDate, maDvcs } = options;

    const query = this.warehouseReleaseRepository.createQueryBuilder('warehouseRelease');

    if (maDvcs) {
      query.andWhere('warehouseRelease.maDvcs = :maDvcs', { maDvcs });
    }

    if (startDate && endDate) {
      query.andWhere('warehouseRelease.ngayCt BETWEEN :startDate AND :endDate', {
        startDate,
        endDate,
      });
    } else if (startDate) {
      query.andWhere('warehouseRelease.ngayCt >= :startDate', { startDate });
    } else if (endDate) {
      query.andWhere('warehouseRelease.ngayCt <= :endDate', { endDate });
    }

    const total = await query.getCount();
    const success = await query.clone().andWhere('warehouseRelease.status = :status', { status: 1 }).getCount();
    const failed = await query.clone().andWhere('warehouseRelease.status = :status', { status: 0 }).getCount();

    return {
      total,
      success,
      failed,
      successRate: total > 0 ? ((success / total) * 100).toFixed(2) : '0.00',
    };
  }
}

