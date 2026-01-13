import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, FindOptionsWhere, Like } from 'typeorm';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';

@Injectable()
export class FastApiInvoiceService {
  private readonly logger = new Logger(FastApiInvoiceService.name);

  constructor(
    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
  ) {}

  /**
   * Lấy danh sách bảng kê hóa đơn với pagination và filter
   */
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
    const {
      page = 1,
      limit = 50,
      status,
      docCode,
      maKh,
      tenKh,
      maDvcs,
      startDate,
      endDate,
    } = options;

    const query = this.fastApiInvoiceRepository.createQueryBuilder('invoice');

    // Apply filters
    if (status !== undefined) {
      query.andWhere('invoice.status = :status', { status });
    }

    if (docCode) {
      query.andWhere('invoice.docCode LIKE :docCode', {
        docCode: `%${docCode}%`,
      });
    }

    if (maKh) {
      query.andWhere('invoice.maKh LIKE :maKh', { maKh: `%${maKh}%` });
    }

    if (tenKh) {
      query.andWhere('invoice.tenKh LIKE :tenKh', { tenKh: `%${tenKh}%` });
    }

    if (maDvcs) {
      query.andWhere('invoice.maDvcs = :maDvcs', { maDvcs });
    }

    if (startDate) {
      // Set thời gian về đầu ngày (00:00:00)
      const startDateNormalized = new Date(startDate);
      startDateNormalized.setHours(0, 0, 0, 0);
      query.andWhere('invoice.ngayCt >= :startDate', {
        startDate: startDateNormalized,
      });
    }

    if (endDate) {
      // Set thời gian về cuối ngày (23:59:59.999) để lấy tất cả invoice trong ngày đó
      const endDateNormalized = new Date(endDate);
      endDateNormalized.setHours(23, 59, 59, 999);
      query.andWhere('invoice.ngayCt <= :endDate', {
        endDate: endDateNormalized,
      });
    }

    // Order by created date descending
    query.orderBy('invoice.createdAt', 'DESC');

    // Pagination
    const skip = (page - 1) * limit;
    query.skip(skip).take(limit);

    const [items, total] = await query.getManyAndCount();

    return {
      items,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
        hasNext: skip + limit < total,
        hasPrev: page > 1,
      },
    };
  }

  /**
   * Lấy chi tiết một hóa đơn theo ID
   */
  async findOne(id: string): Promise<FastApiInvoice> {
    const invoice = await this.fastApiInvoiceRepository.findOne({
      where: { id },
    });

    if (!invoice) {
      throw new NotFoundException(`FastApiInvoice with ID ${id} not found`);
    }

    return invoice;
  }

  /**
   * Lấy hóa đơn theo docCode
   */
  async findByDocCode(docCode: string): Promise<FastApiInvoice | null> {
    return await this.fastApiInvoiceRepository.findOne({
      where: { docCode },
    });
  }

  /**
   * Lấy thống kê
   */
  async getStatistics(options?: {
    startDate?: Date;
    endDate?: Date;
    maDvcs?: string;
  }) {
    const query = this.fastApiInvoiceRepository.createQueryBuilder('invoice');

    if (options?.startDate) {
      // Set thời gian về đầu ngày (00:00:00)
      const startDateNormalized = new Date(options.startDate);
      startDateNormalized.setHours(0, 0, 0, 0);
      query.andWhere('invoice.ngayCt >= :startDate', {
        startDate: startDateNormalized,
      });
    }

    if (options?.endDate) {
      // Set thời gian về cuối ngày (23:59:59.999) để lấy tất cả invoice trong ngày đó
      const endDateNormalized = new Date(options.endDate);
      endDateNormalized.setHours(23, 59, 59, 999);
      query.andWhere('invoice.ngayCt <= :endDate', {
        endDate: endDateNormalized,
      });
    }

    if (options?.maDvcs) {
      query.andWhere('invoice.maDvcs = :maDvcs', { maDvcs: options.maDvcs });
    }

    const total = await query.getCount();
    const success = await query
      .clone()
      .andWhere('invoice.status = :status', { status: 1 })
      .getCount();
    const failed = await query
      .clone()
      .andWhere('invoice.status = :status', { status: 0 })
      .getCount();

    return {
      total,
      success,
      failed,
      successRate: total > 0 ? ((success / total) * 100).toFixed(2) : '0.00',
    };
  }

  /**
   * Lấy danh sách invoice thất bại theo khoảng thời gian (để đồng bộ)
   */
  async getFailedInvoicesByDateRange(options: {
    startDate: Date;
    endDate: Date;
    maDvcs?: string;
  }): Promise<FastApiInvoice[]> {
    const query = this.fastApiInvoiceRepository.createQueryBuilder('invoice');

    // Chỉ lấy invoice thất bại
    query.andWhere('invoice.status = :status', { status: 0 });

    // Filter theo khoảng thời gian (chuẩn xác)
    const startDateNormalized = new Date(options.startDate);
    startDateNormalized.setHours(0, 0, 0, 0);
    query.andWhere('invoice.ngayCt >= :startDate', {
      startDate: startDateNormalized,
    });

    const endDateNormalized = new Date(options.endDate);
    endDateNormalized.setHours(23, 59, 59, 999);
    query.andWhere('invoice.ngayCt <= :endDate', {
      endDate: endDateNormalized,
    });

    // Filter theo maDvcs nếu có
    if (options.maDvcs) {
      query.andWhere('invoice.maDvcs = :maDvcs', { maDvcs: options.maDvcs });
    }

    // Order by ngayCt để đồng bộ theo thứ tự thời gian
    query.orderBy('invoice.ngayCt', 'ASC');

    return await query.getMany();
  }
}
