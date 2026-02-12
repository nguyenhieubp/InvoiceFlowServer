import { Injectable, BadRequestException, Logger } from '@nestjs/common';
import { InjectRepository, InjectDataSource } from '@nestjs/typeorm';
import { DataSource, Repository } from 'typeorm';
import * as XLSX from 'xlsx';
import { PlatformFeeImportShopee } from '../../entities/platform-fee-import-shopee.entity';
import { PlatformFeeImportTiktok } from '../../entities/platform-fee-import-tiktok.entity';
import { PlatformFeeImportLazada } from '../../entities/platform-fee-import-lazada.entity';
import { PlatformFeeMap } from '../../entities/platform-fee-map.entity';
import { ShopeeFee } from '../../entities/shopee-fee.entity';
import { TikTokFee } from '../../entities/tiktok-fee.entity';
import { In } from 'typeorm';
// Using crypto for UUID generation (built-in Node.js)

type Platform = 'shopee' | 'tiktok' | 'lazada';
type PlatformFeeEntity =
  | PlatformFeeImportShopee
  | PlatformFeeImportTiktok
  | PlatformFeeImportLazada;

@Injectable()
export class PlatformFeeImportService {
  private readonly logger = new Logger(PlatformFeeImportService.name);

  constructor(
    @InjectRepository(PlatformFeeImportShopee)
    private readonly shopeeRepo: Repository<PlatformFeeImportShopee>,

    @InjectRepository(PlatformFeeImportTiktok)
    private readonly tiktokRepo: Repository<PlatformFeeImportTiktok>,

    @InjectRepository(PlatformFeeImportLazada)
    private readonly lazadaRepo: Repository<PlatformFeeImportLazada>,

    @InjectRepository(PlatformFeeMap)
    private readonly feeMapRepo: Repository<PlatformFeeMap>,

    @InjectRepository(ShopeeFee)
    private readonly shopeeFeeRepo: Repository<ShopeeFee>,

    @InjectRepository(TikTokFee)
    private readonly tiktokFeeRepo: Repository<TikTokFee>,

    @InjectDataSource()
    private readonly dataSource: DataSource,
  ) { }

  async importFromExcel(
    file: Express.Multer.File,
    platform: Platform,
  ): Promise<{
    total: number;
    success: number;
    failed: number;
    errors: Array<{ row: number; error: string }>;
  }> {
    try {
      const workbook = XLSX.read(file.buffer, {
        type: 'buffer',
        cellDates: false,
        cellNF: false,
        cellText: false,
      });

      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, {
        raw: true,
        defval: null,
      }) as Record<string, any>[];

      if (data.length === 0) {
        throw new BadRequestException('File Excel không có dữ liệu');
      }

      const importBatchId = `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
      const errors: Array<{ row: number; error: string }> = [];
      let success = 0;
      let failed = 0;

      // Normalize header
      const normalizeHeader = (header: string): string => {
        return String(header || '')
          .trim()
          .toLowerCase()
          .replace(/\s+/g, ' ');
      };

      // Get headers from first row
      const firstRow = data[0];
      const headers = Object.keys(firstRow).map(normalizeHeader);

      const repo = this.getRepositoryByPlatform(platform) as Repository<any>;

      // Process each row
      for (let i = 0; i < data.length; i++) {
        const row = data[i];
        const rowNumber = i + 2; // +2 because Excel rows start at 1 and we skip header

        try {
          const entity = this.mapRowToEntity(
            row,
            headers,
            platform,
            importBatchId,
            rowNumber,
          );

          // Apply fee mapping for Lazada rows (map tên phí text -> mã phí hạch toán)
          if (platform === 'lazada') {
            await this.applyLazadaFeeMapping(
              entity as PlatformFeeImportLazada,
            );
          }

          // Determine order id from imported entity
          // NOTE: "cột mã đơn hàng" user refers to the main order code on each platform
          // (Mã Shopee/Tiktok/Lazada => maSan). "Mã đơn hàng hoàn" is refund/return code.
          const orderId: string | null =
            this.toText((entity as any).maSan) ||
            this.toText((entity as any).maDonHangHoan) ||
            null;

          if (!orderId) {
            throw new Error('Không tìm thấy mã đơn hàng trong dòng dữ liệu');
          }

          const exists = await this.checkPancakeOrderExists(orderId);
          if (!exists) {
            throw new Error(
              `Mã đơn hàng "${orderId}" không tồn tại trong bảng platform_fee`,
            );
          }

          await repo.save(entity as any);
          success++;
        } catch (error: any) {
          failed++;
          errors.push({
            row: rowNumber,
            error: error.message || 'Lỗi không xác định',
          });
          this.logger.error(`Error importing row ${rowNumber}: ${error.message}`);
        }
      }

      return {
        total: data.length,
        success,
        failed,
        errors,
      };
    } catch (error: any) {
      this.logger.error(`Error importing file: ${error.message}`, error.stack);
      throw new BadRequestException(
        error.message || 'Lỗi khi import file Excel',
      );
    }
  }

  private getRepositoryByPlatform(platform: Platform) {
    if (platform === 'shopee') return this.shopeeRepo;
    if (platform === 'tiktok') return this.tiktokRepo;
    return this.lazadaRepo;
  }

  private toText(value: any): string | null {
    if (value === null || value === undefined) return null;
    const s = typeof value === 'string' ? value : String(value);
    const trimmed = s.trim();
    return trimmed.length ? trimmed : null;
  }

  private async checkPancakeOrderExists(orderId: string): Promise<boolean> {
    const rows = await this.dataSource.query(
      'SELECT 1 FROM public.platform_fee WHERE pancake_order_id = $1 LIMIT 1',
      [orderId],
    );
    return Array.isArray(rows) && rows.length > 0;
  }

  private normalizeFeeName(name: string | null): string | null {
    if (!name) return null;
    return name
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim();
  }

  private async applyLazadaFeeMapping(
    entity: PlatformFeeImportLazada,
  ): Promise<void> {
    const rawName = entity.tenPhiDoanhThu;
    const normalized = this.normalizeFeeName(rawName);
    if (!normalized) {
      return;
    }

    const mapping = await this.feeMapRepo.findOne({
      where: {
        platform: 'lazada',
        normalizedFeeName: normalized,
        active: true,
      },
    });

    if (mapping) {
      // Ghi đè/điền mã phí hạch toán theo bảng mapping
      entity.maPhiNhanDienHachToan = mapping.accountCode;
    }
  }

  private mapRowToEntity(
    row: Record<string, any>,
    headers: string[],
    platform: Platform,
    importBatchId: string,
    rowNumber: number,
  ): PlatformFeeEntity {
    const entity: PlatformFeeEntity =
      platform === 'shopee'
        ? new PlatformFeeImportShopee()
        : platform === 'tiktok'
          ? new PlatformFeeImportTiktok()
          : new PlatformFeeImportLazada();

    (entity as any).importBatchId = importBatchId;
    (entity as any).rowNumber = rowNumber;

    // Helper to get value by header name (case-insensitive, flexible matching)
    const getValue = (headerPatterns: string[]): any => {
      for (const pattern of headerPatterns) {
        const normalizedPattern = normalizeHeader(pattern);
        const headerIndex = headers.findIndex((h) =>
          h.includes(normalizedPattern) || normalizedPattern.includes(h),
        );
        if (headerIndex !== -1) {
          const originalKey = Object.keys(row)[headerIndex];
          return row[originalKey];
        }
      }
      return null;
    };

    const normalizeHeader = (header: string): string => {
      return String(header || '').trim().toLowerCase().replace(/\s+/g, ' ');
    };

    // Common fields
    if (platform === 'shopee') {
      const e = entity as PlatformFeeImportShopee;
      e.maSan = this.toText(getValue(['mã shopee', 'ma shopee', 'mã shopee']));
      e.maNoiBoSp = this.toText(getValue(['mã nội bộ sp', 'ma noi bo sp']));
      e.ngayDoiSoat = this.parseDate(
        getValue(['ngày đối soát', 'ngay doi soat']),
      ) || null;
      e.maDonHangHoan = this.toText(getValue([
        'mã đơn hàng hoàn',
        'ma don hang hoan',
      ]));
      e.shopPhatHanhTrenSan = this.toText(getValue([
        'shop phát hành trên sàn',
        'shop phat hanh tren san',
      ]));
      e.giaTriGiamGiaCtkm = this.parseDecimal(
        getValue([
          'giá trị giảm giá theo ctkm của mình ban hành',
          'gia tri giam gia theo ctkm cua minh ban hanh',
        ]),
      ) || null;
      e.doanhThuDonHang = this.parseDecimal(
        getValue(['doanh thu đơn hàng', 'doanh thu don hang']),
      ) || null;

      // Shopee specific fees (6 fees) - Map by exact column names
      e.phiCoDinh605MaPhi164020 = this.parseDecimal(
        getValue([
          'phí cố định 6.05% mã phí 164020',
          'phi co dinh 6.05% ma phi 164020',
          'phí cố định 6.05% mã phí 164020',
        ]),
      ) || null;
      e.phiDichVu6MaPhi164020 = this.parseDecimal(
        getValue([
          'phí dịch vụ 6% mã phí 164020',
          'phi dich vu 6% ma phi 164020',
          'phí dịch vụ 6% mã phí 164020',
        ]),
      ) || null;
      e.phiThanhToan5MaPhi164020 = this.parseDecimal(
        getValue([
          'phí thanh toán 5% mã phí 164020',
          'phi thanh toan 5% ma phi 164020',
          'phí thanh toán 5% mã phí 164020',
        ]),
      ) || null;
      e.phiHoaHongTiepThiLienKet21150050 = this.parseDecimal(
        getValue([
          'phí hoa hồng tiếp thị liên kết 21% 150050',
          'phi hoa hong tiep thi lien ket 21% 150050',
          'phí hoa hồng tiếp thị liên kết 21% 150050',
        ]),
      ) || null;
      e.chiPhiDichVuShippingFeeSaver164010 = this.parseDecimal(
        getValue([
          'chi phí dịch vụ shipping fee saver 164010',
          'chi phi dich vu shipping fee saver 164010',
          'chi phí dịch vụ shipping fee saver 164010',
        ]),
      ) || null;
      e.phiPiShipDoMktDangKy164010 = this.parseDecimal(
        getValue([
          'phí pi ship ( do mkt đăng ký) 164010',
          'phi pi ship ( do mkt dang ky) 164010',
          'phí pi ship ( do mkt đăng ký) 164010',
          'phí pi ship do mkt đăng ký 164010',
        ]),
      ) || null;

      e.maCacBenTiepThiLienKet = this.toText(getValue([
        'mã các bên tiếp thị liên kết',
        'ma cac ben tiep thi lien ket',
      ]));
      e.sanTmdt = this.toText(getValue(['sàn tmđt', 'san tmdt', 'sàn tmđt shopee']));

      // MKT columns
      const rowKeysShopee = Object.keys(row);
      const mktColumns = rowKeysShopee
        .map((key) => ({
          originalKey: key,
          normalizedHeader: normalizeHeader(key),
        }))
        .filter(
          (item) =>
            item.normalizedHeader.includes(
              'cột chờ bs nếu mkt đăng ký thêm',
            ) ||
            item.normalizedHeader.includes(
              'cot cho bs neu mkt dang ky them',
            ),
        );

      if (mktColumns.length > 0) {
        e.cotChoBsMkt1 = String(row[mktColumns[0].originalKey] || '');
      }
      if (mktColumns.length > 1) {
        e.cotChoBsMkt2 = String(row[mktColumns[1].originalKey] || '');
      }
      if (mktColumns.length > 2) {
        e.cotChoBsMkt3 = String(row[mktColumns[2].originalKey] || '');
      }
      if (mktColumns.length > 3) {
        e.cotChoBsMkt4 = String(row[mktColumns[3].originalKey] || '');
      }
      if (mktColumns.length > 4) {
        e.cotChoBsMkt5 = String(row[mktColumns[4].originalKey] || '');
      }

      e.boPhan = this.toText(getValue(['bộ phận', 'bo phan']));
    } else if (platform === 'tiktok') {
      const e = entity as PlatformFeeImportTiktok;
      e.maSan = this.toText(getValue(['mã tiktok', 'ma tiktok']));
      e.maNoiBoSp = this.toText(getValue(['mã nội bộ sp', 'ma noi bo sp']));
      e.ngayDoiSoat = this.parseDate(
        getValue(['ngày đối soát', 'ngay doi soat']),
      ) || null;
      e.maDonHangHoan = this.toText(getValue([
        'mã đơn hàng hoàn',
        'ma don hang hoan',
      ]));
      e.shopPhatHanhTrenSan = this.toText(getValue([
        'shop phát hành trên sàn',
        'shop phat hanh tren san',
      ]));
      e.giaTriGiamGiaCtkm = this.parseDecimal(
        getValue([
          'giá trị giảm giá theo ctkm của mình ban hành',
          'gia tri giam gia theo ctkm cua minh ban hanh',
        ]),
      ) || null;
      e.doanhThuDonHang = this.parseDecimal(
        getValue(['doanh thu đơn hàng', 'doanh thu don hang']),
      ) || null;

      // TikTok specific fees (4 fees) - Map by exact column names
      e.phiGiaoDichTyLe5164020 = this.parseDecimal(
        getValue([
          'phí giao dịch tỷ lệ 5% 164020',
          'phi giao dich ty le 5% 164020',
          'phí giao dịch tỷ lệ 5% 164020',
        ]),
      ) || null;
      e.phiHoaHongTraChoTiktok454164020 = this.parseDecimal(
        getValue([
          'phí hoa hồng trả cho tiktok 4.54% 164020',
          'phi hoa hong tra cho tiktok 4.54% 164020',
          'phí hoa hồng trả cho tiktok 4.54% 164020',
        ]),
      ) || null;
      e.phiHoaHongTiepThiLienKet150050 = this.parseDecimal(
        getValue([
          'phí hoa hồng tiếp thị liên kết 150050',
          'phi hoa hong tiep thi lien ket 150050',
          'phí hoa hồng tiếp thị liên kết 150050',
        ]),
      ) || null;
      e.phiDichVuSfp6164020 = this.parseDecimal(
        getValue([
          'phí dịch vụ sfp 6% 164020',
          'phi dich vu sfp 6% 164020',
          'phí dịch vụ sfp 6% 164020',
        ]),
      ) || null;

      e.maCacBenTiepThiLienKet = this.toText(getValue([
        'mã các bên tiếp thị liên kết',
        'ma cac ben tiep thi lien ket',
      ]));
      e.sanTmdt = this.toText(getValue(['sàn tmđt', 'san tmdt', 'sàn tmđt tiktok']));

      // MKT columns
      const rowKeysTiktok = Object.keys(row);
      const mktColumns = rowKeysTiktok
        .map((key) => ({
          originalKey: key,
          normalizedHeader: normalizeHeader(key),
        }))
        .filter(
          (item) =>
            item.normalizedHeader.includes(
              'cột chờ bs nếu mkt đăng ký thêm',
            ) ||
            item.normalizedHeader.includes(
              'cot cho bs neu mkt dang ky them',
            ),
        );

      if (mktColumns.length > 0) {
        e.cotChoBsMkt1 = String(row[mktColumns[0].originalKey] || '');
      }
      if (mktColumns.length > 1) {
        e.cotChoBsMkt2 = String(row[mktColumns[1].originalKey] || '');
      }
      if (mktColumns.length > 2) {
        e.cotChoBsMkt3 = String(row[mktColumns[2].originalKey] || '');
      }
      if (mktColumns.length > 3) {
        e.cotChoBsMkt4 = String(row[mktColumns[3].originalKey] || '');
      }
      if (mktColumns.length > 4) {
        e.cotChoBsMkt5 = String(row[mktColumns[4].originalKey] || '');
      }

      e.boPhan = this.toText(getValue(['bộ phận', 'bo phan']));
    } else if (platform === 'lazada') {
      const e = entity as PlatformFeeImportLazada;
      e.maSan = this.toText(getValue(['mã lazada', 'ma lazada']));
      e.maNoiBoSp = this.toText(getValue(['mã nội bộ sp', 'ma noi bo sp']));
      e.ngayDoiSoat = this.parseDate(
        getValue(['ngày đối soát', 'ngay doi soat']),
      ) || null;
      e.tenPhiDoanhThu = this.toText(getValue([
        'tên phí/ doanh thu đơn hàng',
        'ten phi/ doanh thu don hang',
      ]));
      e.quangCaoTiepThiLienKet = this.toText(getValue([
        'quảng cáo tiếp thị liên kết',
        'quang cao tiep thi lien ket',
      ]));
      e.maDonHangHoan = this.toText(getValue([
        'mã đơn hàng hoàn',
        'ma don hang hoan',
      ]));
      e.maPhiNhanDienHachToan = this.toText(getValue([
        'mã phí để nhận diện hạch toán',
        'ma phi de nhan dien hach toan',
        'mã phí để nhận diện hạch toán',
      ]));
      e.soTienPhi = this.parseDecimal(getValue([
        'số tiền phí',
        'so tien phi',
        'số tiền',
        'so tien',
        'số tiền phí đã đối soát',
        'so tien phi da doi soat',
      ])) || null;
      e.sanTmdt = this.toText(getValue(['sàn tmđt', 'san tmdt', 'sàn tmđt lazada']));
      e.ghiChu = this.toText(getValue(['ghi chú', 'ghi chu']));
      e.boPhan = this.toText(getValue(['bộ phận', 'bo phan']));
    }

    return entity;
  }

  private parseDate(value: any): Date | null {
    if (!value) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'number') {
      // Excel date serial number
      const excelEpoch = new Date(1899, 11, 30);
      return new Date(excelEpoch.getTime() + value * 86400000);
    }
    if (typeof value === 'string') {
      const date = new Date(value);
      return isNaN(date.getTime()) ? null : date;
    }
    return null;
  }

  private parseDecimal(value: any): number | null {
    if (value === null || value === undefined || value === '') return null;
    if (typeof value === 'number') return value;
    if (typeof value === 'string') {
      const cleaned = value.replace(/[^\d.-]/g, '');
      const parsed = parseFloat(cleaned);
      return isNaN(parsed) ? null : parsed;
    }
    return null;
  }

  async findAll(params?: {
    platform?: string;
    page?: number;
    limit?: number;
    startDate?: string;
    endDate?: string;
    search?: string;
  }) {
    if (!params?.platform) {
      throw new BadRequestException('Platform là bắt buộc (shopee | tiktok | lazada)');
    }

    const page = params?.page || 1;
    const limit = params?.limit || 10;
    const skip = (page - 1) * limit;

    const repo = this.getRepositoryByPlatform(params.platform as Platform);
    const qb = repo.createQueryBuilder('pfi');

    if (params?.startDate && params?.endDate) {
      qb.andWhere('pfi.ngayDoiSoat BETWEEN :startDate AND :endDate', {
        startDate: params.startDate,
        endDate: params.endDate,
      });
    }

    if (params?.search) {
      qb.andWhere(
        '(pfi.maSan ILIKE :search OR pfi.maNoiBoSp ILIKE :search OR pfi.maDonHangHoan ILIKE :search)',
        { search: `%${params.search}%` },
      );
    }

    qb.orderBy('pfi.createdAt', 'DESC');
    qb.skip(skip);
    qb.take(limit);

    const [data, total] = await qb.getManyAndCount();

    // Enrich with ERP Order Code and Order Date
    let enrichedData: any[] = data;
    const orderSns = data.map((i: any) => i.maSan).filter(Boolean);

    if (orderSns.length > 0) {
      if (params.platform === 'shopee') {
        const fees = await this.shopeeFeeRepo.find({
          where: { orderSn: In(orderSns) },
          select: ['orderSn', 'erpOrderCode', 'orderCreatedAt'],
        });

        enrichedData = data.map((item: any) => {
          const fee = fees.find((f) => f.orderSn === item.maSan);
          return {
            ...item,
            erpOrderCode: fee?.erpOrderCode || null,
            orderDate: fee?.orderCreatedAt || null,
          };
        });
      } else if (params.platform === 'tiktok') {
        const fees = await this.tiktokFeeRepo.find({
          where: { orderSn: In(orderSns) },
          select: ['orderSn', 'erpOrderCode', 'orderCreatedAt'],
        });

        enrichedData = data.map((item: any) => {
          const fee = fees.find((f) => f.orderSn === item.maSan);
          return {
            ...item,
            erpOrderCode: fee?.erpOrderCode || null,
            orderDate: fee?.orderCreatedAt || null,
          };
        });
      }
    }

    return {
      data: enrichedData,
      meta: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  async generateTemplate(
    platform: Platform,
  ): Promise<Buffer> {
    const workbook = XLSX.utils.book_new();
    let headers: string[] = [];
    let sampleData: any[] = [];

    if (platform === 'shopee') {
      headers = [
        'Mã shopee',
        'Mã nội bộ sp',
        'Ngày đối soát',
        'Mã đơn hàng hoàn',
        'Shop phát hành trên sàn',
        'Giá trị giảm giá theo CTKM của mình ban hành',
        'Doanh thu đơn hàng',
        'Phí cố định 6.05% Mã phí 164020',
        'Phí Dịch Vụ 6% Mã phí 164020',
        'Phí thanh toán 5% Mã phí 164020',
        'Phí hoa hồng Tiếp thị liên kết 21% 150050',
        'Chi phí dịch vụ Shipping Fee Saver 164010',
        'Phí Pi Ship ( Do MKT đăng ký) 164010',
        'Mã các bên tiếp thị liên kết',
        'Sàn TMĐT SHOPEE',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Bộ phận',
      ];
      // Create sample data with all columns
      const sampleRow: any = {};
      headers.forEach((header) => {
        if (header.includes('Cột chờ bs')) {
          sampleRow[header] = '';
        } else if (header.includes('Phí') || header.includes('Chi phí')) {
          sampleRow[header] = 0;
        } else if (header.includes('Giá trị') || header.includes('Doanh thu')) {
          sampleRow[header] = 0;
        } else if (header.includes('Ngày')) {
          sampleRow[header] = '2024-01-01';
        } else {
          sampleRow[header] = 'Mẫu';
        }
      });
      sampleData = [sampleRow];
    } else if (platform === 'tiktok') {
      headers = [
        'Mã Tiktok',
        'Mã nội bộ sp',
        'Ngày đối soát',
        'Mã đơn hàng hoàn',
        'Shop phát hành trên sàn',
        'Giá trị giảm giá theo CTKM của mình ban hành',
        'Doanh thu đơn hàng',
        'Phí giao dịch Tỷ lệ 5% 164020',
        'Phí hoa hồng trả cho Tiktok 4.54% 164020',
        'Phí hoa hồng Tiếp thị liên kết 150050',
        'Phí dịch vụ SFP 6% 164020',
        'Mã các bên tiếp thị liên kết',
        'Sàn TMĐT TIKTOK',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Cột chờ bs nếu MKT đăng ký thêm',
        'Bộ phận',
      ];
      // Create sample data with all columns
      const sampleRowTiktok: any = {};
      headers.forEach((header) => {
        if (header.includes('Cột chờ bs')) {
          sampleRowTiktok[header] = '';
        } else if (header.includes('Phí') || header.includes('Chi phí')) {
          sampleRowTiktok[header] = 0;
        } else if (header.includes('Giá trị') || header.includes('Doanh thu')) {
          sampleRowTiktok[header] = 0;
        } else if (header.includes('Ngày')) {
          sampleRowTiktok[header] = '2024-01-01';
        } else {
          sampleRowTiktok[header] = 'Mẫu';
        }
      });
      sampleData = [sampleRowTiktok];
    } else if (platform === 'lazada') {
      headers = [
        'Mã Lazada',
        'Mã nội bộ sp',
        'Ngày đối soát',
        'Tên phí/ doanh thu đơn hàng',
        'Quảng cáo tiếp thị liên kết',
        'Mã đơn hàng hoàn',
        'MÃ PHÍ ĐỂ NHẬN DIỆN HẠCH TOÁN',
        'Số tiền phí',
        'Sàn TMĐT LAZADA',
        'GHI CHÚ',
        'Bộ phận',
      ];
      // Create sample data with all columns
      const sampleRowLazada: any = {};
      headers.forEach((header) => {
        if (header.includes('Ngày')) {
          sampleRowLazada[header] = '2024-01-01';
        } else if (header.includes('Số tiền')) {
          sampleRowLazada[header] = 0;
        } else if (header.includes('Mã') || header.includes('Tên') || header.includes('Quảng') || header.includes('GHI') || header.includes('Bộ')) {
          sampleRowLazada[header] = 'Mẫu';
        } else {
          sampleRowLazada[header] = '';
        }
      });
      sampleData = [sampleRowLazada];
    }

    const worksheet = XLSX.utils.json_to_sheet(sampleData, {
      header: headers,
    });
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Mẫu');

    const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
    return buffer;
  }

  // Fee Map CRUD methods
  async findAllFeeMaps(params?: {
    platform?: string;
    page?: number;
    limit?: number;
    search?: string;
    active?: boolean;
  }) {
    const page = params?.page || 1;
    const limit = params?.limit || 10;
    const skip = (page - 1) * limit;

    const qb = this.feeMapRepo.createQueryBuilder('fm');

    if (params?.platform) {
      qb.andWhere('fm.platform = :platform', { platform: params.platform });
    }

    if (params?.active !== undefined) {
      qb.andWhere('fm.active = :active', { active: params.active });
    }

    if (params?.search) {
      qb.andWhere(
        '(fm.rawFeeName ILIKE :search OR fm.normalizedFeeName ILIKE :search OR fm.internalCode ILIKE :search OR fm.accountCode ILIKE :search OR fm.systemCode ILIKE :search)',
        { search: `%${params.search}%` },
      );
    }

    qb.orderBy('fm.platform', 'ASC');
    qb.addOrderBy('fm.rawFeeName', 'ASC');
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

  async createFeeMap(data: {
    platform: string;
    rawFeeName: string;
    internalCode: string;
    systemCode?: string;
    accountCode: string;
    description?: string;
    active?: boolean;
  }) {
    const normalized = this.normalizeFeeName(data.rawFeeName);
    if (!normalized) {
      throw new BadRequestException('Tên phí không hợp lệ');
    }

    // Check if already exists
    const existing = await this.feeMapRepo.findOne({
      where: {
        platform: data.platform,
        normalizedFeeName: normalized,
      },
    });

    if (existing) {
      throw new BadRequestException(
        `Mapping đã tồn tại cho platform "${data.platform}" và tên phí "${data.rawFeeName}"`,
      );
    }

    const feeMap = this.feeMapRepo.create({
      platform: data.platform,
      rawFeeName: data.rawFeeName,
      normalizedFeeName: normalized,
      internalCode: data.internalCode,
      systemCode: data.systemCode || null,
      accountCode: data.accountCode,
      description: data.description || null,
      active: data.active !== undefined ? data.active : true,
    });

    return await this.feeMapRepo.save(feeMap);
  }

  async updateFeeMap(
    id: string,
    data: {
      platform?: string;
      rawFeeName?: string;
      internalCode?: string;
      systemCode?: string;
      accountCode?: string;
      description?: string;
      active?: boolean;
    },
  ) {
    const feeMap = await this.feeMapRepo.findOne({ where: { id } });
    if (!feeMap) {
      throw new BadRequestException('Không tìm thấy mapping phí');
    }

    if (data.rawFeeName) {
      const normalized = this.normalizeFeeName(data.rawFeeName);
      if (!normalized) {
        throw new BadRequestException('Tên phí không hợp lệ');
      }

      // Check if another record exists with same platform + normalized name
      const existing = await this.feeMapRepo.findOne({
        where: {
          platform: data.platform || feeMap.platform,
          normalizedFeeName: normalized,
        },
      });

      if (existing && existing.id !== id) {
        throw new BadRequestException(
          `Mapping đã tồn tại cho platform "${data.platform || feeMap.platform}" và tên phí "${data.rawFeeName}"`,
        );
      }

      feeMap.rawFeeName = data.rawFeeName;
      feeMap.normalizedFeeName = normalized;
    }

    if (data.platform) feeMap.platform = data.platform;
    if (data.internalCode !== undefined) feeMap.internalCode = data.internalCode;
    if (data.systemCode !== undefined) feeMap.systemCode = data.systemCode || null;
    if (data.accountCode !== undefined) feeMap.accountCode = data.accountCode;
    if (data.description !== undefined) feeMap.description = data.description;
    if (data.active !== undefined) feeMap.active = data.active;

    return await this.feeMapRepo.save(feeMap);
  }

  async deleteFeeMap(id: string) {
    const feeMap = await this.feeMapRepo.findOne({ where: { id } });
    if (!feeMap) {
      throw new BadRequestException('Không tìm thấy mapping phí');
    }

    await this.feeMapRepo.remove(feeMap);
    return { message: 'Xóa mapping phí thành công' };
  }
}
