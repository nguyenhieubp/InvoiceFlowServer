import { Injectable, BadRequestException, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import * as XLSX from 'xlsx';
import { PlatformFeeImport } from '../../entities/platform-fee-import.entity';
// Using crypto for UUID generation (built-in Node.js)

@Injectable()
export class PlatformFeeImportService {
  private readonly logger = new Logger(PlatformFeeImportService.name);

  constructor(
    @InjectRepository(PlatformFeeImport)
    private readonly platformFeeImportRepository: Repository<PlatformFeeImport>,
  ) {}

  async importFromExcel(
    file: Express.Multer.File,
    platform: 'shopee' | 'tiktok' | 'lazada',
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

      // Process each row
      for (let i = 0; i < data.length; i++) {
        const row = data[i];
        const rowNumber = i + 2; // +2 because Excel rows start at 1 and we skip header

        try {
          const entity = this.mapRowToEntity(row, headers, platform, importBatchId, rowNumber);
          await this.platformFeeImportRepository.save(entity);
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

  private mapRowToEntity(
    row: Record<string, any>,
    headers: string[],
    platform: 'shopee' | 'tiktok' | 'lazada',
    importBatchId: string,
    rowNumber: number,
  ): PlatformFeeImport {
    const entity = new PlatformFeeImport();
    entity.platform = platform;
    entity.importBatchId = importBatchId;
    entity.rowNumber = rowNumber;

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
      entity.maSan = getValue(['mã shopee', 'ma shopee', 'mã shopee']);
      entity.maNoiBoSp = getValue(['mã nội bộ sp', 'ma noi bo sp']);
      entity.ngayDoiSoat = this.parseDate(
        getValue(['ngày đối soát', 'ngay doi soat']),
      ) || null;
      entity.maDonHangHoan = getValue([
        'mã đơn hàng hoàn',
        'ma don hang hoan',
      ]);
      entity.shopPhatHanhTrenSan = getValue([
        'shop phát hành trên sàn',
        'shop phat hanh tren san',
      ]);
      entity.giaTriGiamGiaCtkm = this.parseDecimal(
        getValue([
          'giá trị giảm giá theo ctkm của mình ban hành',
          'gia tri giam gia theo ctkm cua minh ban hanh',
        ]),
      ) || null;
      entity.doanhThuDonHang = this.parseDecimal(
        getValue(['doanh thu đơn hàng', 'doanh thu don hang']),
      ) || null;

      // Shopee specific fees (6 fees) - Map by exact column names
      entity.phiCoDinh605MaPhi164020 = this.parseDecimal(
        getValue([
          'phí cố định 6.05% mã phí 164020',
          'phi co dinh 6.05% ma phi 164020',
          'phí cố định 6.05% mã phí 164020',
        ]),
      ) || null;
      entity.phiDichVu6MaPhi164020 = this.parseDecimal(
        getValue([
          'phí dịch vụ 6% mã phí 164020',
          'phi dich vu 6% ma phi 164020',
          'phí dịch vụ 6% mã phí 164020',
        ]),
      ) || null;
      entity.phiThanhToan5MaPhi164020 = this.parseDecimal(
        getValue([
          'phí thanh toán 5% mã phí 164020',
          'phi thanh toan 5% ma phi 164020',
          'phí thanh toán 5% mã phí 164020',
        ]),
      ) || null;
      entity.phiHoaHongTiepThiLienKet21150050 = this.parseDecimal(
        getValue([
          'phí hoa hồng tiếp thị liên kết 21% 150050',
          'phi hoa hong tiep thi lien ket 21% 150050',
          'phí hoa hồng tiếp thị liên kết 21% 150050',
        ]),
      ) || null;
      entity.chiPhiDichVuShippingFeeSaver164010 = this.parseDecimal(
        getValue([
          'chi phí dịch vụ shipping fee saver 164010',
          'chi phi dich vu shipping fee saver 164010',
          'chi phí dịch vụ shipping fee saver 164010',
        ]),
      ) || null;
      entity.phiPiShipDoMktDangKy164010 = this.parseDecimal(
        getValue([
          'phí pi ship ( do mkt đăng ký) 164010',
          'phi pi ship ( do mkt dang ky) 164010',
          'phí pi ship ( do mkt đăng ký) 164010',
          'phí pi ship do mkt đăng ký 164010',
        ]),
      ) || null;

      entity.maCacBenTiepThiLienKet = getValue([
        'mã các bên tiếp thị liên kết',
        'ma cac ben tiep thi lien ket',
      ]);
      entity.sanTmdt = getValue(['sàn tmđt', 'san tmdt', 'sàn tmđt shopee']);
      
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
        entity.cotChoBsMkt1 = String(row[mktColumns[0].originalKey] || '');
      }
      if (mktColumns.length > 1) {
        entity.cotChoBsMkt2 = String(row[mktColumns[1].originalKey] || '');
      }
      if (mktColumns.length > 2) {
        entity.cotChoBsMkt3 = String(row[mktColumns[2].originalKey] || '');
      }
      if (mktColumns.length > 3) {
        entity.cotChoBsMkt4 = String(row[mktColumns[3].originalKey] || '');
      }
      if (mktColumns.length > 4) {
        entity.cotChoBsMkt5 = String(row[mktColumns[4].originalKey] || '');
      }

      entity.boPhan = getValue(['bộ phận', 'bo phan']);
    } else if (platform === 'tiktok') {
      entity.maSan = getValue(['mã tiktok', 'ma tiktok']);
      entity.maNoiBoSp = getValue(['mã nội bộ sp', 'ma noi bo sp']);
      entity.ngayDoiSoat = this.parseDate(
        getValue(['ngày đối soát', 'ngay doi soat']),
      ) || null;
      entity.maDonHangHoan = getValue([
        'mã đơn hàng hoàn',
        'ma don hang hoan',
      ]);
      entity.shopPhatHanhTrenSan = getValue([
        'shop phát hành trên sàn',
        'shop phat hanh tren san',
      ]);
      entity.giaTriGiamGiaCtkm = this.parseDecimal(
        getValue([
          'giá trị giảm giá theo ctkm của mình ban hành',
          'gia tri giam gia theo ctkm cua minh ban hanh',
        ]),
      ) || null;
      entity.doanhThuDonHang = this.parseDecimal(
        getValue(['doanh thu đơn hàng', 'doanh thu don hang']),
      ) || null;

      // TikTok specific fees (4 fees) - Map by exact column names
      entity.phiGiaoDichTyLe5164020 = this.parseDecimal(
        getValue([
          'phí giao dịch tỷ lệ 5% 164020',
          'phi giao dich ty le 5% 164020',
          'phí giao dịch tỷ lệ 5% 164020',
        ]),
      ) || null;
      entity.phiHoaHongTraChoTiktok454164020 = this.parseDecimal(
        getValue([
          'phí hoa hồng trả cho tiktok 4.54% 164020',
          'phi hoa hong tra cho tiktok 4.54% 164020',
          'phí hoa hồng trả cho tiktok 4.54% 164020',
        ]),
      ) || null;
      entity.phiHoaHongTiepThiLienKet150050 = this.parseDecimal(
        getValue([
          'phí hoa hồng tiếp thị liên kết 150050',
          'phi hoa hong tiep thi lien ket 150050',
          'phí hoa hồng tiếp thị liên kết 150050',
        ]),
      ) || null;
      entity.phiDichVuSfp6164020 = this.parseDecimal(
        getValue([
          'phí dịch vụ sfp 6% 164020',
          'phi dich vu sfp 6% 164020',
          'phí dịch vụ sfp 6% 164020',
        ]),
      ) || null;

      entity.maCacBenTiepThiLienKet = getValue([
        'mã các bên tiếp thị liên kết',
        'ma cac ben tiep thi lien ket',
      ]);
      entity.sanTmdt = getValue(['sàn tmđt', 'san tmdt', 'sàn tmđt tiktok']);
      
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
        entity.cotChoBsMkt1 = String(row[mktColumns[0].originalKey] || '');
      }
      if (mktColumns.length > 1) {
        entity.cotChoBsMkt2 = String(row[mktColumns[1].originalKey] || '');
      }
      if (mktColumns.length > 2) {
        entity.cotChoBsMkt3 = String(row[mktColumns[2].originalKey] || '');
      }
      if (mktColumns.length > 3) {
        entity.cotChoBsMkt4 = String(row[mktColumns[3].originalKey] || '');
      }
      if (mktColumns.length > 4) {
        entity.cotChoBsMkt5 = String(row[mktColumns[4].originalKey] || '');
      }

      entity.boPhan = getValue(['bộ phận', 'bo phan']);
    } else if (platform === 'lazada') {
      entity.maSan = getValue(['mã lazada', 'ma lazada']);
      entity.maNoiBoSp = getValue(['mã nội bộ sp', 'ma noi bo sp']);
      entity.ngayDoiSoat = this.parseDate(
        getValue(['ngày đối soát', 'ngay doi soat']),
      ) || null;
      entity.tenPhiDoanhThu = getValue([
        'tên phí/ doanh thu đơn hàng',
        'ten phi/ doanh thu don hang',
      ]);
      entity.quangCaoTiepThiLienKet = getValue([
        'quảng cáo tiếp thị liên kết',
        'quang cao tiep thi lien ket',
      ]);
      entity.maDonHangHoan = getValue([
        'mã đơn hàng hoàn',
        'ma don hang hoan',
      ]);
      entity.maPhiNhanDienHachToan = getValue([
        'mã phí để nhận diện hạch toán',
        'ma phi de nhan dien hach toan',
        'mã phí để nhận diện hạch toán',
      ]);
      entity.sanTmdt = getValue(['sàn tmđt', 'san tmdt', 'sàn tmđt lazada']);
      entity.ghiChu = getValue(['ghi chú', 'ghi chu']);
      entity.boPhan = getValue(['bộ phận', 'bo phan']);
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
    const page = params?.page || 1;
    const limit = params?.limit || 10;
    const skip = (page - 1) * limit;

    const qb = this.platformFeeImportRepository.createQueryBuilder('pfi');

    if (params?.platform) {
      qb.andWhere('pfi.platform = :platform', { platform: params.platform });
    }

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

  async generateTemplate(
    platform: 'shopee' | 'tiktok' | 'lazada',
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
        'Sàn TMĐT LAZADA',
        'GHI CHÚ',
        'Bộ phận',
      ];
      // Create sample data with all columns
      const sampleRowLazada: any = {};
      headers.forEach((header) => {
        if (header.includes('Ngày')) {
          sampleRowLazada[header] = '2024-01-01';
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
}
