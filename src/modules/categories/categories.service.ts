import {
  Injectable,
  Logger,
  NotFoundException,
  BadRequestException,
} from '@nestjs/common';
import { InjectRepository, InjectDataSource } from '@nestjs/typeorm';
import { Repository, Like, ILike, DataSource, In } from 'typeorm';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { ProductItem } from '../../entities/product-item.entity';
import { PromotionItem } from '../../entities/promotion-item.entity';
import { WarehouseItem } from '../../entities/warehouse-item.entity';
import { WarehouseCodeMapping } from '../../entities/warehouse-code-mapping.entity';
import { PaymentMethod } from '../../entities/payment-method.entity';
import { Customer } from '../../entities/customer.entity';
import { Sale } from '../../entities/sale.entity';
import { EcommerceCustomer } from '../../entities/ecommerce-customer.entity';
import {
  CreateProductItemDto,
  UpdateProductItemDto,
} from '../../dto/create-product-item.dto';
import {
  CreatePromotionItemDto,
  UpdatePromotionItemDto,
} from '../../dto/create-promotion-item.dto';
import {
  CreateWarehouseItemDto,
  UpdateWarehouseItemDto,
} from '../../dto/create-warehouse-item.dto';
import {
  CreateWarehouseCodeMappingDto,
  UpdateWarehouseCodeMappingDto,
} from '../../dto/create-warehouse-code-mapping.dto';
import {
  CreatePaymentMethodDto,
  UpdatePaymentMethodDto,
} from '../../dto/create-payment-method.dto';
import {
  CreateEcommerceCustomerDto,
  UpdateEcommerceCustomerDto,
} from '../../dto/create-ecommerce-customer.dto';
import * as XLSX from 'xlsx';

@Injectable()
export class CategoriesService {
  private readonly logger = new Logger(CategoriesService.name);

  constructor(
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(PromotionItem)
    private promotionItemRepository: Repository<PromotionItem>,
    @InjectRepository(WarehouseItem)
    private warehouseItemRepository: Repository<WarehouseItem>,
    @InjectRepository(WarehouseCodeMapping)
    private warehouseCodeMappingRepository: Repository<WarehouseCodeMapping>,
    @InjectRepository(PaymentMethod)
    private paymentMethodRepository: Repository<PaymentMethod>,
    @InjectRepository(Customer)
    private customerRepository: Repository<Customer>,
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(EcommerceCustomer)
    private ecommerceCustomerRepository: Repository<EcommerceCustomer>,
    @InjectDataSource()
    private dataSource: DataSource,
    private httpService: HttpService,
  ) {}

  // Cache for ecommerce customers
  private ecommerceCache: Map<string, EcommerceCustomer> | null = null;
  private readonly CACHE_TTL = 300000; // 5 minutes
  private lastCacheTime = 0;

  async findAll(options: { page?: number; limit?: number; search?: string }) {
    const { page = 1, limit = 50, search } = options;

    const query = this.productItemRepository
      .createQueryBuilder('product')
      .orderBy('product.ngayTao', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (search) {
      query.andWhere(
        '(product.maVatTu ILIKE :search OR product.tenVatTu ILIKE :search OR product.barcode ILIKE :search OR product.maERP ILIKE :search OR product.maNhanHieu ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    const [data, total] = await query.getManyAndCount();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOne(id: string): Promise<ProductItem> {
    const product = await this.productItemRepository.findOne({
      where: { id },
    });

    if (!product) {
      throw new NotFoundException(`Product with ID ${id} not found`);
    }

    return product;
  }

  async create(createDto: CreateProductItemDto): Promise<ProductItem> {
    const product = this.productItemRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    return await this.productItemRepository.save(product);
  }

  async update(
    id: string,
    updateDto: UpdateProductItemDto,
  ): Promise<ProductItem> {
    const product = await this.findOne(id);

    Object.assign(product, updateDto);

    return await this.productItemRepository.save(product);
  }

  async delete(id: string): Promise<void> {
    const product = await this.findOne(id);
    await this.productItemRepository.remove(product);
  }

  async importFromExcel(file: Express.Multer.File): Promise<{
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
      // Sử dụng raw: true để lấy giá trị gốc (số), sau đó convert sang string nếu cần
      const data = XLSX.utils.sheet_to_json(worksheet, {
        raw: true,
        defval: null,
      }) as Record<string, any>[];

      const errors: Array<{ row: number; error: string }> = [];
      let success = 0;
      let failed = 0;

      // Normalize header để xử lý khoảng trắng và chữ hoa/thường
      const normalizeHeader = (header: string): string => {
        return String(header).trim().toLowerCase().replace(/\s+/g, ' ');
      };

      // Mapping từ header Excel sang field của entity (với nhiều biến thể)
      const fieldMappingVariants: Record<string, string[]> = {
        maNhanHieu: ['mã nhãn hiệu', 'ma nhan hieu', 'mã nhãn hiệu'],
        loai: ['loại', 'loai'],
        lop: ['lớp', 'lop'],
        nhom: ['nhóm', 'nhom'],
        line: ['line'],
        maERP: ['mã erp', 'ma erp', 'mã erp'],
        maVatTuNhaCungCap: [
          'mã vật tư nhà cung cấp',
          'ma vat tu nha cung cap',
          'mã vật tư nhà cung cấp',
        ],
        maVatTu: ['mã vật tư', 'ma vat tu', 'mã vật tư', 'mã vt', 'ma vt'],
        tenVatTu: ['tên vật tư', 'ten vat tu', 'tên vật tư'],
        tenKhac: ['tên khác', 'ten khac'],
        tenHD: ['tên (hd)', 'ten (hd)', 'tên hd', 'ten hd'],
        barcode: ['barcode'],
        dvt: ['đvt', 'dvt', 'đơn vị tính'],
        nhieuDvt: ['nhiều đvt', 'nhieu dvt'],
        theoDoiTonKho: ['theo dõi tồn kho', 'theo doi ton kho'],
        theoDoiLo: ['theo dõi lô', 'theo doi lo'],
        theoDoiKiemKe: ['theo dõi kiểm kê', 'theo doi kiem ke'],
        theoDoiSerial: ['theo dõi serial', 'theo doi serial'],
        cachTinhGia: ['cách tính giá', 'cach tinh gia'],
        loaiVatTu: ['loại vật tư', 'loai vat tu'],
        tkVatTu: ['tk vật tư', 'tk vat tu'],
        loaiHang: ['loại hàng', 'loai hang'],
        nhomGia: ['nhóm giá', 'nhom gia'],
        maKho: ['mã kho', 'ma kho'],
        maViTri: ['mã vị trí', 'ma vi tri'],
        thueGiaTriGiaTang: ['thuế giá trị gia tăng', 'thue gia tri gia tang'],
        thueNhapKhau: ['thuế nhập khẩu', 'thue nhap khau'],
        suaTkVatTu: ['sửa tk vật tư', 'sua tk vat tu'],
        tkGiaVonBanBuon: ['tk giá vốn bán buôn', 'tk gia von ban buon'],
        tkDoanhThuBanBuon: ['tk doanh thu bán buôn', 'tk doanh thu ban buon'],
        tkDoanhThuNoiBo: ['tk doanh thu nội bộ', 'tk doanh thu noi bo'],
        tkHangBanTraLai: ['tk hàng bán trả lại', 'tk hang ban tra lai'],
        tkDaiLy: ['tk đại lý', 'tk dai ly'],
        tkSanPhamDoDang: ['tk sản phẩm dở dang', 'tk san pham do dang'],
        tkChenhLechGiaVon: ['tk chênh lệch giá vốn', 'tk chenh lech gia von'],
        tkChietKhau: ['tk chiết khấu', 'tk chiet khau'],
        tkChiPhiKhuyenMai: ['tk chi phí khuyến mãi', 'tk chi phi khuyen mai'],
        kieuLo: ['kiểu lô', 'kieu lo'],
        cachXuat: ['cách xuất', 'cach xuat'],
        vongDoiSP: [
          'vòng đời sp (số ngày)',
          'vong doi sp (so ngay)',
          'vòng đời sp',
        ],
        tgBaoHanh: [
          'tg bảo hành (số ngày)',
          'tg bao hanh (so ngay)',
          'thời gian bảo hành',
        ],
        choPhepTaoLoNgayKhiNhap: [
          'cho phép tạo lô ngay khi nhập',
          'cho phep tao lo ngay khi nhap',
        ],
        abc: ['abc'],
        soLuongTonToiThieu: [
          'số lượng tồn tối thiểu',
          'so luong ton toi thieu',
        ],
        soLuongTonToiDa: ['số lượng tồn tối đa', 'so luong ton toi da'],
        theTich: ['thể tích', 'the tich'],
        donViTinhTheTich: ['đơn vị tính thể tích', 'don vi tinh the tich'],
        khoiLuong: ['khối lượng', 'khoi luong'],
        donViTinhKhoiLuong: [
          'đơn vị tính khối lượng',
          'don vi tinh khoi luong',
        ],
        giaDichVu: ['giá dịch vụ', 'gia dich vu'],
        loaiHinhDichVu: ['loại hình dịch vụ', 'loai hinh dich vu'],
        maVatTuGoc: ['mã vật tư gốc', 'ma vat tu goc'],
        tkGiaVonBanLe: ['tk giá vốn bán lẻ', 'tk gia von ban le'],
        tkDoanhThuBanLe: ['tk doanh thu bán lẻ', 'tk doanh thu ban le'],
        tkChiPhiKhauHaoCCDC: [
          'tk chi phí khấu hao ccdc',
          'tk chi phi khau hao ccdc',
        ],
        tkChiPhiKhauHaoTSDC: [
          'tk chi phí khấu hao tsdc',
          'tk chi phi khau hao tsdc',
        ],
        tkDoanhThuHangNo: ['tk doanh thu hàng nợ', 'tk doanh thu hang no'],
        tkGiaVonHangNo: ['tk giá vốn hàng nợ', 'tk gia von hang no'],
        tkVatTuHangNo: ['tk vật tư hàng nợ', 'tk vat tu hang no'],
        danhSachDonVi: ['danh sách đơn vị', 'danh sach don vi'],
        maNhaCungCap: ['mã nhà cung cấp', 'ma nha cung cap'],
        tyLeTrichGiaVon: ['tỷ lệ trích giá vốn', 'ty le trich gia von'],
        trangThai: ['trạng thái', 'trang thai'],
      };

      // Mapping từ header Excel sang field của entity (cho backward compatibility)
      const fieldMapping: Record<string, string> = {
        'Mã nhãn hiệu': 'maNhanHieu',
        Loại: 'loai',
        Lớp: 'lop',
        Nhóm: 'nhom',
        Line: 'line',
        'Mã ERP': 'maERP',
        'Mã Vật tư nhà cung cấp': 'maVatTuNhaCungCap',
        'Mã vật tư': 'maVatTu',
        'Tên vật tư': 'tenVatTu',
        'Tên khác': 'tenKhac',
        'Tên (HD)': 'tenHD',
        Barcode: 'barcode',
        Đvt: 'dvt',
        'Nhiều Đvt': 'nhieuDvt',
        'Theo dõi tồn kho': 'theoDoiTonKho',
        'Theo dõi lô': 'theoDoiLo',
        'Theo dõi kiểm kê': 'theoDoiKiemKe',
        'Theo dõi serial': 'theoDoiSerial',
        'Cách tính giá': 'cachTinhGia',
        'Loại vật tư': 'loaiVatTu',
        'Tk vật tư': 'tkVatTu',
        'Loại hàng': 'loaiHang',
        'Nhóm giá': 'nhomGia',
        'Mã kho': 'maKho',
        'Mã vị trí': 'maViTri',
        'Thuế giá trị gia tăng': 'thueGiaTriGiaTang',
        'Thuế nhập khẩu': 'thueNhapKhau',
        'Sửa tk vật tư': 'suaTkVatTu',
        'Tk giá vốn bán buôn': 'tkGiaVonBanBuon',
        'Tk doanh thu bán buôn': 'tkDoanhThuBanBuon',
        'Tk doanh thu nội bộ': 'tkDoanhThuNoiBo',
        'Tk hàng bán trả lại': 'tkHangBanTraLai',
        'Tk đại lý': 'tkDaiLy',
        'Tk sản phẩm dở dang': 'tkSanPhamDoDang',
        'Tk chênh lệch giá vốn': 'tkChenhLechGiaVon',
        'Tk chiết khấu': 'tkChietKhau',
        'Tk chi phí khuyến mãi': 'tkChiPhiKhuyenMai',
        'Kiểu lô': 'kieuLo',
        'Cách xuất': 'cachXuat',
        'Vòng đời sp (số ngày)': 'vongDoiSP',
        'TG bảo hành (số ngày)': 'tgBaoHanh',
        'Cho phép tạo lô ngay khi nhập': 'choPhepTaoLoNgayKhiNhap',
        ABC: 'abc',
        'Số lượng tồn tối thiểu': 'soLuongTonToiThieu',
        'Số lượng tồn tối đa': 'soLuongTonToiDa',
        'Thể tích': 'theTich',
        'Đơn vị tính thể tích': 'donViTinhTheTich',
        'Khối lượng': 'khoiLuong',
        'Đơn vị tính khối lượng': 'donViTinhKhoiLuong',
        'Giá dịch vụ': 'giaDichVu',
        'Loại hình dịch vụ': 'loaiHinhDichVu',
        'Mã vật tư gốc': 'maVatTuGoc',
        'Tk giá vốn bán lẻ': 'tkGiaVonBanLe',
        'Tk doanh thu bán lẻ': 'tkDoanhThuBanLe',
        'Tk chi phí khấu hao CCDC': 'tkChiPhiKhauHaoCCDC',
        'Tk chi phí khấu hao TSDC': 'tkChiPhiKhauHaoTSDC',
        'Tk doanh thu hàng nợ': 'tkDoanhThuHangNo',
        'Tk giá vốn hàng nợ': 'tkGiaVonHangNo',
        'Tk vật tư hàng nợ': 'tkVatTuHangNo',
        'Danh sách đơn vị': 'danhSachDonVi',
        'Mã nhà cung cấp': 'maNhaCungCap',
        'Tỷ lệ trích giá vốn': 'tyLeTrichGiaVon',
        'Trạng thái': 'trangThai',
      };

      // Normalize boolean values
      const normalizeBoolean = (value: any): boolean | undefined => {
        // Xử lý null/undefined/empty
        if (value === null || value === undefined) return undefined;

        // Xử lý boolean trực tiếp
        if (typeof value === 'boolean') return value;

        // Xử lý số: 1 = true, 0 = false, các số khác = undefined
        if (typeof value === 'number') {
          // Kiểm tra NaN và Infinity
          if (isNaN(value) || !isFinite(value)) return undefined;
          if (value === 1 || value === 1.0 || Math.abs(value - 1) < 0.0001)
            return true;
          if (value === 0 || value === 0.0 || Math.abs(value) < 0.0001)
            return false;
          return undefined;
        }

        // Xử lý string
        if (typeof value === 'string') {
          const str = value.trim();

          // Nếu là string rỗng hoặc chỉ có khoảng trắng, return undefined
          if (str === '') return undefined;

          // Bỏ qua các ký tự đặc biệt không hợp lệ (như $ü)
          // Chỉ xử lý nếu string chứa ký tự hợp lệ
          if (
            !/^[\d\s\.,\-+eE]+$/.test(str) &&
            !/^(true|false|yes|no|có|không|x|y|n|1|0)$/i.test(str)
          ) {
            // Nếu không phải số hoặc giá trị boolean text hợp lệ, return undefined
            return undefined;
          }

          const lowerStr = str.toLowerCase();

          // Thử parse thành số trước (để xử lý "1.0", " 1 ", "0", "0.0", etc.)
          const numValue = parseFloat(lowerStr);
          if (!isNaN(numValue) && isFinite(numValue)) {
            if (
              numValue === 1 ||
              numValue === 1.0 ||
              Math.abs(numValue - 1) < 0.0001
            )
              return true;
            if (
              numValue === 0 ||
              numValue === 0.0 ||
              Math.abs(numValue) < 0.0001
            )
              return false;
          }

          // Xử lý các giá trị text
          if (
            lowerStr === 'true' ||
            lowerStr === 'yes' ||
            lowerStr === 'có' ||
            lowerStr === 'x' ||
            lowerStr === 'y' ||
            lowerStr === '1'
          ) {
            return true;
          }
          if (
            lowerStr === 'false' ||
            lowerStr === 'no' ||
            lowerStr === 'không' ||
            lowerStr === 'n' ||
            lowerStr === '0'
          ) {
            return false;
          }
        }

        // Nếu không match, return undefined (không set giá trị)
        return undefined;
      };

      // Normalize number values
      const normalizeNumber = (value: any): number | undefined => {
        if (value === null || value === undefined || value === '')
          return undefined;
        if (typeof value === 'number') return value;
        const str = String(value).trim().replace(/,/g, '');
        const num = parseFloat(str);
        return isNaN(num) ? undefined : num;
      };

      // Tạo reverse mapping từ normalized header sang field name (merge cả fieldMappingVariants và fieldMapping)
      const normalizedMapping: Record<string, string> = {};

      // Thêm từ fieldMappingVariants (đã có nhiều biến thể)
      for (const [fieldName, variants] of Object.entries(
        fieldMappingVariants,
      )) {
        for (const variant of variants) {
          normalizedMapping[normalizeHeader(variant)] = fieldName;
        }
      }

      // Thêm từ fieldMapping (cho backward compatibility)
      for (const [excelHeader, fieldName] of Object.entries(fieldMapping)) {
        const normalized = normalizeHeader(excelHeader);
        if (!normalizedMapping[normalized]) {
          normalizedMapping[normalized] = fieldName;
        }
      }

      // Lấy danh sách headers thực tế từ Excel
      const actualHeaders = data.length > 0 ? Object.keys(data[0]) : [];
      this.logger.log(`Excel headers found: ${actualHeaders.join(', ')}`);
      this.logger.log(
        `Looking for 'Mã vật tư' in normalized headers: ${normalizeHeader('Mã vật tư')}`,
      );

      // Lọc bỏ các dòng trống trước khi xử lý và lưu lại index gốc
      const nonEmptyRowsWithIndex = data
        .map((row, index) => ({ row, originalIndex: index }))
        .filter(({ row }) => {
          return !actualHeaders.every((header) => {
            const value = row[header];
            if (value === null || value === undefined) return true;
            if (typeof value === 'string' && value.trim() === '') return true;
            return false;
          });
        });

      // Parse tất cả dữ liệu trước
      const parsedProducts: Array<{
        productData: Partial<ProductItem>;
        rowNumber: number;
      }> = [];

      for (let i = 0; i < nonEmptyRowsWithIndex.length; i++) {
        const { row, originalIndex } = nonEmptyRowsWithIndex[i];
        const rowNumber = originalIndex + 2; // +2 vì bắt đầu từ row 2 (row 1 là header)

        try {
          const productData: Partial<ProductItem> = {
            trangThai: 'active',
          };

          // Map các fields từ Excel với flexible matching
          for (const actualHeader of actualHeaders) {
            const normalizedHeader = normalizeHeader(actualHeader);
            const fieldName = normalizedMapping[normalizedHeader];

            if (
              fieldName &&
              row[actualHeader] !== undefined &&
              row[actualHeader] !== null
            ) {
              const rawValue = row[actualHeader];

              // Xử lý boolean fields trước (để xử lý cả giá trị 0)
              if (
                fieldName.includes('theoDoi') ||
                fieldName === 'nhieuDvt' ||
                fieldName === 'suaTkVatTu' ||
                fieldName === 'choPhepTaoLoNgayKhiNhap'
              ) {
                const boolValue = normalizeBoolean(rawValue);
                // Lưu cả true và false (chỉ bỏ qua khi undefined)
                if (boolValue !== undefined) {
                  productData[fieldName] = boolValue;
                  this.logger.debug(
                    `Row ${rowNumber}: Set ${fieldName} = ${boolValue} (from rawValue: ${rawValue}, type: ${typeof rawValue})`,
                  );
                } else {
                  this.logger.debug(
                    `Row ${rowNumber}: Skip ${fieldName} (rawValue: ${rawValue}, type: ${typeof rawValue}, normalized: undefined)`,
                  );
                }
                continue; // Đã xử lý boolean, skip các xử lý khác
              }

              // Kiểm tra nếu là string rỗng hoặc chỉ có khoảng trắng (cho các field khác)
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue;
              }
              // Xử lý number fields
              else if (
                fieldName.includes('thue') ||
                fieldName.includes('soLuong') ||
                fieldName === 'theTich' ||
                fieldName === 'khoiLuong' ||
                fieldName === 'giaDichVu' ||
                fieldName === 'tyLeTrichGiaVon' ||
                fieldName === 'vongDoiSP' ||
                fieldName === 'tgBaoHanh'
              ) {
                const numValue = normalizeNumber(rawValue);
                if (numValue !== undefined) {
                  productData[fieldName] = numValue;
                }
              }
              // Xử lý string fields
              else {
                const stringValue = String(rawValue).trim();
                if (stringValue !== '') {
                  productData[fieldName] = stringValue;
                }
              }
            }
          }

          // Validate required fields với thông báo chi tiết hơn
          if (
            !productData.maVatTu ||
            (typeof productData.maVatTu === 'string' &&
              productData.maVatTu.trim() === '')
          ) {
            const availableHeaders = actualHeaders.join(', ');
            errors.push({
              row: rowNumber,
              error: `Mã vật tư là bắt buộc nhưng không tìm thấy trong dòng ${rowNumber}. Headers trong file: ${availableHeaders}`,
            });
            failed++;
            continue;
          }

          parsedProducts.push({ productData, rowNumber });
        } catch (error: any) {
          this.logger.error(`Error parsing row ${rowNumber}: ${error.message}`);
          errors.push({
            row: rowNumber,
            error: error.message || 'Unknown error',
          });
          failed++;
        }
      }

      // Tối ưu: Load tất cả existing records trong một query
      const maVatTuList = parsedProducts
        .map((p) => p.productData.maVatTu)
        .filter(Boolean) as string[];
      const existingProducts = await this.productItemRepository.find({
        where: { maVatTu: In(maVatTuList) },
      });
      const existingMap = new Map(existingProducts.map((p) => [p.maVatTu, p]));

      // Batch processing: xử lý theo từng batch 1000 records
      const BATCH_SIZE = 1000;
      const queryRunner = this.dataSource.createQueryRunner();

      try {
        await queryRunner.connect();
        await queryRunner.startTransaction();

        for (let i = 0; i < parsedProducts.length; i += BATCH_SIZE) {
          const batch = parsedProducts.slice(i, i + BATCH_SIZE);
          const productsToSave: ProductItem[] = [];
          const productsToDelete: ProductItem[] = [];

          for (const { productData, rowNumber } of batch) {
            try {
              const existing = existingMap.get(productData.maVatTu);

              if (existing) {
                // Xóa record cũ
                productsToDelete.push(existing);
              }

              // Tạo entity mới
              const product = this.productItemRepository.create(productData);
              productsToSave.push(product);
            } catch (error: any) {
              this.logger.error(
                `Error processing row ${rowNumber}: ${error.message}`,
              );
              errors.push({
                row: rowNumber,
                error: error.message || 'Unknown error',
              });
              failed++;
            }
          }

          // Bulk delete
          if (productsToDelete.length > 0) {
            await queryRunner.manager.remove(productsToDelete);
          }

          // Bulk insert
          if (productsToSave.length > 0) {
            await queryRunner.manager.save(ProductItem, productsToSave);
            success += productsToSave.length;
          }

          this.logger.log(
            `Processed batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(parsedProducts.length / BATCH_SIZE)} (${productsToSave.length} records)`,
          );
        }

        await queryRunner.commitTransaction();
      } catch (error: any) {
        await queryRunner.rollbackTransaction();
        this.logger.error(`Transaction error: ${error.message}`);
        throw error;
      } finally {
        await queryRunner.release();
      }

      return {
        total: nonEmptyRowsWithIndex.length,
        success,
        failed,
        errors,
      };
    } catch (error: any) {
      this.logger.error(`Error importing Excel file: ${error.message}`);
      throw new Error(`Lỗi khi import file Excel: ${error.message}`);
    }
  }

  // ========== PROMOTION METHODS ==========

  async findAllPromotions(options: {
    page?: number;
    limit?: number;
    search?: string;
  }) {
    const { page = 1, limit = 50, search } = options;

    const query = this.promotionItemRepository
      .createQueryBuilder('promotion')
      .orderBy('promotion.ngayTao', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (search) {
      query.andWhere(
        '(promotion.maChuongTrinh ILIKE :search OR promotion.tenChuongTrinh ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    const [data, total] = await query.getManyAndCount();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOnePromotion(id: string): Promise<PromotionItem> {
    const promotion = await this.promotionItemRepository.findOne({
      where: { id },
    });

    if (!promotion) {
      throw new NotFoundException(`Promotion with ID ${id} not found`);
    }

    return promotion;
  }

  async createPromotion(
    createDto: CreatePromotionItemDto,
  ): Promise<PromotionItem> {
    const promotion = this.promotionItemRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    return await this.promotionItemRepository.save(promotion);
  }

  async updatePromotion(
    id: string,
    updateDto: UpdatePromotionItemDto,
  ): Promise<PromotionItem> {
    const promotion = await this.findOnePromotion(id);

    Object.assign(promotion, updateDto);

    return await this.promotionItemRepository.save(promotion);
  }

  async deletePromotion(id: string): Promise<void> {
    const promotion = await this.findOnePromotion(id);
    await this.promotionItemRepository.remove(promotion);
  }

  async importPromotionsFromExcel(file: Express.Multer.File): Promise<{
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
      // Sử dụng raw: true để lấy giá trị gốc (số), sau đó convert sang string nếu cần
      const data = XLSX.utils.sheet_to_json(worksheet, {
        raw: true,
        defval: null,
      }) as Record<string, any>[];

      const errors: Array<{ row: number; error: string }> = [];
      let success = 0;
      let failed = 0;

      // Normalize header để xử lý khoảng trắng và chữ hoa/thường
      const normalizeHeader = (header: string): string => {
        return String(header).trim().toLowerCase().replace(/\s+/g, ' ');
      };

      // Mapping từ header Excel sang field của entity (với nhiều biến thể)
      const fieldMappingVariants: Record<string, string[]> = {
        maChuongTrinh: ['mã chương trình', 'ma chuong trinh'],
        tenChuongTrinh: ['tên chương trình', 'ten chuong trinh'],
        muaHangGiamGia: ['mua hàng giảm giá', 'mua hang giam gia'],
        ckTheoCS: ['ck theo cs', 'ck theo cs, ck vip', 'cktheocs'],
        ckVIP: ['ck vip', 'ck vip', 'ckvip'],
        voucher: ['voucher', 'voucher, coupon, ecode', 'voucher coupon ecode'],
        coupon: ['coupon', 'coupon'],
        ecode: ['ecode', 'ecode'],
        tangHang: ['tặng hàng', 'tang hang'],
        nskm: ['nskm'],
        combo: ['combo'],
        maPhi: ['mã phí', 'ma phi'],
        maBoPhan: ['mã bộ phận', 'ma bo phan'],
        taiKhoanChietKhau: ['tài khoản chiết khấu', 'tai khoan chiet khau'],
        taiKhoanChiPhiKhuyenMai: [
          'tài khoản chi phí khuyến mãi',
          'tai khoan chi phi khuyen mai',
        ],
        trangThai: ['trạng thái', 'trang thai'],
      };

      // Mapping từ header Excel sang field của entity (cho backward compatibility)
      const fieldMapping: Record<string, string> = {
        'Mã chương trình': 'maChuongTrinh',
        'Tên chương trình': 'tenChuongTrinh',
        'Mua hàng giảm giá': 'muaHangGiamGia',
        'CK theo CS': 'ckTheoCS',
        'CK VIP': 'ckVIP',
        'CK theo CS, CK VIP': 'ckTheoCS', // Nếu có cả 2 trong 1 field, map vào ckTheoCS
        VOUCHER: 'voucher',
        COUPON: 'coupon',
        ECODE: 'ecode',
        'VOUCHER, COUPON, ECODE...': 'voucher', // Nếu có nhiều trong 1 field, map vào voucher
        'Tặng hàng': 'tangHang',
        NSKM: 'nskm',
        Combo: 'combo',
        'Mã phí': 'maPhi',
        'Mã bộ phận': 'maBoPhan',
        'Tài khoản chiết khấu': 'taiKhoanChietKhau',
        'Tài khoản chi phí khuyến mãi': 'taiKhoanChiPhiKhuyenMai',
        'Trạng thái': 'trangThai',
      };

      // Merge cả fieldMappingVariants và fieldMapping vào normalizedMapping
      const normalizedMapping: Record<string, string> = {};

      // Thêm từ fieldMappingVariants (đã có nhiều biến thể)
      for (const [fieldName, variants] of Object.entries(
        fieldMappingVariants,
      )) {
        for (const variant of variants) {
          normalizedMapping[normalizeHeader(variant)] = fieldName;
        }
      }

      // Thêm từ fieldMapping (cho backward compatibility)
      for (const [excelHeader, fieldName] of Object.entries(fieldMapping)) {
        const normalized = normalizeHeader(excelHeader);
        if (!normalizedMapping[normalized]) {
          normalizedMapping[normalized] = fieldName;
        }
      }

      // Normalize boolean values
      const normalizeBoolean = (value: any): boolean | undefined => {
        // Xử lý null/undefined/empty
        if (value === null || value === undefined) return undefined;

        // Xử lý boolean trực tiếp
        if (typeof value === 'boolean') return value;

        // Xử lý số: 1 = true, 0 = false, các số khác = undefined
        if (typeof value === 'number') {
          // Kiểm tra NaN và Infinity
          if (isNaN(value) || !isFinite(value)) return undefined;
          if (value === 1 || value === 1.0 || Math.abs(value - 1) < 0.0001)
            return true;
          if (value === 0 || value === 0.0 || Math.abs(value) < 0.0001)
            return false;
          return undefined;
        }

        // Xử lý string
        if (typeof value === 'string') {
          const str = value.trim();

          // Nếu là string rỗng hoặc chỉ có khoảng trắng, return undefined
          if (str === '') return undefined;

          // Bỏ qua các ký tự đặc biệt không hợp lệ (như $ü)
          // Chỉ xử lý nếu string chứa ký tự hợp lệ
          if (
            !/^[\d\s\.,\-+eE]+$/.test(str) &&
            !/^(true|false|yes|no|có|không|x|y|n|1|0)$/i.test(str)
          ) {
            // Nếu không phải số hoặc giá trị boolean text hợp lệ, return undefined
            return undefined;
          }

          const lowerStr = str.toLowerCase();

          // Thử parse thành số trước (để xử lý "1.0", " 1 ", "0", "0.0", etc.)
          const numValue = parseFloat(lowerStr);
          if (!isNaN(numValue) && isFinite(numValue)) {
            if (
              numValue === 1 ||
              numValue === 1.0 ||
              Math.abs(numValue - 1) < 0.0001
            )
              return true;
            if (
              numValue === 0 ||
              numValue === 0.0 ||
              Math.abs(numValue) < 0.0001
            )
              return false;
          }

          // Xử lý các giá trị text
          if (
            lowerStr === 'true' ||
            lowerStr === 'yes' ||
            lowerStr === 'có' ||
            lowerStr === 'x' ||
            lowerStr === 'y' ||
            lowerStr === '1'
          ) {
            return true;
          }
          if (
            lowerStr === 'false' ||
            lowerStr === 'no' ||
            lowerStr === 'không' ||
            lowerStr === 'n' ||
            lowerStr === '0'
          ) {
            return false;
          }
        }

        // Nếu không match, return undefined (không set giá trị)
        return undefined;
      };

      // Lấy danh sách headers thực tế từ Excel
      const actualHeaders = data.length > 0 ? Object.keys(data[0]) : [];
      this.logger.log(`Excel headers found: ${actualHeaders.join(', ')}`);
      this.logger.log(
        `Looking for 'Mã chương trình' in normalized headers: ${normalizeHeader('Mã chương trình')}`,
      );

      // Lọc bỏ các dòng trống trước khi xử lý và lưu lại index gốc
      const nonEmptyRowsWithIndex = data
        .map((row, index) => ({ row, originalIndex: index }))
        .filter(({ row }) => {
          return !actualHeaders.every((header) => {
            const value = row[header];
            if (value === null || value === undefined) return true;
            if (typeof value === 'string' && value.trim() === '') return true;
            return false;
          });
        });

      for (let i = 0; i < nonEmptyRowsWithIndex.length; i++) {
        const { row, originalIndex } = nonEmptyRowsWithIndex[i];
        const rowNumber = originalIndex + 2; // +2 vì bắt đầu từ row 2 (row 1 là header)

        try {
          const promotionData: Partial<PromotionItem> = {
            trangThai: 'active',
          };

          // Map các fields từ Excel với flexible matching
          for (const actualHeader of actualHeaders) {
            const normalizedHeader = normalizeHeader(actualHeader);
            const fieldName = normalizedMapping[normalizedHeader];

            if (
              fieldName &&
              row[actualHeader] !== undefined &&
              row[actualHeader] !== null
            ) {
              const rawValue = row[actualHeader];

              // Kiểm tra nếu là string rỗng hoặc chỉ có khoảng trắng
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue; // Không set, để null
              }

              // Kiểm tra nếu là số 0 hoặc string "0" (kể cả có khoảng trắng) → không set (để null)
              if (
                typeof rawValue === 'number' &&
                (rawValue === 0 || rawValue === 0.0)
              ) {
                continue; // Không set, để null
              }
              if (typeof rawValue === 'string') {
                const trimmed = rawValue.trim();
                if (trimmed === '0' || trimmed === '0.0' || trimmed === '') {
                  continue; // Không set, để null
                }
              }

              // Lưu trực tiếp giá trị từ Excel (không normalize boolean)
              // Dữ liệu từ Excel trả ra gì thì viết vào như vậy
              if (rawValue !== null && rawValue !== undefined) {
                promotionData[fieldName] = rawValue;
              }
            }
          }

          // Validate required fields
          if (
            !promotionData.maChuongTrinh ||
            (typeof promotionData.maChuongTrinh === 'string' &&
              promotionData.maChuongTrinh.trim() === '')
          ) {
            const availableHeaders = actualHeaders.join(', ');
            errors.push({
              row: rowNumber,
              error: `Mã chương trình là bắt buộc nhưng không tìm thấy trong dòng ${rowNumber}. Headers trong file: ${availableHeaders}`,
            });
            failed++;
            continue;
          }

          // Kiểm tra xem đã tồn tại chưa (dựa trên maChuongTrinh)
          const existing = await this.promotionItemRepository.findOne({
            where: { maChuongTrinh: promotionData.maChuongTrinh },
          });

          if (existing) {
            // Xóa record cũ và tạo mới thay vì cập nhật
            await this.promotionItemRepository.remove(existing);
          }

          // Tạo mới (hoặc tạo lại sau khi xóa)
          const promotion = this.promotionItemRepository.create(promotionData);
          await this.promotionItemRepository.save(promotion);

          success++;
        } catch (error: any) {
          this.logger.error(
            `Error importing promotion row ${rowNumber}: ${error.message}`,
          );
          errors.push({
            row: rowNumber,
            error: error.message || 'Unknown error',
          });
          failed++;
        }
      }

      return {
        total: nonEmptyRowsWithIndex.length,
        success,
        failed,
        errors,
      };
    } catch (error: any) {
      this.logger.error(
        `Error importing promotions Excel file: ${error.message}`,
      );
      throw new Error(`Lỗi khi import file Excel: ${error.message}`);
    }
  }

  // ========== WAREHOUSE METHODS ==========

  async findAllWarehouses(options: {
    page?: number;
    limit?: number;
    search?: string;
  }) {
    const { page = 1, limit = 50, search } = options;

    const query = this.warehouseItemRepository
      .createQueryBuilder('warehouse')
      .orderBy('warehouse.ngayTao', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (search) {
      query.andWhere(
        '(warehouse.maKho ILIKE :search OR warehouse.tenKho ILIKE :search OR warehouse.maERP ILIKE :search OR warehouse.donVi ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    const [data, total] = await query.getManyAndCount();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOneWarehouse(id: string): Promise<WarehouseItem> {
    const warehouse = await this.warehouseItemRepository.findOne({
      where: { id },
    });

    if (!warehouse) {
      throw new NotFoundException(`Warehouse with ID ${id} not found`);
    }

    return warehouse;
  }

  async createWarehouse(
    createDto: CreateWarehouseItemDto,
  ): Promise<WarehouseItem> {
    const warehouse = this.warehouseItemRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    return await this.warehouseItemRepository.save(warehouse);
  }

  async updateWarehouse(
    id: string,
    updateDto: UpdateWarehouseItemDto,
  ): Promise<WarehouseItem> {
    const warehouse = await this.findOneWarehouse(id);

    Object.assign(warehouse, updateDto);

    return await this.warehouseItemRepository.save(warehouse);
  }

  async deleteWarehouse(id: string): Promise<void> {
    const warehouse = await this.findOneWarehouse(id);
    await this.warehouseItemRepository.remove(warehouse);
  }

  async importWarehousesFromExcel(file: Express.Multer.File): Promise<{
    total: number;
    success: number;
    failed: number;
    errors: Array<{ row: number; error: string }>;
  }> {
    try {
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, {
        raw: false,
      }) as Record<string, any>[];

      const errors: Array<{ row: number; error: string }> = [];
      let success = 0;
      let failed = 0;

      // Normalize header để xử lý khoảng trắng và chữ hoa/thường
      const normalizeHeader = (header: string): string => {
        return String(header).trim().toLowerCase().replace(/\s+/g, ' ');
      };

      // Mapping từ header Excel sang field của entity
      const fieldMapping: Record<string, string> = {
        'Đơn vị': 'donVi',
        'Mã kho': 'maKho',
        'Mã ERP': 'maERP',
        'Tên kho': 'tenKho',
        'Mã bộ phận': 'maBoPhan',
        'Tên bộ phận': 'tenBoPhan',
        'Trạng thái': 'trangThai',
      };

      // Tạo reverse mapping với normalized keys
      const normalizedMapping: Record<string, string> = {};
      for (const [excelHeader, fieldName] of Object.entries(fieldMapping)) {
        normalizedMapping[normalizeHeader(excelHeader)] = fieldName;
      }

      // Lấy danh sách headers thực tế từ Excel
      const actualHeaders = data.length > 0 ? Object.keys(data[0]) : [];
      this.logger.log(`Excel headers found: ${actualHeaders.join(', ')}`);

      // Lọc bỏ các dòng trống trước khi xử lý và lưu lại index gốc
      const nonEmptyRowsWithIndex = data
        .map((row, index) => ({ row, originalIndex: index }))
        .filter(({ row }) => {
          return !actualHeaders.every((header) => {
            const value = row[header];
            if (value === null || value === undefined) return true;
            if (typeof value === 'string' && value.trim() === '') return true;
            return false;
          });
        });

      for (let i = 0; i < nonEmptyRowsWithIndex.length; i++) {
        const { row, originalIndex } = nonEmptyRowsWithIndex[i];
        const rowNumber = originalIndex + 2; // +2 vì bắt đầu từ row 2 (row 1 là header)

        try {
          const warehouseData: Partial<WarehouseItem> = {
            trangThai: 'active',
          };

          // Map các fields từ Excel với flexible matching
          for (const actualHeader of actualHeaders) {
            const normalizedHeader = normalizeHeader(actualHeader);
            const fieldName = normalizedMapping[normalizedHeader];

            if (
              fieldName &&
              row[actualHeader] !== undefined &&
              row[actualHeader] !== null
            ) {
              const rawValue = row[actualHeader];
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue;
              }

              // Tất cả đều là string fields
              const stringValue = String(rawValue).trim();
              if (stringValue !== '') {
                warehouseData[fieldName] = stringValue;
              }
            }
          }

          // Validate required fields
          if (
            !warehouseData.maKho ||
            (typeof warehouseData.maKho === 'string' &&
              warehouseData.maKho.trim() === '')
          ) {
            const availableHeaders = actualHeaders.join(', ');
            errors.push({
              row: rowNumber,
              error: `Mã kho là bắt buộc nhưng không tìm thấy trong dòng ${rowNumber}. Headers trong file: ${availableHeaders}. Giá trị trong row: ${JSON.stringify(row)}`,
            });
            failed++;
            continue;
          }

          // Kiểm tra xem đã tồn tại chưa (dựa trên maKho)
          const existing = await this.warehouseItemRepository.findOne({
            where: { maKho: warehouseData.maKho },
          });

          if (existing) {
            // Xóa record cũ và tạo mới thay vì cập nhật
            await this.warehouseItemRepository.remove(existing);
          }

          // Tạo mới (hoặc tạo lại sau khi xóa)
          const warehouse = this.warehouseItemRepository.create(warehouseData);
          await this.warehouseItemRepository.save(warehouse);

          success++;
        } catch (error: any) {
          this.logger.error(
            `Error importing warehouse row ${rowNumber}: ${error.message}`,
          );
          errors.push({
            row: rowNumber,
            error: error.message || 'Unknown error',
          });
          failed++;
        }
      }

      return {
        total: nonEmptyRowsWithIndex.length,
        success,
        failed,
        errors,
      };
    } catch (error: any) {
      this.logger.error(
        `Error importing warehouses Excel file: ${error.message}`,
      );
      throw new Error(`Lỗi khi import file Excel: ${error.message}`);
    }
  }

  // ========== WAREHOUSE CODE MAPPING METHODS ==========

  async findAllWarehouseCodeMappings(options: {
    page?: number;
    limit?: number;
    search?: string;
  }) {
    const { page = 1, limit = 50, search } = options;

    const query = this.warehouseCodeMappingRepository
      .createQueryBuilder('mapping')
      .orderBy('mapping.ngayTao', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (search) {
      query.andWhere(
        '(mapping.maCu ILIKE :search OR mapping.maMoi ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    const [data, total] = await query.getManyAndCount();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOneWarehouseCodeMapping(id: string): Promise<WarehouseCodeMapping> {
    const mapping = await this.warehouseCodeMappingRepository.findOne({
      where: { id },
    });

    if (!mapping) {
      throw new NotFoundException(
        `Warehouse code mapping with ID ${id} not found`,
      );
    }

    return mapping;
  }

  async findWarehouseCodeMappingByMaCu(
    maCu: string,
  ): Promise<WarehouseCodeMapping | null> {
    return await this.warehouseCodeMappingRepository.findOne({
      where: { maCu },
    });
  }

  /**
   * Map mã kho cũ sang mã mới
   * @param maCu - Mã kho cũ
   * @returns Mã mới nếu tìm thấy, null nếu không tìm thấy
   */
  async mapWarehouseCode(
    maCu: string | null | undefined,
  ): Promise<string | null> {
    if (!maCu || maCu.trim() === '') {
      return null;
    }

    const mapping = await this.warehouseCodeMappingRepository.findOne({
      where: {
        maCu: maCu.trim(),
        trangThai: 'active', // Chỉ lấy mapping đang active
      },
    });

    return mapping ? mapping.maMoi : null;
  }

  async createWarehouseCodeMapping(
    createDto: CreateWarehouseCodeMappingDto,
  ): Promise<WarehouseCodeMapping> {
    // Kiểm tra xem maCu đã tồn tại chưa
    const existing = await this.warehouseCodeMappingRepository.findOne({
      where: { maCu: createDto.maCu },
    });

    if (existing) {
      throw new BadRequestException(`Mã cũ "${createDto.maCu}" đã tồn tại`);
    }

    const mapping = this.warehouseCodeMappingRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    return await this.warehouseCodeMappingRepository.save(mapping);
  }

  async updateWarehouseCodeMapping(
    id: string,
    updateDto: UpdateWarehouseCodeMappingDto,
  ): Promise<WarehouseCodeMapping> {
    const mapping = await this.findOneWarehouseCodeMapping(id);

    // Nếu maCu thay đổi, kiểm tra xem có trùng với record khác không
    if (updateDto.maCu && updateDto.maCu !== mapping.maCu) {
      const existing = await this.warehouseCodeMappingRepository.findOne({
        where: { maCu: updateDto.maCu },
      });

      if (existing && existing.id !== id) {
        throw new BadRequestException(`Mã cũ "${updateDto.maCu}" đã tồn tại`);
      }
    }

    Object.assign(mapping, updateDto);

    return await this.warehouseCodeMappingRepository.save(mapping);
  }

  async deleteWarehouseCodeMapping(id: string): Promise<void> {
    const mapping = await this.findOneWarehouseCodeMapping(id);
    await this.warehouseCodeMappingRepository.remove(mapping);
  }

  async importWarehouseCodeMappingsFromExcel(
    file: Express.Multer.File,
  ): Promise<{
    total: number;
    success: number;
    failed: number;
    errors: Array<{ row: number; error: string }>;
  }> {
    try {
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, {
        raw: false,
      }) as Record<string, any>[];

      const errors: Array<{ row: number; error: string }> = [];
      let success = 0;
      let failed = 0;

      // Normalize header để xử lý khoảng trắng và chữ hoa/thường
      const normalizeHeader = (header: string): string => {
        return String(header).trim().toLowerCase().replace(/\s+/g, ' ');
      };

      // Mapping từ header Excel sang field của entity
      const fieldMapping: Record<string, string> = {
        Cũ: 'maCu',
        'Mã cũ': 'maCu',
        'Mã Cũ': 'maCu',
        Mới: 'maMoi',
        'Mã mới': 'maMoi',
        'Mã Mới': 'maMoi',
      };

      // Tạo reverse mapping với normalized keys
      const normalizedMapping: Record<string, string> = {};
      for (const [excelHeader, fieldName] of Object.entries(fieldMapping)) {
        normalizedMapping[normalizeHeader(excelHeader)] = fieldName;
      }

      // Lấy danh sách headers thực tế từ Excel
      const actualHeaders = data.length > 0 ? Object.keys(data[0]) : [];
      this.logger.log(`Excel headers found: ${actualHeaders.join(', ')}`);

      // Lọc bỏ các dòng trống trước khi xử lý và lưu lại index gốc
      const nonEmptyRowsWithIndex = data
        .map((row, index) => ({ row, originalIndex: index }))
        .filter(({ row }) => {
          return !actualHeaders.every((header) => {
            const value = row[header];
            if (value === null || value === undefined) return true;
            if (typeof value === 'string' && value.trim() === '') return true;
            return false;
          });
        });

      for (let i = 0; i < nonEmptyRowsWithIndex.length; i++) {
        const { row, originalIndex } = nonEmptyRowsWithIndex[i];
        const rowNumber = originalIndex + 2; // +2 vì bắt đầu từ row 2 (row 1 là header)

        try {
          const mappingData: Partial<WarehouseCodeMapping> = {
            trangThai: 'active',
          };

          // Map các fields từ Excel với flexible matching
          for (const actualHeader of actualHeaders) {
            const normalizedHeader = normalizeHeader(actualHeader);
            const fieldName = normalizedMapping[normalizedHeader];

            if (
              fieldName &&
              row[actualHeader] !== undefined &&
              row[actualHeader] !== null
            ) {
              const rawValue = row[actualHeader];
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue;
              }

              // Tất cả đều là string fields
              const stringValue = String(rawValue).trim();
              if (stringValue !== '') {
                mappingData[fieldName] = stringValue;
              }
            }
          }

          // Validate required fields
          if (
            !mappingData.maCu ||
            (typeof mappingData.maCu === 'string' &&
              mappingData.maCu.trim() === '')
          ) {
            const availableHeaders = actualHeaders.join(', ');
            errors.push({
              row: rowNumber,
              error: `Mã cũ là bắt buộc nhưng không tìm thấy trong dòng ${rowNumber}. Headers trong file: ${availableHeaders}`,
            });
            failed++;
            continue;
          }

          if (
            !mappingData.maMoi ||
            (typeof mappingData.maMoi === 'string' &&
              mappingData.maMoi.trim() === '')
          ) {
            const availableHeaders = actualHeaders.join(', ');
            errors.push({
              row: rowNumber,
              error: `Mã mới là bắt buộc nhưng không tìm thấy trong dòng ${rowNumber}. Headers trong file: ${availableHeaders}`,
            });
            failed++;
            continue;
          }

          // Kiểm tra xem đã tồn tại chưa (dựa trên maCu)
          const existing = await this.warehouseCodeMappingRepository.findOne({
            where: { maCu: mappingData.maCu },
          });

          if (existing) {
            // Xóa record cũ và tạo mới thay vì cập nhật
            await this.warehouseCodeMappingRepository.remove(existing);
          }

          // Tạo mới (hoặc tạo lại sau khi xóa)
          const mapping =
            this.warehouseCodeMappingRepository.create(mappingData);
          await this.warehouseCodeMappingRepository.save(mapping);

          success++;
        } catch (error: any) {
          this.logger.error(
            `Error importing warehouse code mapping row ${rowNumber}: ${error.message}`,
          );
          errors.push({
            row: rowNumber,
            error: error.message || 'Unknown error',
          });
          failed++;
        }
      }

      return {
        total: nonEmptyRowsWithIndex.length,
        success,
        failed,
        errors,
      };
    } catch (error: any) {
      this.logger.error(
        `Error importing warehouse code mappings Excel file: ${error.message}`,
      );
      throw new Error(`Lỗi khi import file Excel: ${error.message}`);
    }
  }

  async getWarehouseCodeMap(): Promise<Map<string, string>> {
    const mappings = await this.warehouseCodeMappingRepository.find({
      where: { trangThai: 'active' },
    });
    return new Map(mappings.map((m) => [m.maCu, m.maMoi]));
  }

  // ========== PAYMENT METHOD METHODS ==========

  async findAllPaymentMethods(options: {
    page?: number;
    limit?: number;
    search?: string;
  }) {
    const { page = 1, limit = 50, search } = options;

    const query = this.paymentMethodRepository
      .createQueryBuilder('paymentMethod')
      .orderBy('paymentMethod.ngayTao', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (search) {
      query.andWhere(
        '(paymentMethod.code ILIKE :search OR paymentMethod.description ILIKE :search OR paymentMethod.documentType ILIKE :search OR paymentMethod.systemCode ILIKE :search OR paymentMethod.erp ILIKE :search OR paymentMethod.bankUnit ILIKE :search OR paymentMethod.externalId ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    const [data, total] = await query.getManyAndCount();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOnePaymentMethod(id: string): Promise<PaymentMethod> {
    const paymentMethod = await this.paymentMethodRepository.findOne({
      where: { id },
    });

    if (!paymentMethod) {
      throw new NotFoundException(`Payment method with ID ${id} not found`);
    }

    return paymentMethod;
  }

  async getGiayBaoCoPaymentMethodCodes(): Promise<string[]> {
    const paymentMethods = await this.paymentMethodRepository.find({
      where: { documentType: 'Giấy báo có', trangThai: 'active' },
      select: ['code'],
    });
    return paymentMethods.map((pm) => pm.code);
  }

  async findPaymentMethodByCode(
    code: string,
    dvcs: string,
  ): Promise<PaymentMethod | null> {
    return await this.paymentMethodRepository.findOne({
      where: { code, erp: dvcs },
    });
  }

  async createPaymentMethod(
    createDto: CreatePaymentMethodDto,
  ): Promise<PaymentMethod> {
    const paymentMethod = this.paymentMethodRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    return await this.paymentMethodRepository.save(paymentMethod);
  }

  async updatePaymentMethod(
    id: string,
    updateDto: UpdatePaymentMethodDto,
  ): Promise<PaymentMethod> {
    const paymentMethod = await this.findOnePaymentMethod(id);

    // Nếu code thay đổi, kiểm tra xem có trùng với record khác không
    if (updateDto.code && updateDto.code !== paymentMethod.code) {
      const existing = await this.paymentMethodRepository.findOne({
        where: { code: updateDto.code },
      });

      if (existing && existing.id !== id) {
        throw new BadRequestException(
          `Mã phương thức thanh toán "${updateDto.code}" đã tồn tại`,
        );
      }
    }

    // Only update fields that are provided in the DTO
    if (updateDto.code !== undefined) paymentMethod.code = updateDto.code;
    if (updateDto.externalId !== undefined)
      paymentMethod.externalId = updateDto.externalId;
    if (updateDto.description !== undefined)
      paymentMethod.description = updateDto.description;
    if (updateDto.systemCode !== undefined)
      paymentMethod.systemCode = updateDto.systemCode;
    if (updateDto.documentType !== undefined)
      paymentMethod.documentType = updateDto.documentType;
    if (updateDto.erp !== undefined) paymentMethod.erp = updateDto.erp;
    if (updateDto.bankUnit !== undefined)
      paymentMethod.bankUnit = updateDto.bankUnit;
    if (updateDto.trangThai !== undefined)
      paymentMethod.trangThai = updateDto.trangThai;
    if (updateDto.maDoiTac !== undefined)
      paymentMethod.maDoiTac = updateDto.maDoiTac;

    return await this.paymentMethodRepository.save(paymentMethod);
  }

  async deletePaymentMethod(id: string): Promise<void> {
    const paymentMethod = await this.findOnePaymentMethod(id);
    await this.paymentMethodRepository.remove(paymentMethod);
  }

  async importPaymentMethodsFromExcel(file: Express.Multer.File): Promise<{
    total: number;
    processed?: number;
    emptyRows?: number;
    success: number;
    failed: number;
    errors: Array<{ row: number; error: string }>;
  }> {
    try {
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, {
        raw: false,
      }) as Record<string, any>[];

      const errors: Array<{ row: number; error: string }> = [];
      let success = 0;
      let failed = 0;
      const totalRowsInFile = data.length; // Tổng số dòng trong file (không tính header)

      // Normalize header để xử lý khoảng trắng, chữ hoa/thường và Unicode (NFC)
      const normalizeHeader = (header: string): string => {
        return String(header).trim().toLowerCase().replace(/\s+/g, ' ');
      };

      // Mapping từ header Excel sang field của entity (với nhiều biến thể)
      const fieldMappingVariants: Record<string, string[]> = {
        externalId: ['id', 'id (hệ thống cũ)', 'externalid', 'old id'],
        code: [
          'mã',
          'ma',
          'code',
          'mã phương thức thanh toán',
          'ma phuong thuc thanh toan',
        ],
        description: [
          'diễn giải',
          'dien giai',
          'description',
          'name',
          'tên',
          'ten',
        ],
        systemCode: ['mã hệ thống', 'ma he thong', 'systemcode', 'system code'],
        documentType: [
          'loại chứng từ',
          'loai chung tu',
          'documenttype',
          'document type',
        ],
        erp: ['erp', 'erp code'],
        bankUnit: [
          'đơn vị ngân hàng',
          'don vi ngan hang',
          'bankunit',
          'bank unit',
        ],
        trangThai: ['trạng thái', 'trang thai', 'status', 'active'],
        maDoiTac: [
          'mã đối tác',
          'ma doi tac',
          'partner code',
          'partnercode',
          'Mã đối tác',
          'maDoiTac',
          'madoitac',
        ], // Thêm cả header từ ví dụ user gửi
      };

      // Mapping từ header Excel sang field của entity (cho backward compatibility)
      const fieldMapping: Record<string, string> = {
        Id: 'externalId',
        Mã: 'code',
        'Diễn giải': 'description',
        'Mã hệ thống': 'systemCode',
        'Loại chứng từ': 'documentType',
        ERP: 'erp',
        'Đơn vị ngân hàng': 'bankUnit',
        'Trạng thái': 'trangThai',
        'Mã đối tác': 'maDoiTac',
      };

      // Tạo reverse mapping từ normalized header sang field name (merge cả fieldMappingVariants và fieldMapping)
      const normalizedMapping: Record<string, string> = {};

      // Thêm từ fieldMappingVariants (đã có nhiều biến thể)
      for (const [fieldName, variants] of Object.entries(
        fieldMappingVariants,
      )) {
        for (const variant of variants) {
          normalizedMapping[normalizeHeader(variant)] = fieldName;
        }
      }

      // Thêm từ fieldMapping (cho backward compatibility)
      for (const [excelHeader, fieldName] of Object.entries(fieldMapping)) {
        const normalized = normalizeHeader(excelHeader);
        if (!normalizedMapping[normalized]) {
          normalizedMapping[normalized] = fieldName;
        }
      }

      // Lấy danh sách headers thực tế từ Excel (tổng hợp từ tất cả các dòng vì có thể dòng đầu tiên thiếu cột nếu value rỗng)
      const allHeadersSet = new Set<string>();
      data.forEach((row) =>
        Object.keys(row).forEach((key) => allHeadersSet.add(key)),
      );
      const actualHeaders = Array.from(allHeadersSet);
      this.logger.log(`Excel headers found: ${actualHeaders.join(', ')}`);
      this.logger.debug(
        `Normalized mapping keys: ${Object.keys(normalizedMapping).join(', ')}`,
      );

      // Xử lý tất cả các dòng, không lọc bỏ dòng trống
      const allRowsWithIndex = data.map((row, index) => ({
        row,
        originalIndex: index,
      }));

      this.logger.log(
        `Total rows in file: ${totalRowsInFile}, Processing all rows`,
      );

      for (let i = 0; i < allRowsWithIndex.length; i++) {
        const { row, originalIndex } = allRowsWithIndex[i];
        const rowNumber = originalIndex + 2; // +2 vì bắt đầu từ row 2 (row 1 là header)

        try {
          const paymentMethodData: Partial<PaymentMethod> = {
            trangThai: 'active',
          };

          // Map các fields từ Excel với flexible matching
          for (const actualHeader of actualHeaders) {
            const normalizedHeader = normalizeHeader(actualHeader);
            const fieldName = normalizedMapping[normalizedHeader];

            if (
              fieldName &&
              row[actualHeader] !== undefined &&
              row[actualHeader] !== null
            ) {
              const rawValue = row[actualHeader];
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue;
              }

              // Tất cả đều là string fields
              let stringValue = String(rawValue).trim();

              // Normalize trạng thái
              if (fieldName === 'trangThai') {
                if (
                  stringValue.toLowerCase() === 'đang sử dụng' ||
                  stringValue.toLowerCase() === 'active' ||
                  stringValue === '1'
                ) {
                  stringValue = 'active';
                } else if (
                  stringValue.toLowerCase() === 'ngưng sử dụng' ||
                  stringValue.toLowerCase() === 'inactive' ||
                  stringValue === '0'
                ) {
                  stringValue = 'inactive';
                }
              }

              if (stringValue !== '') {
                paymentMethodData[fieldName] = stringValue;
                // Debug log cho maDoiTac
                if (fieldName === 'maDoiTac') {
                  this.logger.debug(
                    `Row ${rowNumber}: Found maDoiTac = "${stringValue}" from header "${actualHeader}"`,
                  );
                }
              }
            } else {
              // Log nếu không tìm thấy mapping hoặc giá trị null cho header này
              if (rowNumber === 2) {
                // Chỉ log dòng đầu tiên
                const normalizedHeader = normalizeHeader(actualHeader);
                this.logger.warn(
                  `Unmapped Header: "${actualHeader}" -> Normalized: "${normalizedHeader}"`,
                );
                const codes: number[] = [];
                for (let k = 0; k < normalizedHeader.length; k++)
                  codes.push(normalizedHeader.charCodeAt(k));
                this.logger.warn(
                  `Char codes for "${normalizedHeader}": [${codes.join(', ')}]`,
                );
              }
            }
          }

          // Validate required fields
          if (
            !paymentMethodData.code ||
            (typeof paymentMethodData.code === 'string' &&
              paymentMethodData.code.trim() === '')
          ) {
            const availableHeaders = actualHeaders.join(', ');
            errors.push({
              row: rowNumber,
              error: `Mã là bắt buộc nhưng không tìm thấy trong dòng ${rowNumber}. Headers trong file: ${availableHeaders}`,
            });
            failed++;
            continue;
          }

          // Không check trùng, luôn tạo mới
          const paymentMethod =
            this.paymentMethodRepository.create(paymentMethodData);
          await this.paymentMethodRepository.save(paymentMethod);

          success++;
        } catch (error: any) {
          this.logger.error(
            `Error importing payment method row ${rowNumber}: ${error.message}`,
          );
          errors.push({
            row: rowNumber,
            error: error.message || 'Unknown error',
          });
          failed++;
        }
      }

      return {
        total: totalRowsInFile, // Tổng số dòng trong file
        processed: totalRowsInFile, // Số dòng đã xử lý (tất cả)
        success,
        failed,
        errors,
      };
    } catch (error: any) {
      this.logger.error(
        `Error importing payment methods Excel file: ${error.message}`,
      );
      throw new Error(`Lỗi khi import file Excel: ${error.message}`);
    }
  }

  async exportPaymentMethodsToExcel(): Promise<Buffer> {
    const paymentMethods = await this.paymentMethodRepository.find({
      order: { ngayTao: 'DESC' },
    });

    const data = paymentMethods.map((item) => ({
      Id: item.externalId,
      Mã: item.code,
      'Diễn giải': item.description,
      'Mã hệ thống': item.systemCode,
      'Loại chứng từ': item.documentType,
      ERP: item.erp,
      'Đơn vị ngân hàng': item.bankUnit,
      'Trạng thái': item.trangThai,
      'Mã đối tác': item.maDoiTac,
    }));

    const worksheet = XLSX.utils.json_to_sheet(data);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'PaymentMethods');

    return XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
  }

  // ========== CUSTOMER METHODS ==========

  async findAllCustomers(options: {
    page?: number;
    limit?: number;
    search?: string;
  }) {
    const { page = 1, limit = 50, search } = options;

    const query = this.customerRepository
      .createQueryBuilder('customer')
      .orderBy('customer.createdAt', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (search) {
      query.andWhere(
        '(customer.code ILIKE :search OR customer.name ILIKE :search OR customer.mobile ILIKE :search OR customer.address ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    const [data, total] = await query.getManyAndCount();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findCustomerByCode(code: string) {
    const customer = await this.customerRepository.findOne({
      where: { code },
      relations: ['sales'],
    });

    if (!customer) {
      throw new NotFoundException(`Customer with code ${code} not found`);
    }

    // Format response theo cấu trúc data_customer
    const sales = customer.sales || [];

    // Map sales to match the expected format
    // Note: Some fields from the API response may not exist in Sale entity
    // We'll map available fields and set others to null
    const formattedSales = sales.map((sale) => {
      // Format docmonth from docDate
      const docDate = sale.docDate ? new Date(sale.docDate) : null;
      const docmonth = docDate
        ? `${docDate.getFullYear()}/${String(docDate.getMonth() + 1).padStart(2, '0')}`
        : null;

      return {
        qty: sale.qty,
        cat1: null, // Not in Sale entity
        cat2: null, // Not in Sale entity
        cat3: null, // Not in Sale entity
        ck_tm: null, // Not in Sale entity
        docid: null, // Not in Sale entity
        ck_dly: null, // Not in Sale entity
        serial: sale.soSerial || null,
        cm_code: null, // Not in Sale entity
        doccode: sale.docCode,
        docdate: sale.docDate,
        line_id: null, // Not in Sale entity
        revenue: sale.revenue,
        catcode1: null, // Not in Sale entity
        catcode2: null, // Not in Sale entity
        catcode3: null, // Not in Sale entity
        disc_amt: null, // Not in Sale entity
        docmonth: docmonth,
        itemcode: sale.itemCode,
        itemcost: null, // Not in Sale entity
        itemname: sale.itemName,
        linetotal: sale.tienHang || null,
        ordertype: null, // Not in Sale entity
        prom_code: sale.promCode || null,
        totalcost: null, // Not in Sale entity
        crm_emp_id: null, // Not in Sale entity
        branch_code: sale.branchCode,
        description: sale.description || null,
        doctype_name: null, // Not in Sale entity
        order_source: null, // Not in Sale entity
        partner_code: sale.partnerCode || null,
        partner_name: null, // Not in Sale entity
        crm_branch_id: null, // Not in Sale entity
        docsourcetype: sale.docSourceType || null,
        grade_discamt: null, // Not in Sale entity
        revenue_wsale: null, // Not in Sale entity
        saleperson_id: null, // Not in Sale entity
        revenue_retail: null, // Not in Sale entity
        paid_by_voucher_ecode_ecoin_bp: null, // Not in Sale entity
      };
    });

    return {
      data_customer: {
        Personal_Info: {
          code: customer.code,
          name: customer.name,
          mobile: customer.mobile,
          sexual: customer.sexual,
          idnumber: customer.idnumber,
          enteredat: customer.enteredat,
          crm_lead_source: customer.crm_lead_source,
          address: customer.address,
          province_name: customer.province_name,
          birthday: customer.birthday,
          grade_name: customer.grade_name,
          branch_code: customer.branch_code,
        },
        Sales: formattedSales,
      },
    };
  }

  /**
   * Lấy product từ Loyalty API theo itemCode
   */
  async getProductFromLoyaltyAPI(itemCode: string): Promise<any> {
    try {
      const response = await firstValueFrom(
        this.httpService.get(
          `https://loyaltyapi.vmt.vn/products/code/${encodeURIComponent(itemCode)}`,
          {
            headers: { accept: 'application/json' },
          },
        ),
      );

      if (response?.data?.data?.item) {
        return response.data.data.item;
      }

      return null;
    } catch (error: any) {
      this.logger.error(
        `Error fetching product ${itemCode} from Loyalty API: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Lấy department từ Loyalty API theo branchcode
   */
  async getDepartmentFromLoyaltyAPI(branchcode: string): Promise<any> {
    try {
      const response = await firstValueFrom(
        this.httpService.get(
          `https://loyaltyapi.vmt.vn/departments?page=1&limit=25&branchcode=${branchcode}`,
          {
            headers: { accept: 'application/json' },
          },
        ),
      );

      const department = response?.data?.data?.items?.[0] || null;
      return department;
    } catch (error: any) {
      this.logger.error(
        `Error fetching department ${branchcode} from Loyalty API: ${error?.message || error}`,
      );
      throw error;
    }
  }

  async createPromotionFromLoyaltyAPI(promotionData: any): Promise<any> {
    try {
      const promotion = await firstValueFrom(
        this.httpService.post(
          `https://loyaltyapi.vmt.vn/promotional`,
          promotionData,
          {
            headers: { accept: 'application/json' },
          },
        ),
      );

      return promotion.data.data;
    } catch (error: any) {
      this.logger.error(
        `Error creating promotion from Loyalty API: ${error?.message || error}`,
      );
      throw error;
    }
  }

  // ========== ECOMMERCE CUSTOMER METHODS ==========

  async findAllEcommerceCustomers(options: {
    page?: number;
    limit?: number;
    search?: string;
  }) {
    const { page = 1, limit = 50, search } = options;

    const query = this.ecommerceCustomerRepository
      .createQueryBuilder('ec')
      .orderBy('ec.ngayTao', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    if (search) {
      query.andWhere(
        '(ec.brand ILIKE :search OR ec.customerCode ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    const [data, total] = await query.getManyAndCount();

    return {
      data,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  async findOneEcommerceCustomer(id: string): Promise<EcommerceCustomer> {
    const ec = await this.ecommerceCustomerRepository.findOne({
      where: { id },
    });

    if (!ec) {
      throw new NotFoundException(`Ecommerce Customer with ID ${id} not found`);
    }

    return ec;
  }

  async findActiveEcommerceCustomers(): Promise<EcommerceCustomer[]> {
    return this.ecommerceCustomerRepository.find({
      where: { trangThai: 'active' },
      order: { brand: 'ASC', customerCode: 'ASC' },
    });
  }

  async findActiveEcommerceCustomerByCode(
    code: string,
  ): Promise<EcommerceCustomer | null> {
    const now = Date.now();

    // Refresh cache if needed
    if (!this.ecommerceCache || now - this.lastCacheTime > this.CACHE_TTL) {
      const activeCustomers = await this.ecommerceCustomerRepository.find({
        where: { trangThai: 'active' },
      });

      this.ecommerceCache = new Map<string, EcommerceCustomer>();
      for (const ec of activeCustomers) {
        // Map by customerCode
        if (ec.customerCode) {
          this.ecommerceCache.set(ec.customerCode.trim(), ec);
        }
      }
      this.lastCacheTime = now;
    }

    const trimmedCode = code?.trim();
    if (!trimmedCode) return null;

    return this.ecommerceCache.get(trimmedCode) || null;
  }

  async createEcommerceCustomer(
    createDto: CreateEcommerceCustomerDto,
  ): Promise<EcommerceCustomer> {
    const ec = this.ecommerceCustomerRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    const saved = await this.ecommerceCustomerRepository.save(ec);
    this.ecommerceCache = null; // Invalidate cache
    return saved;
  }

  async updateEcommerceCustomer(
    id: string,
    updateDto: UpdateEcommerceCustomerDto,
  ): Promise<EcommerceCustomer> {
    const ec = await this.findOneEcommerceCustomer(id);

    Object.assign(ec, updateDto);

    const saved = await this.ecommerceCustomerRepository.save(ec);
    this.ecommerceCache = null; // Invalidate cache
    return saved;
  }

  async deleteEcommerceCustomer(id: string): Promise<void> {
    const ec = await this.findOneEcommerceCustomer(id);
    await this.ecommerceCustomerRepository.remove(ec);
    this.ecommerceCache = null; // Invalidate cache
  }

  async importEcommerceCustomersFromExcel(file: Express.Multer.File): Promise<{
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

      const errors: Array<{ row: number; error: string }> = [];
      let success = 0;
      let failed = 0;

      const normalizeHeader = (header: string): string => {
        return String(header).trim().toLowerCase().replace(/\s+/g, ' ');
      };

      // Mapping từ header Excel sang field
      const fieldMappingVariants: Record<string, string[]> = {
        brand: ['brand', 'thương hiệu', 'thuong hieu'],
        customerCode: [
          'customer code',
          'customercode',
          'mã khách hàng',
          'ma khach hang',
          'mã kh',
          'ma kh',
        ],
        ecomName: [
          'ecom name',
          'ecomname',
          'tên sàn',
          'ten san',
          'ten s',
          'ten s',
        ],
      };

      const normalizedMapping: Record<string, string> = {};
      for (const [fieldName, variants] of Object.entries(
        fieldMappingVariants,
      )) {
        for (const variant of variants) {
          normalizedMapping[normalizeHeader(variant)] = fieldName;
        }
      }

      const actualHeaders = data.length > 0 ? Object.keys(data[0]) : [];
      this.logger.log(`Excel headers found: ${actualHeaders.join(', ')}`);

      // Lọc bỏ các dòng trống
      const nonEmptyRowsWithIndex = data
        .map((row, index) => ({ row, originalIndex: index }))
        .filter(({ row }) => {
          return !actualHeaders.every((header) => {
            const value = row[header];
            if (value === null || value === undefined) return true;
            if (typeof value === 'string' && value.trim() === '') return true;
            return false;
          });
        });

      // Parse data
      const parsedItems: Array<{
        itemData: Partial<EcommerceCustomer>;
        rowNumber: number;
      }> = [];

      for (let i = 0; i < nonEmptyRowsWithIndex.length; i++) {
        const { row, originalIndex } = nonEmptyRowsWithIndex[i];
        const rowNumber = originalIndex + 2;

        try {
          const itemData: Partial<EcommerceCustomer> = {
            trangThai: 'active',
          };

          for (const actualHeader of actualHeaders) {
            const normalizedHeader = normalizeHeader(actualHeader);
            const fieldName = normalizedMapping[normalizedHeader];

            if (
              fieldName &&
              row[actualHeader] !== undefined &&
              row[actualHeader] !== null
            ) {
              const rawValue = row[actualHeader];
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue;
              }
              itemData[fieldName] = String(rawValue).trim();
            }
          }

          if (!itemData.brand || !itemData.customerCode) {
            errors.push({
              row: rowNumber,
              error: `Brand và Customer Code là bắt buộc`,
            });
            failed++;
            continue;
          }

          parsedItems.push({ itemData, rowNumber });
        } catch (error: any) {
          this.logger.error(`Error parsing row ${rowNumber}: ${error.message}`);
          errors.push({
            row: rowNumber,
            error: error.message || 'Unknown error',
          });
          failed++;
        }
      }

      // Load existing records
      const customerCodeList = parsedItems
        .map((p) => p.itemData.customerCode)
        .filter(Boolean) as string[];
      const existingItems = await this.ecommerceCustomerRepository.find({
        where: { customerCode: In(customerCodeList) },
      });
      const existingMap = new Map(
        existingItems.map((p) => [p.customerCode, p]),
      );

      // Batch processing
      const BATCH_SIZE = 1000;
      const queryRunner = this.dataSource.createQueryRunner();

      try {
        await queryRunner.connect();
        await queryRunner.startTransaction();

        for (let i = 0; i < parsedItems.length; i += BATCH_SIZE) {
          const batch = parsedItems.slice(i, i + BATCH_SIZE);
          const itemsToSave: EcommerceCustomer[] = [];
          const itemsToDelete: EcommerceCustomer[] = [];

          for (const { itemData, rowNumber } of batch) {
            try {
              const existing = existingMap.get(itemData.customerCode!);

              if (existing) {
                itemsToDelete.push(existing);
              }

              const item = this.ecommerceCustomerRepository.create(itemData);
              itemsToSave.push(item);
            } catch (error: any) {
              this.logger.error(
                `Error processing row ${rowNumber}: ${error.message}`,
              );
              errors.push({
                row: rowNumber,
                error: error.message || 'Unknown error',
              });
              failed++;
            }
          }

          if (itemsToDelete.length > 0) {
            await queryRunner.manager.remove(itemsToDelete);
          }

          if (itemsToSave.length > 0) {
            await queryRunner.manager.save(EcommerceCustomer, itemsToSave);
            success += itemsToSave.length;
          }
        }

        await queryRunner.commitTransaction();
      } catch (error: any) {
        await queryRunner.rollbackTransaction();
        this.logger.error(`Transaction error: ${error.message}`);
        throw error;
      } finally {
        await queryRunner.release();
      }

      return {
        total: nonEmptyRowsWithIndex.length,
        success,
        failed,
        errors,
      };
    } catch (error: any) {
      this.logger.error(`Error importing Excel file: ${error.message}`);
      throw new Error(`Lỗi khi import file Excel: ${error.message}`);
    }
  }
}
