import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Like, ILike } from 'typeorm';
import { ProductItem } from '../../entities/product-item.entity';
import { PromotionItem } from '../../entities/promotion-item.entity';
import { WarehouseItem } from '../../entities/warehouse-item.entity';
import { CreateProductItemDto, UpdateProductItemDto } from '../../dto/create-product-item.dto';
import { CreatePromotionItemDto, UpdatePromotionItemDto } from '../../dto/create-promotion-item.dto';
import { CreateWarehouseItemDto, UpdateWarehouseItemDto } from '../../dto/create-warehouse-item.dto';
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
  ) {}

  async findAll(options: {
    page?: number;
    limit?: number;
    search?: string;
  }) {
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

  async update(id: string, updateDto: UpdateProductItemDto): Promise<ProductItem> {
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
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, { raw: false }) as any[];

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
        maVatTuNhaCungCap: ['mã vật tư nhà cung cấp', 'ma vat tu nha cung cap', 'mã vật tư nhà cung cấp'],
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
        vongDoiSP: ['vòng đời sp (số ngày)', 'vong doi sp (so ngay)', 'vòng đời sp'],
        tgBaoHanh: ['tg bảo hành (số ngày)', 'tg bao hanh (so ngay)', 'thời gian bảo hành'],
        choPhepTaoLoNgayKhiNhap: ['cho phép tạo lô ngay khi nhập', 'cho phep tao lo ngay khi nhap'],
        abc: ['abc'],
        soLuongTonToiThieu: ['số lượng tồn tối thiểu', 'so luong ton toi thieu'],
        soLuongTonToiDa: ['số lượng tồn tối đa', 'so luong ton toi da'],
        theTich: ['thể tích', 'the tich'],
        donViTinhTheTich: ['đơn vị tính thể tích', 'don vi tinh the tich'],
        khoiLuong: ['khối lượng', 'khoi luong'],
        donViTinhKhoiLuong: ['đơn vị tính khối lượng', 'don vi tinh khoi luong'],
        giaDichVu: ['giá dịch vụ', 'gia dich vu'],
        loaiHinhDichVu: ['loại hình dịch vụ', 'loai hinh dich vu'],
        maVatTuGoc: ['mã vật tư gốc', 'ma vat tu goc'],
        tkGiaVonBanLe: ['tk giá vốn bán lẻ', 'tk gia von ban le'],
        tkDoanhThuBanLe: ['tk doanh thu bán lẻ', 'tk doanh thu ban le'],
        tkChiPhiKhauHaoCCDC: ['tk chi phí khấu hao ccdc', 'tk chi phi khau hao ccdc'],
        tkChiPhiKhauHaoTSDC: ['tk chi phí khấu hao tsdc', 'tk chi phi khau hao tsdc'],
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
        'Loại': 'loai',
        'Lớp': 'lop',
        'Nhóm': 'nhom',
        'Line': 'line',
        'Mã ERP': 'maERP',
        'Mã Vật tư nhà cung cấp': 'maVatTuNhaCungCap',
        'Mã vật tư': 'maVatTu',
        'Tên vật tư': 'tenVatTu',
        'Tên khác': 'tenKhac',
        'Tên (HD)': 'tenHD',
        'Barcode': 'barcode',
        'Đvt': 'dvt',
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
        'ABC': 'abc',
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
        if (value === null || value === undefined || value === '') return undefined;
        if (typeof value === 'boolean') return value;
        const str = String(value).toLowerCase().trim();
        return str === 'true' || str === '1' || str === 'yes' || str === 'có' || str === 'x';
      };

      // Normalize number values
      const normalizeNumber = (value: any): number | undefined => {
        if (value === null || value === undefined || value === '') return undefined;
        if (typeof value === 'number') return value;
        const str = String(value).trim().replace(/,/g, '');
        const num = parseFloat(str);
        return isNaN(num) ? undefined : num;
      };

      // Tạo reverse mapping từ normalized header sang field name (merge cả fieldMappingVariants và fieldMapping)
      const normalizedMapping: Record<string, string> = {};
      
      // Thêm từ fieldMappingVariants (đã có nhiều biến thể)
      for (const [fieldName, variants] of Object.entries(fieldMappingVariants)) {
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
      this.logger.log(`Looking for 'Mã vật tư' in normalized headers: ${normalizeHeader('Mã vật tư')}`);

      // Lọc bỏ các dòng trống trước khi xử lý và lưu lại index gốc
      const nonEmptyRowsWithIndex = data
        .map((row, index) => ({ row, originalIndex: index }))
        .filter(({ row }) => {
          return !actualHeaders.every(header => {
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
          const productData: Partial<ProductItem> = {
            trangThai: 'active',
          };

          // Map các fields từ Excel với flexible matching
          for (const actualHeader of actualHeaders) {
            const normalizedHeader = normalizeHeader(actualHeader);
            const fieldName = normalizedMapping[normalizedHeader];
            
            if (fieldName && row[actualHeader] !== undefined && row[actualHeader] !== null) {
              const rawValue = row[actualHeader];
              // Kiểm tra nếu là string rỗng hoặc chỉ có khoảng trắng
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue;
              }

              // Xử lý boolean fields
              if (fieldName.includes('theoDoi') || fieldName === 'nhieuDvt' || fieldName === 'suaTkVatTu' || fieldName === 'choPhepTaoLoNgayKhiNhap') {
                const boolValue = normalizeBoolean(rawValue);
                if (boolValue !== undefined) {
                  productData[fieldName] = boolValue;
                }
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
          if (!productData.maVatTu || (typeof productData.maVatTu === 'string' && productData.maVatTu.trim() === '')) {
            const availableHeaders = actualHeaders.join(', ');
            const normalizedHeaders = actualHeaders.map(h => normalizeHeader(h)).join(', ');
            errors.push({
              row: rowNumber,
              error: `Mã vật tư là bắt buộc nhưng không tìm thấy trong dòng ${rowNumber}. Headers trong file: ${availableHeaders}. Giá trị trong row: ${JSON.stringify(row)}`,
            });
            failed++;
            continue;
          }

          // Kiểm tra xem đã tồn tại chưa (dựa trên maVatTu)
          const existing = await this.productItemRepository.findOne({
            where: { maVatTu: productData.maVatTu },
          });

          if (existing) {
            // Xóa record cũ và tạo mới thay vì cập nhật
            await this.productItemRepository.remove(existing);
          }
          
          // Tạo mới (hoặc tạo lại sau khi xóa)
          const product = this.productItemRepository.create(productData);
          await this.productItemRepository.save(product);

          success++;
        } catch (error: any) {
          this.logger.error(`Error importing row ${rowNumber}: ${error.message}`);
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

  async createPromotion(createDto: CreatePromotionItemDto): Promise<PromotionItem> {
    const promotion = this.promotionItemRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    return await this.promotionItemRepository.save(promotion);
  }

  async updatePromotion(id: string, updateDto: UpdatePromotionItemDto): Promise<PromotionItem> {
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
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, { raw: false }) as any[];

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
        taiKhoanChiPhiKhuyenMai: ['tài khoản chi phí khuyến mãi', 'tai khoan chi phi khuyen mai'],
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
        'VOUCHER': 'voucher',
        'COUPON': 'coupon',
        'ECODE': 'ecode',
        'VOUCHER, COUPON, ECODE...': 'voucher', // Nếu có nhiều trong 1 field, map vào voucher
        'Tặng hàng': 'tangHang',
        'NSKM': 'nskm',
        'Combo': 'combo',
        'Mã phí': 'maPhi',
        'Mã bộ phận': 'maBoPhan',
        'Tài khoản chiết khấu': 'taiKhoanChietKhau',
        'Tài khoản chi phí khuyến mãi': 'taiKhoanChiPhiKhuyenMai',
        'Trạng thái': 'trangThai',
      };

      // Merge cả fieldMappingVariants và fieldMapping vào normalizedMapping
      const normalizedMapping: Record<string, string> = {};
      
      // Thêm từ fieldMappingVariants (đã có nhiều biến thể)
      for (const [fieldName, variants] of Object.entries(fieldMappingVariants)) {
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
        if (value === null || value === undefined || value === '') return undefined;
        if (typeof value === 'boolean') return value;
        const str = String(value).toLowerCase().trim();
        return str === 'true' || str === '1' || str === 'yes' || str === 'có' || str === 'x';
      };

      // Lấy danh sách headers thực tế từ Excel
      const actualHeaders = data.length > 0 ? Object.keys(data[0]) : [];
      this.logger.log(`Excel headers found: ${actualHeaders.join(', ')}`);
      this.logger.log(`Looking for 'Mã chương trình' in normalized headers: ${normalizeHeader('Mã chương trình')}`);

      // Lọc bỏ các dòng trống trước khi xử lý và lưu lại index gốc
      const nonEmptyRowsWithIndex = data
        .map((row, index) => ({ row, originalIndex: index }))
        .filter(({ row }) => {
          return !actualHeaders.every(header => {
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
            
            if (fieldName && row[actualHeader] !== undefined && row[actualHeader] !== null) {
              const rawValue = row[actualHeader];
              if (typeof rawValue === 'string' && rawValue.trim() === '') {
                continue;
              }

              // Xử lý boolean fields
              if (fieldName.includes('muaHangGiamGia') || 
                  fieldName.includes('ck') || 
                  fieldName === 'voucher' || 
                  fieldName === 'coupon' || 
                  fieldName === 'ecode' || 
                  fieldName === 'tangHang' || 
                  fieldName === 'nskm' || 
                  fieldName === 'combo') {
                const boolValue = normalizeBoolean(rawValue);
                if (boolValue !== undefined) {
                  promotionData[fieldName] = boolValue;
                }
              }
              // Xử lý string fields
              else {
                const stringValue = String(rawValue).trim();
                if (stringValue !== '') {
                  promotionData[fieldName] = stringValue;
                }
              }
            }
          }

          // Validate required fields
          if (!promotionData.maChuongTrinh || (typeof promotionData.maChuongTrinh === 'string' && promotionData.maChuongTrinh.trim() === '')) {
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
          this.logger.error(`Error importing promotion row ${rowNumber}: ${error.message}`);
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
      this.logger.error(`Error importing promotions Excel file: ${error.message}`);
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

  async createWarehouse(createDto: CreateWarehouseItemDto): Promise<WarehouseItem> {
    const warehouse = this.warehouseItemRepository.create({
      ...createDto,
      trangThai: createDto.trangThai || 'active',
    });

    return await this.warehouseItemRepository.save(warehouse);
  }

  async updateWarehouse(id: string, updateDto: UpdateWarehouseItemDto): Promise<WarehouseItem> {
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
      const data = XLSX.utils.sheet_to_json(worksheet, { raw: false }) as any[];

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
          return !actualHeaders.every(header => {
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
            
            if (fieldName && row[actualHeader] !== undefined && row[actualHeader] !== null) {
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
          if (!warehouseData.maKho || (typeof warehouseData.maKho === 'string' && warehouseData.maKho.trim() === '')) {
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
          this.logger.error(`Error importing warehouse row ${rowNumber}: ${error.message}`);
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
      this.logger.error(`Error importing warehouses Excel file: ${error.message}`);
      throw new Error(`Lỗi khi import file Excel: ${error.message}`);
    }
  }
}

