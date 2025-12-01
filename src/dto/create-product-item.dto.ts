import { IsOptional, IsString, IsBoolean, IsNumber, IsInt } from 'class-validator';
import { Type } from 'class-transformer';

export class CreateProductItemDto {
  @IsOptional()
  @IsString()
  maNhanHieu?: string;

  @IsOptional()
  @IsString()
  loai?: string;

  @IsOptional()
  @IsString()
  lop?: string;

  @IsOptional()
  @IsString()
  nhom?: string;

  @IsOptional()
  @IsString()
  line?: string;

  @IsOptional()
  @IsString()
  maERP?: string;

  @IsOptional()
  @IsString()
  maVatTuNhaCungCap?: string;

  @IsOptional()
  @IsString()
  maVatTu?: string;

  @IsOptional()
  @IsString()
  tenVatTu?: string;

  @IsOptional()
  @IsString()
  tenKhac?: string;

  @IsOptional()
  @IsString()
  tenHD?: string;

  @IsOptional()
  @IsString()
  barcode?: string;

  @IsOptional()
  @IsString()
  dvt?: string;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  nhieuDvt?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  theoDoiTonKho?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  theoDoiLo?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  theoDoiKiemKe?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  theoDoiSerial?: boolean;

  @IsOptional()
  @IsString()
  cachTinhGia?: string;

  @IsOptional()
  @IsString()
  loaiVatTu?: string;

  @IsOptional()
  @IsString()
  tkVatTu?: string;

  @IsOptional()
  @IsString()
  loaiHang?: string;

  @IsOptional()
  @IsString()
  nhomGia?: string;

  @IsOptional()
  @IsString()
  maKho?: string;

  @IsOptional()
  @IsString()
  maViTri?: string;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  thueGiaTriGiaTang?: number;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  thueNhapKhau?: number;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  suaTkVatTu?: boolean;

  @IsOptional()
  @IsString()
  tkGiaVonBanBuon?: string;

  @IsOptional()
  @IsString()
  tkDoanhThuBanBuon?: string;

  @IsOptional()
  @IsString()
  tkDoanhThuNoiBo?: string;

  @IsOptional()
  @IsString()
  tkHangBanTraLai?: string;

  @IsOptional()
  @IsString()
  tkDaiLy?: string;

  @IsOptional()
  @IsString()
  tkSanPhamDoDang?: string;

  @IsOptional()
  @IsString()
  tkChenhLechGiaVon?: string;

  @IsOptional()
  @IsString()
  tkChietKhau?: string;

  @IsOptional()
  @IsString()
  tkChiPhiKhuyenMai?: string;

  @IsOptional()
  @IsString()
  kieuLo?: string;

  @IsOptional()
  @IsString()
  cachXuat?: string;

  @IsOptional()
  @IsInt()
  @Type(() => Number)
  vongDoiSP?: number;

  @IsOptional()
  @IsInt()
  @Type(() => Number)
  tgBaoHanh?: number;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  choPhepTaoLoNgayKhiNhap?: boolean;

  @IsOptional()
  @IsString()
  abc?: string;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  soLuongTonToiThieu?: number;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  soLuongTonToiDa?: number;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  theTich?: number;

  @IsOptional()
  @IsString()
  donViTinhTheTich?: string;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  khoiLuong?: number;

  @IsOptional()
  @IsString()
  donViTinhKhoiLuong?: string;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  giaDichVu?: number;

  @IsOptional()
  @IsString()
  loaiHinhDichVu?: string;

  @IsOptional()
  @IsString()
  maVatTuGoc?: string;

  @IsOptional()
  @IsString()
  tkGiaVonBanLe?: string;

  @IsOptional()
  @IsString()
  tkDoanhThuBanLe?: string;

  @IsOptional()
  @IsString()
  tkChiPhiKhauHaoCCDC?: string;

  @IsOptional()
  @IsString()
  tkChiPhiKhauHaoTSDC?: string;

  @IsOptional()
  @IsString()
  tkDoanhThuHangNo?: string;

  @IsOptional()
  @IsString()
  tkGiaVonHangNo?: string;

  @IsOptional()
  @IsString()
  tkVatTuHangNo?: string;

  @IsOptional()
  @IsString()
  danhSachDonVi?: string;

  @IsOptional()
  @IsString()
  maNhaCungCap?: string;

  @IsOptional()
  @IsNumber()
  @Type(() => Number)
  tyLeTrichGiaVon?: number;

  @IsOptional()
  @IsString()
  trangThai?: string;

  @IsOptional()
  @IsString()
  nguoiTao?: string;
}

export class UpdateProductItemDto extends CreateProductItemDto {
  @IsOptional()
  @IsString()
  nguoiSua?: string;
}

