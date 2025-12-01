import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('product_items')
export class ProductItem {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ nullable: true })
  maNhanHieu?: string;

  @Column({ nullable: true })
  loai?: string;

  @Column({ nullable: true })
  lop?: string;

  @Column({ nullable: true })
  nhom?: string;

  @Column({ nullable: true })
  line?: string;

  @Column({ nullable: true })
  maERP?: string;

  @Column({ nullable: true })
  maVatTuNhaCungCap?: string;

  @Column({ nullable: true })
  maVatTu?: string;

  @Column({ type: 'text', nullable: true })
  tenVatTu?: string;

  @Column({ type: 'text', nullable: true })
  tenKhac?: string;

  @Column({ type: 'text', nullable: true })
  tenHD?: string;

  @Column({ nullable: true })
  barcode?: string;

  @Column({ nullable: true })
  dvt?: string;

  @Column({ type: 'boolean', default: false })
  nhieuDvt?: boolean;

  @Column({ type: 'boolean', default: false })
  theoDoiTonKho?: boolean;

  @Column({ type: 'boolean', default: false })
  theoDoiLo?: boolean;

  @Column({ type: 'boolean', default: false })
  theoDoiKiemKe?: boolean;

  @Column({ type: 'boolean', default: false })
  theoDoiSerial?: boolean;

  @Column({ nullable: true })
  cachTinhGia?: string;

  @Column({ nullable: true })
  loaiVatTu?: string;

  @Column({ nullable: true })
  tkVatTu?: string;

  @Column({ nullable: true })
  loaiHang?: string;

  @Column({ nullable: true })
  nhomGia?: string;

  @Column({ nullable: true })
  maKho?: string;

  @Column({ nullable: true })
  maViTri?: string;

  @Column({ type: 'decimal', precision: 10, scale: 2, nullable: true })
  thueGiaTriGiaTang?: number;

  @Column({ type: 'decimal', precision: 10, scale: 2, nullable: true })
  thueNhapKhau?: number;

  @Column({ type: 'boolean', default: false })
  suaTkVatTu?: boolean;

  @Column({ nullable: true })
  tkGiaVonBanBuon?: string;

  @Column({ nullable: true })
  tkDoanhThuBanBuon?: string;

  @Column({ nullable: true })
  tkDoanhThuNoiBo?: string;

  @Column({ nullable: true })
  tkHangBanTraLai?: string;

  @Column({ nullable: true })
  tkDaiLy?: string;

  @Column({ nullable: true })
  tkSanPhamDoDang?: string;

  @Column({ nullable: true })
  tkChenhLechGiaVon?: string;

  @Column({ nullable: true })
  tkChietKhau?: string;

  @Column({ nullable: true })
  tkChiPhiKhuyenMai?: string;

  @Column({ nullable: true })
  kieuLo?: string;

  @Column({ nullable: true })
  cachXuat?: string;

  @Column({ type: 'int', nullable: true })
  vongDoiSP?: number; // Vòng đời sản phẩm (số ngày)

  @Column({ type: 'int', nullable: true })
  tgBaoHanh?: number; // Thời gian bảo hành (số ngày)

  @Column({ type: 'boolean', default: false })
  choPhepTaoLoNgayKhiNhap?: boolean;

  @Column({ nullable: true })
  abc?: string;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  soLuongTonToiThieu?: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  soLuongTonToiDa?: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  theTich?: number;

  @Column({ nullable: true })
  donViTinhTheTich?: string;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  khoiLuong?: number;

  @Column({ nullable: true })
  donViTinhKhoiLuong?: string;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  giaDichVu?: number;

  @Column({ nullable: true })
  loaiHinhDichVu?: string;

  @Column({ nullable: true })
  maVatTuGoc?: string;

  @Column({ nullable: true })
  tkGiaVonBanLe?: string;

  @Column({ nullable: true })
  tkDoanhThuBanLe?: string;

  @Column({ nullable: true })
  tkChiPhiKhauHaoCCDC?: string;

  @Column({ nullable: true })
  tkChiPhiKhauHaoTSDC?: string;

  @Column({ nullable: true })
  tkDoanhThuHangNo?: string;

  @Column({ nullable: true })
  tkGiaVonHangNo?: string;

  @Column({ nullable: true })
  tkVatTuHangNo?: string;

  @Column({ type: 'text', nullable: true })
  danhSachDonVi?: string;

  @Column({ nullable: true })
  maNhaCungCap?: string;

  @Column({ type: 'decimal', precision: 10, scale: 2, nullable: true })
  tyLeTrichGiaVon?: number;

  @Column({ nullable: true, default: 'active' })
  trangThai?: string;

  @Column({ nullable: true })
  nguoiTao?: string;

  @Column({ nullable: true })
  nguoiSua?: string;

  @CreateDateColumn()
  ngayTao: Date;

  @UpdateDateColumn()
  ngaySua: Date;
}

