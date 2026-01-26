import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('platform_fee_import')
@Index(['platform', 'maSan', 'ngayDoiSoat'])
export class PlatformFeeImport {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Platform type: 'shopee', 'tiktok', 'lazada'
  @Column({ name: 'platform', length: 50 })
  @Index()
  platform: string;

  // Common fields across all platforms
  @Column({ name: 'ma_san', length: 200, nullable: true })
  maSan: string; // Mã shopee/Tiktok/Lazada

  @Column({ name: 'ma_noi_bo_sp', length: 200, nullable: true })
  maNoiBoSp: string; // Mã nội bộ sp

  @Column({ name: 'ngay_doi_soat', type: 'date', nullable: true })
  @Index()
  ngayDoiSoat: Date | null; // Ngày đối soát

  @Column({ name: 'ma_don_hang_hoan', length: 200, nullable: true })
  maDonHangHoan: string; // Mã đơn hàng hoàn

  @Column({ name: 'shop_phat_hanh_tren_san', length: 500, nullable: true })
  shopPhatHanhTrenSan: string; // Shop phát hành trên sàn

  @Column({
    name: 'gia_tri_giam_gia_ctkm',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  giaTriGiamGiaCtkm: number | null; // Giá trị giảm giá theo CTKM của mình ban hành

  @Column({
    name: 'doanh_thu_don_hang',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  doanhThuDonHang: number | null; // Doanh thu đơn hàng

  // Shopee specific fees (6 fees)
  @Column({
    name: 'phi_co_dinh_605_ma_phi_164020',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiCoDinh605MaPhi164020: number | null; // Phí cố định 6.05% Mã phí 164020

  @Column({
    name: 'phi_dich_vu_6_ma_phi_164020',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiDichVu6MaPhi164020: number | null; // Phí Dịch Vụ 6% Mã phí 164020

  @Column({
    name: 'phi_thanh_toan_5_ma_phi_164020',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiThanhToan5MaPhi164020: number | null; // Phí thanh toán 5% Mã phí 164020

  @Column({
    name: 'phi_hoa_hong_tiep_thi_lien_ket_21_150050',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiHoaHongTiepThiLienKet21150050: number | null; // Phí hoa hồng Tiếp thị liên kết 21% 150050

  @Column({
    name: 'chi_phi_dich_vu_shipping_fee_saver_164010',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  chiPhiDichVuShippingFeeSaver164010: number | null; // Chi phí dịch vụ Shipping Fee Saver 164010

  @Column({
    name: 'phi_pi_ship_do_mkt_dang_ky_164010',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiPiShipDoMktDangKy164010: number | null; // Phí Pi Ship ( Do MKT đăng ký) 164010

  // TikTok specific fees (4 fees)
  @Column({
    name: 'phi_giao_dich_ty_le_5_164020',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiGiaoDichTyLe5164020: number | null; // Phí giao dịch Tỷ lệ 5% 164020

  @Column({
    name: 'phi_hoa_hong_tra_cho_tiktok_454_164020',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiHoaHongTraChoTiktok454164020: number | null; // Phí hoa hồng trả cho Tiktok 4.54% 164020

  @Column({
    name: 'phi_hoa_hong_tiep_thi_lien_ket_150050',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiHoaHongTiepThiLienKet150050: number | null; // Phí hoa hồng Tiếp thị liên kết 150050

  @Column({
    name: 'phi_dich_vu_sfp_6_164020',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiDichVuSfp6164020: number | null; // Phí dịch vụ SFP 6% 164020

  // Generic fee fields (for Lazada or future use)
  @Column({
    name: 'phi_1',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phi1: number | null;

  @Column({
    name: 'phi_2',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phi2: number | null;

  @Column({
    name: 'phi_3',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phi3: number | null;

  @Column({
    name: 'phi_4',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phi4: number | null;

  @Column({
    name: 'phi_5',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phi5: number | null;

  @Column({
    name: 'phi_6',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phi6: number | null;

  // Shopee/TikTok specific
  @Column({ name: 'ma_cac_ben_tiep_thi_lien_ket', length: 500, nullable: true })
  maCacBenTiepThiLienKet: string; // Mã các bên tiếp thị liên kết

  @Column({ name: 'san_tmdt', length: 100, nullable: true })
  sanTmdt: string; // "Sàn TMĐT SHOPEE" / "Sàn TMĐT TIKTOK" / "Sàn TMĐT LAZADA"

  // Additional columns for MKT (Shopee/TikTok)
  @Column({ name: 'cot_cho_bs_mkt_1', type: 'text', nullable: true })
  cotChoBsMkt1: string; // Cột chờ bs nếu MKT đăng ký thêm

  @Column({ name: 'cot_cho_bs_mkt_2', type: 'text', nullable: true })
  cotChoBsMkt2: string;

  @Column({ name: 'cot_cho_bs_mkt_3', type: 'text', nullable: true })
  cotChoBsMkt3: string;

  @Column({ name: 'cot_cho_bs_mkt_4', type: 'text', nullable: true })
  cotChoBsMkt4: string;

  @Column({ name: 'cot_cho_bs_mkt_5', type: 'text', nullable: true })
  cotChoBsMkt5: string;

  @Column({ name: 'bo_phan', length: 200, nullable: true })
  boPhan: string; // Bộ phận

  // Lazada specific fields
  @Column({ name: 'ten_phi_doanh_thu', length: 500, nullable: true })
  tenPhiDoanhThu: string; // Tên phí/ doanh thu đơn hàng (Lazada)

  @Column({ name: 'quang_cao_tiep_thi_lien_ket', length: 500, nullable: true })
  quangCaoTiepThiLienKet: string; // Quảng cáo tiếp thị liên kết (Lazada)

  @Column({ name: 'ma_phi_nhan_dien_hach_toan', length: 200, nullable: true })
  maPhiNhanDienHachToan: string; // MÃ PHÍ ĐỂ NHẬN DIỆN HẠCH TOÁN (Lazada)

  @Column({ name: 'ghi_chu', type: 'text', nullable: true })
  ghiChu: string; // GHI CHÚ (Lazada)

  // Metadata
  @Column({ name: 'import_batch_id', length: 100, nullable: true })
  @Index()
  importBatchId: string; // To track which import batch this record belongs to

  @Column({ name: 'row_number', type: 'int', nullable: true })
  rowNumber: number; // Original row number in Excel

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
