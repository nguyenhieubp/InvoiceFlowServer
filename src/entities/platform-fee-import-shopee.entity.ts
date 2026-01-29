import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('platform_fee_import_shopee')
@Index(['maSan', 'ngayDoiSoat'])
export class PlatformFeeImportShopee {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'ma_san', type: 'varchar', length: 200, nullable: true })
  @Index()
  maSan: string | null;

  @Column({ name: 'ma_noi_bo_sp', type: 'varchar', length: 200, nullable: true })
  maNoiBoSp: string | null;

  @Column({ name: 'ngay_doi_soat', type: 'date', nullable: true })
  @Index()
  ngayDoiSoat: Date | null;

  @Column({ name: 'ma_don_hang_hoan', type: 'varchar', length: 200, nullable: true })
  maDonHangHoan: string | null;

  @Column({
    name: 'shop_phat_hanh_tren_san',
    type: 'varchar',
    length: 500,
    nullable: true,
  })
  shopPhatHanhTrenSan: string | null;

  @Column({
    name: 'gia_tri_giam_gia_ctkm',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  giaTriGiamGiaCtkm: number | null;

  @Column({
    name: 'doanh_thu_don_hang',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  doanhThuDonHang: number | null;

  // Shopee fee columns (each fee has its own accounting code embedded in the name)
  @Column({
    name: 'phi_co_dinh',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiCoDinh605MaPhi164020: number | null;

  @Column({
    name: 'phi_dich_vu',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiDichVu6MaPhi164020: number | null;

  @Column({
    name: 'phi_thanh_toan',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiThanhToan5MaPhi164020: number | null;

  @Column({
    name: 'phi_hoa_hong_tiep_thi_lien_ket',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiHoaHongTiepThiLienKet21150050: number | null;

  @Column({
    name: 'chi_phi_dich_vu_shipping_fee_saver',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  chiPhiDichVuShippingFeeSaver164010: number | null;

  @Column({
    name: 'phi_pi_ship_do_mkt_dang_ky',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  phiPiShipDoMktDangKy164010: number | null;

  @Column({
    name: 'ma_cac_ben_tiep_thi_lien_ket',
    type: 'varchar',
    length: 500,
    nullable: true,
  })
  maCacBenTiepThiLienKet: string | null;

  @Column({ name: 'san_tmdt', type: 'varchar', length: 100, nullable: true })
  sanTmdt: string | null;

  @Column({ name: 'cot_cho_bs_mkt_1', type: 'text', nullable: true })
  cotChoBsMkt1: string | null;

  @Column({ name: 'cot_cho_bs_mkt_2', type: 'text', nullable: true })
  cotChoBsMkt2: string | null;

  @Column({ name: 'cot_cho_bs_mkt_3', type: 'text', nullable: true })
  cotChoBsMkt3: string | null;

  @Column({ name: 'cot_cho_bs_mkt_4', type: 'text', nullable: true })
  cotChoBsMkt4: string | null;

  @Column({ name: 'cot_cho_bs_mkt_5', type: 'text', nullable: true })
  cotChoBsMkt5: string | null;

  @Column({ name: 'bo_phan', type: 'varchar', length: 200, nullable: true })
  boPhan: string | null;

  // Metadata
  @Column({ name: 'import_batch_id', type: 'varchar', length: 100, nullable: true })
  @Index()
  importBatchId: string | null;

  @Column({ name: 'row_number', type: 'int', nullable: true })
  rowNumber: number | null;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}

