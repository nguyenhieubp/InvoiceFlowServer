import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('platform_fee_import_lazada')
@Index(['maSan', 'ngayDoiSoat'])
export class PlatformFeeImportLazada {
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

  @Column({ name: 'ten_phi_doanh_thu', type: 'varchar', length: 500, nullable: true })
  tenPhiDoanhThu: string | null;

  @Column({
    name: 'quang_cao_tiep_thi_lien_ket',
    type: 'varchar',
    length: 500,
    nullable: true,
  })
  quangCaoTiepThiLienKet: string | null;

  @Column({ name: 'ma_don_hang_hoan', type: 'varchar', length: 200, nullable: true })
  maDonHangHoan: string | null;

  @Column({
    name: 'ma_phi_nhan_dien_hach_toan',
    type: 'varchar',
    length: 200,
    nullable: true,
  })
  maPhiNhanDienHachToan: string | null;

  @Column({
    name: 'so_tien_phi',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  soTienPhi: number | null;

  @Column({ name: 'san_tmdt', type: 'varchar', length: 100, nullable: true })
  sanTmdt: string | null;

  @Column({ name: 'ghi_chu', type: 'text', nullable: true })
  ghiChu: string | null;

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

