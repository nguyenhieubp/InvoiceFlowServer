import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('warehouse_items')
export class WarehouseItem {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ nullable: true })
  donVi?: string; // Đơn vị

  @Column({ nullable: true })
  maKho?: string; // Mã kho

  @Column({ nullable: true })
  maERP?: string; // Mã ERP

  @Column({ type: 'text', nullable: true })
  tenKho?: string; // Tên kho

  @Column({ nullable: true })
  maBoPhan?: string; // Mã bộ phận

  @Column({ type: 'text', nullable: true })
  tenBoPhan?: string; // Tên bộ phận

  @Column({ nullable: true, default: 'active' })
  trangThai?: string; // Trạng thái

  @Column({ nullable: true })
  nguoiTao?: string; // Người tạo

  @Column({ nullable: true })
  nguoiSua?: string; // Người sửa

  @CreateDateColumn()
  ngayTao: Date; // Ngày tạo

  @UpdateDateColumn()
  ngaySua: Date; // Ngày sửa
}
