import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('warehouse_code_mappings')
@Index(['maCu'], { unique: true })
export class WarehouseCodeMapping {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ nullable: false })
  maCu: string; // Mã cũ

  @Column({ nullable: false })
  maMoi: string; // Mã mới

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
