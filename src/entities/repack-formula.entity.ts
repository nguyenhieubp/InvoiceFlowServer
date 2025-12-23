import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';
import { RepackFormulaItem } from './repack-formula-item.entity';

@Entity('repack_formulas')
@Index(['api_id', 'brand'], { unique: true })
@Index(['repack_cat_name', 'brand'])
@Index(['valid_fromdate'])
@Index(['sync_date_from', 'sync_date_to', 'brand'])
export class RepackFormula {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Dữ liệu từ API get_repack_formula
  @Column({ unique: false })
  @Index()
  api_id: number; // id từ API (155698071)

  @Column({ type: 'text', nullable: true })
  name: string; // "Tách_PHOIMVC5TR_KM sang -> V5TR_DV"

  @Column({ nullable: true })
  check_qty_constraint: string; // "Y" hoặc "N"

  @Column({ type: 'decimal', precision: 5, scale: 2, default: 0 })
  depr_pct: number; // 0, 20, etc.

  @Column({ type: 'text', nullable: true })
  branch_codes: string | null; // null hoặc danh sách branch codes

  @Column({ nullable: true })
  repack_cat_name: string; // "Chia/tách SP", "Gộp"

  @Column({ type: 'timestamp', nullable: true })
  valid_fromdate: Date; // 05/11/2025 00:00

  @Column({ type: 'timestamp', nullable: true })
  valid_todate: Date | null; // null hoặc ngày kết thúc

  @Column({ nullable: true })
  enteredby: string; // THUY.VUTHITHANH@MENARD.COM.VN

  @Column({ type: 'timestamp', nullable: true })
  enteredat: Date; // 05/11/2025 11:32

  @Column({ nullable: true })
  locked: string; // "N" hoặc "Y"

  // Relationship với items
  @OneToMany(() => RepackFormulaItem, (item) => item.repackFormula, { cascade: true, eager: false })
  items: RepackFormulaItem[];

  // Metadata
  @Column({ nullable: true })
  sync_date_from: string; // Ngày sync từ (format: DDMMMYYYY)

  @Column({ nullable: true })
  sync_date_to: string; // Ngày sync đến (format: DDMMMYYYY)

  @Column({ nullable: true })
  @Index()
  brand: string; // Brand name (menard, f3, labhair, yaman)

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

