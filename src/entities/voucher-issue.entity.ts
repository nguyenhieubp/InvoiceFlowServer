import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';
import { VoucherIssueDetail } from './voucher-issue-detail.entity';

@Entity('voucher_issues')
@Index(['api_id', 'brand'], { unique: true })
@Index(['code', 'brand'])
@Index(['docdate'])
@Index(['sync_date_from', 'sync_date_to', 'brand'])
export class VoucherIssue {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Dữ liệu từ API get_voucher_issue
  @Column({ type: 'int', unique: false })
  @Index()
  api_id: number; // id từ API (579852)

  @Column({ type: 'varchar', nullable: true })
  code: string; // "6_BOCTHAM11.25"

  @Column({ type: 'varchar', nullable: true })
  status_lov: string; // "8-Hoàn tất"

  @Column({ type: 'timestamp', nullable: true })
  docdate: Date; // 13/11/2025 18:45

  @Column({ type: 'text', nullable: true })
  description: string; // "E_Vc 150k mua hàng bán Chando ct bốc thăm"

  @Column({ type: 'varchar', nullable: true })
  brand_code: string; // "NH_FB"

  @Column({ type: 'varchar', nullable: true })
  apply_for_branch_types: string; // "SHOP"

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  val: number; // 150000

  @Column({
    type: 'decimal',
    precision: 5,
    scale: 2,
    default: 0,
    nullable: true,
  })
  percent: number | null; // 0

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  max_value: number; // 0

  @Column({ type: 'varchar', nullable: true })
  saletype: string; // "Bán/Tặng"

  @Column({ type: 'varchar', nullable: true })
  enable_precost: string; // "N"

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  supplier_support_fee: number; // 0

  @Column({ type: 'timestamp', nullable: true })
  valid_fromdate: Date; // 13/11/2025 00:00

  @Column({ type: 'timestamp', nullable: true })
  valid_todate: Date | null; // 31/12/2025 00:00

  @Column({ type: 'int', default: 0 })
  valid_days_from_so: number; // 0

  @Column({ type: 'varchar', nullable: true })
  check_ownership: string; // "Y"

  @Column({ type: 'varchar', nullable: true })
  allow_cashback: string; // "N"

  @Column({ type: 'varchar', nullable: true })
  prom_for_employee: string; // "N"

  @Column({ type: 'varchar', nullable: true })
  bonus_for_sale_employee: string; // "N"

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  so_percent: number | null;

  @Column({ type: 'varchar', nullable: true })
  r_total_scope: string; // "SO"

  @Column({ type: 'varchar', nullable: true })
  ecode_item_code: string | null;

  @Column({ type: 'varchar', nullable: true })
  voucher_item_code: string; // "V01282"

  @Column({ type: 'text', nullable: true })
  voucher_item_name: string; // "Voucher 150K mua hàng Chando"

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  cost_for_gl: number; // 0

  @Column({ type: 'varchar', nullable: true })
  buy_items_by_date_range: string; // "N"

  @Column({ type: 'varchar', nullable: true })
  buy_items_option_name: string; // "Nhiều SP"

  @Column({ type: 'varchar', nullable: true })
  disable_bonus_point_for_sale: string; // "Y"

  @Column({ type: 'varchar', nullable: true })
  disable_bonus_point: string; // "Y"

  @Column({ type: 'varchar', nullable: true })
  for_mkt_kol: string; // "Y"

  @Column({ type: 'varchar', nullable: true })
  for_mkt_prom: string; // "N"

  @Column({ type: 'varchar', nullable: true })
  allow_apply_for_promoted_so: string; // "N"

  @Column({ type: 'varchar', nullable: true })
  campaign_code: string | null; // "CM.030982"

  @Column({ type: 'int', default: 0 })
  sl_max_sudung_cho_1_kh: number; // 1

  @Column({ type: 'varchar', nullable: true })
  is_locked: string; // "N"

  @Column({ type: 'timestamp', nullable: true })
  enteredat: Date; // 13/11/2025 18:12

  @Column({ type: 'varchar', nullable: true })
  enteredby: string; // "tu.nguyen@facialbar.vn"

  @Column({ type: 'varchar', nullable: true })
  material_type: string; // "DIENTU"

  @Column({ type: 'varchar', nullable: true })
  applyfor_wso: string | null;

  // Relationship với details
  @OneToMany(() => VoucherIssueDetail, (detail) => detail.voucherIssue, {
    cascade: true,
    eager: false,
  })
  details: VoucherIssueDetail[];

  // Metadata
  @Column({ type: 'varchar', nullable: true })
  sync_date_from: string; // Ngày sync từ (format: DDMMMYYYY)

  @Column({ type: 'varchar', nullable: true })
  sync_date_to: string; // Ngày sync đến (format: DDMMMYYYY)

  @Column({ type: 'varchar', nullable: true })
  @Index()
  brand: string; // Brand name (menard, f3, labhair, yaman)

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
