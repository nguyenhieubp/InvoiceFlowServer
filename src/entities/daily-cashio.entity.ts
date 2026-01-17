import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('daily_cashio')
export class DailyCashio {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Dữ liệu từ API get_daily_cashio
  @Column({ nullable: true })
  api_id: number; // id từ API

  @Column()
  code: string; // CI03.01478607_2

  @Column({ nullable: true })
  fop_syscode: string; // CASH, VOUCHER, ECOIN, BANK375, VNPAY, etc.

  @Column({ type: 'text', nullable: true })
  fop_description: string; // Mô tả

  @Column({ nullable: true })
  cat_sys_code: string;

  @Column({ nullable: true })
  shift_code: string;

  @Column()
  so_code: string; // SO03.01478607 - Mã đơn hàng để join với sales.docCode

  @Column({ nullable: true })
  master_code: string; // SI03.01478607_1 hoặc SO03.01478607 - Cũng có thể join với sales.docCode

  @Column({ nullable: true })
  invoice_no: string;

  @Column({ nullable: true })
  invoice_code: string;

  @Column({ type: 'timestamp' })
  docdate: Date; // 03-10-2025 10:30

  @Column({ nullable: true })
  branch_code: string; // HMS03

  @Column({ nullable: true })
  partner_code: string; // KH253653998

  @Column({ nullable: true })
  partner_name: string; // Lê Thị Kim Hoa

  @Column({ type: 'text', nullable: true })
  description: string;

  @Column({ nullable: true })
  refno: string; // Số tham chiếu

  @Column({ type: 'timestamp', nullable: true })
  refno_idate: Date; // Ngày tham chiếu

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  total_in: number; // Tổng tiền vào

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  total_out: number; // Tổng tiền ra

  // Metadata
  @Column({ nullable: true })
  sync_date: string; // Ngày sync (format: DDMMMYYYY)

  @Column({ nullable: true })
  brand: string; // Brand name (menard, labhair, yaman, etc.)

  @Column({ nullable: true })
  bank_code: string; // Mã ngân hàng

  @Column({ nullable: true })
  period_code: string; // Mã kỳ

  @Column({ nullable: true })
  partner_type: string; // CUSTOMER, VENDOR, etc.

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
