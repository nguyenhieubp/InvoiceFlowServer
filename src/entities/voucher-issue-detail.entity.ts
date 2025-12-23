import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { VoucherIssue } from './voucher-issue.entity';

@Entity('voucher_issue_details')
export class VoucherIssueDetail {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Relationship với VoucherIssue
  @ManyToOne(() => VoucherIssue, (voucherIssue) => voucherIssue.details, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'voucherIssueId' })
  voucherIssue: VoucherIssue;

  @Column()
  voucherIssueId: string;

  // Dữ liệu từ API get_1voucher_issue - lưu dạng JSON để linh hoạt
  @Column({ type: 'jsonb', nullable: true })
  detail_data: any; // Lưu toàn bộ dữ liệu chi tiết dạng JSON

  // Hoặc có thể tách ra các trường cụ thể nếu biết cấu trúc
  @Column({ type: 'int', nullable: true })
  seq: number | null;

  @Column({ type: 'varchar', nullable: true })
  itemcode: string | null;

  @Column({ type: 'varchar', nullable: true })
  itemname: string | null;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  qty: number | null;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  price: number | null;

  @Column({ type: 'text', nullable: true })
  description: string | null;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

