import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { ShiftEndCash } from './shift-end-cash.entity';

@Entity('shift_end_cash_lines')
export class ShiftEndCashLine {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Relationship với ShiftEndCash
  @ManyToOne(() => ShiftEndCash, (shiftEndCash) => shiftEndCash.lines, {
    onDelete: 'CASCADE',
  })
  @JoinColumn({ name: 'shiftEndCashId' })
  shiftEndCash: ShiftEndCash;

  @Column()
  shiftEndCashId: string;

  // Dữ liệu từ API lines array
  @Column({ nullable: true })
  fop_code: string; // CASH, VOUCHER, BANK301, etc.

  @Column({ type: 'text', nullable: true })
  fop_name: string; // "Tiền mặt", "Voucher", etc.

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  system_amt: number; // Số tiền hệ thống

  @Column({ nullable: true })
  sys_acct_code: string; // Mã tài khoản hệ thống

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  actual_amt: number; // Số tiền thực tế

  @Column({ nullable: true })
  actual_acct_code: string; // Mã tài khoản thực tế

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  diff_amount: number; // Chênh lệch

  @Column({ nullable: true })
  diff_acct_code: string; // Mã tài khoản chênh lệch

  @Column({ type: 'int', nullable: true })
  template_id: number | null; // ID template

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
