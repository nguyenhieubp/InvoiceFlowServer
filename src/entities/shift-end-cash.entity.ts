import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';
import { ShiftEndCashLine } from './shift-end-cash-line.entity';

@Entity('shift_end_cash')
@Index(['api_id', 'brand'], { unique: true })
@Index(['draw_code', 'brand'])
@Index(['docdate'])
@Index(['sync_date', 'brand'])
export class ShiftEndCash {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Dữ liệu từ API get_shift_end_cash
  @Column({ unique: false })
  @Index()
  api_id: number; // id từ API (25969982)

  @Column()
  draw_code: string; // HMH04_1, HMS15_05, etc.

  @Column({ nullable: true })
  @Index()
  branch_code: string; // SMS04A, HMS02, etc.

  @Column({ nullable: true })
  status: string; // "9-Ghi sổ"

  @Column({ nullable: true })
  teller_code: string; // NGOC.NGUYENANH@MENARD.COM.VN

  @Column({ type: 'timestamp', nullable: true })
  openat: Date; // 2025-10-30T01:15:42Z

  @Column({ type: 'timestamp', nullable: true })
  closedat: Date; // 2025-10-30T15:10:06Z

  @Column({ nullable: true })
  shift_status: string; // "Closed"

  @Column({ type: 'timestamp', nullable: true })
  docdate: Date; // 01/11/2025 10:16

  @Column({ type: 'timestamp', nullable: true })
  gl_date: Date; // 01/11/2025 10:16

  @Column({ type: 'text', nullable: true })
  description: string; // "HMH04_Nộp tiền ca ngày 30.10.2025"

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  total: number; // 45772000

  @Column({ type: 'timestamp', nullable: true })
  enteredat: Date; // 01/11/2025 10:16

  @Column({ nullable: true })
  enteredby: string; // NGOC.NGUYENANH@MENARD.COM.VN

  // Relationship với lines
  @OneToMany(() => ShiftEndCashLine, (line) => line.shiftEndCash, {
    cascade: true,
    eager: false,
  })
  lines: ShiftEndCashLine[];

  // Metadata
  @Column({ nullable: true })
  sync_date: string; // Ngày sync (format: DDMMMYYYY)

  @Column({ nullable: true })
  @Index()
  brand: string; // Brand name (menard, f3, labhair, yaman)

  // Payment status fields
  @Column({ nullable: true })
  payment_success: boolean;

  @Column({ type: 'text', nullable: true })
  payment_message: string;

  @Column({ type: 'timestamp', nullable: true })
  payment_date: Date;

  @Column({ type: 'text', nullable: true })
  payment_response: string;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
