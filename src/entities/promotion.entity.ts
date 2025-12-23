import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';
import { PromotionLine } from './promotion-line.entity';

@Entity('promotions')
@Index(['api_id', 'brand'], { unique: true })
@Index(['code', 'brand'])
@Index(['fromdate'])
@Index(['sync_date_from', 'sync_date_to', 'brand'])
export class Promotion {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Dữ liệu từ API get_promotion
  @Column({ type: 'int', unique: false })
  @Index()
  api_id: number; // id từ API (567721)

  @Column({ type: 'varchar', nullable: true })
  code: string; // "PRFB.001042"

  @Column({ type: 'int', nullable: true })
  seq: number; // 747

  @Column({ type: 'varchar', nullable: true })
  name: string; // "R511PTDT"

  @Column({ type: 'timestamp', nullable: true })
  fromdate: Date; // 07/11/2025 21:29

  @Column({ type: 'timestamp', nullable: true })
  todate: Date | null; // 31/12/2025 00:00

  @Column({ type: 'varchar', nullable: true })
  ptype: string; // "V" hoặc "P"

  @Column({ type: 'varchar', nullable: true })
  pricetype: string; // "R"

  @Column({ type: 'varchar', nullable: true })
  brand_code: string; // "NH_FB"

  @Column({ type: 'varchar', nullable: true })
  locked: string; // "N" hoặc "Y"

  @Column({ type: 'text', nullable: true })
  status: string; // "8-Hoàn tất"

  @Column({ type: 'varchar', nullable: true })
  enteredby: string; // "LAN.NGUYENHUONG@FACIALBAR.VN"

  @Column({ type: 'timestamp', nullable: true })
  enteredat: Date; // 07/11/2025 21:29

  // Relationship với lines
  @OneToMany(() => PromotionLine, (line) => line.promotion, { cascade: true, eager: false })
  lines: PromotionLine[];

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

