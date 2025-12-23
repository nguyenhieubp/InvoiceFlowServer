import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { Promotion } from './promotion.entity';

@Entity('promotion_lines')
export class PromotionLine {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Relationship với Promotion
  @ManyToOne(() => Promotion, (promotion) => promotion.lines, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'promotionId' })
  promotion: Promotion;

  @Column()
  promotionId: string;

  // Loại line: 'i_lines' hoặc 'v_lines'
  @Column({ type: 'varchar' })
  line_type: string; // 'i_lines' hoặc 'v_lines'

  // Dữ liệu từ API get_1promotion_line
  @Column({ type: 'int', nullable: true })
  seq: number | null; // 1

  @Column({ type: 'varchar', nullable: true })
  buy_items: string | null; // "F00006"

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  buy_qty: number; // 1

  @Column({ type: 'varchar', nullable: true })
  buy_type: string | null;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  buy_combined_qty: number | null;

  @Column({ type: 'varchar', nullable: true })
  prom_group: string | null;

  @Column({ type: 'varchar', nullable: true })
  card_pattern: string | null;

  @Column({ type: 'varchar', nullable: true })
  get_items: string | null;

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  get_item_price: number | null;

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  get_qty: number; // 1

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  get_discamt: number | null;

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  get_max_discamt: number; // 0

  @Column({ type: 'decimal', precision: 5, scale: 2, nullable: true })
  get_discpct: number | null; // 100

  @Column({ type: 'varchar', nullable: true })
  get_item_option: string | null; // "PKG"

  @Column({ type: 'int', nullable: true })
  svc_card_months: number | null; // 1

  @Column({ type: 'text', nullable: true })
  guideline: string | null; // "TẶNG BUỔI KANMI..."

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

