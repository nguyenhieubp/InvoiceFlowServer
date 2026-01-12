import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('platform_fee')
@Index(['erpOrderCode', 'pancakeOrderId'], { unique: true })
export class PlatformFee {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'erp_order_code', length: 100 })
  @Index()
  erpOrderCode: string;

  @Column({ name: 'pancake_order_id', length: 100 })
  @Index()
  pancakeOrderId: string;

  @Column({ name: 'amount', type: 'decimal', precision: 15, scale: 2 })
  amount: number;

  @Column({ name: 'formula_description', type: 'text', nullable: true })
  formulaDescription: string;

  @Column({ name: 'synced_at', type: 'timestamp' })
  syncedAt: Date;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
