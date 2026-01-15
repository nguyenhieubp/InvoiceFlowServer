import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('order_fee')
@Index(['feeId'], { unique: true })
export class OrderFee {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'fee_id', length: 100 })
  feeId: string;

  @Column({ name: 'brand', length: 50, nullable: true })
  brand: string;

  @Column({ name: 'erp_order_code', length: 100 })
  @Index()
  erpOrderCode: string;

  @Column({ name: 'platform', length: 50 })
  @Index()
  platform: string; // Sàn TMĐT: shopee, lazada, tiktok, etc.

  @Column({ name: 'raw_data', type: 'jsonb' })
  rawData: any;

  @Column({ name: 'synced_at', type: 'timestamp' })
  syncedAt: Date;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
