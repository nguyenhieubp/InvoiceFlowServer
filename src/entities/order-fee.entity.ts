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

  @Column({ name: 'erp_order_code', length: 100 })
  @Index()
  erpOrderCode: string;

  @Column({ name: 'pancake_order_id', length: 100 })
  @Index()
  pancakeOrderId: string;

  @Column({ name: 'fee_type', length: 100, nullable: true })
  feeType: string;

  @Column({
    name: 'fee_amount',
    type: 'decimal',
    precision: 15,
    scale: 2,
    nullable: true,
  })
  feeAmount: number;

  @Column({ name: 'raw_data', type: 'jsonb' })
  rawData: any;

  @Column({ name: 'synced_at', type: 'timestamp' })
  syncedAt: Date;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
