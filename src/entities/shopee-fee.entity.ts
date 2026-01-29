import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('shopee_fee')
@Index(['erpOrderCode', 'orderSn'], { unique: true })
export class ShopeeFee {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'brand', length: 50, nullable: true })
  @Index()
  brand: string;

  @Column({ name: 'platform', length: 50, default: 'shopee' })
  platform: string;

  @Column({ name: 'erp_order_code', length: 100 })
  @Index()
  erpOrderCode: string;

  @Column({ name: 'order_sn', length: 100, nullable: true })
  @Index()
  orderSn: string;

  @Column({
    name: 'voucher_shop',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  voucherShop: number;

  @Column({
    name: 'commission_fee',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  commissionFee: number;

  @Column({
    name: 'service_fee',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  serviceFee: number;

  @Column({
    name: 'payment_fee',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  paymentFee: number;

  @Column({ name: 'order_created_at', type: 'timestamp', nullable: true })
  @Index()
  orderCreatedAt: Date;

  @Column({ name: 'synced_at', type: 'timestamp' })
  @Index()
  syncedAt: Date;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
