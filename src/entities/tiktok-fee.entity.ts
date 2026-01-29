import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('tiktok_fee')
@Index(['erpOrderCode', 'orderSn'], { unique: true })
export class TikTokFee {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'brand', length: 50, nullable: true })
  @Index()
  brand: string;

  @Column({ name: 'platform', length: 50, default: 'tiktok' })
  platform: string;

  @Column({ name: 'erp_order_code', length: 100 })
  @Index()
  erpOrderCode: string;

  @Column({ name: 'order_sn', length: 100, nullable: true })
  @Index()
  orderSn: string;

  @Column({ name: 'order_status', length: 100, nullable: true })
  orderStatus: string;

  @Column({ name: 'order_created_at', type: 'timestamp', nullable: true })
  @Index()
  orderCreatedAt: Date;

  @Column({ name: 'synced_at', type: 'timestamp' })
  @Index()
  syncedAt: Date;

  // Detailed TikTok Fee fields
  @Column({ name: 'tax', type: 'decimal', precision: 15, scale: 2, default: 0 })
  tax: number;

  @Column({ name: 'currency', length: 20, nullable: true })
  currency: string;

  @Column({
    name: 'sub_total',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  subTotal: number;

  @Column({
    name: 'shipping_fee',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  shippingFee: number;

  @Column({
    name: 'total_amount',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  totalAmount: number;

  @Column({
    name: 'seller_discount',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  sellerDiscount: number;

  @Column({
    name: 'platform_discount',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  platformDiscount: number;

  @Column({
    name: 'original_shipping_fee',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  originalShippingFee: number;

  @Column({
    name: 'original_total_product_price',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  originalTotalProductPrice: number;

  @Column({
    name: 'shipping_fee_seller_discount',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  shippingFeeSellerDiscount: number;

  @Column({
    name: 'shipping_fee_cofunded_discount',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  shippingFeeCofundedDiscount: number;

  @Column({
    name: 'shipping_fee_platform_discount',
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  shippingFeePlatformDiscount: number;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
