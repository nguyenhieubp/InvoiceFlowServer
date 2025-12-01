import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { Customer } from './customer.entity';

@Entity('sales')
export class Sale {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column()
  branchCode: string;

  @Column()
  docCode: string;

  @Column({ type: 'timestamp' })
  docDate: Date;

  @Column({ nullable: true, default: 'sale' })
  docSourceType: string;

  @Column({ type: 'text', nullable: true })
  description: string;

  @Column({ nullable: true })
  partnerCode: string;

  @Column()
  itemCode: string;

  @Column({ type: 'text' })
  itemName: string;

  @Column({ type: 'decimal', precision: 10, scale: 2 })
  qty: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  revenue: number;

  @Column({ nullable: true })
  kenh: string;

  @Column({ nullable: true })
  promCode: string;

  @ManyToOne(() => Customer, (customer) => customer.sales, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'customerId' })
  customer: Customer;

  @Column()
  customerId: string;

  @Column({ default: false })
  isProcessed: boolean; // Đã xử lý in hóa đơn chưa

  @Column({ nullable: true })
  invoiceKey: string; // Key của hóa đơn đã in

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

