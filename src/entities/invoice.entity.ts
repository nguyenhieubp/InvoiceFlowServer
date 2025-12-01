import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { InvoiceItem } from './invoice-item.entity';

@Entity('invoices')
export class Invoice {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ unique: true })
  key: string; // Key định danh hóa đơn (Maxlength = 32)

  @Column({ type: 'date' })
  invoiceDate: Date;

  @Column()
  customerCode: string;

  @Column()
  customerName: string;

  @Column({ nullable: true })
  customerTaxCode: string;

  @Column({ nullable: true })
  address: string;

  @Column({ nullable: true })
  phoneNumber: string;

  @Column({ nullable: true })
  idCardNo: string;

  @Column({ default: 'VND' })
  currency: string;

  @Column({ type: 'decimal', precision: 10, scale: 2, default: 1.0 })
  exchangeRate: number;

  @Column({ type: 'decimal', precision: 15, scale: 2 })
  amount: number; // Tổng tiền hàng chưa thuế

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  discountAmount: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  taxAmount: number; // Tổng tiền thuế GTGT

  @Column({ type: 'decimal', precision: 15, scale: 2 })
  totalAmount: number; // Tổng tiền thanh toán

  @Column({ type: 'text' })
  amountInWords: string;

  @Column({ nullable: true })
  humanName: string;

  @Column()
  voucherBook: string; // C23MKT

  @OneToMany(() => InvoiceItem, (item) => item.invoice, { cascade: true })
  items: InvoiceItem[];

  @Column({ default: false })
  isPrinted: boolean; // Đã in chưa

  @Column({ nullable: true })
  printResponse: string; // Response từ API in

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

