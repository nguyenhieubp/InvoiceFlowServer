import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('payment_methods')
export class PaymentMethod {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ unique: true })
  code: string; // Mã phương thức thanh toán (VD: BANK298)

  @Column({ type: 'text', nullable: true })
  description: string; // Diễn giải (VD: ACB_TTM Phú Lâm_JCB, nội địa)

  @Column({ nullable: true })
  documentType: string; // Loại chứng từ (VD: Giấy báo có)

  @Column({ default: 'active' })
  trangThai: string; // Trạng thái (active/inactive)

  @CreateDateColumn()
  ngayTao: Date;

  @UpdateDateColumn()
  ngaySua: Date;

  @Column({ nullable: true })
  nguoiTao?: string;

  @Column({ nullable: true })
  nguoiSua?: string;
}

