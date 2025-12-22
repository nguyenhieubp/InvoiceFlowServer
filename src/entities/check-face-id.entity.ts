import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';
import { Customer } from './customer.entity';

@Entity('check_face_id')
@Index(['partnerCode', 'date']) // Index để query nhanh hơn
export class CheckFaceId {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ nullable: true })
  apiId: number; // ID từ API (id: 915559)

  @Column({ type: 'timestamp', nullable: true })
  startTime: Date; // start_time từ API

  @Column({ type: 'timestamp', nullable: true })
  checking: Date; // checking từ API

  @Column({ type: 'boolean', default: false })
  isFirstInDay: boolean; // is_first_in_day từ API

  @Column({ type: 'text', nullable: true })
  image: string; // image path từ API

  @Column({ nullable: true })
  @Index()
  partnerCode: string; // code (partner_code) - liên kết với Customer

  @Column({ nullable: true })
  name: string; // name từ API

  @Column({ type: 'varchar', length: 50, nullable: true })
  mobile: string; // mobile từ API (đảm bảo đủ độ dài cho số điện thoại)

  @Column({ nullable: true })
  isNv: number; // is_nv từ API (1, 2, etc.)

  @Column({ nullable: true })
  shopCode: string; // shop_code từ API

  @Column({ nullable: true })
  shopName: string; // shop_name từ API

  @Column({ nullable: true })
  camId: string; // cam_id từ API

  @Column({ type: 'date' })
  @Index()
  date: Date; // Ngày check (từ fromDate/toDate)

  @Column({ type: 'boolean', default: false })
  isExplained: boolean; // Đã giải trình chưa

  @Column({ type: 'text', nullable: true })
  explanationMessage: string; // Thông tin giải trình

  @Column({ type: 'date', nullable: true })
  explanationDate: Date; // Ngày giải trình

  @ManyToOne(() => Customer, { nullable: true, onDelete: 'SET NULL' })
  @JoinColumn({ name: 'partnerCode', referencedColumnName: 'code' })
  customer: Customer;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

