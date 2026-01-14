import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('payment_sync_log')
export class PaymentSyncLog {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ nullable: true })
  docCode: string; // so_code or docCode

  @Column({ nullable: true })
  docDate: Date; // ngay_pt or docDate

  @Column({ type: 'text', nullable: true }) // Using text to be safe across DBs, acts as JSON
  requestPayload: string;

  @Column({ type: 'text', nullable: true }) // Using text to be safe
  responsePayload: string;

  @Column()
  status: string; // SUCCESS, ERROR

  @Column({ type: 'text', nullable: true })
  errorMessage: string | null;

  @Column({ default: 0 })
  retryCount: number;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
