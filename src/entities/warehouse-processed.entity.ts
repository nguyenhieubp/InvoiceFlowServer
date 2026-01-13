import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('warehouse_processed')
@Index(['docCode']) // Index để query nhanh hơn
@Index(['processedDate']) // Index cho filter theo ngày
export class WarehouseProcessed {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ unique: true })
  docCode: string; // docCode của stock transfer đã được xử lý

  @Column()
  ioType: string; // 'I' hoặc 'O'

  @Column({ type: 'timestamp' })
  processedDate: Date; // Ngày xử lý

  @Column({ type: 'text', nullable: true })
  result?: string; // Kết quả từ API (JSON string)

  @Column({ default: true })
  success: boolean; // Thành công hay thất bại

  @Column({ type: 'text', nullable: true })
  errorMessage?: string; // Thông báo lỗi nếu có

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
