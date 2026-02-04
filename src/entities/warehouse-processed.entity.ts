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

  @Column({ nullable: true })
  doctype?: string; // STOCK_REPACK, STOCK_RETURN, etc.

  @Column({ type: 'timestamp' })
  processedDate: Date; // Ngày xử lý

  @Column({ type: 'text', nullable: true })
  result?: string; // Kết quả từ API (JSON string)

  @Column({ default: true })
  success: boolean; // Thành công hay thất bại

  @Column({ type: 'text', nullable: true })
  errorMessage?: string; // Thông báo lỗi nếu có

  @Column({ type: 'text', nullable: true })
  payload?: string; // Dữ liệu gửi sang Fast API (JSON string) - For debugging

  @Column({ type: 'text', nullable: true })
  fastApiResponse?: string; // Toàn bộ response từ Fast API (JSON string)

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
