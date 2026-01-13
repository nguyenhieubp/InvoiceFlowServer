import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('fast_api_invoices')
@Index(['docCode'], { unique: true })
export class FastApiInvoice {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ unique: true })
  docCode: string; // Mã đơn hàng (so_ct)

  @Column({ nullable: true })
  maDvcs: string; // Mã đơn vị cơ sở

  @Column({ nullable: true })
  maKh: string; // Mã khách hàng

  @Column({ nullable: true })
  tenKh: string; // Tên khách hàng

  @Column({ type: 'timestamp', nullable: true })
  ngayCt: Date; // Ngày chứng từ

  @Column({ type: 'int', default: 0 })
  status: number; // Status từ Fast API (0 = lỗi, 1 = thành công)

  @Column({ type: 'text', nullable: true })
  message: string; // Message từ Fast API

  @Column({ type: 'text', nullable: true })
  guid: string; // GUID từ Fast API response

  @Column({ type: 'text', nullable: true })
  fastApiResponse: string; // Toàn bộ response từ Fast API (JSON string)

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
