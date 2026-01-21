import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from 'typeorm';

@Entity('stock_transfers')
@Index(['transDate']) // Index để query nhanh hơn
@Index(['transDate', 'docCode']) // Composite index cho order by
@Index(['branchCode']) // Index cho filter branch
@Index(['brandCode']) // Index cho filter brand
@Index(['soCode']) // Index cho filter so_code
@Index(['itemCode']) // Index cho filter item code
@Index(['docCode']) // Index cho filter document code
@Index(['brand']) // Index cho filter brand name
export class StockTransfer {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column()
  doctype: string; // SALE_STOCKOUT

  @Column()
  docCode: string; // ST37.00131367_1

  @Column({ type: 'timestamp' })
  transDate: Date; // 01/11/2025 19:00

  @Column({ type: 'text', nullable: true })
  docDesc: string; // Đơn hàng bán lẻ

  @Column()
  branchCode: string; // FS07

  @Column()
  brandCode: string; // NH_FB

  @Column()
  itemCode: string; // F00011

  @Column({ type: 'text' })
  itemName: string; // Joukin_Liệu trình...

  @Column({ nullable: true })
  materialCode?: string; // Material code từ Loyalty API

  @Column()
  stockCode: string; // BFS07

  @Column({ nullable: true })
  relatedStockCode?: string; // null

  @Column()
  ioType: string; // O

  @Column({ type: 'decimal', precision: 10, scale: 2 })
  qty: number; // -5

  @Column({ nullable: true })
  batchSerial?: string; // null

  @Column({ type: 'text', nullable: true })
  lineInfo1?: string; // null

  @Column({ type: 'text', nullable: true })
  lineInfo2?: string; // null

  @Column({ nullable: true })
  soCode?: string; // SO37.00131367

  @Column({ nullable: true })
  syncDate?: string; // Ngày sync (DDMMMYYYY format)

  @Column({ nullable: true })
  brand?: string; // Brand name (f3, labhair, yaman, menard)

  @Column({ nullable: true })
  compositeKey?: string; // Composite key: docCode + itemCode + qty + stockCode + soCode + timestamp

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
