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

@Entity('sales')
@Index(['docDate']) // Index để query nhanh hơn
@Index(['docDate', 'docCode']) // Composite index cho order by
@Index(['customerId']) // Index cho join với customer
@Index(['isProcessed']) // Index cho filter isProcessed
@Index(['branchCode', 'docDate']) // OPTIMIZATION: Index cho filter by branch + date
@Index(['itemCode']) // OPTIMIZATION: Index cho join với products
@Index(['docCode', 'customerId']) // OPTIMIZATION: Index cho group by customer
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

  @Column({ nullable: true })
  ordertype?: string; // Loại đơn hàng (LAM_DV, NORMAL, etc.)

  @Column({ nullable: true })
  ordertypeName?: string; // Tên loại đơn hàng (01.Thường, etc.)

  @Column({ type: 'text', nullable: true })
  description: string;

  @Column({ nullable: true })
  partnerCode: string;

  @Column({ nullable: true })
  mobile: string; // Số điện thoại khách hàng tại thời điểm bán

  @Column()
  itemCode: string;

  @Column({ type: 'text' })
  itemName: string;

  @Column({ type: 'decimal', precision: 10, scale: 2 })
  qty: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  revenue: number;

  @Column({ nullable: true })
  promCode: string;

  @ManyToOne(() => Customer, (customer) => customer.sales, {
    onDelete: 'CASCADE',
  })
  @JoinColumn({ name: 'customerId' })
  customer: Customer;

  @Column()
  customerId: string;

  @Column({ default: false })
  isProcessed: boolean; // Đã xử lý in hóa đơn chưa

  @Column({ default: true })
  statusAsys: boolean; // Trạng thái đồng bộ: true = đồng bộ thành công, false = sản phẩm không tồn tại trong Loyalty API (404)

  // ========== CÁC TRƯỜNG BẮT BUỘC (*) ==========

  @Column({ nullable: true })
  cucThue?: string; // Cục thuế

  // ========== CÁC TRƯỜNG KHÁC ==========

  @Column({ nullable: true })
  dvt?: string; // Đơn vị tính

  @Column({ nullable: true })
  productType?: string; // Product type từ Loyalty API (VOUC, SKIN, TPCN, GIFT, ...)

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  giaBan?: number; // Giá bán

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  tienHang?: number; // Tiền hàng

  @Column({ nullable: true })
  maCa?: string; // Mã ca

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauMuaHangGiamGia?: number; // Chiết khấu mua hàng giảm giá

  @Column({ nullable: true })
  muaHangCkVip?: string; // Mua hàng CK VIP

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauMuaHangCkVip?: number; // Chiết khấu mua hàng CK VIP

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  thanhToanVoucher?: number; // Thanh toán voucher

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThanhToanTkTienAo?: number; // Chiết khấu thanh toán TK tiền ảo

  @Column({ nullable: true })
  voucherDp1?: string; // Voucher DP1

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp1?: number; // Chiết khấu Voucher DP1

  @Column({ nullable: true })
  maThe?: string; // Mã thẻ

  @Column({ nullable: true })
  soSerial?: string; // Số serial

  // ========== CÁC TRƯỜNG BỔ SUNG TỪ API ==========

  @Column({ nullable: true })
  serial?: string; // Serial number

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  disc_amt?: number; // Discount amount

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  linetotal?: number; // Line total

  @Column({ nullable: true })
  order_source?: string; // Order source

  @Column({ nullable: true })
  partner_name?: string; // Partner name

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  grade_discamt?: number; // Grade discount amount

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  other_discamt?: number; // Other discount amount

  @Column({ nullable: true })
  saleperson_id?: number; // Salesperson ID

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  paid_by_voucher_ecode_ecoin_bp?: number; // Paid by voucher/ecoin/BP

  @Column({ nullable: true })
  api_id?: number; // id từ Zappy API (có thể trùng giữa các dòng khác nhau)

  @Column({ nullable: true })
  compositeKey?: string; // Composite key tổng hợp: docCode + itemCode + qty + giaBan + disc_amt + grade_discamt + other_discamt + revenue + promCode + serial + customerId + api_id

  @Column({ nullable: true })
  brand?: string;

  @Column({ nullable: true, name: 'type_sale' })
  type_sale?: string; // Type sale (WS, WS_WH, WS_RETAIL, WS_RETAIL_WH)

  @Column({ nullable: true }) //
  disc_tm?: string; // For WHOLESALE

  @Column({ nullable: true }) //
  disc_ctkm?: string; // For WHOLESALE

  @Column({ nullable: true }) //
  svc_code?: string;

  @Column({ nullable: true })
  disc_reasons?: string;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
