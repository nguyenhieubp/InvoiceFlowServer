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

  // ========== CÁC TRƯỜNG BẮT BUỘC (*) ==========
  @Column({ nullable: true })
  kyHieu?: string; // Ký hiệu

  @Column({ nullable: true })
  maKho?: string; // Mã kho

  @Column({ nullable: true })
  maLo?: string; // Mã lô

  @Column({ nullable: true })
  maThue?: string; // Mã thuế

  @Column({ nullable: true })
  tkNo?: string; // Tk nợ

  @Column({ nullable: true })
  tkDoanhThu?: string; // Tk doanh thu

  @Column({ nullable: true })
  tkGiaVon?: string; // Tk giá vốn

  @Column({ nullable: true })
  tkChiPhiKhuyenMai?: string; // Tk chi phí khuyến mãi

  @Column({ nullable: true })
  tkThueCo?: string; // Tk thuế có

  @Column({ nullable: true })
  cucThue?: string; // Cục thuế

  // ========== CÁC TRƯỜNG KHÁC ==========
  @Column({ nullable: true })
  nhanVienBan?: string; // Nhân viên bán

  @Column({ nullable: true })
  tenNhanVienBan?: string; // Tên nhân viên bán

  @Column({ nullable: true })
  dvt?: string; // Đơn vị tính

  @Column({ nullable: true })
  loai?: string; // Loại

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  giaBan?: number; // Giá bán

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  tienHang?: number; // Tiền hàng

  @Column({ nullable: true })
  maNt?: string; // Mã ngoại tệ

  @Column({ type: 'decimal', precision: 10, scale: 4, nullable: true })
  tyGia?: number; // Tỷ giá

  @Column({ nullable: true })
  maThanhToan?: string; // Mã thanh toán

  @Column({ nullable: true })
  vuViec?: string; // Vụ việc

  @Column({ nullable: true })
  boPhan?: string; // Bộ phận

  @Column({ nullable: true })
  lsx?: string; // LSX

  @Column({ nullable: true })
  sanPham?: string; // Sản phẩm

  @Column({ nullable: true })
  hopDong?: string; // Hợp đồng

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  phi?: number; // Phí

  @Column({ nullable: true })
  kol?: string; // KOL

  @Column({ nullable: true })
  kheUoc?: string; // Khế ước

  @Column({ nullable: true })
  maCa?: string; // Mã ca

  @Column({ type: 'boolean', nullable: true })
  isRewardLine?: boolean; // is_reward_line

  @Column({ type: 'boolean', nullable: true })
  isBundleRewardLine?: boolean; // is_bundle_reward_line

  @Column({ nullable: true })
  dongThuocGoi?: string; // Dòng thuộc gói

  @Column({ nullable: true })
  trangThai?: string; // Trạng thái

  @Column({ nullable: true })
  barcode?: string; // Barcode

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  muaHangGiamGia?: number; // Mua hàng giảm giá

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauMuaHangGiamGia?: number; // Chiết khấu mua hàng giảm giá

  @Column({ nullable: true })
  ckTheoChinhSach?: string; // CK theo chính sách

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauCkTheoChinhSach?: number; // Chiết khấu ck theo chính sách

  @Column({ nullable: true })
  muaHangCkVip?: string; // Mua hàng CK VIP

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauMuaHangCkVip?: number; // Chiết khấu mua hàng CK VIP

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  thanhToanCoupon?: number; // Thanh toán coupon

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThanhToanCoupon?: number; // Chiết khấu thanh toán coupon

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  thanhToanVoucher?: number; // Thanh toán voucher

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThanhToanVoucher?: number; // Chiết khấu thanh toán voucher

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  duPhong1?: number; // Dự phòng 1

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauDuPhong1?: number; // Chiết khấu dự phòng 1

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  duPhong2?: number; // Dự phòng 2

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauDuPhong2?: number; // Chiết khấu dự phòng 2

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  duPhong3?: number; // Dự phòng 3

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauDuPhong3?: number; // Chiết khấu dự phòng 3

  @Column({ nullable: true })
  hang?: string; // Hãng

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauHang?: number; // Chiết khấu hãng

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  thuongBangHang?: number; // Thưởng bằng hàng

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThuongMuaBangHang?: number; // Chiết khấu thưởng mua bằng hàng

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  thanhToanTkTienAo?: number; // Thanh toán TK tiền ảo

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThanhToanTkTienAo?: number; // Chiết khấu thanh toán TK tiền ảo

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  ckThem1?: number; // CK thêm 1

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThem1?: number; // Chiết khấu thêm 1

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  ckThem2?: number; // CK thêm 2

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThem2?: number; // Chiết khấu thêm 2

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  ckThem3?: number; // CK thêm 3

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauThem3?: number; // Chiết khấu thêm 3

  @Column({ nullable: true })
  voucherDp1?: string; // Voucher DP1

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp1?: number; // Chiết khấu Voucher DP1

  @Column({ nullable: true })
  voucherDp2?: string; // Voucher DP2

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp2?: number; // Chiết khấu Voucher DP2

  @Column({ nullable: true })
  voucherDp3?: string; // Voucher DP3

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp3?: number; // Chiết khấu Voucher DP3

  @Column({ nullable: true })
  voucherDp4?: string; // Voucher DP4

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp4?: number; // Chiết khấu Voucher DP4

  @Column({ nullable: true })
  voucherDp5?: string; // Voucher DP5

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp5?: number; // Chiết khấu Voucher DP5

  @Column({ nullable: true })
  voucherDp6?: string; // Voucher DP6

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp6?: number; // Chiết khấu Voucher DP6

  @Column({ nullable: true })
  voucherDp7?: string; // Voucher DP7

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp7?: number; // Chiết khấu Voucher DP7

  @Column({ nullable: true })
  voucherDp8?: string; // Voucher DP8

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  chietKhauVoucherDp8?: number; // Chiết khấu Voucher DP8

  @Column({ type: 'decimal', precision: 15, scale: 2, nullable: true })
  troGia?: number; // Trợ giá

  @Column({ nullable: true })
  maCtkmTangHang?: string; // Mã CTKM tặng hàng

  @Column({ nullable: true })
  maThe?: string; // Mã thẻ

  @Column({ nullable: true })
  soSerial?: string; // Số serial

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

