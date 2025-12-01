import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('promotion_items')
export class PromotionItem {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ nullable: true })
  maChuongTrinh?: string;

  @Column({ type: 'text', nullable: true })
  tenChuongTrinh?: string;

  @Column({ type: 'boolean', default: false })
  muaHangGiamGia?: boolean;

  @Column({ type: 'boolean', default: false })
  ckTheoCS?: boolean; // CK theo CS

  @Column({ type: 'boolean', default: false })
  ckVIP?: boolean; // CK VIP

  @Column({ type: 'boolean', default: false })
  voucher?: boolean; // VOUCHER

  @Column({ type: 'boolean', default: false })
  coupon?: boolean; // COUPON

  @Column({ type: 'boolean', default: false })
  ecode?: boolean; // ECODE

  @Column({ type: 'boolean', default: false })
  tangHang?: boolean; // Tặng hàng

  @Column({ type: 'boolean', default: false })
  nskm?: boolean; // NSKM

  @Column({ type: 'boolean', default: false })
  combo?: boolean; // Combo

  @Column({ nullable: true })
  maPhi?: string;

  @Column({ nullable: true })
  maBoPhan?: string;

  @Column({ nullable: true })
  taiKhoanChietKhau?: string; // Tài khoản chiết khấu

  @Column({ nullable: true })
  taiKhoanChiPhiKhuyenMai?: string; // Tài khoản chi phí khuyến mãi

  @Column({ nullable: true, default: 'active' })
  trangThai?: string;

  @Column({ nullable: true })
  nguoiTao?: string;

  @Column({ nullable: true })
  nguoiSua?: string;

  @CreateDateColumn()
  ngayTao: Date;

  @UpdateDateColumn()
  ngaySua: Date;
}

