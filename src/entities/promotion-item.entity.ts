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

  @Column({ nullable: true })
  muaHangGiamGia?: string;

  @Column({ nullable: true })
  ckTheoCS?: string; // CK theo CS

  @Column({ nullable: true })
  ckVIP?: string; // CK VIP

  @Column({ nullable: true })
  voucher?: string; // VOUCHER

  @Column({ nullable: true })
  coupon?: string; // COUPON

  @Column({ nullable: true })
  ecode?: string; // ECODE

  @Column({ nullable: true })
  tangHang?: string; // Tặng hàng

  @Column({ nullable: true })
  nskm?: string; // NSKM

  @Column({ nullable: true })
  combo?: string; // Combo

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
