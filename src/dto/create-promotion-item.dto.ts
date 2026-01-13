import { IsOptional, IsString } from 'class-validator';

export class CreatePromotionItemDto {
  @IsOptional()
  @IsString()
  maChuongTrinh?: string;

  @IsOptional()
  @IsString()
  tenChuongTrinh?: string;

  @IsOptional()
  @IsString()
  muaHangGiamGia?: string;

  @IsOptional()
  @IsString()
  ckTheoCS?: string;

  @IsOptional()
  @IsString()
  ckVIP?: string;

  @IsOptional()
  @IsString()
  voucher?: string;

  @IsOptional()
  @IsString()
  coupon?: string;

  @IsOptional()
  @IsString()
  ecode?: string;

  @IsOptional()
  @IsString()
  tangHang?: string;

  @IsOptional()
  @IsString()
  nskm?: string;

  @IsOptional()
  @IsString()
  combo?: string;

  @IsOptional()
  @IsString()
  maPhi?: string;

  @IsOptional()
  @IsString()
  maBoPhan?: string;

  @IsOptional()
  @IsString()
  taiKhoanChietKhau?: string;

  @IsOptional()
  @IsString()
  taiKhoanChiPhiKhuyenMai?: string;

  @IsOptional()
  @IsString()
  trangThai?: string;

  @IsOptional()
  @IsString()
  nguoiTao?: string;
}

export class UpdatePromotionItemDto extends CreatePromotionItemDto {
  @IsOptional()
  @IsString()
  nguoiSua?: string;
}
