import { IsOptional, IsString, IsBoolean } from 'class-validator';
import { Type } from 'class-transformer';

export class CreatePromotionItemDto {
  @IsOptional()
  @IsString()
  maChuongTrinh?: string;

  @IsOptional()
  @IsString()
  tenChuongTrinh?: string;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  muaHangGiamGia?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  ckTheoCS?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  ckVIP?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  voucher?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  coupon?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  ecode?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  tangHang?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  nskm?: boolean;

  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  combo?: boolean;

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

