import { IsOptional, IsString } from 'class-validator';

export class CreateWarehouseItemDto {
  @IsOptional()
  @IsString()
  donVi?: string;

  @IsOptional()
  @IsString()
  maKho?: string;

  @IsOptional()
  @IsString()
  maERP?: string;

  @IsOptional()
  @IsString()
  tenKho?: string;

  @IsOptional()
  @IsString()
  maBoPhan?: string;

  @IsOptional()
  @IsString()
  tenBoPhan?: string;

  @IsOptional()
  @IsString()
  trangThai?: string;

  @IsOptional()
  @IsString()
  nguoiTao?: string;
}

export class UpdateWarehouseItemDto extends CreateWarehouseItemDto {
  @IsOptional()
  @IsString()
  nguoiSua?: string;
}
