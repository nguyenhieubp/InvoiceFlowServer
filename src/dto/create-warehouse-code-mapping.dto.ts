import { IsOptional, IsString, IsNotEmpty } from 'class-validator';

export class CreateWarehouseCodeMappingDto {
  @IsNotEmpty()
  @IsString()
  maCu: string; // Mã cũ

  @IsNotEmpty()
  @IsString()
  maMoi: string; // Mã mới

  @IsOptional()
  @IsString()
  trangThai?: string;

  @IsOptional()
  @IsString()
  nguoiTao?: string;
}

export class UpdateWarehouseCodeMappingDto extends CreateWarehouseCodeMappingDto {
  @IsOptional()
  @IsString()
  nguoiSua?: string;
}
