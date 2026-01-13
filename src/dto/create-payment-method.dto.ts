import { IsOptional, IsString } from 'class-validator';

export class CreatePaymentMethodDto {
  @IsString()
  code: string;

  @IsOptional()
  @IsString()
  externalId?: string;

  @IsOptional()
  @IsString()
  description?: string;

  @IsOptional()
  @IsString()
  documentType?: string;

  @IsOptional()
  @IsString()
  systemCode?: string;

  @IsOptional()
  @IsString()
  erp?: string;

  @IsOptional()
  @IsString()
  bankUnit?: string;

  @IsOptional()
  @IsString()
  trangThai?: string;

  @IsOptional()
  @IsString()
  maDoiTac?: string;
}

export class UpdatePaymentMethodDto {
  @IsOptional()
  @IsString()
  code?: string;

  @IsOptional()
  @IsString()
  externalId?: string;

  @IsOptional()
  @IsString()
  description?: string;

  @IsOptional()
  @IsString()
  documentType?: string;

  @IsOptional()
  @IsString()
  systemCode?: string;

  @IsOptional()
  @IsString()
  erp?: string;

  @IsOptional()
  @IsString()
  bankUnit?: string;

  @IsOptional()
  @IsString()
  trangThai?: string;

  @IsOptional()
  @IsString()
  maDoiTac?: string;
}
