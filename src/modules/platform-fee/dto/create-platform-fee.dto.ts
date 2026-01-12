import { IsNumber, IsOptional, IsString } from 'class-validator';

export class CreatePlatformFeeDto {
  @IsString()
  erpOrderCode: string;

  @IsString()
  pancakeOrderId: string;

  @IsNumber()
  amount: number;

  @IsString()
  @IsOptional()
  formulaDescription?: string;

  @IsString()
  @IsOptional()
  brand?: string;
}
