import { IsString, IsOptional, IsNumber, IsArray, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';

export class InvoiceItemDto {
  @IsString()
  itemCode: string;

  @IsString()
  itemName: string;

  @IsString()
  @IsOptional()
  uom?: string;

  @IsNumber()
  quantity: number;

  @IsNumber()
  price: number;

  @IsNumber()
  @IsOptional()
  taxRate?: number;

  @IsNumber()
  @IsOptional()
  discountRate?: number;
}

export class CreateInvoiceDto {
  @IsString()
  voucherBook: string;

  @IsString()
  customerCode: string;

  @IsString()
  customerName: string;

  @IsString()
  @IsOptional()
  customerTaxCode?: string;

  @IsString()
  @IsOptional()
  address?: string;

  @IsString()
  @IsOptional()
  phoneNumber?: string;

  @IsString()
  @IsOptional()
  idCardNo?: string;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => InvoiceItemDto)
  items: InvoiceItemDto[];

  @IsString()
  @IsOptional()
  invoiceDate?: string; // Format: DD/MM/YYYY

  @IsString()
  @IsOptional()
  key?: string; // Optional key, nếu không có sẽ tự động tạo
}

