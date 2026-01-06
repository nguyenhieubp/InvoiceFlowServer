import { IsString, IsNotEmpty, IsOptional } from 'class-validator';

export class CreateEcommerceCustomerDto {
    @IsNotEmpty({ message: 'Brand là bắt buộc' })
    @IsString()
    brand: string;

    @IsNotEmpty({ message: 'Customer Code là bắt buộc' })
    @IsString()
    customerCode: string;

    @IsNotEmpty({ message: 'Ecom Name là bắt buộc' })
    @IsString()
    ecomName: string;

    @IsOptional()
    @IsString()
    trangThai?: string;
}

export class UpdateEcommerceCustomerDto {
    @IsOptional()
    @IsString()
    brand?: string;

    @IsOptional()
    @IsString()
    customerCode?: string;

    @IsOptional()
    @IsString()
    ecomName?: string;

    @IsOptional()
    @IsString()
    trangThai?: string;
}
