import { IsString, IsNotEmpty } from 'class-validator';

export class ExplainFaceIdDto {
  @IsString()
  @IsNotEmpty()
  docCode: string; // Mã đơn hàng

  @IsString()
  @IsNotEmpty()
  explanationDate: string; // Ngày giải trình (format: YYYY-MM-DD)

  @IsString()
  @IsNotEmpty()
  explanationMessage: string; // Thông tin giải trình
}

