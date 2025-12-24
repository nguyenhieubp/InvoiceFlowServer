export class CreatePaymentMethodDto {
  code: string;
  description?: string;
  documentType?: string;
  trangThai?: string;
}

export class UpdatePaymentMethodDto {
  code?: string;
  description?: string;
  documentType?: string;
  trangThai?: string;
}

