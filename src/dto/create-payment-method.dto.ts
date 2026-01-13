export class CreatePaymentMethodDto {
  code: string;
  externalId?: string;
  description?: string;
  documentType?: string;
  systemCode?: string;
  erp?: string;
  bankUnit?: string;
  trangThai?: string;
}

export class UpdatePaymentMethodDto {
  code?: string;
  externalId?: string;
  description?: string;
  documentType?: string;
  systemCode?: string;
  erp?: string;
  bankUnit?: string;
  trangThai?: string;
}
