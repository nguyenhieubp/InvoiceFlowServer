export class PersonalInfoDto {
  code: string;
  name: string;
  street?: string;
  address?: string; // Một số API dùng address thay vì street
  birthday?: string;
  sexual?: string;
  phone?: string;
  mobile?: string; // Một số API dùng mobile thay vì phone
}

export class SaleDto {
  branch_code: string;
  doccode: string;
  docdate: string;
  docsourcetype?: string; // Optional vì một số API không có
  description?: string;
  partner_code?: string; // Optional vì một số API không có
  itemcode: string;
  itemname: string;
  qty: number;
  revenue: number;
  kenh?: string;
  prom_code?: string;
}

export class CustomerDataDto {
  Personal_Info: PersonalInfoDto;
  Sales: SaleDto[];
}

export class SyncDataDto {
  data_customer: CustomerDataDto;
}

export class SyncResponseDto {
  data: SyncDataDto[];
}

// Response thực tế là mảng các object, mỗi object có key "data"
export type SyncApiResponse = Array<SyncResponseDto>;

