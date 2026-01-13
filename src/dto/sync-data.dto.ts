export class PersonalInfoDto {
  code: string;
  name: string;
  street?: string;
  address?: string; // Một số API dùng address thay vì street
  birthday?: string;
  sexual?: string;
  phone?: string;
  mobile?: string; // Một số API dùng mobile thay vì phone
  idnumber?: string;
  enteredat?: string;
  crm_lead_source?: string;
  province_name?: string;
  grade_name?: string;
  branch_code?: string;
}

export class SaleDto {
  branch_code: string;
  doccode: string;
  docdate: string;
  docsourcetype?: string; // Optional vì một số API không có
  description?: string;
  partner_code?: string; // Optional vì một số API không có
  partner_name?: string;
  itemcode: string;
  itemname: string;
  qty: number;
  revenue: number;
  kenh?: string;
  prom_code?: string;
  ordertype?: string; // Loại đơn hàng (LAM_DV, NORMAL, etc.)
  // Các trường bổ sung từ API
  cat1?: string;
  cat2?: string;
  cat3?: string;
  catcode1?: string;
  catcode2?: string;
  catcode3?: string;
  ck_tm?: number | null;
  ck_dly?: number | null;
  docid?: number;
  serial?: string | null;
  cm_code?: string | null;
  line_id?: number;
  disc_amt?: number;
  docmonth?: string;
  itemcost?: number;
  linetotal?: number;
  totalcost?: number;
  crm_emp_id?: number;
  doctype_name?: string;
  order_source?: string | null;
  crm_branch_id?: number;
  grade_discamt?: number;
  revenue_wsale?: number;
  saleperson_id?: number;
  revenue_retail?: number;
  paid_by_voucher_ecode_ecoin_bp?: number;
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
