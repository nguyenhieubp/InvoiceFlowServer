/**
 * Order Types for Backend
 * Các type definitions cho orders
 */

export interface OrderCustomer {
  code: string;
  name: string;
  brand: string;
  mobile?: string;
  sexual?: string;
  idnumber?: string;
  enteredat?: string;
  crm_lead_source?: string;
  address?: string;
  province_name?: string;
  birthday?: string;
  grade_name?: string;
  branch_code?: string;
}

export interface SaleItem {
  id?: string;
  promCode?: string;
  itemCode?: string;
  itemName?: string;
  description?: string;
  partnerCode?: string;
  ordertype?: string;
  branchCode?: string;
  serial?: string;
  qty?: number;
  revenue?: number;
  linetotal?: number;
  tienHang?: number;
  giaBan?: number;
  disc_amt?: number;
  grade_discamt?: number;
  paid_by_voucher_ecode_ecoin_bp?: number;
  saleperson_id?: number;
  order_source?: string;
  partner_name?: string;
  producttype?: string;
  pkg_code?: string;
  social_page_id?: string;
  sp_email?: string;
  mvc_serial?: string;
  vc_promotion_code?: string;
  cat1?: string;
  cat2?: string;
  cat3?: string;
  catcode1?: string;
  catcode2?: string;
  catcode3?: string;
  [key: string]: any;
}

export interface Order {
  docCode: string;
  docDate: string;
  branchCode: string;
  docSourceType: string;
  customer: OrderCustomer;
  totalRevenue: number;
  totalQty: number;
  totalItems: number;
  isProcessed: boolean;
  sales?: SaleItem[];
}

