import {
  Entity,
  Column,
  PrimaryColumn,
  Index,
  CreateDateColumn,
} from 'typeorm';

@Entity('purchase_orders')
@Index(['poDate'])
@Index(['poCode'])
@Index(['itemCode'])
export class PurchaseOrder {
  @PrimaryColumn({ name: 'id', generated: 'uuid' }) // Use UUID as primary key since po_code might not be unique per line item
  id: string;

  @Column({ name: 'po_code', length: 50 })
  poCode: string;

  @Column({ name: 'po_date', type: 'timestamp', nullable: true })
  poDate: Date | null;

  @Column({ name: 'cat_name', length: 255, nullable: true })
  catName: string;

  @Column({ name: 'item_code', length: 50, nullable: true })
  itemCode: string;

  @Column({ name: 'item_name', length: 500, nullable: true })
  itemName: string;

  @Column({ name: 'supplier_item_code', length: 50, nullable: true })
  supplierItemCode: string;

  @Column({ name: 'supplier_item_name', length: 500, nullable: true })
  supplierItemName: string;

  @Column({ name: 'manage_type', length: 50, nullable: true })
  manageType: string;

  @Column({ name: 'qty', type: 'decimal', precision: 18, scale: 4, default: 0 })
  qty: number;

  @Column({
    name: 'received_qty',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  receivedQty: number;

  @Column({
    name: 'returned_qty',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  returnedQty: number;

  @Column({
    name: 'sale_price',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  salePrice: number;

  @Column({
    name: 'price',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  price: number;

  @Column({
    name: 'vat_pct',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  vatPct: number;

  @Column({
    name: 'import_tax_pct',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  importTaxPct: number;

  @Column({
    name: 'import_tax_total',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  importTaxTotal: number;

  @Column({
    name: 'amount',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  amount: number;

  @Column({
    name: 'prom_amount',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  promAmount: number;

  @Column({
    name: 'disc_pct',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  discPct: number;

  @Column({
    name: 'vat_total',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  vatTotal: number;

  @Column({
    name: 'disc_total',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  discTotal: number;

  @Column({
    name: 'line_total',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  lineTotal: number;

  @Column({ name: 'note_category', type: 'text', nullable: true })
  noteCategory: string;

  @Column({ name: 'note_detail', type: 'text', nullable: true })
  noteDetail: string;

  @Column({ name: 'shipto_branch_code', length: 50, nullable: true })
  shipToBranchCode: string;

  @Column({ name: 'shipto_branch_name', length: 255, nullable: true })
  shipToBranchName: string;

  @Column({
    name: 'item_cost',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  itemCost: number;

  @Column({
    name: 'po_cost',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  poCost: number;

  @Column({
    name: 'on_gr_cost',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  onGrCost: number;

  @Column({
    name: 'after_gr_cost',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  afterGrCost: number;

  @Column({ name: 'is_supplier_promotion_item', length: 1, default: 'N' })
  isSupplierPromotionItem: string;

  @Column({ name: 'is_promotion_prod', length: 1, default: 'N' })
  isPromotionProd: string;

  @Column({ name: 'purchase_type_name', length: 100, nullable: true })
  purchaseTypeName: string;

  @Column({ name: 'price_code', length: 50, nullable: true })
  priceCode: string;

  @Column({ name: 'sale_price_code', length: 50, nullable: true })
  salePriceCode: string;

  @Column({
    name: 'saved_price_for_prom_item',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  savedPriceForPromItem: number;

  @Column({ name: 'shipment_code', length: 50, nullable: true })
  shipmentCode: string;

  @Column({ name: 'shipment_name', length: 255, nullable: true })
  shipmentName: string;

  @Column({ name: 'shipment_plan_date', type: 'timestamp', nullable: true })
  shipmentPlanDate: Date | null;

  @Column({ name: 'shipment_trans_method', length: 100, nullable: true })
  shipmentTransMethod: string;

  @CreateDateColumn({ name: 'synced_at' })
  syncedAt: Date;
}
