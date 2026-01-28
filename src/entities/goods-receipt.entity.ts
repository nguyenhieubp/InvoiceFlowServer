import {
  Entity,
  Column,
  PrimaryColumn,
  Index,
  CreateDateColumn,
} from 'typeorm';

@Entity('goods_receipts')
@Index(['grDate'])
@Index(['grCode'])
@Index(['poCode'])
@Index(['itemCode'])
export class GoodsReceipt {
  @PrimaryColumn({ name: 'id', generated: 'uuid' })
  id: string;

  @Column({ name: 'gr_code', length: 50 })
  grCode: string;

  @Column({ name: 'gr_date', type: 'timestamp', nullable: true })
  grDate: Date | null;

  @Column({ name: 'po_code', length: 50, nullable: true })
  poCode: string;

  @Column({ name: 'cat_name', length: 255, nullable: true })
  catName: string;

  @Column({ name: 'item_code', length: 50, nullable: true })
  itemCode: string;

  @Column({ name: 'item_name', length: 500, nullable: true })
  itemName: string;

  @Column({ name: 'manage_type', length: 50, nullable: true })
  manageType: string;

  @Column({ name: 'qty', type: 'decimal', precision: 18, scale: 4, default: 0 })
  qty: number;

  @Column({
    name: 'returned_qty',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  returnedQty: number;

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
    name: 'import_tax_total',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  importTaxTotal: number;

  @Column({
    name: 'disc_total',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  discTotal: number;

  @Column({
    name: 'cuoc_vcqt',
    type: 'decimal',
    precision: 18,
    scale: 4,
    nullable: true,
  })
  cuocVcqt: number;

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

  @Column({
    name: 'item_cost',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  itemCost: number;

  @Column({
    name: 'total_item_cost',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  totalItemCost: number;

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
    nullable: true,
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

  @Column({ name: 'shipto_branch_code', length: 50, nullable: true })
  shipToBranchCode: string;

  @Column({ name: 'is_supplier_promotion_item', length: 1, default: 'N' })
  isSupplierPromotionItem: string;

  @Column({ name: 'is_promotion_prod', length: 1, default: 'N' })
  isPromotionProd: string;

  @Column({ name: 'purchase_type_name', length: 100, nullable: true })
  purchaseTypeName: string;

  @Column({
    name: 'saved_price_for_prom_item',
    type: 'decimal',
    precision: 18,
    scale: 4,
    default: 0,
  })
  savedPriceForPromItem: number;

  @Column({
    name: 'purchase_request_shipment_code',
    length: 50,
    nullable: true,
  })
  purchaseRequestShipmentCode: string;

  @Column({ name: 'batch_serial', length: 100, nullable: true })
  batchSerial: string;

  @CreateDateColumn({ name: 'synced_at' })
  syncedAt: Date;
}
