import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { RepackFormula } from './repack-formula.entity';

@Entity('repack_formula_items')
export class RepackFormulaItem {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Relationship với RepackFormula
  @ManyToOne(() => RepackFormula, (repackFormula) => repackFormula.items, {
    onDelete: 'CASCADE',
  })
  @JoinColumn({ name: 'repackFormulaId' })
  repackFormula: RepackFormula;

  @Column()
  repackFormulaId: string;

  // Loại item: 'from' hoặc 'to'
  @Column()
  item_type: string; // 'from' hoặc 'to'

  // Dữ liệu từ API from_items hoặc to_items
  @Column({ nullable: true })
  itemcode: string; // PHOIMVC5TR_KM, V5TR_DV, etc.

  @Column({ type: 'decimal', precision: 15, scale: 2, default: 0 })
  qty: number; // Số lượng

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
