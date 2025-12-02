import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { Sale } from './sale.entity';

@Entity('customers')
export class Customer {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ unique: true })
  code: string;

  @Column()
  name: string;

  @Column({ nullable: true })
  mobile: string;

  @Column({ nullable: true })
  sexual: string; // NU, NAM

  @Column({ nullable: true })
  idnumber: string; // CMND/CCCD

  @Column({ type: 'timestamp', nullable: true })
  enteredat: Date;

  @Column({ nullable: true })
  crm_lead_source: string;

  @Column({ nullable: true })
  address: string;

  @Column({ nullable: true })
  province_name: string;

  @Column({ type: 'date', nullable: true })
  birthday: Date;

  @Column({ nullable: true })
  grade_name: string; // Hạng khách hàng

  @Column({ nullable: true })
  branch_code: string;

  @Column({ nullable: true })
  street: string; // Legacy field

  @Column({ nullable: true })
  phone: string; // Legacy field

  @Column({ nullable: true })
  brand: string; // chando, f3, labhair, yaman, menard

  @OneToMany(() => Sale, (sale) => sale.customer)
  sales: Sale[];

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

