import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  Index,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

@Entity('platform_fee_map')
@Index(['platform', 'normalizedFeeName'])
export class PlatformFeeMap {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'platform', type: 'varchar', length: 50 })
  platform: string;

  @Column({ name: 'raw_fee_name', type: 'varchar', length: 500 })
  rawFeeName: string;

  @Column({ name: 'normalized_fee_name', type: 'varchar', length: 500 })
  normalizedFeeName: string;

  @Column({ name: 'internal_code', type: 'varchar', length: 100 })
  internalCode: string;

  @Column({ name: 'system_code', type: 'varchar', length: 100, nullable: true })
  systemCode: string | null;

  @Column({ name: 'account_code', type: 'varchar', length: 50 })
  accountCode: string;

  @Column({ name: 'description', type: 'text', nullable: true })
  description: string | null;

  @Column({ name: 'active', type: 'boolean', default: true })
  active: boolean;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}

