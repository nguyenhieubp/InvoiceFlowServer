import { Entity, Column, PrimaryColumn, UpdateDateColumn } from 'typeorm';

@Entity('po_charge_history')
export class POChargeHistory {
    @PrimaryColumn()
    dh_so: string;

    @PrimaryColumn()
    dong: number;

    @Column({ nullable: true })
    ma_cp: string;

    @Column({ type: 'decimal', precision: 18, scale: 4, default: 0 })
    cp01_nt: number;

    @Column({ type: 'decimal', precision: 18, scale: 4, default: 0 })
    cp02_nt: number;

    @Column({ type: 'decimal', precision: 18, scale: 4, default: 0 })
    cp03_nt: number;

    @Column({ type: 'decimal', precision: 18, scale: 4, default: 0 })
    cp04_nt: number;

    @Column({ type: 'decimal', precision: 18, scale: 4, default: 0 })
    cp05_nt: number;

    @Column({ type: 'decimal', precision: 18, scale: 4, default: 0 })
    cp06_nt: number;

    @UpdateDateColumn()
    updated_at: Date;
}
