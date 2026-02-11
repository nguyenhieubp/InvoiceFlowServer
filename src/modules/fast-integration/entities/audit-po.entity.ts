import { Entity, Column, PrimaryGeneratedColumn, CreateDateColumn } from 'typeorm';

@Entity('audit_po')
export class AuditPo {
    @PrimaryGeneratedColumn()
    id: number;

    @Column({ type: 'varchar', nullable: true })
    dh_so: string | null;

    @Column({ type: 'date', nullable: true })
    dh_ngay: Date | null;

    @Column({ type: 'varchar' })
    action: string;

    @Column({ type: 'json', nullable: true })
    payload: any;

    @Column({ type: 'json', nullable: true })
    response: any;

    @Column({ type: 'varchar' })
    status: string;

    @Column({ type: 'text', nullable: true })
    error: string | null;

    @CreateDateColumn()
    created_at: Date;
}
