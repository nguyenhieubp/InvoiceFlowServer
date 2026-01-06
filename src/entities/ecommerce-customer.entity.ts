import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    UpdateDateColumn,
    Index,
} from 'typeorm';

@Entity('ecommerce_customers')
@Index(['customerCode'], { unique: true })
export class EcommerceCustomer {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ nullable: false })
    brand: string; // Thương hiệu (vd: menard)

    @Column({ nullable: false })
    customerCode: string; // Mã khách hàng (vd: KH254032258)

    @Column({ nullable: true, default: 'active' })
    trangThai?: string; // Trạng thái

    @CreateDateColumn()
    ngayTao: Date; // Ngày tạo

    @UpdateDateColumn()
    ngaySua: Date; // Ngày sửa
}
