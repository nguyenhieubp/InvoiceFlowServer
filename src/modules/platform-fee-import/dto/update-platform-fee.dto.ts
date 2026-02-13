import { PartialType } from '@nestjs/mapped-types';
import { ImportPlatformFeeDto } from './import-platform-fee.dto';

// Since we want to allow updating almost any field that was imported
// We can extend PartialType of ImportPlatformFeeDto if it exists and covers all fields,
// OR we can just define a loose DTO since mapped-types might not be installed or ImportDto might not be comprehensive.
// Let's check ImportPlatformFeeDto first.

export class UpdatePlatformFeeDto {
    // Allow updating common fields
    maSan?: string;
    maNoiBoSp?: string;
    ngayDoiSoat?: Date | string;
    maDonHangHoan?: string;
    shopPhatHanhTrenSan?: string;
    giaTriGiamGiaCtkm?: number;
    doanhThuDonHang?: number;

    // Shopee specific
    phiCoDinh605MaPhi164020?: number;
    phiDichVu6MaPhi164020?: number;
    phiThanhToan5MaPhi164020?: number;
    phiHoaHongTiepThiLienKet21150050?: number;
    chiPhiDichVuShippingFeeSaver164010?: number;
    phiPiShipDoMktDangKy164010?: number;

    // TikTok specific
    phiGiaoDichTyLe5164020?: number;
    phiHoaHongTraChoTiktok454164020?: number;
    phiHoaHongTiepThiLienKet150050?: number;
    phiDichVuSfp6164020?: number;

    // Lazada specific
    tenPhiDoanhThu?: string;
    quangCaoTiepThiLienKet?: string;
    maPhiNhanDienHachToan?: string;
    soTienPhi?: number;
    ghiChu?: string;

    // Common MKT
    cotChoBsMkt1?: string;
    cotChoBsMkt2?: string;
    cotChoBsMkt3?: string;
    cotChoBsMkt4?: string;
    cotChoBsMkt5?: string;
    boPhan?: string;

    // Generic Fees
    phi1?: number;
    phi2?: number;
    phi3?: number;
    phi4?: number;
    phi5?: number;
    phi6?: number;
}
