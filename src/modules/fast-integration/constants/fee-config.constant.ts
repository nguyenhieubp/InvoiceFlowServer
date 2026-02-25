export interface FeeMappingRule {
    field: string;
    rawName: string;
    defaultCode: string;
    row: number;
    targetCol?: 'cp01_nt' | 'cp02_nt';
}

export const SHOPEE_FEE_CONFIG: FeeMappingRule[] = [
    {
        field: 'commissionFee',
        rawName: 'Phí cố định 6.05% Mã phí 164020',
        defaultCode: '164020',
        row: 1,
    },
    {
        field: 'serviceFee',
        rawName: 'Phí Dịch Vụ 6% Mã phí 164020',
        defaultCode: '164020',
        row: 2,
    },
    {
        field: 'paymentFee',
        rawName: 'Phí thanh toán 5% Mã phí 164020',
        defaultCode: '164020',
        row: 3,
    },
    {
        field: 'affiliateFee',
        rawName: 'Phí hoa hồng Tiếp thị liên kết 21% 150050',
        defaultCode: '150050',
        row: 4,
    },
    {
        field: 'shippingFeeSaver',
        rawName: 'Chi phí dịch vụ Shipping Fee Saver 164010',
        defaultCode: '164010',
        row: 5,
        targetCol: 'cp02_nt',
    },
    {
        field: 'marketingFee',
        rawName: 'Phí Pi Ship ( Do MKT đăng ký) 164010',
        defaultCode: '164010',
        row: 5,
    },
];

export const SHOPEE_IMPORT_FEE_CONFIG: FeeMappingRule[] = [
    {
        field: 'phiCoDinh605MaPhi164020',
        rawName: 'Phí cố định 6.05% Mã phí 164020',
        defaultCode: '164020',
        row: 1,
    },
    {
        field: 'phiDichVu6MaPhi164020',
        rawName: 'Phí Dịch Vụ 6% Mã phí 164020',
        defaultCode: '164020',
        row: 2,
    },
    {
        field: 'phiThanhToan5MaPhi164020',
        rawName: 'Phí thanh toán 5% Mã phí 164020',
        defaultCode: '164020',
        row: 3,
    },
    {
        field: 'phiHoaHongTiepThiLienKet21150050',
        rawName: 'Phí hoa hồng Tiếp thị liên kết 21% 150050',
        defaultCode: '150050',
        row: 4,
    },
    {
        field: 'chiPhiDichVuShippingFeeSaver164010',
        rawName: 'Chi phí dịch vụ Shipping Fee Saver 164010',
        defaultCode: '164010',
        row: 5,
        targetCol: 'cp02_nt',
    },
    {
        field: 'phiPiShipDoMktDangKy164010',
        rawName: 'Phí Pi Ship ( Do MKT đăng ký) 164010',
        defaultCode: '164010',
        row: 6,
    },
];

export const TIKTOK_IMPORT_FEE_CONFIG: FeeMappingRule[] = [
    {
        field: 'phiGiaoDichTyLe5164020',
        rawName: 'Phí giao dịch Tỷ lệ 5% 164020',
        defaultCode: '164020',
        row: 1,
    },
    {
        field: 'phiHoaHongTraChoTiktok454164020',
        rawName: 'Phí hoa hồng trả cho Tiktok 4.54% 164020',
        defaultCode: '164020',
        row: 2,
    },
    {
        field: 'phiDichVuSfp6164020',
        rawName: 'Phí dịch vụ SFP 6% 164020',
        defaultCode: '164020',
        row: 3,
    },
    {
        field: 'phiHoaHongTiepThiLienKet150050',
        rawName: 'Phí hoa hồng Tiếp thị liên kết 150050',
        defaultCode: '164020',
        row: 4,
    },
];

export const TIKTOK_FEE_CONFIG: FeeMappingRule[] = [
    {
        field: 'subTotal',
        rawName: 'Phí hoa hồng trả cho Tiktok 4.54% 164020',
        defaultCode: '164020',
        row: 1,
    },
    {
        field: 'totalAmount',
        rawName: 'Phí giao dịch Tỷ lệ 5% 164020',
        defaultCode: '164020',
        row: 2,
    },
    {
        field: 'sellerDiscount',
        rawName: 'Phí dịch vụ SFP 6% 164020',
        defaultCode: '164020',
        row: 3,
    },
    {
        field: 'affiliateCommission',
        rawName: 'Phí hoa hồng Tiếp thị liên kết 150050',
        defaultCode: '150050',
        row: 4,
    },
];
