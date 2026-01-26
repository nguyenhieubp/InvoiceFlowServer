-- Create table for Platform Fee Import
-- Supports Shopee (21 columns), TikTok (19 columns), and Lazada (10 columns)

CREATE TABLE IF NOT EXISTS platform_fee_import (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Platform type: 'shopee', 'tiktok', 'lazada'
    platform VARCHAR(50) NOT NULL,
    
    -- Common fields across all platforms
    ma_san VARCHAR(200), -- Mã shopee/Tiktok/Lazada
    ma_noi_bo_sp VARCHAR(200), -- Mã nội bộ sp
    ngay_doi_soat DATE, -- Ngày đối soát
    ma_don_hang_hoan VARCHAR(200), -- Mã đơn hàng hoàn
    shop_phat_hanh_tren_san VARCHAR(500), -- Shop phát hành trên sàn
    gia_tri_giam_gia_ctkm DECIMAL(15, 2), -- Giá trị giảm giá theo CTKM của mình ban hành
    doanh_thu_don_hang DECIMAL(15, 2), -- Doanh thu đơn hàng
    
    -- Shopee specific fees (6 fees)
    phi_co_dinh_605_ma_phi_164020 DECIMAL(15, 2), -- Phí cố định 6.05% Mã phí 164020
    phi_dich_vu_6_ma_phi_164020 DECIMAL(15, 2), -- Phí Dịch Vụ 6% Mã phí 164020
    phi_thanh_toan_5_ma_phi_164020 DECIMAL(15, 2), -- Phí thanh toán 5% Mã phí 164020
    phi_hoa_hong_tiep_thi_lien_ket_21_150050 DECIMAL(15, 2), -- Phí hoa hồng Tiếp thị liên kết 21% 150050
    chi_phi_dich_vu_shipping_fee_saver_164010 DECIMAL(15, 2), -- Chi phí dịch vụ Shipping Fee Saver 164010
    phi_pi_ship_do_mkt_dang_ky_164010 DECIMAL(15, 2), -- Phí Pi Ship ( Do MKT đăng ký) 164010
    
    -- TikTok specific fees (4 fees)
    phi_giao_dich_ty_le_5_164020 DECIMAL(15, 2), -- Phí giao dịch Tỷ lệ 5% 164020
    phi_hoa_hong_tra_cho_tiktok_454_164020 DECIMAL(15, 2), -- Phí hoa hồng trả cho Tiktok 4.54% 164020
    phi_hoa_hong_tiep_thi_lien_ket_150050 DECIMAL(15, 2), -- Phí hoa hồng Tiếp thị liên kết 150050
    phi_dich_vu_sfp_6_164020 DECIMAL(15, 2), -- Phí dịch vụ SFP 6% 164020
    
    -- Generic fee fields (for Lazada or future use)
    phi_1 DECIMAL(15, 2),
    phi_2 DECIMAL(15, 2),
    phi_3 DECIMAL(15, 2),
    phi_4 DECIMAL(15, 2),
    phi_5 DECIMAL(15, 2),
    phi_6 DECIMAL(15, 2),
    
    -- Shopee/TikTok specific
    ma_cac_ben_tiep_thi_lien_ket VARCHAR(500), -- Mã các bên tiếp thị liên kết
    san_tmdt VARCHAR(100), -- "Sàn TMĐT SHOPEE" / "Sàn TMĐT TIKTOK" / "Sàn TMĐT LAZADA"
    
    -- Additional columns for MKT (Shopee/TikTok)
    cot_cho_bs_mkt_1 TEXT, -- Cột chờ bs nếu MKT đăng ký thêm
    cot_cho_bs_mkt_2 TEXT,
    cot_cho_bs_mkt_3 TEXT,
    cot_cho_bs_mkt_4 TEXT,
    cot_cho_bs_mkt_5 TEXT,
    
    bo_phan VARCHAR(200), -- Bộ phận
    
    -- Lazada specific fields
    ten_phi_doanh_thu VARCHAR(500), -- Tên phí/ doanh thu đơn hàng (Lazada)
    quang_cao_tiep_thi_lien_ket VARCHAR(500), -- Quảng cáo tiếp thị liên kết (Lazada)
    ma_phi_nhan_dien_hach_toan VARCHAR(200), -- MÃ PHÍ ĐỂ NHẬN DIỆN HẠCH TOÁN (Lazada)
    ghi_chu TEXT, -- GHI CHÚ (Lazada)
    
    -- Metadata
    import_batch_id VARCHAR(100), -- To track which import batch this record belongs to
    row_number INTEGER, -- Original row number in Excel
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_platform_fee_import_platform ON platform_fee_import(platform);
CREATE INDEX IF NOT EXISTS idx_platform_fee_import_ma_san ON platform_fee_import(ma_san);
CREATE INDEX IF NOT EXISTS idx_platform_fee_import_ngay_doi_soat ON platform_fee_import(ngay_doi_soat);
CREATE INDEX IF NOT EXISTS idx_platform_fee_import_batch_id ON platform_fee_import(import_batch_id);
CREATE INDEX IF NOT EXISTS idx_platform_fee_import_platform_ma_san_ngay ON platform_fee_import(platform, ma_san, ngay_doi_soat);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_platform_fee_import_updated_at 
    BEFORE UPDATE ON platform_fee_import 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
