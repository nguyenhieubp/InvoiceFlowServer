# Các Case Tạo Hóa Đơn Sang Fast API

Tài liệu này mô tả các trường hợp (case) xử lý khi tạo hóa đơn sang Fast API trong hệ thống InvoiceFlow.

## Tổng Quan

Khi double-click vào một đơn hàng (order line), hệ thống sẽ kiểm tra các điều kiện và gọi các API tương ứng:

1. **Đơn dịch vụ** (02. Làm dịch vụ) - **Lưu ý**: "08. Tách thẻ" KHÔNG phải đơn dịch vụ
2. **Đơn hàng trả lại** (SALE_RETURN)
3. **Đơn hàng có đuôi _X** (ví dụ: SO45.01574458_X)
4. **Đơn hàng bình thường** (01.Thường, 06. Đầu tư, 07. Bán tài khoản, 9. Sàn TMDT, Đổi vỏ)
5. **Đơn đổi điểm** (03. Đổi điểm)
6. **Đơn đổi dịch vụ** (04. Đổi DV)
7. **Đơn tặng sinh nhật** (05. Tặng sinh nhật)
8. **Warehouse Processing** (STOCK_IO, STOCK_TRANSFER)

**Lưu ý:** Tất cả các API calls đều được tập trung qua `fast-api-invoice-flow.service.ts` để đảm bảo tính nhất quán và dễ bảo trì.

## Bảng Tóm Tắt Các Case

| Case | Tên Case | Điều Kiện | Customer | Sales Order | Sales Invoice | Sales Return | Gxt Invoice | Cash Receipt | Credit Advice | Action |
|------|----------|-----------|----------|-------------|---------------|--------------|-------------|--------------|---------------|--------|
| 1 | Đơn Dịch Vụ | `ordertypeName` = "02. Làm dịch vụ" | ✅ | ✅ (0) | ✅ (0) | ❌ | ✅ (0) | ❌ | ❌ | 0 |
| 2 | Đơn có đuôi _X | `docCode` có `_X` hoặc có đơn tương ứng | ❌ | ✅ (1) | ❌ | ❌ | ❌ | ❌ | ❌ | 1 |
| 3 | SALE_RETURN không ST | `docSourceType = 'SALE_RETURN'` + không có stock transfer | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | - |
| 4 | SALE_RETURN có ST | `docSourceType = 'SALE_RETURN'` + có stock transfer | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | - |
| 5 | Đơn Bình Thường | `ordertypeName` = "01.Thường" | ✅ | ✅ (0) | ✅ (0) | ❌ | ❌ | ✅* | ✅* | 0 |
| 6 | Đổi điểm | `ordertypeName` = "03. Đổi điểm" | ✅ | ✅ (0) | ✅ (0) | ❌ | ❌ | ❌ | ❌ | 0 |
| 7 | Đổi DV | `ordertypeName` = "04. Đổi DV" | ✅ | ✅ (0) | ✅ (0) | ❌ | ❌ | ❌ | ❌ | 0 |
| 8 | Tặng sinh nhật | `ordertypeName` = "05. Tặng sinh nhật" | ✅ | ✅ (0) | ✅ (0) | ❌ | ❌ | ❌ | ❌ | 0 |
| 9 | Đầu tư/Bán TK/Sàn TMDT | `ordertypeName` = "06. Đầu tư" / "07. Bán tài khoản" / "9. Sàn TMDT" | ✅ | ✅ (0) | ✅ (0) | ❌ | ❌ | ❌ | ❌ | 0 |
| 10 | Tách thẻ | `ordertypeName` = "08. Tách thẻ" | ✅ | ✅ (0) | ✅ (0) | ❌ | ❌ | ❌ | ❌ | 0 |
| 11 | Warehouse I/O | `doctype = "STOCK_IO"` + `soCode = "null"` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | - |
| 12 | Warehouse Transfer | `doctype = "STOCK_TRANSFER"` + `relatedStockCode` có | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | - |
| 13 | Đổi vỏ | `ordertypeName` = "Đổi vỏ" | ✅ | ✅ (0) | ✅ (0) | ❌ | ❌ | ❌ | ❌ | 0 |

**Chú thích:**
- ✅ = Có gọi API
- ❌ = Không gọi API
- ✅* = Có gọi API (nếu có cashio data và điều kiện phù hợp - chỉ áp dụng cho Case 5)
- (0) = `action = 0`
- (1) = `action = 1`
- ST = Stock Transfer
- **Cash Receipt**: Gọi khi `fop_syscode = "CASH"` và `total_in > 0` (chỉ Case 5)
- **Credit Advice**: Gọi khi `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"` (chỉ Case 5)

---

## Case 1: Đơn Dịch Vụ (02. Làm dịch vụ)

### Điều kiện
- `ordertypeName` = "02. Làm dịch vụ" hoặc "02.Làm dịch vụ"

### Flow xử lý
1. **Tạo/Cập nhật Customer** (`Fast/Customer`)
   - Tạo hoặc cập nhật thông tin khách hàng trong Fast API

2. **Tạo Sales Order** (`Fast/salesOrder`)
   - Gọi cho **TẤT CẢ** các dòng (I, S, V, ...)
   - `action: 0`

3. **Tạo Sales Invoice** (`Fast/salesInvoice`)
   - Chỉ tạo cho các dòng có `productType = 'S'` (dịch vụ)
   - `action: 0`

4. **Tạo Gxt Invoice** (`Fast/gxtInvoice`)
   - `detail`: Các dòng có `productType = 'I'` (xuất)
   - `ndetail`: Các dòng có `productType = 'S'` (nhập)
   - `ma_nx`: "NX01" (cố định)
   - `ma_gd`: "2" (Xuất tách)
   - `action`: 0
   - `so_ct`: Sử dụng `docCode` gốc (không thêm suffix "-GXT")
   - Mỗi item trong `detail` và `ndetail` có:
     - `dong`: Số thứ tự tăng dần (1, 2, 3, ...)
     - `dong_vt_goc`: 1 (cố định)

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`
- `POST http://103.145.79.169:6688/Fast/gxtInvoice`

### GxtInvoice Payload Example
```json
{
  "ma_dvcs": "FBV",
  "ma_kho_n": "LFH10",
  "ma_kho_x": "LFH10",
  "ong_ba": "Ms Lucrezia",
  "ma_gd": "2",
  "ngay_ct": "2025-11-01T12:16:00.000Z",
  "ngay_lct": "2025-11-01T12:16:00.000Z",
  "so_ct": "SO10.00131386",
  "dien_giai": "SO10.00131386",
  "action": 0,
  "detail": [
    {
      "ma_vt": "G00182L01",
      "dvt": "Cái",
      "so_luong": 1,
      "gia_nt2": 0,
      "tien_nt2": 0,
      "ma_nx": "NX01",
      "ma_bp": "FH10",
      "dong": 1,
      "dong_vt_goc": 1
    }
  ],
  "ndetail": [
    {
      "ma_vt": "F00002",
      "dvt": "Lần",
      "so_luong": 1,
      "gia_nt2": 588000,
      "tien_nt2": 588000,
      "ma_nx": "NX01",
      "ma_bp": "FH10",
      "dong": 1,
      "dong_vt_goc": 1
    }
  ]
}
```

---

## Case 2: Đơn Hàng Có Đuôi _X (ví dụ: SO45.01574458_X)

### Điều kiện
- Đơn hàng có đuôi `_X` (ví dụ: `SO45.01574458_X`)
- Hoặc đơn gốc (không có `_X`) nhưng có đơn tương ứng với `_X` (ví dụ: `SO45.01574458` nếu có `SO45.01574458_X`)

### Flow xử lý
1. **Tạo Sales Order** (`Fast/salesOrder`)
   - Gọi với `action: 1` (đơn hàng có đuôi _X)
   - Sử dụng data từ `buildFastApiInvoiceData`
   - **KHÔNG** cần tạo/cập nhật Customer trước
   - Cả đơn có `_X` (ví dụ: `SO45.01574458_X`) và đơn gốc (ví dụ: `SO45.01574458`) đều sẽ có `action = 1`

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/salesOrder` (với `action: 1`)

### Payload Example
```json
{
  "action": 1,
  "ma_dvcs": "...",
  "ma_kh": "...",
  "ong_ba": "...",
  "ma_gd": "1",
  "ngay_lct": "...",
  "ngay_ct": "...",
  "so_ct": "...",
  "detail": [...]
}
```

---

## Case 3: Đơn Hàng Trả Lại (SALE_RETURN) - Không Có Stock Transfer

### Điều kiện
- `docSourceType = 'SALE_RETURN'`
- **KHÔNG** có stock transfer (không có nhập/xuất kho)

### Flow xử lý
- **KHÔNG xử lý** - Trường hợp này được bỏ qua
- Lưu vào bảng kê hóa đơn với `status: 0` và message: "SALE_RETURN không có stock transfer - không cần xử lý"

### Lưu ý
Trường hợp SALE_RETURN không có stock transfer không cần xử lý và sẽ được bỏ qua.

---

## Case 4: Đơn Hàng Trả Lại (SALE_RETURN) - Có Stock Transfer

### Điều kiện
- `docSourceType = 'SALE_RETURN'`
- **CÓ** stock transfer (có nhập/xuất kho)

### Flow xử lý
1. **Tạo Sales Return** (`Fast/salesReturn`)
   - Gọi API hàng bán trả lại
   - Sử dụng data từ `buildSalesReturnData`
   - **KHÔNG** cần tạo/cập nhật Customer trước

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/salesReturn`

### Payload Structure
```json
{
  "ma_dvcs": "string (8)",
  "ma_kh": "string (16)",
  "ong_ba": "string (128)",
  "ma_gd": "1", // Mã giao dịch (mặc định 1 - Hàng bán trả lại)
  "ma_ca": "string (32)",
  "tk_co": "131", // Tài khoản có (mặc định 131)
  "so_ct0": "string (16)", // Số hóa đơn gốc
  "ngay_ct0": "datetime", // Ngày hóa đơn gốc
  "dien_giai": "string (432)",
  "ngay_lct": "datetime",
  "ngay_ct": "datetime",
  "so_ct": "string (16)",
  "so_seri": "string (12)",
  "ma_nt": "VND",
  "ty_gia": 1.0,
  "ma_nvbh": "string (8)",
  "detail": [
    {
      "ma_vt": "string (16)",
      "dvt": "string (32)",
      "so_serial": "string (64)",
      "loai": "string (2)",
      "ma_ctkm_th": "string (32)",
      "ma_kho": "string (16)",
      "so_luong": "decimal",
      "gia_ban": "decimal",
      "tien_hang": "decimal",
      "tk_ck": "string (16)", // Tài khoản chiết khấu
      "tk_dt": "511", // Tài khoản trả lại (mặc định 511)
      "tk_gv": "632", // Tài khoản giá vốn (mặc định 632)
      "is_reward_line": "int",
      "is_bundle_reward_line": "int",
      "km_yn": "int",
      "dong_thuoc_goi": "string (32)",
      "trang_thai": "string (32)",
      "barcode": "string (32)",
      "ma_ck01": "string (32)",
      "ck01_nt": "decimal",
      // ... các chiết khấu khác (ck02_nt đến ck22_nt)
      "ma_thue": "string (8)",
      "thue_suat": "decimal",
      "tien_thue": "decimal",
      "tk_thue": "string (16)",
      "tk_cpbh": "string (16)",
      "ma_bp": "string (8)",
      "ma_the": "string (256)",
      "ma_lo": "string (16)"
    }
  ]
}
```

### Các Field Đặc Biệt
- `so_ct0`: Số hóa đơn gốc (lấy từ `stockTransfer.soCode` hoặc `orderData.docCode`)
- `ngay_ct0`: Ngày hóa đơn gốc (lấy từ `stockTransfer.transDate` hoặc `orderData.docDate`)
- `tk_co`: Tài khoản có (mặc định "131")
- `tk_dt`: Tài khoản trả lại (mặc định "511")
- `tk_gv`: Tài khoản giá vốn (mặc định "632")

---

## Case 5: Đơn Hàng Bình Thường (01.Thường)

### Điều kiện
- `ordertypeName` = "01.Thường" hoặc "01. Thường"
- Không phải đơn dịch vụ
- Không phải SALE_RETURN

### Flow xử lý
1. **Validate điều kiện tạo hóa đơn**
   - Kiểm tra các điều kiện cần thiết (sử dụng `InvoiceValidationService`)

2. **Tạo/Cập nhật Customer** (`Fast/Customer`)
   - Tạo hoặc cập nhật thông tin khách hàng trong Fast API

3. **Tạo Sales Order** (`Fast/salesOrder`)
   - `action: 0`

4. **Tạo Sales Invoice** (`Fast/salesInvoice`)
   - `action: 0`
   - Với đơn hàng "01.Thường": Sử dụng số lượng từ stock transfer thay vì từ sale
   - Tính lại `tien_hang` = `qty (stock transfer) * gia_ban (sale)`
   - Phân bổ lại tất cả các khoản tiền (chiết khấu, thuế, trợ giá) theo tỷ lệ: `qty (stock transfer) / qty (sale)`

5. **Xử lý Cashio Payment** (chỉ khi Sales Invoice thành công)
   - Gọi `processCashioPayment()` để xử lý thanh toán
   - Lấy cashio data theo `soCode = docCode`
   - Xử lý tất cả các phương thức thanh toán của đơn hàng:
     - **Nếu `fop_syscode = "CASH"` và `total_in > 0`**: Gọi `POST /Fast/cashReceipt` (Phiếu thu tiền mặt)
     - **Nếu `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"`**: Gọi `POST /Fast/creditAdvice` (Giấy báo có)
   - Một đơn hàng có thể có nhiều phương thức thanh toán (nhiều cashio records)
   - Validate response: chỉ `status = 1` mới được coi là thành công

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`
- `POST http://103.145.79.169:6688/Fast/cashReceipt` (nếu có thanh toán bằng tiền mặt)
- `POST http://103.145.79.169:6688/Fast/creditAdvice` (nếu có thanh toán bằng phương thức khác có "Giấy báo có")

### Logic Đặc Biệt cho "01.Thường"

#### Số Lượng
- **API Đơn hàng** (`salesOrder`): Dùng `sale.qty` và `sale.revenue` từ sale
- **API Hóa đơn** (`salesInvoice`): Dùng `qty` từ stock transfer và tính `tien_hang` = `qty (stock transfer) * gia_ban (sale)`

#### Phân Bổ Tỷ Lệ
Tỷ lệ phân bổ = `qty (stock transfer) / qty (sale)`

Ví dụ: Mua 2, xuất 1 → tỷ lệ = 1/2 = 0.5

Tất cả các khoản tiền được phân bổ lại:
- `ck01_nt` đến `ck22_nt` (chiết khấu)
- `tien_thue` (tiền thuế)
- `dt_tg_nt` (tiền trợ giá)

#### Cashio Payment Processing
- **Điều kiện**: Chỉ xử lý khi Sales Invoice tạo thành công (`status = 1`)
- **Lấy cashio data**: Query theo `soCode = docCode`
- **Xử lý nhiều phương thức thanh toán**: Một đơn hàng có thể có nhiều cashio records

**Trường hợp 1: Thanh toán bằng tiền mặt (CASH)**
- Điều kiện: `fop_syscode = "CASH"` và `total_in > 0`
- API: `POST /Fast/cashReceipt`
- Payload được build từ `FastApiPayloadHelper.buildCashReceiptPayload()`

**Trường hợp 2: Thanh toán bằng phương thức khác**
- Điều kiện: `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"`
- API: `POST /Fast/creditAdvice`
- Payload được build từ `FastApiPayloadHelper.buildCreditAdvicePayload()`
- Nếu payment method không có `documentType` hoặc không phải "Giấy báo có" → không gọi API nào

**Lưu ý:**
- Nếu không tìm thấy cashio data → bỏ qua, không throw error
- Nếu có lỗi khi xử lý cashio → log warning nhưng không chặn flow chính
- Validate response: chỉ `status = 1` mới được coi là thành công

---

## Case 6: Đơn "03. Đổi điểm"

### Điều kiện
- `ordertypeName` = "03. Đổi điểm", "03.Đổi điểm", hoặc "03.  Đổi điểm"

### Flow xử lý
1. **Tạo/Cập nhật Customer** (`Fast/Customer`)
   - Tạo hoặc cập nhật thông tin khách hàng trong Fast API

2. **Tạo Sales Order** (`Fast/salesOrder`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

3. **Tạo Sales Invoice** (`Fast/salesInvoice`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`

### Logic Đặc Biệt cho "03. Đổi điểm"

#### Giá và Tiền Hàng
- `gia_ban`: Luôn = 0
- `tien_hang`: Luôn = 0
- `linetotal`: Luôn = 0

#### Chiết Khấu
- `ma_ck01`: Luôn = "TT DIEM DO"
- `ck01_nt`: Luôn = 0
- `ma_ck05`: Luôn = '' (empty string)
- `ck05_nt`: Luôn = 0
- `other_discamt`: Luôn = 0

#### Mã CTKM Tặng Hàng
- `ma_ctkm_th`: Không hiển thị voucher (set = null)

#### Lưu Ý
- Đơn "03. Đổi điểm" được xử lý như đơn hàng bình thường nhưng với giá trị tiền = 0
- Nếu `salesInvoice` thất bại nhưng `salesOrder` thành công, vẫn lưu kết quả `salesOrder` với `status = 0`

---

## Case 7: Đơn "04. Đổi DV"

### Điều kiện
- `ordertypeName` = "04. Đổi DV", "04.Đổi DV", hoặc "04.  Đổi DV"

### Flow xử lý
1. **Tạo/Cập nhật Customer** (`Fast/Customer`)
   - Tạo hoặc cập nhật thông tin khách hàng trong Fast API

2. **Tạo Sales Order** (`Fast/salesOrder`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

3. **Tạo Sales Invoice** (`Fast/salesInvoice`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`

### Logic Đặc Biệt cho "04. Đổi DV"

#### loai_gd (Loại Giao Dịch)
- **Nếu `qty < 0`** (số lượng âm): `loai_gd = '11'`
- **Nếu `qty > 0`** (số lượng dương): `loai_gd = '12'`
- Sử dụng số lượng gốc từ `sale.qty` để xác định
- Các đơn khác (không phải "04. Đổi DV" hoặc "08. Tách thẻ"): `loai_gd = '01'` (mặc định)

### Lưu Ý
- Đơn "04. Đổi DV" được xử lý tương tự như đơn hàng bình thường
- Nếu `salesInvoice` thất bại nhưng `salesOrder` thành công, vẫn lưu kết quả `salesOrder` với `status = 0`

---

## Case 8: Đơn "05. Tặng sinh nhật"

### Điều kiện
- `ordertypeName` = "05. Tặng sinh nhật", "05.Tặng sinh nhật", hoặc "05.  Tặng sinh nhật"

### Flow xử lý
1. **Tạo/Cập nhật Customer** (`Fast/Customer`)
   - Tạo hoặc cập nhật thông tin khách hàng trong Fast API

2. **Tạo Sales Order** (`Fast/salesOrder`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

3. **Tạo Sales Invoice** (`Fast/salesInvoice`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`

### Lưu Ý
- Đơn "05. Tặng sinh nhật" được xử lý tương tự như đơn hàng bình thường
- Nếu `salesInvoice` thất bại nhưng `salesOrder` thành công, vẫn lưu kết quả `salesOrder` với `status = 0`

---

## Case 9: Đơn "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT"

### Điều kiện
- `ordertypeName` = "06. Đầu tư", "06.Đầu tư", "07. Bán tài khoản", "07.Bán tài khoản", "9. Sàn TMDT", hoặc "9.Sàn TMDT"

### Flow xử lý
- **Xử lý giống Case 5: Đơn Hàng Bình Thường (01.Thường)**
- Các đơn này được validate và xử lý như đơn "01.Thường"

### Logic Đặc Biệt

#### Đơn "06. Đầu tư"
- Nếu là hàng tặng → `ma_ctkm_th` = "TT DAU TU"
- Được xử lý như đơn "01.Thường" với validation và flow tương tự

#### Đơn "07. Bán tài khoản" và "9. Sàn TMDT"
- Được xử lý hoàn toàn giống đơn "01.Thường"
- Có validation điều kiện tạo hóa đơn
- Sử dụng số lượng từ stock transfer cho invoice

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`

### Lưu Ý
- Các đơn này được nhóm chung với "01.Thường" trong logic validation
- Cần có stock transfer để tạo invoice (giống "01.Thường")

---

## Case 10: Đơn "08. Tách thẻ"

### Điều kiện
- `ordertypeName` = "08. Tách thẻ" hoặc các biến thể tương tự

### Flow xử lý
1. **Tạo/Cập nhật Customer** (`Fast/Customer`)
   - Tạo hoặc cập nhật thông tin khách hàng trong Fast API

2. **Tạo Sales Order** (`Fast/salesOrder`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

3. **Tạo Sales Invoice** (`Fast/salesInvoice`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`
   - **KHÔNG** gọi Gxt Invoice API (khác với "02. Làm dịch vụ")

### Logic Đặc Biệt

#### loai_gd (Loại Giao Dịch) - Áp dụng cho Sales Invoice
- **Nếu `qty < 0`** (số lượng âm): `loai_gd = '11'`
- **Nếu `qty > 0`** (số lượng dương): `loai_gd = '12'`
- Sử dụng số lượng gốc từ `sale.qty` để xác định
- Logic này áp dụng cho các detail items trong Sales Invoice
- Các đơn khác (không phải "04. Đổi DV" hoặc "08. Tách thẻ"): `loai_gd = '01'` (mặc định)

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`
- ❌ **KHÔNG** gọi `POST /Fast/gxtInvoice`

### Lưu Ý
- Đơn "08. Tách thẻ" được xử lý như đơn hàng bình thường (Customer → Sales Order → Sales Invoice)
- **KHÔNG** được nhóm chung với "02. Làm dịch vụ" trong logic xử lý
- **KHÔNG** gọi Gxt Invoice API (khác với "02. Làm dịch vụ")
- `loai_gd` áp dụng cho Sales Invoice detail items: '11' (qty < 0) hoặc '12' (qty > 0)
- Nếu `salesInvoice` thất bại nhưng `salesOrder` thành công, vẫn lưu kết quả `salesOrder` với `status = 0`

---

## Sơ Đồ Quyết Định

```
Double-click Order
    │
    ├─→ docSourceType = 'SALE_RETURN'?
    │   ├─→ YES → Có stock transfer?
    │   │   ├─→ YES → Case 4: SALE_RETURN có stock transfer
    │   │   └─→ NO → Bỏ qua (Case 3: không xử lý)
    │   │
    │   └─→ NO → Tiếp tục
    │
    ├─→ Có đuôi _X hoặc có đơn tương ứng _X?
    │   └─→ YES → Case 2: Đơn hàng có đuôi _X
    │
    ├─→ ordertypeName = "02. Làm dịch vụ"?
    │   └─→ YES → Case 1: Đơn Dịch Vụ
    │
    ├─→ ordertypeName = "08. Tách thẻ"?
    │   └─→ YES → Case 10: Đơn "08. Tách thẻ" (xử lý như đơn hàng bình thường, không gọi Gxt Invoice)
    │
    ├─→ ordertypeName = "03. Đổi điểm"?
    │   └─→ YES → Case 6: Đơn "03. Đổi điểm"
    │
    ├─→ ordertypeName = "04. Đổi DV"?
    │   └─→ YES → Case 7: Đơn "04. Đổi DV"
    │
    ├─→ ordertypeName = "05. Tặng sinh nhật"?
    │   └─→ YES → Case 8: Đơn "05. Tặng sinh nhật"
    │
    ├─→ ordertypeName = "06. Đầu tư" / "07. Bán tài khoản" / "9. Sàn TMDT"?
    │   └─→ YES → Case 9: Xử lý như "01.Thường"
    │
    ├─→ ordertypeName = "Đổi vỏ"?
    │   └─→ YES → Case 13: Đơn "Đổi vỏ"
    │
    └─→ Case 5: Đơn Hàng Bình Thường (01.Thường)
```

---

## Các Method Chính

### `createInvoiceViaFastApi(docCode: string, forceRetry?: boolean)`
- Method chính xử lý tất cả các case
- Location: `backend/src/modules/sales/sales.service.ts`

### `executeServiceOrderFlow(orderData: any, docCode: string)`
- Xử lý flow đơn dịch vụ
- Location: `backend/src/modules/sales/sales.service.ts`

### `buildFastApiInvoiceData(orderData: any)`
- Build data cho sales invoice/order
- Location: `backend/src/modules/sales/sales.service.ts`

### `buildSalesReturnData(orderData: any, stockTransfers: StockTransfer[])`
- Build data cho sales return
- Location: `backend/src/modules/sales/sales.service.ts`

### `buildGxtInvoiceData(orderData: any, importLines: any[], exportLines: any[])`
- Build data cho gxt invoice (đơn dịch vụ)
- Location: `backend/src/modules/sales/sales.service.ts`

---

## Fast API Invoice Flow Service

Tất cả các API calls đều được tập trung qua `fast-api-invoice-flow.service.ts`:

### Methods trong FastApiInvoiceFlowService

1. **`createOrUpdateCustomer(customerData)`**
   - Tạo/cập nhật khách hàng trong Fast API
   - Endpoint: `POST /Fast/Customer`

2. **`createSalesOrder(orderData, action = 0)`**
   - Tạo đơn hàng bán
   - Endpoint: `POST /Fast/salesOrder`
   - `action`: 0 (mặc định) cho đơn hàng bán, 1 cho đơn hàng trả lại

3. **`createSalesInvoice(invoiceData)`**
   - Tạo hóa đơn bán hàng
   - Endpoint: `POST /Fast/salesInvoice`
   - Có validation mã CTKM với Loyalty API

4. **`createSalesReturn(salesReturnData)`**
   - Tạo hàng bán trả lại (SALE_RETURN có stock transfer)
   - Endpoint: `POST /Fast/salesReturn`

5. **`createGxtInvoice(gxtInvoiceData)`**
   - Tạo phiếu tạo gộp – xuất tách (đơn dịch vụ)
   - Endpoint: `POST /Fast/gxtInvoice`

6. **`processWarehouseFromStockTransfer(stockTransfer)`**
   - Xử lý warehouse receipt/release từ stock transfer (I/O kho)
   - Endpoint: `POST /Fast/warehouseReceipt` hoặc `POST /Fast/warehouseRelease`
   - Tự động phân biệt nhập kho (I) và xuất kho (O) dựa trên `ioType`
   - Gọi Customer API trước khi xử lý warehouse

7. **`processWarehouseTransferFromStockTransfers(stockTransfers[])`**
   - Xử lý warehouse transfer từ stock transfer (điều chuyển kho)
   - Endpoint: `POST /Fast/warehouseTransfer`
   - Xử lý nhóm stock transfers cùng `docCode` với `doctype = "STOCK_TRANSFER"`
   - Gọi Customer API trước khi xử lý warehouse transfer

8. **`processCashioPayment(docCode, orderData, invoiceData)`**
   - Xử lý cashio payment (thanh toán) cho đơn hàng "01. Thường"
   - Chỉ được gọi khi Sales Invoice tạo thành công
   - Endpoints: `POST /Fast/cashReceipt` hoặc `POST /Fast/creditAdvice`
   - Xử lý nhiều phương thức thanh toán (một đơn hàng có thể có nhiều cashio records)
   - Trường hợp 1: `fop_syscode = "CASH"` và `total_in > 0` → Gọi `cashReceipt`
   - Trường hợp 2: `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"` → Gọi `creditAdvice`
   - Validate response: chỉ `status = 1` mới được coi là thành công
   - Nếu có lỗi, log warning nhưng không throw error (không chặn flow chính)

**Lưu ý:** Tất cả các methods đều tự động:
- Loại bỏ các field null/undefined/empty string
- Xử lý token authentication và retry
- Logging chi tiết cho debugging

---

## Case 11: Xử Lý Warehouse I/O Từ Stock Transfer

### Điều kiện
- `doctype = "STOCK_IO"` (bắt buộc)
- `soCode = "null"` (string) hoặc `null` (bắt buộc)
- `ioType = "I"` (nhập kho) hoặc `ioType = "O"` (xuất kho)

### Flow xử lý
1. **Validate điều kiện**
   - Kiểm tra `doctype` phải là `"STOCK_IO"`
   - Kiểm tra `soCode` phải là `"null"` hoặc `null`

2. **Lấy ma_dvcs từ Department API**
   - Gọi Loyalty API: `GET https://loyaltyapi.vmt.vn/departments?branchcode={branchCode}`
   - Ưu tiên lấy `department.ma_dvcs` hoặc `department.ma_dvcs_ht`
   - Fallback: chuỗi rỗng nếu không có

3. **Fetch Material Catalog từ Loyalty API**
   - Gọi Loyalty API để lấy thông tin material catalog dựa trên `itemCode`
   - Thử các endpoint theo thứ tự:
     - `/material-catalogs/code/{itemCode}`
     - `/material-catalogs/old-code/{itemCode}`
     - `/material-catalogs/material-code/{itemCode}`
   - Lấy `materialCode` và `unit` từ catalog
   - Fallback về `stockTransfer.materialCode` hoặc `stockTransfer.itemCode` nếu không có catalog

4. **Map Warehouse Code**
   - Gọi API mapping: `GET /categories/warehouse-code-mappings/map?maCu={stockCode}`
   - Nếu có mapping (`maMoi`) → dùng `maMoi`
   - Nếu không có mapping → dùng `stockCode` gốc

5. **Gọi Customer API trước** (`Fast/Customer`)
   - Tạo/cập nhật customer với `ma_kh = branchCode`
   - `ten_kh` = `department?.name || department?.ten || branchCode`
   - Gọi trước khi xử lý warehouse operations

6. **Xử lý Warehouse**
   - Nếu `ioType = "I"`: Gọi `POST /Fast/warehouseReceipt` (Nhập kho)
   - Nếu `ioType = "O"`: Gọi `POST /Fast/warehouseRelease` (Xuất kho)

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/warehouseReceipt` (ioType = "I")
- `POST http://103.145.79.169:6688/Fast/warehouseRelease` (ioType = "O")

### Payload Structure
```json
{
  "ma_dvcs": "string (8)", // Từ department API
  "ma_kh": "string (16)", // branchCode
  "ma_gd": "2", // Cố định = 2
  "ngay_ct": "datetime", // transDate
  "so_ct": "string (16)", // docCode
  "ma_nt": "VND",
  "ty_gia": 1,
  "dien_giai": "string",
  "detail": [
    {
      "ma_vt": "string (16)", // materialCode từ catalog hoặc stockTransfer
      "dvt": "string (32)", // unit từ catalog
      "so_serial": "string (64)", // batchSerial
      "ma_kho": "string (16)", // Mã kho đã được map qua warehouse-code-mappings
      "so_luong": "decimal", // qty (giá trị tuyệt đối)
      "gia_nt": 0,
      "tien_nt": 0,
      "ma_lo": "string (16)", // batchSerial
      "ma_nx": "string (16)", // lineInfo1
      "ma_vv": "",
      "so_lsx": "",
      "ma_sp": "string (16)", // itemCode
      "ma_hd": "",
      // Các field chỉ có trong warehouseRelease (ioType = "O")
      "px_gia_dd": 0,
      "ma_phi": "",
      "ma_ku": "",
      "ma_phi_hh": "",
      "ma_phi_ttlk": "",
      "tien_hh_nt": 0,
      "tien_ttlk_nt": 0,
      // Hoặc field chỉ có trong warehouseReceipt (ioType = "I")
      "pn_gia_tb": 0
    }
  ]
}
```

### Logic Đặc Biệt

#### Lấy ma_dvcs
- Ưu tiên: `department.ma_dvcs` từ Loyalty API
- Thứ hai: `department.ma_dvcs_ht` từ Loyalty API
- Fallback: chuỗi rỗng

#### Lấy Material Code và Unit
- Ưu tiên: `materialCatalog.materialCode` và `materialCatalog.unit` từ Loyalty API
- Fallback: `stockTransfer.materialCode` hoặc `stockTransfer.itemCode`

#### Map Warehouse Code
- Ưu tiên: `maMoi` từ warehouse-code-mappings API
- Fallback: `stockTransfer.stockCode` gốc

#### Số Lượng
- Luôn lấy giá trị tuyệt đối: `Math.abs(parseFloat(qty))`

### Endpoint Gọi API
- Controller: `POST /sales/stock-transfer/:id/warehouse`
- Service: `processWarehouseFromStockTransfer(stockTransfer)`

### Lưu Ý
- Chỉ xử lý stock transfer có `doctype = "STOCK_IO"`
- Chỉ xử lý stock transfer có `soCode = "null"` hoặc `null`
- Tự động phân biệt nhập kho (I) và xuất kho (O) qua `ioType`
- **Bắt buộc gọi Customer API trước** khi xử lý warehouse operations
- Tất cả các API calls đều có error handling và fallback về giá trị gốc nếu có lỗi
- Validate response: chỉ `status = 1` mới được coi là thành công

---

## Case 12: Xử Lý Warehouse Transfer Từ Stock Transfer (Điều Chuyển Kho)

### Điều kiện
- `doctype = "STOCK_TRANSFER"` (bắt buộc)
- `relatedStockCode` có giá trị (bắt buộc - không được để trống)
- Tất cả các stock transfers cùng `docCode` sẽ được nhóm lại để xử lý

### Flow xử lý
1. **Validate điều kiện**
   - Kiểm tra `doctype` phải là `"STOCK_TRANSFER"`
   - Kiểm tra `relatedStockCode` phải có giá trị (không được để trống)

2. **Lấy ma_dvcs từ Department API**
   - Gọi Loyalty API: `GET https://loyaltyapi.vmt.vn/departments?branchcode={branchCode}`
   - Ưu tiên lấy `department.ma_dvcs` hoặc `department.ma_dvcs_ht`
   - Fallback: chuỗi rỗng nếu không có

3. **Gọi Customer API trước** (`Fast/Customer`)
   - Tạo/cập nhật customer với `ma_kh = branchCode`
   - `ten_kh = branchCode` (không dùng department name)
   - Gọi trước khi xử lý warehouse transfer operations

4. **Map Warehouse Codes**
   - **Kho xuất (ma_kho_x)**: Map từ `stockCode` qua warehouse-code-mappings API
   - **Kho nhập (ma_kho_n)**: Map từ `relatedStockCode` qua warehouse-code-mappings API
   - Ưu tiên: `maMoi` từ mapping (nếu có)
   - Fallback: giá trị gốc (`stockCode` hoặc `relatedStockCode`)

5. **Fetch Material Catalog từ Loyalty API**
   - Lặp qua từng stock transfer trong nhóm cùng `docCode`
   - Gọi Loyalty API để lấy thông tin material catalog dựa trên `itemCode`
   - Thử các endpoint theo thứ tự:
     - `/material-catalogs/code/{itemCode}`
     - `/material-catalogs/old-code/{itemCode}`
     - `/material-catalogs/material-code/{itemCode}`
   - Lấy `materialCode` và `unit` từ catalog
   - Fallback về `stockTransfer.materialCode` hoặc `stockTransfer.itemCode` nếu không có catalog

6. **Build Detail Array**
   - Nhóm tất cả stock transfers cùng `docCode`
   - Build `detail` array với các thông tin:
     - `ma_vt`: materialCode từ catalog hoặc stockTransfer
     - `dvt`: unit từ catalog
     - `so_serial`: batchSerial
     - `so_luong`: giá trị tuyệt đối của qty
     - `gia_nt`: 0
     - `tien_nt`: 0
     - `ma_lo`: batchSerial
     - `ma_bp`: branchCode
     - `px_gia_dd`: 0
     - **Lưu ý**: Không có `ma_nx` trong detail (khác với warehouse I/O)

7. **Gọi Warehouse Transfer API**
   - Endpoint: `POST /Fast/warehouseTransfer`
   - `ma_gd = "3"` (cố định - xuất điều chuyển)
   - `so_buoc = 2` (mặc định)

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer` (trước)
- `POST http://103.145.79.169:6688/Fast/warehouseTransfer`

### Payload Structure
```json
{
  "ma_dvcs": "string (8)", // Từ department API
  "ma_kho_n": "string (16)", // Kho nhập (từ relatedStockCode, đã map)
  "ma_kho_x": "string (16)", // Kho xuất (từ stockCode, đã map)
  "ma_gd": "3", // Cố định = 3 (xuất điều chuyển)
  "ngay_ct": "datetime", // transDate
  "so_ct": "string (16)", // docCode
  "ma_nt": "VND",
  "ty_gia": 1,
  "dien_giai": "string",
  "so_buoc": 2, // Mặc định 2
  "detail": [
    {
      "ma_vt": "string (16)", // materialCode từ catalog hoặc stockTransfer
      "dvt": "string (32)", // unit từ catalog
      "so_serial": "string (64)", // batchSerial
      "so_luong": "decimal", // qty (giá trị tuyệt đối)
      "gia_nt": 0,
      "tien_nt": 0,
      "ma_lo": "string (16)", // batchSerial
      "ma_bp": "string (16)", // branchCode
      "px_gia_dd": 0
      // Lưu ý: KHÔNG có ma_nx trong detail
    }
  ]
}
```

### Logic Đặc Biệt

#### Lấy ma_dvcs
- Ưu tiên: `department.ma_dvcs` từ Loyalty API
- Thứ hai: `department.ma_dvcs_ht` từ Loyalty API
- Fallback: chuỗi rỗng

#### Lấy Material Code và Unit
- Ưu tiên: `materialCatalog.materialCode` và `materialCatalog.unit` từ Loyalty API
- Fallback: `stockTransfer.materialCode` hoặc `stockTransfer.itemCode`

#### Map Warehouse Codes
- **ma_kho_x** (kho xuất): Map từ `stockCode`
- **ma_kho_n** (kho nhập): Map từ `relatedStockCode`
- Ưu tiên: `maMoi` từ warehouse-code-mappings API
- Fallback: giá trị gốc

#### Số Lượng
- Luôn lấy giá trị tuyệt đối: `Math.abs(parseFloat(qty))`

#### Customer API
- `ma_kh = branchCode`
- `ten_kh = branchCode` (không dùng department name, khác với warehouse I/O)

### Endpoint Gọi API
- Controller: `POST /sales/stock-transfer/:id/warehouse`
- Service: `processWarehouseTransferFromStockTransfers(stockTransfers[])`

### Lưu Ý
- Chỉ xử lý stock transfer có `doctype = "STOCK_TRANSFER"`
- Chỉ xử lý stock transfer có `relatedStockCode` (bắt buộc)
- Tất cả stock transfers cùng `docCode` được nhóm lại để xử lý cùng lúc
- **Bắt buộc gọi Customer API trước** khi xử lý warehouse transfer
- Detail items **KHÔNG có** `ma_nx` (khác với warehouse I/O)
- Validate response: chỉ `status = 1` mới được coi là thành công
- Tracking: `ioType = 'T'` (Transfer) trong bảng `warehouse_processed`

---

## Case 13: Đơn "Đổi vỏ"

### Điều kiện
- `ordertypeName` = "Đổi vỏ" hoặc các biến thể tương tự

### Flow xử lý
1. **Tạo/Cập nhật Customer** (`Fast/Customer`)
   - Tạo hoặc cập nhật thông tin khách hàng trong Fast API

2. **Tạo Sales Order** (`Fast/salesOrder`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

3. **Tạo Sales Invoice** (`Fast/salesInvoice`)
   - `action: 0`
   - Sử dụng data từ `buildFastApiInvoiceData`

### API Endpoints
- `POST http://103.145.79.169:6688/Fast/Customer`
- `POST http://103.145.79.169:6688/Fast/salesOrder`
- `POST http://103.145.79.169:6688/Fast/salesInvoice`

### Logic Đặc Biệt
- Đơn "Đổi vỏ" được xử lý **giống như đơn hàng bình thường** (Case 5)
- Sử dụng cùng flow: Customer → Sales Order → Sales Invoice
- `action = 0` cho cả salesOrder và salesInvoice

### Lưu Ý
- Đơn "Đổi vỏ" được validate và xử lý như đơn "01.Thường"
- Nếu `salesInvoice` thất bại nhưng `salesOrder` thành công, vẫn lưu kết quả `salesOrder` với `status = 0`
- Được thêm vào danh sách các order types được phép tạo hóa đơn

---

## Lưu Ý Quan Trọng

1. **Action Field**
   - `action: 0` cho sales order và sales invoice bình thường
   - `action: 1` cho sales order trả lại (đơn có đuôi _X)
   - `action: 0` cho gxt invoice (luôn là 0)

2. **Stock Transfer**
   - Với đơn "01.Thường", "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT": Số lượng trong invoice lấy từ stock transfer (xuất kho)
   - Chỉ tính các record có `doctype = 'SALE_STOCKOUT'` hoặc `qty < 0` (xuất kho)
   - Bỏ qua các record `RETURN` với `qty > 0` (nhập lại)

3. **Product Type**
   - `'S'`: Dịch vụ (Service) - nhập kho → vào `ndetail` trong GxtInvoice
   - `'I'`: Hàng hóa (Item) - xuất kho → vào `detail` trong GxtInvoice
   - `'V'`: Voucher

4. **GxtInvoice Fields**
   - `so_ct`: Sử dụng `docCode` gốc, không thêm suffix "-GXT"
   - `dong`: Số thứ tự tăng dần (1, 2, 3, ...) cho mỗi mảng `detail` và `ndetail` riêng biệt
   - `dong_vt_goc`: Luôn là 1 (cố định)

5. **SALE_RETURN Flow**
   - **Không có stock transfer**: Bỏ qua, không xử lý (Case 3)
   - **Có stock transfer**: Gọi `salesReturn`, **KHÔNG** cần tạo/cập nhật Customer trước (Case 4)

6. **Đơn Có Đuôi _X**
   - Cả đơn có `_X` và đơn gốc (nếu có đơn tương ứng `_X`) đều được xử lý với `action = 1`
   - **KHÔNG** cần tạo/cập nhật Customer trước

7. **Đơn Đặc Biệt (03, 04, 05)**
   - Đơn "03. Đổi điểm", "04. Đổi DV", "05. Tặng sinh nhật" đều cần tạo/cập nhật Customer trước
   - Nếu `salesInvoice` thất bại nhưng `salesOrder` thành công, vẫn lưu kết quả với `status = 0`

8. **Đơn Dịch Vụ**
   - Chỉ bao gồm: "02. Làm dịch vụ"
   - Flow: Customer → Sales Order → Sales Invoice → Gxt Invoice
   - **Lưu ý**: "08. Tách thẻ" KHÔNG phải đơn dịch vụ, được xử lý như đơn hàng bình thường (không gọi Gxt Invoice)
   - **Lưu ý**: Đơn "08. Tách thẻ" có logic đặc biệt cho `loai_gd` trong Sales Invoice detail items:
     - `qty < 0` → `loai_gd = '11'`
     - `qty > 0` → `loai_gd = '12'`

9. **Validation**
   - Chỉ các đơn "01.Thường", "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT", "Đổi vỏ" cần validation điều kiện tạo hóa đơn
   - Các đơn đặc biệt (03, 04, 05, 02, 08) không cần validation

10. **Error Handling**
    - Tất cả các lỗi đều được lưu vào bảng `fast_api_invoices` với `status = 0`
    - Response thành công được lưu với `status = 1`
    - Nếu một phần của flow thành công (ví dụ: salesOrder thành công nhưng salesInvoice thất bại), vẫn lưu kết quả với `status = 0`
    - **Tất cả Fast API calls** đều validate response: chỉ `status = 1` mới được coi là thành công
    - Nếu response có `status != 1`, sẽ throw `BadRequestException` với message từ API

11. **Cashio Payment Processing (cho đơn "01. Thường")**
    - Chỉ được gọi khi Sales Invoice tạo thành công (`status = 1`)
    - Lấy cashio data từ database theo `soCode = docCode`
    - Xử lý tất cả các phương thức thanh toán của đơn hàng:
      - **Thanh toán bằng tiền mặt**: `fop_syscode = "CASH"` và `total_in > 0` → Gọi `POST /Fast/cashReceipt` (Phiếu thu tiền mặt)
      - **Thanh toán bằng phương thức khác**: `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"` → Gọi `POST /Fast/creditAdvice` (Giấy báo có)
    - Một đơn hàng có thể có nhiều phương thức thanh toán (nhiều cashio records)
    - Nếu không tìm thấy cashio data → bỏ qua, không throw error (không chặn flow chính)
    - Nếu có lỗi khi xử lý cashio → log warning nhưng không throw error (không chặn flow chính)
    - Validate response: chỉ `status = 1` mới được coi là thành công

12. **Warehouse Processing từ Stock Transfer**
    - **Case 11: Warehouse I/O** (`doctype = "STOCK_IO"`)
      - Chỉ xử lý stock transfer có `doctype = "STOCK_IO"` (bắt buộc)
      - Chỉ xử lý stock transfer có `soCode = "null"` hoặc `null` (bắt buộc)
      - `ma_dvcs` được lấy từ department API (Loyalty API) dựa trên `branchCode`
        - Ưu tiên: `department.ma_dvcs` hoặc `department.ma_dvcs_ht`
        - Fallback: chuỗi rỗng
      - `materialCode` và `unit` được lấy từ material catalog API (Loyalty API) dựa trên `itemCode`
        - Ưu tiên: `materialCatalog.materialCode` và `materialCatalog.unit`
        - Fallback: `stockTransfer.materialCode` hoặc `stockTransfer.itemCode`
      - `ma_kho` được map qua warehouse-code-mappings API
        - Ưu tiên: `maMoi` từ mapping (nếu có)
        - Fallback: `stockTransfer.stockCode` gốc
      - Phân biệt nhập kho (I) và xuất kho (O) qua `ioType` để gọi API tương ứng
      - **Bắt buộc gọi Customer API trước** với `ten_kh = department?.name || department?.ten || branchCode`
    - **Case 12: Warehouse Transfer** (`doctype = "STOCK_TRANSFER"`)
      - Chỉ xử lý stock transfer có `doctype = "STOCK_TRANSFER"` (bắt buộc)
      - Chỉ xử lý stock transfer có `relatedStockCode` (bắt buộc)
      - Nhóm tất cả stock transfers cùng `docCode` để xử lý
      - `ma_dvcs` được lấy từ department API (Loyalty API) dựa trên `branchCode`
      - `ma_kho_x` (kho xuất) được map từ `stockCode`
      - `ma_kho_n` (kho nhập) được map từ `relatedStockCode`
      - `materialCode` và `unit` được lấy từ material catalog API cho từng item
      - **Bắt buộc gọi Customer API trước** với `ten_kh = branchCode` (không dùng department name)
      - Detail items **KHÔNG có** `ma_nx` (khác với warehouse I/O)
      - Tracking: `ioType = 'T'` (Transfer) trong bảng `warehouse_processed`

---

## Tham Khảo

- Fast API Base URL: `http://103.145.79.169:6688/Fast`
- Loyalty API Base URL: `https://loyaltyapi.vmt.vn`
- Authentication: Bearer Token (tự động refresh)
- File xử lý chính: `backend/src/modules/sales/sales.service.ts`
- Service gọi API: `backend/src/services/fast-api-client.service.ts` (HTTP client layer)
- Flow service: `backend/src/services/fast-api-invoice-flow.service.ts` (Business flow orchestrator)
- Loyalty service: `backend/src/services/loyalty.service.ts`
- Categories service: `backend/src/modules/categories/categories.service.ts`

### API Endpoints Tham Khảo

#### Loyalty API
- Departments: `GET https://loyaltyapi.vmt.vn/departments?branchcode={branchCode}`
- Material Catalogs:
  - `GET https://loyaltyapi.vmt.vn/material-catalogs/code/{code}`
  - `GET https://loyaltyapi.vmt.vn/material-catalogs/old-code/{oldCode}`
  - `GET https://loyaltyapi.vmt.vn/material-catalogs/material-code/{materialCode}`

#### Warehouse Code Mapping API
- Map warehouse code: `GET /categories/warehouse-code-mappings/map?maCu={maCu}`
- List mappings: `GET /categories/warehouse-code-mappings?page=1&limit=50&search={search}`

