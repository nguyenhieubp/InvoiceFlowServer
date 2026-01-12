# Các Case Tạo Hóa Đơn Sang Fast API

Tài liệu mô tả các trường hợp xử lý khi tạo hóa đơn sang Fast API trong hệ thống InvoiceFlow.

**Lưu ý:** Tất cả các API calls đều được tập trung qua `fast-api-invoice-flow.service.ts`

---

## 1. SALE_ORDER (Đơn Hàng Bán)

### Điều kiện

- `docSourceType != 'SALE_RETURN'`
- `docCode` không có đuôi `_X`

### Các Dạng Đơn Hàng

#### Dạng 1: Thường (01.Thường)

**Điều kiện:** `ordertypeName` = "01.Thường" hoặc "01. Thường"

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - Sử dụng số lượng từ stock transfer
   - Tính lại `tien_hang = qty (stock transfer) * gia_ban (sale)`
4. `POST /Fast/cashReceipt` - Nếu `fop_syscode = "CASH"` và `total_in > 0`
5. `POST /Fast/creditAdvice` - Nếu `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"`
6. `POST /Fast/payment` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, `fop_syscode = "CASH"`
7. `POST /Fast/debitAdvice` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, payment method có `documentType = "Giấy báo nợ"`

**Code xử lý:** `processSingleOrder()` → Validate → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 2: Dịch Vụ (02. Làm dịch vụ)

**Điều kiện:** `ordertypeName` = "02. Làm dịch vụ" hoặc "02.Làm dịch vụ"

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`) - Gọi cho TẤT CẢ các dòng (I, S, V, ...)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`) - Chỉ tạo cho các dòng có `productType = 'S'` (dịch vụ)
4. `POST /Fast/cashReceipt` - Nếu `fop_syscode = "CASH"` và `total_in > 0`
5. `POST /Fast/creditAdvice` - Nếu `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"`
6. `POST /Fast/payment` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, `fop_syscode = "CASH"`
7. `POST /Fast/debitAdvice` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, payment method có `documentType = "Giấy báo nợ"`
8. `POST /Fast/gxtInvoice` - Phiếu tạo gộp - Xuất tách
   - `detail`: Các dòng có `productType = 'I'` (xuất - NVL)
   - `ndetail`: Các dòng có `productType = 'S'` (nhập - mã dịch vụ)
   - `ma_gd = "2"` (Xuất tách)

**Code xử lý:** `executeServiceOrderFlow()` → Gọi các API theo thứ tự

---

#### Dạng 3: Đổi Điểm (03. Đổi điểm)

**Điều kiện:** `ordertypeName` = "03. Đổi điểm" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - `gia_ban = 0`, `tien_hang = 0`
   - `ma_ck01 = "TT DIEM DO"`, `ck01_nt = 0`
   - `ma_ctkm_th = null` (không hiển thị voucher)

**Code xử lý:** `processSingleOrder()` → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 4: Đổi DV (04. Đổi DV)

**Điều kiện:** `ordertypeName` = "04. Đổi DV" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - `loai_gd = '11'` nếu `qty < 0` (Đổi dịch vụ trước)
   - `loai_gd = '12'` nếu `qty > 0` (Đổi dịch vụ sau)
4. `POST /Fast/cashReceipt` - Nếu `fop_syscode = "CASH"` và `total_in > 0`
5. `POST /Fast/creditAdvice` - Nếu `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"`
6. `POST /Fast/payment` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, `fop_syscode = "CASH"`
7. `POST /Fast/debitAdvice` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, payment method có `documentType = "Giấy báo nợ"`

**Code xử lý:** `processSingleOrder()` → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 5: Tặng Sinh Nhật (05. Tặng sinh nhật)

**Điều kiện:** `ordertypeName` = "05. Tặng sinh nhật" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)

**Code xử lý:** `processSingleOrder()` → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 6: Đầu Tư (06. Đầu tư)

**Điều kiện:** `ordertypeName` = "06. Đầu tư" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - Sử dụng số lượng từ stock transfer
   - Nếu là hàng tặng → `ma_ctkm_th = "TT DAU TU"`

**Code xử lý:** `processSingleOrder()` → Validate → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 7: Bán Tài Khoản (07. Bán tài khoản)

**Điều kiện:** `ordertypeName` = "07. Bán tài khoản" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - Sử dụng số lượng từ stock transfer
   - Phải có thông tin mã thẻ trên từng hóa đơn
4. `POST /Fast/cashReceipt` - Nếu `fop_syscode = "CASH"` và `total_in > 0`
5. `POST /Fast/creditAdvice` - Nếu `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"`
6. `POST /Fast/payment` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, `fop_syscode = "CASH"`
7. `POST /Fast/debitAdvice` - Nếu `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'` và có mã kho và `total_out > 0`, payment method có `documentType = "Giấy báo nợ"`

**Code xử lý:** `processSingleOrder()` → Validate → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 8: Tách Thẻ (08. Tách thẻ)

**Điều kiện:** `ordertypeName` = "08. Tách thẻ" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - `loai_gd = '11'` nếu `qty < 0`
   - `loai_gd = '12'` nếu `qty > 0`
   - **KHÔNG** gọi Gxt Invoice API (khác với đơn dịch vụ)

**Code xử lý:** `processSingleOrder()` → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 9: Sàn TMDT (9. Sàn TMDT)

**Điều kiện:** `ordertypeName` = "9. Sàn TMDT" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - Sử dụng số lượng từ stock transfer

**Code xử lý:** `processSingleOrder()` → Validate → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

#### Dạng 10: Đổi Vỏ

**Điều kiện:** `ordertypeName` = "Đổi vỏ" hoặc các biến thể

**APIs được gọi:**

1. `POST /Fast/Customer` - Tạo/cập nhật khách hàng
2. `POST /Fast/salesOrder` - Tạo đơn hàng bán (`action = 0`)
3. `POST /Fast/salesInvoice` - Tạo hóa đơn bán hàng (`action = 0`)
   - Được xử lý giống như đơn "01.Thường"

**Code xử lý:** `processSingleOrder()` → Validate → `buildFastApiInvoiceData()` → Gọi các API theo thứ tự

---

## 2. SALE_RETURN (Đơn Hàng Trả Lại)

### Điều kiện

- `docSourceType = 'SALE_RETURN'`

### Trường Hợp 1: Có Stock Transfer

**Điều kiện:** `docSourceType = 'SALE_RETURN'` + có stock transfer

**APIs được gọi:**

1. `POST /Fast/salesReturn` - Tạo phiếu nhập hàng bán trả lại
   - `so_ct0`: Số hóa đơn gốc
   - `ngay_ct0`: Ngày hóa đơn gốc
   - `tk_co = "131"`, `tk_dt = "511"`, `tk_gv = "632"`
   - **KHÔNG** cần tạo/cập nhật Customer trước
2. `POST /Fast/payment` - Nếu có mã kho và `total_out > 0`, `fop_syscode = "CASH"`
   - **Lưu ý:** Trong SALE_RETURN flow riêng, payment được gọi cho SALE_RETURN (khác với SALE_ORDER flow)
3. `POST /Fast/debitAdvice` - Nếu có mã kho và `total_out > 0`, payment method có `documentType = "Giấy báo nợ"`
   - `loai_ct = "2"` (Chi cho khách hàng)

**Code xử lý:** `processSingleOrder()` → Kiểm tra `docSourceType = 'SALE_RETURN'` → `handleSaleReturnFlow()` → `buildSalesReturnData()` → Gọi các API theo thứ tự

---

### Trường Hợp 2: Không Có Stock Transfer

**Điều kiện:** `docSourceType = 'SALE_RETURN'` + không có stock transfer

**Xử lý:** **BỎ QUA, KHÔNG XỬ LÝ**

- Lưu vào bảng kê hóa đơn với `status: 0` và message: "SALE_RETURN không có stock transfer - không cần xử lý"

**Code xử lý:** `processSingleOrder()` → Kiểm tra `docSourceType = 'SALE_RETURN'` → Kiểm tra stock transfer → Nếu không có → Bỏ qua

---

## 3. ĐƠN HỦY (Đơn Có Đuôi \_X)

### Điều kiện

- `docCode` có đuôi `_X` (ví dụ: `SO45.01574458_X`)
- **Lưu ý:** Đơn hủy không có khái niệm xuất kho. Nếu hủy mà đã xuất kho thì sẽ là SALE_RETURN

### APIs được gọi:

1. `POST /Fast/salesOrder` - Cập nhật đơn hàng với trạng thái [Đóng] (`action = 1`)
   - **KHÔNG** cần tạo/cập nhật Customer trước
2. `POST /Fast/cashReceipt` - Nếu `fop_syscode = "CASH"` và `total_in > 0`
3. `POST /Fast/creditAdvice` - Nếu `fop_syscode != "CASH"` và payment method có `documentType = "Giấy báo có"`
4. `POST /Fast/payment` - Nếu có cashio data với `total_out > 0`, `fop_syscode = "CASH"` (cho phép không có mã kho)
5. `POST /Fast/debitAdvice` - Nếu có cashio data với `total_out > 0`, payment method có `documentType = "Giấy báo nợ"` (cho phép không có mã kho)

**Code xử lý:** `createInvoiceViaFastApi()` → Kiểm tra `hasUnderscoreX()` → `handleSaleOrderWithUnderscoreX()` → Gọi các API theo thứ tự

---

## Sơ Đồ Quyết Định

```
Double-click Order
    │
    ├─→ docCode có đuôi _X?
    │   └─→ YES → ĐƠN HỦY (action = 1)
    │
    ├─→ docSourceType = 'SALE_RETURN'?
    │   ├─→ YES → Có stock transfer?
    │   │   ├─→ YES → SALE_RETURN (có ST)
    │   │   └─→ NO → Bỏ qua (không xử lý)
    │   │
    │   └─→ NO → Tiếp tục
    │
    └─→ SALE_ORDER
        ├─→ ordertypeName = "02. Làm dịch vụ"? → Dạng 2: Dịch Vụ
        ├─→ ordertypeName = "03. Đổi điểm"? → Dạng 3: Đổi Điểm
        ├─→ ordertypeName = "04. Đổi DV"? → Dạng 4: Đổi DV
        ├─→ ordertypeName = "05. Tặng sinh nhật"? → Dạng 5: Tặng Sinh Nhật
        ├─→ ordertypeName = "06. Đầu tư"? → Dạng 6: Đầu Tư
        ├─→ ordertypeName = "07. Bán tài khoản"? → Dạng 7: Bán Tài Khoản
        ├─→ ordertypeName = "08. Tách thẻ"? → Dạng 8: Tách Thẻ
        ├─→ ordertypeName = "9. Sàn TMDT"? → Dạng 9: Sàn TMDT
        ├─→ ordertypeName = "Đổi vỏ"? → Dạng 10: Đổi Vỏ
        └─→ Dạng 1: Thường (01.Thường)
```

---

## Lưu Ý Quan Trọng

1. **Action Field**
   - `action = 0`: Đơn hàng bán bình thường
   - `action = 1`: Đơn hàng hủy (đơn có đuôi `_X`)

2. **Validation**
   - Chỉ các đơn "01.Thường", "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT", "Đổi vỏ" cần validation điều kiện tạo hóa đơn
   - Các đơn đặc biệt (03, 04, 05, 02, 08) không cần validation

3. **Cashio Payment**
   - Chỉ được gọi khi Sales Invoice (hoặc Sales Order cho đơn `_X`) tạo thành công (`status = 1`)
   - Áp dụng cho: "01.Thường", "02. Làm dịch vụ", "04. Đổi DV", "07. Bán tài khoản", đơn có đuôi `_X`

4. **Payment (Phiếu chi tiền mặt/Giấy báo nợ)**
   - Điều kiện: `docSourceType = 'ORDER_RETURN'` HOẶC `'SALE_RETURN'` VÀ có mã kho (stockCode) từ stock transfers VÀ có cashio data với `total_out > 0`
   - Áp dụng cho: "01.Thường", "02. Làm dịch vụ", "04. Đổi DV", "07. Bán tài khoản" (khi `docSourceType = 'ORDER_RETURN'` hoặc `'SALE_RETURN'`)
   - **Lưu ý:** `ORDER_RETURN` và `SALE_RETURN` được xử lý giống nhau trong SALE_ORDER flow
   - **Ngoại lệ:** Đơn hủy (`_X`) cho phép không có mã kho nếu có cashio với `total_out > 0`

5. **Stock Transfer**
   - Với đơn "01.Thường", "06. Đầu tư", "07. Bán tài khoản", "9. Sàn TMDT": Số lượng trong invoice lấy từ stock transfer (xuất kho)
   - Chỉ tính các record có `doctype = 'SALE_STOCKOUT'` hoặc `qty < 0` (xuất kho)

---

## Tham Khảo

- **Fast API Base URL:** `http://103.145.79.169:6688/Fast`
- **File xử lý chính:** `backend/src/modules/sales/sales.service.ts`
- **Service gọi API:** `backend/src/services/fast-api-invoice-flow.service.ts`
- **Method chính:** `createInvoiceViaFastApi(docCode: string, forceRetry?: boolean)`
