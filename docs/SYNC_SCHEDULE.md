# Lịch Trình Đồng Bộ Dữ Liệu

File này mô tả các cron job tự động chạy đồng bộ dữ liệu trong hệ thống.

## Tổng Quan

Tất cả các cron job đều chạy theo múi giờ **Asia/Ho_Chi_Minh** và xử lý dữ liệu cho **ngày T-1** (ngày hôm qua).

---

## 📅 Lịch Trình Chi Tiết

### 1. **1:00 AM** - Đồng Bộ Dữ Liệu Xuất Kho
- **Cron Expression**: `0 1 * * *`
- **Tên Job**: `daily-stock-transfer-sync`
- **Mô tả**: Đồng bộ dữ liệu xuất kho (stock transfer) từ Zappy API
- **Thời gian xử lý**: Ngày T-1
- **Brands xử lý**: `f3`, `labhair`, `yaman`, `menard`
- **Trạng thái**: ✅ **Đang hoạt động**

**Chi tiết:**
- Đồng bộ tuần tự từng brand
- Mỗi brand được xử lý độc lập
- Lỗi của một brand không ảnh hưởng đến các brand khác
- Format ngày: `DDMMMYYYY` (ví dụ: `21DEC2025`)

---

### 2. **2:30 AM** - Đồng Bộ Báo Cáo Nộp Quỹ Cuối Ca
- **Cron Expression**: `30 2 * * *`
- **Tên Job**: `daily-shift-end-cash-sync-2-30am`
- **Mô tả**: Đồng bộ báo cáo nộp quỹ cuối ca (shift end cash) từ Zappy API
- **Thời gian xử lý**: Ngày T-1
- **Brands xử lý**: `f3`, `labhair`, `yaman`, `menard`
- **Trạng thái**: ✅ **Đang hoạt động**

**Chi tiết:**
- Đồng bộ tuần tự từng brand
- Mỗi brand được xử lý độc lập
- Lỗi của một brand không ảnh hưởng đến các brand khác
- Format ngày: `DDMMMYYYY` (ví dụ: `21DEC2025`)

---

### 3. **3:00 AM** - Đồng Bộ Dữ Liệu Bán Hàng
- **Cron Expression**: `0 3 * * *`
- **Tên Job**: `daily-sales-sync-3am`
- **Mô tả**: Đồng bộ dữ liệu bán hàng (sales) từ Zappy API
- **Thời gian xử lý**: Ngày T-1
- **Brands xử lý**: `f3`, `labhair`, `yaman`, `menard`
- **Trạng thái**: ✅ **Đang hoạt động**

**Chi tiết:**
- Đồng bộ tuần tự từng brand
- Mỗi brand được xử lý độc lập
- Lỗi của một brand không ảnh hưởng đến các brand khác
- **Lưu ý**: Phần tự động tạo invoice sau khi đồng bộ đã bị comment (không tự động tạo)
- Format ngày: `DDMMMYYYY` (ví dụ: `21DEC2025`)

---

## 📊 Tóm Tắt Lịch Trình

| Thời Gian | Cron Job | Mô Tả | Trạng Thái |
|-----------|----------|-------|------------|
| **1:00 AM** | `daily-stock-transfer-sync` | Đồng bộ xuất kho | ✅ Hoạt động |
| **2:30 AM** | `daily-shift-end-cash-sync-2-30am` | Đồng bộ báo cáo nộp quỹ cuối ca | ✅ Hoạt động |
| **3:00 AM** | `daily-sales-sync-3am` | Đồng bộ bán hàng | ✅ Hoạt động |

---

## 🔄 Luồng Xử Lý Tổng Quan

```
1:00 AM → Đồng bộ xuất kho (Stock Transfer)
    ↓
2:30 AM → Đồng bộ báo cáo nộp quỹ cuối ca (Shift End Cash)
    ↓
3:00 AM → Đồng bộ bán hàng (Sales)
```

---

## 📝 Ghi Chú

1. **Tất cả cron job đều xử lý dữ liệu ngày T-1** (ngày hôm qua)
2. **Format ngày**: `DDMMMYYYY` (ví dụ: `21DEC2025`)
3. **Múi giờ**: `Asia/Ho_Chi_Minh`
4. **Xử lý lỗi**: Mỗi brand/đơn hàng được xử lý độc lập, lỗi không ảnh hưởng lẫn nhau
5. **Tạo invoice**: Hiện tại chỉ tạo khi user double-click vào sale, không tự động
6. **Các cronjob đã tắt**: 
   - Tất cả các cronjob đồng bộ FaceID (2AM, 3AM, 12PM) đã được tắt
   - Tất cả các cronjob đồng bộ dữ liệu ngày hiện tại (12PM, 6PM, 9PM, 10PM) đã được tắt

---

## 🛠️ Cách Bật/Tắt Cron Job

### Để tắt một cron job:
Comment decorator `@Cron`:
```typescript
// @Cron('0 2 * * *', {
//   name: 'daily-warehouse-invoice-2am',
//   timeZone: 'Asia/Ho_Chi_Minh',
// })
```

### Để bật lại một cron job:
Uncomment decorator `@Cron`:
```typescript
@Cron('0 2 * * *', {
  name: 'daily-warehouse-invoice-2am',
  timeZone: 'Asia/Ho_Chi_Minh',
})
```

---

**File**: `backend/src/tasks/sync.task.ts`  
**Cập nhật lần cuối**: 2025-01-XX

