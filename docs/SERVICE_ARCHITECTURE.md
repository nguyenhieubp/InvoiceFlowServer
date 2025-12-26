# Kiến trúc Services - Fast API Integration

## Tổng quan

Hệ thống sử dụng 3 lớp service để tích hợp với Fast API:

## 1. FastApiClientService (HTTP Client Layer)
**Vị trí:** `src/services/fast-api-client.service.ts`

**Vai trò:** Lớp HTTP client cấp thấp nhất, giao tiếp trực tiếp với Fast API server

**Nhiệm vụ:**
- ✅ Quản lý authentication (login, token, auto-refresh)
- ✅ Gọi các API endpoints của Fast API
- ✅ Xử lý retry khi token hết hạn (401)
- ✅ Logging và error handling cơ bản
- ❌ Không có business logic phức tạp

**Methods chính:**
- `login()` - Đăng nhập và lấy token
- `submitSalesOrder()` - Gọi API `/salesOrder`
- `submitSalesInvoice()` - Gọi API `/salesInvoice`
- `createOrUpdateCustomer()` - Gọi API `/Customer`
- `submitWarehouseReceipt()` - Gọi API `/warehouseReceipt`
- `submitWarehouseRelease()` - Gọi API `/warehouseRelease`
- `submitWarehouseTransfer()` - Gọi API `/warehouseTransfer`
- `submitCashReceipt()` - Gọi API `/cashReceipt`
- `submitCreditAdvice()` - Gọi API `/creditAdvice`
- `submitSalesReturn()` - Gọi API `/salesReturn`
- `submitGxtInvoice()` - Gọi API `/gxtInvoice`

---

## 2. FastApiInvoiceFlowService (Business Flow Layer)
**Vị trí:** `src/services/fast-api-invoice-flow.service.ts`

**Vai trò:** Lớp điều phối business flows, orchestrate nhiều API calls

**Nhiệm vụ:**
- ✅ Sử dụng `FastApiService` để gọi API
- ✅ Implement business flows: `Customer → SalesOrder → SalesInvoice`
- ✅ Validate responses (status = 1)
- ✅ Transform data và build payloads (sử dụng `FastApiPayloadHelper`)
- ✅ Xử lý warehouse operations từ stock transfers
- ✅ Xử lý cashio payments
- ✅ Fetch thông tin từ Loyalty API (material catalog, department)
- ✅ Map warehouse codes

**Methods chính:**
- `executeFullInvoiceFlow()` - Flow đầy đủ: Customer → SalesOrder → SalesInvoice
- `createOrUpdateCustomer()` - Tạo/cập nhật customer (với validation)
- `createSalesOrder()` - Tạo sales order (với validation)
- `createSalesInvoice()` - Tạo sales invoice (với validation promotion codes)
- `createSalesReturn()` - Tạo sales return
- `createGxtInvoice()` - Tạo gxt invoice
- `processWarehouseFromStockTransfer()` - Xử lý warehouse từ STOCK_IO
- `processWarehouseTransferFromStockTransfers()` - Xử lý warehouse transfer từ STOCK_TRANSFER
- `processCashioPayment()` - Xử lý thanh toán cashio

---

## 3. FastApiInvoiceService (Database Service Layer)
**Vị trí:** `src/modules/fast-api-invoices/fast-api-invoice.service.ts`

**Vai trò:** Quản lý database records của Fast API invoices

**Nhiệm vụ:**
- ✅ CRUD operations cho `FastApiInvoice` entity
- ✅ Query và filter invoices: theo status, docCode, maKh, date range
- ✅ Thống kê: success/failed count, success rate
- ✅ Lấy danh sách invoices thất bại để retry
- ❌ Không gọi Fast API trực tiếp
- ❌ Không có business logic

**Methods chính:**
- `findAll()` - Lấy danh sách invoices với pagination và filters
- `findOne()` - Lấy invoice theo ID
- `findByDocCode()` - Lấy invoice theo docCode
- `getStatistics()` - Lấy thống kê (total, success, failed, successRate)
- `getFailedInvoicesByDateRange()` - Lấy danh sách invoices thất bại trong khoảng thời gian

---

## Sơ đồ Luồng Dữ Liệu

```
┌─────────────────────────────────────────────────────────────┐
│                     SalesService                             │
│  (Entry point cho business logic chính)                     │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ↓
┌─────────────────────────────────────────────────────────────┐
│            FastApiInvoiceFlowService                        │
│  • Orchestrate business flows                              │
│  • Validate và transform data                              │
│  • Build payloads                                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ↓
┌─────────────────────────────────────────────────────────────┐
│              FastApiClientService                           │
│  • HTTP client                                              │
│  • Authentication (token management)                        │
│  • Gọi Fast API endpoints                                  │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ↓ HTTP Request
┌─────────────────────────────────────────────────────────────┐
│              Fast API Server                                │
│        (103.145.79.169:6688/Fast)                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ↓ HTTP Response (status = 1)
┌─────────────────────────────────────────────────────────────┐
│            FastApiInvoiceService                            │
│  • Lưu kết quả vào database                                │
│  • Quản lý records                                         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ↓
┌─────────────────────────────────────────────────────────────┐
│          FastApiInvoice Entity                              │
│              (Database Table)                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Ví dụ Sử Dụng

### Scenario: Tạo invoice từ sales order

```typescript
// 1. SalesService gọi FastApiInvoiceFlowService
const result = await fastApiInvoiceFlowService.executeFullInvoiceFlow({
  ma_kh: 'KH123',
  ten_kh: 'Nguyen Van A',
  so_ct: 'SO123',
  detail: [...]
});

// 2. FastApiInvoiceFlowService orchestrate flow:
//    - Gọi FastApiClientService.createOrUpdateCustomer()
//    - Gọi FastApiClientService.submitSalesOrder()
//    - Gọi FastApiClientService.submitSalesInvoice()

// 3. FastApiClientService thực hiện HTTP calls:
//    - POST /Fast/Customer (với token)
//    - POST /Fast/salesOrder (với token)
//    - POST /Fast/salesInvoice (với token)

// 4. Sau khi thành công, lưu kết quả:
//    await fastApiInvoiceService.save(result);
```

---

## Tóm tắt Khác biệt

| Tiêu chí | FastApiClientService | FastApiInvoiceFlowService | FastApiInvoiceService |
|----------|----------------------|---------------------------|----------------------|
| **Lớp** | HTTP Client | Business Flow | Database |
| **Gọi Fast API?** | ✅ Có (HTTP calls) | ✅ Có (qua FastApiClientService) | ❌ Không |
| **Business Logic?** | ❌ Không | ✅ Có (orchestration) | ❌ Không |
| **Database?** | ❌ Không | ❌ Không | ✅ Có (CRUD) |
| **Authentication?** | ✅ Có (token) | ❌ Không (delegate) | ❌ Không |
| **Validation?** | ❌ Không | ✅ Có (status = 1) | ❌ Không |
| **Payload Building?** | ❌ Không | ✅ Có (helper) | ❌ Không |

