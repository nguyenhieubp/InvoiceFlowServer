# FAST API Sync Cases Documentation

This document records specific cases and configurations regarding the synchronization process with FAST API.

## Disabled Auto-Sync Features

The following features have been disabled to allow for manual control and to prevent unintended automatic invoice/warehouse processing during sync.

### 1. Sales Sync -> Auto Invoice Creation (Disabled)

**File:** `backend/src/modules/sales/services/sales.service.ts`
**Method:** `syncSalesByDateRange`

**Change:**
The "Phase 2" of the sync process, which automatically creates Fast API invoices for the synced orders, has been disabled.

**Code Location:**

```typescript
// backend/src/modules/sales/services/sales.service.ts

// ... inside syncSalesByDateRange ...

// Phase 2: Process Invoices
// DISABLED per user request
// this.logger.log(
//   `[Two-Phase Sync] Phase 2: Processing Fast API Invoices...`,
// );
// const invoiceResult =
//   await this.salesInvoiceService.processInvoicesByDateRange(
//     startDate,
//     endDate,
//   );

return {
  ...syncResult,
  // message: `${syncResult.message}. Phase 2: ${invoiceResult.message}`,
  message: `${syncResult.message}. Phase 2 (Auto Invoice) DISABLED per user request.`,
};
```

**How to Re-enable:**
Uncomment the code block in Phase 2 and restore the original return message.

### 2. Stock Transfer Sync -> Auto Warehouse Processing (Disabled)

**File:** `backend/src/modules/sync/sync.service.ts`
**Methods:** `syncStockTransfer` and `syncStockTransferRange`

**Change:**
The automatic processing of warehouse receipts/issues (creating "Phiếu nhập/xuất kho" on Fast) has been disabled during the sync process.

**Code Location 1: `syncStockTransfer`**

```typescript
// backend/src/modules/sync/sync.service.ts

// ... inside syncStockTransfer ...

// Tự động xử lý warehouse cho các stock transfers mới
// DISABLED per user request: Không tự động tạo phiếu nhập/xuất kho khi sync
// if (!options?.skipWarehouseProcessing) {
//   try {
//     await this.processWarehouseForStockTransfers(date, brand);
//   } ...
// }
```

**Code Location 2: `syncStockTransferRange`**

```typescript
// backend/src/modules/sync/sync.service.ts

// ... inside syncStockTransferRange ...

// Phase 2: Xử lý Warehouse (Sau khi đã có đủ dữ liệu)
// DISABLED per user request: Không tự động xử lý warehouse (Push to Fast)
// for (const dateStr of dateList) {
//   for (const brandItem of brands) {
//     try {
//       ...
//       await this.processWarehouseForStockTransfers(dateStr, brandItem);
//       ...
//     } ...
//   }
// }
```

**How to Re-enable:**
Uncomment the corresponding code blocks in `syncStockTransfer` and `syncStockTransferRange`.
