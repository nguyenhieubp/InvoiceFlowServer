import { StockTransfer } from '../entities/stock-transfer.entity';

/**
 * Stock Transfer Utilities
 */

/**
 * Lấy danh sách docCode cần fetch stock transfers
 * Xử lý đặc biệt cho đơn trả lại (RT): thêm mã đơn gốc (SO) vào danh sách
 * @param docCodes - Danh sách mã đơn hàng
 * @returns Danh sách docCode cần fetch
 */
export function getDocCodesForStockTransfer(docCodes: string[]): string[] {
  const result = new Set<string>();

  for (const docCode of docCodes) {
    result.add(docCode);

    // Nếu là đơn trả lại (RT), thêm mã đơn gốc (SO) vào danh sách
    if (docCode.startsWith('RT')) {
      // RT33.00121928_1 -> SO33.00121928 (chuyển RT thành SO, bỏ _1)
      const originalOrderCode = docCode.replace(/^RT/, 'SO').replace(/_\d+$/, '');
      result.add(originalOrderCode);
    }
  }

  return Array.from(result);
}

/**
 * Build stock transfer maps (stockTransferMap và stockTransferByDocCodeMap)
 * @param stockTransfers - Danh sách stock transfers
 * @param loyaltyProductMap - Map để lấy materialCode từ itemCode
 * @param docCodes - Danh sách docCodes của orders
 * @returns Object chứa stockTransferMap và stockTransferByDocCodeMap
 */
export function buildStockTransferMaps(
  stockTransfers: StockTransfer[],
  loyaltyProductMap: Map<string, any>,
  docCodes: string[]
): {
  stockTransferMap: Map<string, StockTransfer[]>;
  stockTransferByDocCodeMap: Map<string, StockTransfer[]>;
} {
  const stockTransferMap = new Map<string, StockTransfer[]>();
  const stockTransferByDocCodeMap = new Map<string, StockTransfer[]>();

  for (const transfer of stockTransfers) {
    const transferLoyaltyProduct = transfer.itemCode ? loyaltyProductMap.get(transfer.itemCode) : null;
    const materialCode = transfer.materialCode || transferLoyaltyProduct?.materialCode;

    if (!materialCode) continue;

    const orderDocCode = transfer.soCode || transfer.docCode;
    const key = `${orderDocCode}_${materialCode}`;

    if (!stockTransferMap.has(key)) stockTransferMap.set(key, []);
    stockTransferMap.get(key)!.push(transfer);

    if (!stockTransferByDocCodeMap.has(orderDocCode)) stockTransferByDocCodeMap.set(orderDocCode, []);
    stockTransferByDocCodeMap.get(orderDocCode)!.push(transfer);

    // Xử lý đơn trả lại
    if (orderDocCode.startsWith('SO') && docCodes.some(docCode => docCode.startsWith('RT'))) {
      for (const docCode of docCodes) {
        if (docCode.startsWith('RT')) {
          const originalOrderCode = docCode.replace(/^RT/, 'SO').replace(/_\d+$/, '');
          if (originalOrderCode === orderDocCode) {
            const returnKey = `${docCode}_${materialCode}`;
            if (!stockTransferMap.has(returnKey)) stockTransferMap.set(returnKey, []);
            stockTransferMap.get(returnKey)!.push(transfer);

            if (!stockTransferByDocCodeMap.has(docCode)) stockTransferByDocCodeMap.set(docCode, []);
            stockTransferByDocCodeMap.get(docCode)!.push(transfer);
          }
        }
      }
    }
  }

  return { stockTransferMap, stockTransferByDocCodeMap };
}

/**
 * Tìm stock transfer phù hợp
 */
export function findMatchingStockTransfer(
  sale: any,
  docCode: string,
  stockTransfers: StockTransfer[],
  saleMaterialCode?: string | null,
  stockTransferMap?: Map<string, StockTransfer[]>
): StockTransfer | null {
  const isReturnOrder = docCode.startsWith('RT');
  let originalOrderCode: string | null = null;
  let stockOutDocCode: string | null = null;

  if (isReturnOrder) {
    originalOrderCode = docCode.replace(/^RT/, 'SO').replace(/_\d+$/, '');
    stockOutDocCode = docCode.replace(/^RT/, 'ST');
  }

  if (saleMaterialCode && stockTransferMap) {
    let stockTransferKey = `${docCode}_${saleMaterialCode}`;
    let matchedTransfers = stockTransferMap.get(stockTransferKey) || [];

    if (matchedTransfers.length === 0 && isReturnOrder && originalOrderCode) {
      stockTransferKey = `${originalOrderCode}_${saleMaterialCode}`;
      matchedTransfers = stockTransferMap.get(stockTransferKey) || [];
    }

    if (matchedTransfers.length > 0) return matchedTransfers[0];
  }

  if (sale.itemCode) {
    let matched = stockTransfers.find(st => st.soCode === docCode && st.itemCode === sale.itemCode);
    if (!matched && isReturnOrder && originalOrderCode) {
      matched = stockTransfers.find(st => st.soCode === originalOrderCode && st.itemCode === sale.itemCode);
    }
    if (!matched && isReturnOrder && stockOutDocCode) {
      matched = stockTransfers.find(st => st.docCode === stockOutDocCode && st.itemCode === sale.itemCode);
    }
    if (matched) return matched;
  }

  return null;
}

/**
 * Format stock transfer cho frontend
 */
export function formatStockTransferForFrontend(st?: StockTransfer | null): any {
  if (!st) return null;
  return {
    ...st,
    materialCode: st.materialCode || null,
    itemCode: st.itemCode,
    maKho: st.itemCode,
    batchSerial: st.batchSerial,
  };
}
