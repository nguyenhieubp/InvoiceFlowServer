export const PAYMENT_SUPPLIER_MAPPING: Record<string, string> = {
  VNPAY: 'NCC0618',
  PAYOO: 'NCC0559',
};

/**
 * Get supplier code from payment partner identifier
 * @param partnerCode The partner code (e.g. VNPAY, PAYOO)
 * @returns The supplier code (e.g. NCC0618, NCC0559) or the original code if no mapping found
 */
export function getSupplierCode(partnerCode: string | null): string | null {
  if (!partnerCode) return null;
  const normalizedCode = partnerCode.toUpperCase().trim();

  // Check exact match
  if (PAYMENT_SUPPLIER_MAPPING[normalizedCode]) {
    return PAYMENT_SUPPLIER_MAPPING[normalizedCode];
  }

  // Check includes/partial match if needed, but instructions implies exact or direct mapping
  // Let's stick to exact mapping for now based on the request examples

  return partnerCode;
}
