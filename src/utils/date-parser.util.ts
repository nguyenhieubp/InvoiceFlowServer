/**
 * Utility functions for parsing dates in various formats
 */

/**
 * Parse date string from DDMMMYYYY format to Date object
 * @param dateStr - Date string in format DDMMMYYYY (e.g., "01JAN2026", "15DEC2025")
 * @param endOfDay - If true, set time to 23:59:59, otherwise 00:00:00
 * @returns Date object or null if parsing fails
 * @example
 * parseDDMMMYYYY("01JAN2026") // Returns Date(2026, 0, 1, 0, 0, 0)
 * parseDDMMMYYYY("31DEC2025", true) // Returns Date(2025, 11, 31, 23, 59, 59)
 */
export function parseDDMMMYYYY(
  dateStr: string,
  endOfDay: boolean = false,
): Date {
  const day = parseInt(dateStr.substring(0, 2));
  const monthStr = dateStr.substring(2, 5).toUpperCase();
  const year = parseInt(dateStr.substring(5, 9));

  const monthMap: Record<string, number> = {
    JAN: 0,
    FEB: 1,
    MAR: 2,
    APR: 3,
    MAY: 4,
    JUN: 5,
    JUL: 6,
    AUG: 7,
    SEP: 8,
    OCT: 9,
    NOV: 10,
    DEC: 11,
  };

  const month = monthMap[monthStr] || 0;

  if (endOfDay) {
    return new Date(year, month, day, 23, 59, 59);
  }

  return new Date(year, month, day);
}

/**
 * Format Date object to DDMMMYYYY string
 * @param date - Date object to format
 * @returns Formatted string in DDMMMYYYY format
 * @example
 * formatToDDMMMYYYY(new Date(2026, 0, 1)) // Returns "01JAN2026"
 */
export function formatToDDMMMYYYY(date: Date): string {
  const day = date.getDate().toString().padStart(2, '0');
  const monthIdx = date.getMonth();
  const year = date.getFullYear();

  const months = [
    'JAN',
    'FEB',
    'MAR',
    'APR',
    'MAY',
    'JUN',
    'JUL',
    'AUG',
    'SEP',
    'OCT',
    'NOV',
    'DEC',
  ];

  const monthStr = months[monthIdx];
  return `${day}${monthStr}${year}`;
}
