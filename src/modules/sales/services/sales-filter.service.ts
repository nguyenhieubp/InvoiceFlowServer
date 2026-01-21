import { Injectable } from '@nestjs/common';
import { SelectQueryBuilder } from 'typeorm';
import { Sale } from '../../../entities/sale.entity';

/**
 * SalesFilterService
 * Chịu trách nhiệm: Filter logic cho sales queries
 *
 * Handles: date filters, brand filters, search, status filters
 */
@Injectable()
export class SalesFilterService {
  /**
   * Apply common filters to sales query
   */
  applySaleFilters(
    query: SelectQueryBuilder<Sale>,
    options: {
      brand?: string;
      search?: string;
      statusAsys?: boolean;
      typeSale?: string;
      date?: string;
      dateFrom?: string | Date; // Allow Date object
      dateTo?: string | Date; // Allow Date object
      isProcessed?: boolean;
    },
  ): void {
    const {
      brand,
      search,
      statusAsys,
      typeSale,
      date,
      dateFrom,
      dateTo,
      isProcessed,
    } = options;

    if (isProcessed !== undefined) {
      query.andWhere('sale.isProcessed = :isProcessed', { isProcessed });
    }
    if (statusAsys !== undefined) {
      query.andWhere('sale.statusAsys = :statusAsys', { statusAsys });
    }
    if (typeSale && typeSale !== 'ALL') {
      query.andWhere('sale.type_sale = :type_sale', {
        type_sale: typeSale.toUpperCase(),
      });
    }

    if (brand) {
      // Use sale.brand directly instead of joining customer
      query.andWhere('sale.brand = :brand', { brand });
    }

    if (search && search.trim() !== '') {
      const searchPattern = `%${search.trim().toLowerCase()}%`;
      // Searching by customer fields requires customer join
      query.andWhere(
        "(LOWER(sale.docCode) LIKE :search OR LOWER(COALESCE(customer.name, '')) LIKE :search OR LOWER(COALESCE(customer.code, '')) LIKE :search OR LOWER(COALESCE(customer.mobile, '')) LIKE :search)",
        { search: searchPattern },
      );
    }

    // Date logic
    let startDate: Date | undefined;
    let endDate: Date | undefined;

    // Handle string inputs for dateFrom/dateTo (from API query params) or Date objects
    if (dateFrom) {
      startDate = new Date(dateFrom);
      startDate.setHours(0, 0, 0, 0);
    }
    if (dateTo) {
      endDate = new Date(dateTo);
      endDate.setHours(23, 59, 59, 999);
    }

    if (startDate && endDate) {
      query.andWhere(
        'sale.docDate >= :startDate AND sale.docDate <= :endDate',
        {
          startDate,
          endDate,
        },
      );
    } else if (startDate) {
      query.andWhere('sale.docDate >= :startDate', { startDate });
    } else if (endDate) {
      query.andWhere('sale.docDate <= :endDate', { endDate });
    } else if (date) {
      // Special format DDMMMYYYY
      const dateMatch = date.match(/^(\d{2})([A-Z]{3})(\d{4})$/i);
      if (dateMatch) {
        const [, day, monthStr, year] = dateMatch;
        const monthMap: { [key: string]: number } = {
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
        const month = monthMap[monthStr.toUpperCase()];
        if (month !== undefined) {
          const dateObj = new Date(parseInt(year), month, parseInt(day));
          const startOfDay = new Date(dateObj);
          startOfDay.setHours(0, 0, 0, 0);
          const endOfDay = new Date(dateObj);
          endOfDay.setHours(23, 59, 59, 999);
          query.andWhere(
            'sale.docDate >= :startDate AND sale.docDate <= :endDate',
            {
              startDate: startOfDay,
              endDate: endOfDay,
            },
          );
        }
      }
    } else if (brand && !startDate && !endDate && !date) {
      // Default: Last 30 days if only brand is specified
      const end = new Date();
      end.setHours(23, 59, 59, 999);
      const start = new Date();
      start.setDate(start.getDate() - 30);
      start.setHours(0, 0, 0, 0);
      query.andWhere(
        'sale.docDate >= :startDate AND sale.docDate <= :endDate',
        {
          startDate: start,
          endDate: end,
        },
      );
    }
  }
}
