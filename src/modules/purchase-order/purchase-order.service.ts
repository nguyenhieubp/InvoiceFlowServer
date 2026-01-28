import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between } from 'typeorm';
import { PurchaseOrder } from '../../entities/purchase-order.entity';
import { ZappyApiService } from '../../services/zappy-api.service';

@Injectable()
export class PurchaseOrderService {
  private readonly logger = new Logger(PurchaseOrderService.name);

  constructor(
    @InjectRepository(PurchaseOrder)
    private poRepository: Repository<PurchaseOrder>,
    private zappyService: ZappyApiService,
  ) {}

  /**
   * Sync Purchase Orders for a date range
   * @param startDate Date string YYYY-MM-DD
   * @param endDate Date string YYYY-MM-DD
   * @param brand Brand name (optional, default 'menard')
   */
  async syncPurchaseOrders(startDate: string, endDate: string, brand?: string) {
    const brands =
      brand && brand !== 'all' ? [brand] : ['menard', 'f3', 'labhair', 'yaman'];

    this.logger.log(
      `Starting PO sync from ${startDate} to ${endDate} for brands: ${brands.join(', ')}`,
    );

    const dates = this.getDatesInRange(startDate, endDate);
    let totalSynced = 0;

    for (const currentBrand of brands) {
      for (const date of dates) {
        try {
          const formattedDate = this.formatDateForApi(date); // DDMONYYYY
          this.logger.log(
            `Fetching POs for date: ${formattedDate} (Brand: ${currentBrand})`,
          );

          const items = await this.zappyService.getDailyPO(
            formattedDate,
            currentBrand,
          );

          if (items.length > 0) {
            const dayStart = new Date(date);
            dayStart.setHours(0, 0, 0, 0);
            const dayEnd = new Date(date);
            dayEnd.setHours(23, 59, 59, 999);

            // Note: If we want to support overlapping data from different brands on the same date,
            // we should likely filter delete by brand too, assuming the entity supports it.
            // For now, staying consistent with previous logic but being careful.
            // Ideally, we'd add .andWhere('brand = :brand', { brand: currentBrand }) if we had a brand column.
            await this.poRepository.delete({
              poDate: Between(dayStart, dayEnd),
              // TODO: Add brand filter here if Entity has brand column to avoid deleting other brands' data
              // catName might hold brand info?
            });

            const entities = items.map((item) => this.mapToPurchaseOrder(item));
            await this.poRepository.save(entities);
            totalSynced += entities.length;
            this.logger.log(
              `Synced ${entities.length} POs for ${formattedDate} (Brand: ${currentBrand})`,
            );
          } else {
            this.logger.log(
              `No POs found for ${formattedDate} (Brand: ${currentBrand})`,
            );
          }
        } catch (error: any) {
          this.logger.error(
            `Failed to sync PO for date ${date} (Brand: ${currentBrand}): ${error.message}`,
          );
        }
      }
    }
    return { success: true, count: totalSynced };
  }

  /**
   * Get all POs with pagination & filter
   */
  async getPurchaseOrders(params: {
    page?: number;
    limit?: number;
    startDate?: string;
    endDate?: string;
    search?: string;
  }) {
    const page = params.page || 1;
    const limit = params.limit || 20;
    const query = this.poRepository.createQueryBuilder('po');

    if (params.startDate && params.endDate) {
      query.andWhere('po.poDate BETWEEN :start AND :end', {
        start: params.startDate,
        end: params.endDate,
      });
    }
    if (params.search) {
      query.andWhere('(po.poCode LIKE :search OR po.itemName LIKE :search)', {
        search: `%${params.search}%`,
      });
    }

    query.orderBy('po.poDate', 'DESC');
    query.skip((page - 1) * limit).take(limit);

    const [data, total] = await query.getManyAndCount();
    return {
      data,
      meta: { page, limit, total, totalPages: Math.ceil(total / limit) },
    };
  }

  // --- Helpers ---

  private getDatesInRange(startDate: string, endDate: string): Date[] {
    const dates: Date[] = [];
    const current = new Date(startDate);
    const end = new Date(endDate);
    while (current <= end) {
      dates.push(new Date(current));
      current.setDate(current.getDate() + 1);
    }
    return dates;
  }

  private formatDateForApi(date: Date): string {
    const day = String(date.getDate()).padStart(2, '0');
    const monthNames = [
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
    const month = monthNames[date.getMonth()];
    const year = date.getFullYear();
    return `${day}${month}${year}`;
  }

  private mapToPurchaseOrder(item: any): PurchaseOrder {
    const po = new PurchaseOrder();
    po.poCode = item.po_code;
    po.poDate = item.po_date ? new Date(item.po_date) : null;
    po.catName = item.cat_name;
    po.itemCode = item.itemcode;
    po.itemName = item.itemname;
    po.supplierItemCode = item.supplier_item_code;
    po.supplierItemName = item.supplier_item_name;
    po.manageType = item.manage_type;
    po.qty = Number(item.qty || 0);
    po.receivedQty = Number(item.received_qty || 0);
    po.returnedQty = Number(item.returned_qty || 0);
    po.salePrice = Number(item.sale_price || 0);
    po.price = Number(item.price || 0);
    po.vatPct = Number(item.vatpct || 0);
    po.importTaxPct = Number(item.importtaxpct || 0);
    po.importTaxTotal = Number(item.importtaxtotal || 0);
    po.amount = Number(item.amount || 0);
    po.promAmount = Number(item.prom_amount || 0);
    po.discPct = Number(item.discpct || 0);
    po.vatTotal = Number(item.vattotal || 0);
    po.discTotal = Number(item.disctotal || 0);
    po.lineTotal = Number(item.linetotal || 0);
    po.noteCategory = item.note_category;
    po.noteDetail = item.note_detail;
    po.shipToBranchCode = item.shipto_branch_code;
    po.shipToBranchName = item.shipto_branch_name;
    po.itemCost = Number(item.itemcost || 0);
    po.poCost = Number(item.po_cost || 0);
    po.onGrCost = Number(item.on_gr_cost || 0);
    po.afterGrCost = Number(item.after_gr_cost || 0);
    po.isSupplierPromotionItem = item.is_supplier_promotion_item;
    po.isPromotionProd = item.ispromotionprod;
    po.purchaseTypeName = item.purchase_type_name;
    po.priceCode = item.price_code;
    po.salePriceCode = item.sale_price_code;
    po.savedPriceForPromItem = Number(item.saved_price_for_prom_item || 0);
    po.shipmentCode = item.shipment_code;
    po.shipmentName = item.shipment_name;
    po.shipmentPlanDate = item.shipment_plan_date
      ? new Date(item.shipment_plan_date)
      : null;
    po.shipmentTransMethod = item.shipment_trans_method;
    return po;
  }
}
