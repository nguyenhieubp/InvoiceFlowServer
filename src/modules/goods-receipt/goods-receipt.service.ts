import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between } from 'typeorm';
import { GoodsReceipt } from '../../entities/goods-receipt.entity';
import { ZappyApiService } from '../../services/zappy-api.service';

@Injectable()
export class GoodsReceiptService {
  private readonly logger = new Logger(GoodsReceiptService.name);

  constructor(
    @InjectRepository(GoodsReceipt)
    private grRepository: Repository<GoodsReceipt>,
    private zappyService: ZappyApiService,
  ) {}

  /**
   * Sync Goods Receipts for a date range
   */
  async syncGoodsReceipts(startDate: string, endDate: string, brand?: string) {
    const brands =
      brand && brand !== 'all' ? [brand] : ['menard', 'f3', 'labhair', 'yaman'];

    this.logger.log(
      `Starting GR sync from ${startDate} to ${endDate} for brands: ${brands.join(', ')}`,
    );

    const dates = this.getDatesInRange(startDate, endDate);
    let totalSynced = 0;

    for (const currentBrand of brands) {
      for (const date of dates) {
        try {
          const formattedDate = this.formatDateForApi(date); // DDMONYYYY
          this.logger.log(
            `Fetching GRs for date: ${formattedDate} (Brand: ${currentBrand})`,
          );

          const items = await this.zappyService.getDailyGR(
            formattedDate,
            currentBrand,
          );

          if (items.length > 0) {
            const dayStart = new Date(date);
            dayStart.setHours(0, 0, 0, 0);
            const dayEnd = new Date(date);
            dayEnd.setHours(23, 59, 59, 999);

            await this.grRepository.delete({
              grDate: Between(dayStart, dayEnd),
              brand: currentBrand,
            });

            const entities = items.map((item) =>
              this.mapToGoodsReceipt(item, currentBrand),
            );
            await this.grRepository.save(entities);
            totalSynced += entities.length;
            this.logger.log(
              `Synced ${entities.length} GRs for ${formattedDate} (Brand: ${currentBrand})`,
            );
          } else {
            this.logger.log(
              `No GRs found for ${formattedDate} (Brand: ${currentBrand})`,
            );
          }
        } catch (error: any) {
          this.logger.error(
            `Failed to sync GR for date ${date} (Brand: ${currentBrand}): ${error.message}`,
          );
        }
      }
    }
    return { success: true, count: totalSynced };
  }

  /**
   * Get all GRs with pagination & filter
   */
  async getGoodsReceipts(params: {
    page?: number;
    limit?: number;
    startDate?: string;
    endDate?: string;
    search?: string;
    brand?: string;
  }) {
    const page = params.page || 1;
    const limit = params.limit || 20;
    const query = this.grRepository.createQueryBuilder('gr');

    if (params.startDate && params.endDate) {
      query.andWhere('gr.grDate BETWEEN :start AND :end', {
        start: params.startDate,
        end: params.endDate,
      });
    }
    if (params.search) {
      query.andWhere('(gr.grCode LIKE :search OR gr.itemName LIKE :search)', {
        search: `%${params.search}%`,
      });
    }
    if (params.brand) {
      query.andWhere('gr.brand = :brand', { brand: params.brand });
    }

    query.orderBy('gr.grDate', 'DESC');
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

  private mapToGoodsReceipt(item: any, brand?: string): GoodsReceipt {
    const gr = new GoodsReceipt();
    gr.brand = brand || null;
    gr.grCode = item.gr_code;
    gr.grDate = item.gr_date ? new Date(item.gr_date) : null;
    gr.poCode = item.po_code;
    gr.catName = item.cat_name;
    gr.itemCode = item.itemcode;
    gr.itemName = item.itemname;
    gr.manageType = item.manage_type;
    gr.qty = Number(item.qty || 0);
    gr.returnedQty = Number(item.returned_qty || 0);
    gr.price = Number(item.price || 0);
    gr.vatPct = Number(item.vatpct || 0);
    gr.importTaxPct = Number(item.importtaxpct || 0);
    gr.discPct = Number(item.discpct || 0);
    gr.vatTotal = Number(item.vattotal || 0);
    gr.importTaxTotal = Number(item.importtaxtotal || 0);
    gr.discTotal = Number(item.disctotal || 0);
    gr.cuocVcqt = Number(item.cuoc_vcqt || 0);
    gr.lineTotal = Number(item.linetotal || 0);
    gr.noteCategory = item.note_category;
    gr.noteDetail = item.note_detail;
    gr.itemCost = Number(item.itemcost || 0);
    gr.totalItemCost = Number(item.totalitemcost || 0);
    gr.poCost = Number(item.po_cost || 0);
    gr.onGrCost = Number(item.on_gr_cost || 0);
    gr.afterGrCost = Number(item.after_gr_cost || 0);
    gr.shipToBranchCode = item.shipto_branch_code;
    gr.isSupplierPromotionItem = item.is_supplier_promotion_item;
    gr.isPromotionProd = item.ispromotionprod;
    gr.purchaseTypeName = item.purchase_type_name;
    gr.savedPriceForPromItem = Number(item.saved_price_for_prom_item || 0);
    gr.purchaseRequestShipmentCode = item.purchase_request_shipment_code;
    gr.batchSerial = item.batchserial;
    return gr;
  }
}
