import { Injectable, Logger } from '@nestjs/common';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { DataSource, Repository } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';
import { PlatformFee } from '../../entities/platform-fee.entity';

/**
 * Multi-Database Service
 * Quản lý thao tác giữa các databases
 * NOTE: Entities are not registered - using direct SQL queries only
 */
@Injectable()
export class MultiDbService {
  private readonly logger = new Logger(MultiDbService.name);

  constructor(
    // Primary Database (103.145.79.36)
    @InjectDataSource()
    private primaryDataSource: DataSource,

    @InjectRepository(OrderFee)
    private orderFeeRepository: Repository<OrderFee>,

    @InjectRepository(PlatformFee)
    private platformFeeRepository: Repository<PlatformFee>,

    // Secondary Database (103.145.79.165)
    @InjectDataSource('secondary')
    private secondaryDataSource: DataSource,

    // Third Database (103.145.79.37)
    @InjectDataSource('third')
    private thirdDataSource: DataSource,
  ) {}

  /**
   * Lấy thông tin kết nối của tất cả databases
   */
  async getDatabasesInfo() {
    return {
      primary: {
        name: 'Primary Database',
        host: this.primaryDataSource.options['host'],
        database: this.primaryDataSource.options['database'],
        isConnected: this.primaryDataSource.isInitialized,
      },
      secondary: {
        name: 'Secondary Database',
        host: this.secondaryDataSource.options['host'],
        database: this.secondaryDataSource.options['database'],
        isConnected: this.secondaryDataSource.isInitialized,
      },
      third: {
        name: 'Third Database',
        host: this.thirdDataSource.options['host'],
        database: this.thirdDataSource.options['database'],
        isConnected: this.thirdDataSource.isInitialized,
      },
    };
  }

  /**
   * Kiểm tra kết nối đến tất cả databases
   */
  async checkConnections() {
    const results = {
      primary: false,
      secondary: false,
      third: false,
    };

    try {
      await this.primaryDataSource.query('SELECT 1');
      results.primary = true;
      this.logger.log('Primary database connected');
    } catch (error) {
      this.logger.error('Primary database connection failed', error);
    }

    try {
      await this.secondaryDataSource.query('SELECT 1');
      results.secondary = true;
      this.logger.log('Secondary database connected');
    } catch (error) {
      this.logger.error('Secondary database connection failed', error);
    }

    try {
      await this.thirdDataSource.query('SELECT 1');
      results.third = true;
      this.logger.log('Third database connected');
    } catch (error) {
      this.logger.error('Third database connection failed', error);
    }

    return results;
  }

  /**
   * Brand Configuration
   * Maps brand names to their specific table structures
   */
  private readonly brands = [
    {
      name: 'menard',
      tableLogs: '"menard_erp_order_logs"',
      tableDetail: 'public.menard_ecommer_detail_order',
      tableFee: 'public.menard_ecommer_detail_order_fee',
      tableOrders: 'public.menard_orders', // [NEW] Table to check Source
      detailIdColumn: 'menard_ecommer_detail_order_id',
    },
    {
      name: 'yaman',
      tableLogs: '"erp_order_logs"',
      tableDetail: 'public.yaman_ecommer_detail_order',
      tableFee: 'public.yaman_ecommer_detail_order_fee',
      tableOrders: '"yaman_orders"',
      detailIdColumn: 'yaman_ecommer_detail_order_id',
    },
  ];

  /**
   * Get order fees for a specific ERP code
   * Tries to find order in all configured brands
   */
  async getOrderFees(erpCode: string) {
    for (const brand of this.brands) {
      const results = await this.getOrderFeesByBrandConfig(erpCode, brand);
      if (results.length > 0) {
        return results;
      }
    }
    return [];
  }

  /**
   * Generic method to get fees by brand configuration
   */
  private async getOrderFeesByBrandConfig(erpCode: string, brandConfig: any) {
    const erpLogs = await this.secondaryDataSource.query(
      `
    SELECT *
    FROM ${brandConfig.tableLogs}
    WHERE "erpOrderCode" = $1
    `,
      [erpCode],
    );

    if (!erpLogs.length) return [];

    const pancakeOrderIds = erpLogs.map((item) => item.pancakeOrderId);

    const orderWithFees = await this.thirdDataSource.query(
      `
    SELECT 
      o."order_sn",
      f.*
    FROM ${brandConfig.tableDetail} o
    JOIN ${brandConfig.tableFee} f
      ON f.${brandConfig.detailIdColumn} = o.id
    WHERE o."order_sn" = ANY($1)
    `,
      [pancakeOrderIds],
    );

    const result: any[] = [];

    for (const erp of erpLogs) {
      const feesForOrder = orderWithFees.filter(
        (r) => r.order_sn === erp.pancakeOrderId,
      );

      for (const fee of feesForOrder) {
        result.push({
          erpOrderCode: erp.erpOrderCode,
          pancakeOrderId: erp.pancakeOrderId,
          rawData: fee,
          brand: brandConfig.name,
        });
      }
    }

    return result;
  }

  /**
   * Sync all order fees from external databases to primary database
   * This method is called by cronjob at 1 AM daily
   */
  async syncAllOrderFees(startAt?: string, endAt?: string) {
    this.logger.log(
      `Starting order fee sync...${startAt ? ` (From: ${startAt} To: ${endAt})` : ''}`,
    );

    let totalSynced = 0;
    let totalFailed = 0;
    let totalRecords = 0;

    try {
      for (const brand of this.brands) {
        this.logger.log(`Syncing brand: ${brand.name.toUpperCase()}...`);
        const result = await this.syncBrandOrders(brand, startAt, endAt);
        totalSynced += result.synced;
        totalFailed += result.failed;
        totalRecords += result.total;
      }

      this.logger.log(
        `✅ All brands sync completed: ${totalSynced} synced, ${totalFailed} failed`,
      );

      return { synced: totalSynced, failed: totalFailed, total: totalRecords };
    } catch (error) {
      this.logger.error('❌ Sync failed', error);
      throw error;
    }
  }

  private async syncBrandOrders(
    brandConfig: any,
    startAt?: string,
    endAt?: string,
  ) {
    this.logger.log(`Starting sync for brand ${brandConfig.name}...`);
    let synced = 0;
    let failed = 0;

    try {
      // Get ERP order codes from secondary database
      let query = `SELECT "erpOrderCode", "pancakeOrderId" FROM ${brandConfig.tableLogs}`;
      const params: any[] = [];

      if (startAt && endAt) {
        query += ` WHERE "createdAt" BETWEEN $1 AND $2`;
        params.push(startAt, endAt);
      }

      const erpLogs = await this.secondaryDataSource.query(query, params);

      this.logger.log(
        `Found ${erpLogs.length} ERP orders to sync for ${brandConfig.name}`,
      );

      // Process in batches to avoid memory issues
      const batchSize = 100;
      for (let i = 0; i < erpLogs.length; i += batchSize) {
        const batch = erpLogs.slice(i, i + batchSize);

        try {
          // Identify TikTok vs Shopee Logic
          const tiktokIds = await this.checkTikTokSource(
            brandConfig,
            batch.map((l) => l.pancakeOrderId),
          );

          // Group by platform
          const tiktokLogs = batch.filter((l) =>
            tiktokIds.has(l.pancakeOrderId),
          );
          const shopeeLogs = batch.filter(
            (l) => !tiktokIds.has(l.pancakeOrderId),
          );

          // --- 1. Process TikTok Orders ---
          if (tiktokLogs.length > 0) {
            const details = await this.getTikTokDetails(
              brandConfig,
              tiktokLogs.map((l) => l.pancakeOrderId),
            );

            for (const log of tiktokLogs) {
              const detail = details.find(
                (d) => d.order_sn === log.pancakeOrderId,
              );
              if (detail && detail.order_data) {
                // TikTok time is in seconds
                const createTime =
                  detail.order_data.create_time ||
                  detail.order_data.createTime ||
                  0;
                const orderDate = createTime
                  ? new Date(createTime * 1000)
                  : new Date();

                await this.orderFeeRepository.upsert(
                  {
                    feeId: `${log.erpOrderCode}_TIKTOK`,
                    brand: brandConfig.name,
                    erpOrderCode: log.erpOrderCode,
                    platform: 'tiktok',
                    orderSn: detail.order_sn, // [NEW] Map order_sn
                    rawData: detail.order_data,
                    orderCreatedAt: orderDate, // [NEW] Save order date
                    syncedAt: new Date(),
                  },
                  ['feeId'],
                );
                synced++;
              }
            }
          }

          // --- 2. Process Shopee Orders (Existing Logic) ---
          if (shopeeLogs.length > 0) {
            for (const erpLog of shopeeLogs) {
              try {
                const fees = await this.getOrderFeesByBrandConfig(
                  erpLog.erpOrderCode,
                  brandConfig,
                );

                for (const fee of fees) {
                  // Determine creation date early
                  const feeCreatedAt =
                    fee.rawData?.create_at ||
                    fee.rawData?.created_at ||
                    new Date();

                  // 1. Save Raw Order Fee
                  await this.orderFeeRepository.upsert(
                    {
                      feeId: fee.rawData?.id,
                      brand: fee.brand,
                      erpOrderCode: fee.erpOrderCode,
                      platform: 'shopee',
                      orderSn: fee.rawData?.order_sn, // [NEW] Map order_sn
                      rawData: fee.rawData,
                      orderCreatedAt: feeCreatedAt, // [NEW] Save order date
                      syncedAt: new Date(),
                    },
                    ['feeId'],
                  );

                  // 2. Calculate and Save Platform Fee (if applicable)
                  if (
                    fee.rawData?.fee_type === 'order_income' &&
                    fee.rawData?.raw_data
                  ) {
                    const raw = fee.rawData.raw_data;
                    const orderSellingPrice = Number(
                      raw.order_selling_price || 0,
                    );
                    const voucherFromSeller = Number(
                      raw.voucher_from_seller || 0,
                    );
                    const escrowAmount = Number(raw.escrow_amount || 0);

                    const platformFeeAmount =
                      orderSellingPrice - voucherFromSeller - escrowAmount;

                    await this.platformFeeRepository.upsert(
                      {
                        brand: fee.brand,
                        erpOrderCode: fee.erpOrderCode,
                        pancakeOrderId: fee.pancakeOrderId,
                        amount: platformFeeAmount,
                        formulaDescription: `(${orderSellingPrice} - ${voucherFromSeller}) - ${escrowAmount}`,
                        orderFeeCreatedAt: feeCreatedAt,
                        syncedAt: new Date(),
                      },
                      ['erpOrderCode', 'pancakeOrderId'],
                    );
                  }

                  synced++;
                }
              } catch (e) {
                // Ignore individual shopee validation errors to keep sync running
              }
            }
          }
        } catch (error) {
          this.logger.error(
            `Failed to sync batch for brand ${brandConfig.name}`,
            error,
          );
          failed += batch.length;
        }

        this.logger.log(
          `Processed batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(erpLogs.length / batchSize)} for ${brandConfig.name}`,
        );
      }

      return { synced, failed, total: erpLogs.length };
    } catch (error) {
      this.logger.error(`❌ Sync failed for brand ${brandConfig.name}`, error);
      throw error;
    }
  }

  /**
   * Helper: Check if orders are TikTok source
   */
  private async checkTikTokSource(
    brandConfig: any,
    pancakeOrderIds: string[],
  ): Promise<Set<string>> {
    if (!brandConfig.tableOrders) return new Set();

    try {
      const tiktokOrders = await this.secondaryDataSource.query(
        `
        SELECT "pancakeOrderId"
        FROM ${brandConfig.tableOrders}
        WHERE "pancakeOrderId" = ANY($1)
        AND "orderSourceName" = 'Tiktok'
        `,
        [pancakeOrderIds],
      );
      return new Set(tiktokOrders.map((o) => o.pancakeOrderId));
    } catch (error) {
      this.logger.error(
        `Failed to check TikTok source for ${brandConfig.name}`,
        error,
      );
      return new Set();
    }
  }

  private async getTikTokDetails(brandConfig: any, pancakeOrderIds: string[]) {
    if (!pancakeOrderIds.length) return [];

    try {
      const details = await this.thirdDataSource.query(
        `
        SELECT *
        FROM ${brandConfig.tableDetail}
        WHERE "order_sn" = ANY($1)
        `,
        [pancakeOrderIds],
      );
      return details;
    } catch (error) {
      this.logger.error(
        `Failed to get TikTok details for ${brandConfig.name}`,
        error,
      );
      return [];
    }
  }

  /**
   * Sync a single order fee by ERP code
   */
  async syncOrderFeeByCode(erpCode: string, brandName?: string) {
    try {
      let fees: any[] = [];

      if (brandName) {
        const brandConfig = this.brands.find(
          (b) => b.name.toLowerCase() === brandName.toLowerCase(),
        );
        if (!brandConfig) {
          throw new Error(`Brand ${brandName} not found configuration`);
        }
        fees = await this.getOrderFeesByBrandConfig(erpCode, brandConfig);
      } else {
        fees = await this.getOrderFees(erpCode);
      }

      if (fees.length === 0) {
        return { success: false, message: 'No fees found for this order' };
      }

      for (const fee of fees) {
        // 1. Save Raw Order Fee
        await this.orderFeeRepository.upsert(
          {
            feeId: fee.rawData?.id,
            brand: fee.brand,
            erpOrderCode: fee.erpOrderCode,
            platform: 'shopee', // Sàn TMĐT
            orderSn: fee.rawData?.order_sn, // [NEW] Map order_sn
            rawData: fee.rawData,
            syncedAt: new Date(),
          },
          ['feeId'],
        );

        // 2. Calculate and Save Platform Fee (if applicable)
        if (fee.rawData?.fee_type === 'order_income' && fee.rawData?.raw_data) {
          const raw = fee.rawData.raw_data;
          const orderSellingPrice = Number(raw.order_selling_price || 0);
          const voucherFromSeller = Number(raw.voucher_from_seller || 0);
          const escrowAmount = Number(raw.escrow_amount || 0);

          const platformFeeAmount =
            orderSellingPrice - voucherFromSeller - escrowAmount;

          // User requested "create_at" from detail_order_fee
          const feeCreatedAt =
            fee.rawData?.create_at || fee.rawData?.created_at || new Date();

          await this.platformFeeRepository.upsert(
            {
              brand: fee.brand,
              erpOrderCode: fee.erpOrderCode,
              pancakeOrderId: fee.pancakeOrderId,
              amount: platformFeeAmount,
              formulaDescription: `(${orderSellingPrice} - ${voucherFromSeller}) - ${escrowAmount}`,
              orderFeeCreatedAt: feeCreatedAt,
              syncedAt: new Date(),
            },
            ['erpOrderCode', 'pancakeOrderId'],
          );
        }
      }

      return {
        success: true,
        message: `Synced ${fees.length} fee records for order ${erpCode}`,
        data: fees,
      };
    } catch (error) {
      this.logger.error(`❌ Failed to manual sync order ${erpCode}`, error);
      throw error;
    }
  }
}
