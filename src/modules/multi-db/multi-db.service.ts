import { Injectable, Logger } from '@nestjs/common';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { DataSource, Repository } from 'typeorm';
import { OrderFee } from '../../entities/order-fee.entity';
import { PlatformFee } from '../../entities/platform-fee.entity';
import { ShopeeFee } from '../../entities/shopee-fee.entity';
import { TikTokFee } from '../../entities/tiktok-fee.entity';

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

    @InjectRepository(ShopeeFee)
    private shopeeFeeRepository: Repository<ShopeeFee>,

    @InjectRepository(TikTokFee)
    private tiktokFeeRepository: Repository<TikTokFee>,

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
          this.logger.log(
            `Checking TikTok source for ${batch.length} orders...`,
          );
          const tiktokIds = await this.checkTikTokSource(
            brandConfig,
            batch.map((l) => l.pancakeOrderId),
          );
          this.logger.log(
            `Found ${tiktokIds.size} TikTok orders and ${batch.length - tiktokIds.size} Shopee orders.`,
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
            this.logger.log(
              `Fetching TikTok details for ${tiktokLogs.length} orders...`,
            );
            const details = await this.getTikTokDetails(
              brandConfig,
              tiktokLogs.map((l) => l.pancakeOrderId),
            );
            this.logger.log(
              `Found details for ${details.length} TikTok orders.`,
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

                const tiktokFeeData = {
                  brand: brandConfig.name,
                  erpOrderCode: log.erpOrderCode,
                  orderSn: detail.order_sn,
                  orderStatus: detail.order_data?.order_status,
                  orderCreatedAt: orderDate,
                  syncedAt: new Date(),
                  // Detailed fields - TikTok API uses camelCase
                  tax: Number(detail.order_data?.payment?.tax || 0),
                  currency: detail.order_data?.payment?.currency || 'VND',
                  subTotal: Number(detail.order_data?.payment?.subTotal || 0),
                  shippingFee: Number(
                    detail.order_data?.payment?.shippingFee || 0,
                  ),
                  totalAmount: Number(
                    detail.order_data?.payment?.totalAmount || 0,
                  ),
                  sellerDiscount: Number(
                    detail.order_data?.payment?.sellerDiscount || 0,
                  ),
                  platformDiscount: Number(
                    detail.order_data?.payment?.platformDiscount || 0,
                  ),
                  originalTotalProductPrice: Number(
                    detail.order_data?.payment?.originalTotalProductPrice || 0,
                  ),
                  originalShippingFee: Number(
                    detail.order_data?.payment?.originalShippingFee || 0,
                  ),
                  shippingFeeSellerDiscount: Number(
                    detail.order_data?.payment?.shippingFeeSellerDiscount || 0,
                  ),
                  shippingFeeCofundedDiscount: Number(
                    detail.order_data?.payment?.shippingFeeCofundedDiscount ||
                      0,
                  ),
                  shippingFeePlatformDiscount: Number(
                    detail.order_data?.payment?.shippingFeePlatformDiscount ||
                      0,
                  ),
                };

                // 1. Save to OrderFee (Legacy)
                await this.orderFeeRepository.upsert(
                  {
                    feeId: `${log.erpOrderCode}_TIKTOK`,
                    brand: brandConfig.name,
                    erpOrderCode: log.erpOrderCode,
                    platform: 'tiktok',
                    orderSn: detail.order_sn,
                    rawData: detail.order_data,
                    orderCreatedAt: orderDate,
                    syncedAt: new Date(),
                  },
                  ['feeId'],
                );

                // 2. Save to TikTokFee (New structured table)
                try {
                  await this.tiktokFeeRepository.upsert(tiktokFeeData, [
                    'erpOrderCode',
                    'orderSn',
                  ]);
                } catch (err) {
                  this.logger.error(
                    `Failed to upsert TikTokFee for ${log.erpOrderCode}: ${err.message}`,
                  );
                }

                synced++;
              }
            }
          }

          // --- 2. Process Shopee Orders ---
          if (shopeeLogs.length > 0) {
            this.logger.log(`Processing ${shopeeLogs.length} Shopee orders...`);
            for (const erpLog of shopeeLogs) {
              try {
                this.logger.log(
                  `Fetching Shopee fees for ERP code ${erpLog.erpOrderCode}...`,
                );
                const fees = await this.getOrderFeesByBrandConfig(
                  erpLog.erpOrderCode,
                  brandConfig,
                );
                this.logger.log(
                  `Found ${fees.length} Shopee fees for ERP code ${erpLog.erpOrderCode}.`,
                );

                for (const fee of fees) {
                  const feeCreatedAt =
                    fee.rawData?.create_at ||
                    fee.rawData?.created_at ||
                    new Date();

                  // 1. Save Raw Order Fee (Legacy)
                  await this.orderFeeRepository.upsert(
                    {
                      feeId: fee.rawData?.id,
                      brand: fee.brand,
                      erpOrderCode: fee.erpOrderCode,
                      platform: 'shopee',
                      orderSn: fee.rawData?.order_sn,
                      rawData: fee.rawData,
                      orderCreatedAt: feeCreatedAt,
                      syncedAt: new Date(),
                    },
                    ['feeId'],
                  );

                  // 2. Save to ShopeeFee (New structured table)
                  const details = fee.rawData?.raw_data || {};
                  try {
                    await this.shopeeFeeRepository.upsert(
                      {
                        brand: fee.brand,
                        erpOrderCode: fee.erpOrderCode,
                        orderSn: fee.rawData?.order_sn,
                        platform: 'shopee',
                        voucherShop: Number(details.voucher_from_seller || 0),
                        commissionFee: Number(details.commission_fee || 0),
                        serviceFee: Number(details.service_fee || 0),
                        paymentFee: Number(
                          details.credit_card_transaction_fee || 0,
                        ),
                        orderCreatedAt: feeCreatedAt,
                        syncedAt: new Date(),
                      },
                      ['erpOrderCode', 'orderSn'],
                    );
                  } catch (err) {
                    this.logger.error(
                      `Failed to upsert ShopeeFee for ${fee.erpOrderCode}: ${err.message}`,
                    );
                  }

                  // 3. Calculate and Save Platform Fee (Keep existing logic if needed)
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
                // Ignore individual errors
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
      this.logger.error(`Sync failed for brand ${brandConfig.name}`, error);
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
        // Find brand config for this fee
        const brandConfig = this.brands.find(
          (b) => b.name.toLowerCase() === fee.brand.toLowerCase(),
        );

        if (brandConfig) {
          const tiktokIds = await this.checkTikTokSource(brandConfig, [
            fee.pancakeOrderId,
          ]);

          if (tiktokIds.has(fee.pancakeOrderId)) {
            // --- TikTok Order Manual Sync ---
            const details = await this.getTikTokDetails(brandConfig, [
              fee.pancakeOrderId,
            ]);
            const detail = details.find(
              (d) => d.order_sn === fee.pancakeOrderId,
            );

            if (detail && detail.order_data) {
              const createTime =
                detail.order_data.create_time ||
                detail.order_data.createTime ||
                0;
              const orderDate = createTime
                ? new Date(createTime * 1000)
                : new Date();

              const tiktokFeeData = {
                brand: brandConfig.name,
                erpOrderCode: fee.erpOrderCode,
                orderSn: detail.order_sn,
                orderStatus: detail.order_data?.order_status,
                orderCreatedAt: orderDate,
                syncedAt: new Date(),
                // Detailed fields - TikTok API uses camelCase
                tax: Number(detail.order_data?.payment?.tax || 0),
                currency: detail.order_data?.payment?.currency || 'VND',
                subTotal: Number(detail.order_data?.payment?.subTotal || 0),
                shippingFee: Number(
                  detail.order_data?.payment?.shippingFee || 0,
                ),
                totalAmount: Number(
                  detail.order_data?.payment?.totalAmount || 0,
                ),
                sellerDiscount: Number(
                  detail.order_data?.payment?.sellerDiscount || 0,
                ),
                platformDiscount: Number(
                  detail.order_data?.payment?.platformDiscount || 0,
                ),
                originalTotalProductPrice: Number(
                  detail.order_data?.payment?.originalTotalProductPrice || 0,
                ),
                originalShippingFee: Number(
                  detail.order_data?.payment?.originalShippingFee || 0,
                ),
                shippingFeeSellerDiscount: Number(
                  detail.order_data?.payment?.shippingFeeSellerDiscount || 0,
                ),
                shippingFeeCofundedDiscount: Number(
                  detail.order_data?.payment?.shippingFeeCofundedDiscount || 0,
                ),
                shippingFeePlatformDiscount: Number(
                  detail.order_data?.payment?.shippingFeePlatformDiscount || 0,
                ),
              };

              // 1. Save to OrderFee (Legacy)
              await this.orderFeeRepository.upsert(
                {
                  feeId: `${fee.erpOrderCode}_TIKTOK`,
                  brand: brandConfig.name,
                  erpOrderCode: fee.erpOrderCode,
                  platform: 'tiktok',
                  orderSn: detail.order_sn,
                  rawData: detail.order_data,
                  orderCreatedAt: orderDate,
                  syncedAt: new Date(),
                },
                ['feeId'],
              );

              // 2. Save to TikTokFee (New structured table)
              try {
                await this.tiktokFeeRepository.upsert(tiktokFeeData, [
                  'erpOrderCode',
                  'orderSn',
                ]);
              } catch (err) {
                this.logger.error(
                  `Failed to manual upsert TikTokFee for ${fee.erpOrderCode}: ${err.message}`,
                );
              }
            }
          } else {
            // --- Shopee Order Manual Sync ---
            // 1. Save Raw Order Fee
            await this.orderFeeRepository.upsert(
              {
                feeId: fee.rawData?.id,
                brand: fee.brand,
                erpOrderCode: fee.erpOrderCode,
                platform: 'shopee',
                orderSn: fee.rawData?.order_sn,
                rawData: fee.rawData,
                syncedAt: new Date(),
              },
              ['feeId'],
            );

            // 2. Save to ShopeeFee (New structured table)
            const details = fee.rawData?.raw_data || {};
            const feeCreatedAt =
              fee.rawData?.create_at || fee.rawData?.created_at || new Date();

            try {
              await this.shopeeFeeRepository.upsert(
                {
                  brand: fee.brand,
                  erpOrderCode: fee.erpOrderCode,
                  orderSn: fee.rawData?.order_sn,
                  platform: 'shopee',
                  voucherShop: Number(details.voucher_from_seller || 0),
                  commissionFee: Number(details.commission_fee || 0),
                  serviceFee: Number(details.service_fee || 0),
                  paymentFee: Number(details.credit_card_transaction_fee || 0),
                  orderCreatedAt: feeCreatedAt,
                  syncedAt: new Date(),
                },
                ['erpOrderCode', 'orderSn'],
              );
            } catch (err) {
              this.logger.error(
                `Failed to manual upsert ShopeeFee for ${fee.erpOrderCode}: ${err.message}`,
              );
            }

            // 3. Calculate and Save Platform Fee (if applicable)
            if (
              fee.rawData?.fee_type === 'order_income' &&
              fee.rawData?.raw_data
            ) {
              const raw = fee.rawData.raw_data;
              const orderSellingPrice = Number(raw.order_selling_price || 0);
              const voucherFromSeller = Number(raw.voucher_from_seller || 0);
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
          }
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
