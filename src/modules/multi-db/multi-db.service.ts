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
      this.logger.log('✅ Primary database connected');
    } catch (error) {
      this.logger.error('❌ Primary database connection failed', error);
    }

    try {
      await this.secondaryDataSource.query('SELECT 1');
      results.secondary = true;
      this.logger.log('✅ Secondary database connected');
    } catch (error) {
      this.logger.error('❌ Secondary database connection failed', error);
    }

    try {
      await this.thirdDataSource.query('SELECT 1');
      results.third = true;
      this.logger.log('✅ Third database connected');
    } catch (error) {
      this.logger.error('❌ Third database connection failed', error);
    }

    return results;
  }

  async getOrderFees(erpCode: string) {
    const erpLogs = await this.secondaryDataSource.query(
      `
    SELECT *
    FROM "menard_erp_order_logs"
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
    FROM public.menard_ecommer_detail_order o
    JOIN public.menard_ecommer_detail_order_fee f
      ON f.menard_ecommer_detail_order_id = o.id
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
        });
      }
    }

    return result;
  }

  /**
   * Sync all order fees from external databases to primary database
   * This method is called by cronjob at 1 AM daily
   */
  async syncAllOrderFees() {
    this.logger.log('Starting order fee sync...');

    let synced = 0;
    let failed = 0;

    try {
      // Get all ERP order codes from secondary database
      const erpLogs = await this.secondaryDataSource.query(
        'SELECT "erpOrderCode", "pancakeOrderId" FROM "menard_erp_order_logs"',
      );

      this.logger.log(`Found ${erpLogs.length} ERP orders to sync`);

      // Process in batches to avoid memory issues
      const batchSize = 100;
      for (let i = 0; i < erpLogs.length; i += batchSize) {
        const batch = erpLogs.slice(i, i + batchSize);

        for (const erpLog of batch) {
          try {
            const fees = await this.getOrderFees(erpLog.erpOrderCode);

            for (const fee of fees) {
              // 1. Save Raw Order Fee
              await this.orderFeeRepository.upsert(
                {
                  feeId: fee.rawData?.id,
                  erpOrderCode: fee.erpOrderCode,
                  pancakeOrderId: fee.pancakeOrderId,
                  feeType: fee.rawData?.fee_type || null,
                  feeAmount: fee.rawData?.fee_amount || null,
                  rawData: fee.rawData,
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
                const orderSellingPrice = Number(raw.order_selling_price || 0);
                const voucherFromSeller = Number(raw.voucher_from_seller || 0);
                const escrowAmount = Number(raw.escrow_amount || 0);

                const platformFeeAmount =
                  orderSellingPrice - voucherFromSeller - escrowAmount;

                await this.platformFeeRepository.upsert(
                  {
                    erpOrderCode: fee.erpOrderCode,
                    pancakeOrderId: fee.pancakeOrderId,
                    amount: platformFeeAmount,
                    formulaDescription: `(${orderSellingPrice} - ${voucherFromSeller}) - ${escrowAmount}`,
                    syncedAt: new Date(),
                  },
                  ['erpOrderCode', 'pancakeOrderId'],
                );
              }

              synced++;
            }
          } catch (error) {
            this.logger.error(
              `Failed to sync order ${erpLog.erpOrderCode}`,
              error,
            );
            failed++;
          }
        }

        this.logger.log(
          `Processed batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(erpLogs.length / batchSize)}`,
        );
      }

      this.logger.log(`✅ Sync completed: ${synced} synced, ${failed} failed`);

      return { synced, failed, total: erpLogs.length };
    } catch (error) {
      this.logger.error('❌ Sync failed', error);
      throw error;
    }
  }

  /**
   * Sync a single order fee by ERP code
   */
  async syncOrderFeeByCode(erpCode: string) {
    try {
      const fees = await this.getOrderFees(erpCode);

      if (fees.length === 0) {
        return { success: false, message: 'No fees found for this order' };
      }

      for (const fee of fees) {
        // 1. Save Raw Order Fee
        await this.orderFeeRepository.upsert(
          {
            feeId: fee.rawData?.id,
            erpOrderCode: fee.erpOrderCode,
            pancakeOrderId: fee.pancakeOrderId,
            feeType: fee.rawData?.fee_type || null,
            feeAmount: fee.rawData?.fee_amount || null,
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

          await this.platformFeeRepository.upsert(
            {
              erpOrderCode: fee.erpOrderCode,
              pancakeOrderId: fee.pancakeOrderId,
              amount: platformFeeAmount,
              formulaDescription: `(${orderSellingPrice} - ${voucherFromSeller}) - ${escrowAmount}`,
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
