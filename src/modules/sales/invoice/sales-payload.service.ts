import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import { ProductItem } from '../../../entities/product-item.entity';
import { StockTransfer } from '../../../entities/stock-transfer.entity';
import { LoyaltyService } from '../../../services/loyalty.service';
import { N8nService } from '../../../services/n8n.service';
import { CategoriesService } from '../../categories/categories.service';
import * as _ from 'lodash';
import * as SalesUtils from '../../../utils/sales.utils';
import * as StockTransferUtils from '../../../utils/stock-transfer.utils';
import * as ConvertUtils from '../../../utils/convert.utils';
import * as SalesCalculationUtils from '../../../utils/sales-calculation.utils';
import { InvoiceLogicUtils } from '../../../utils/invoice-logic.utils';
import * as SalesFormattingUtils from '../../../utils/sales-formatting.utils';
import { DOC_SOURCE_TYPES } from '../constants/sales-invoice.constants';

@Injectable()
export class SalesPayloadService {
  private readonly logger = new Logger(SalesPayloadService.name);

  constructor(
    @InjectRepository(ProductItem)
    private productItemRepository: Repository<ProductItem>,
    @InjectRepository(StockTransfer)
    private stockTransferRepository: Repository<StockTransfer>,
    private loyaltyService: LoyaltyService,
    private n8nService: N8nService,
    private categoriesService: CategoriesService,
  ) {}

  /**
   * Build invoice data cho Fast API (format mới)
   */
  async buildFastApiInvoiceData(orderData: any): Promise<any> {
    try {
      // 1. Initialize and validate date
      const docDate = this.parseInvoiceDate(orderData.docDate);
      const ngayCt = this.formatDateKeepLocalDay(docDate);
      const ngayLct = ngayCt;

      const allSales = orderData.sales || [];
      if (allSales.length === 0) {
        throw new Error(
          `Đơn hàng ${orderData.docCode} không có sale item nào, bỏ qua không đồng bộ`,
        );
      }

      // 2. Determine order type (from first sale)
      const { isThuong: isNormalOrder } = InvoiceLogicUtils.getOrderTypes(
        allSales[0]?.ordertypeName || allSales[0]?.ordertype || '',
      );

      // 3. Load supporting data
      const { stockTransferMap, transDate } =
        await this.getInvoiceStockTransferMap(orderData.docCode, isNormalOrder);
      const cardSerialMap = await this.getInvoiceCardSerialMap(
        orderData.docCode,
      );

      // [New] Batch lookup svc_code -> materialCode for FAST API
      const svcCodes = Array.from(
        new Set(
          allSales
            .map((sale: any) => sale.svc_code)
            .filter(
              (code: any): code is string => !!code && code.trim() !== '',
            ),
        ),
      ) as string[];
      const svcCodeMap = new Map<string, string>();
      if (svcCodes.length > 0) {
        // OPTIMIZED: Add concurrency limit
        const MAX_CONCURRENT = 5;
        const chunks: string[][] = [];
        for (let i = 0; i < svcCodes.length; i += MAX_CONCURRENT) {
          chunks.push(svcCodes.slice(i, i + MAX_CONCURRENT));
        }

        for (const chunk of chunks) {
          await Promise.all(
            chunk.map(async (code) => {
              try {
                const materialCode =
                  await this.loyaltyService.getMaterialCodeBySvcCode(code);
                if (materialCode) {
                  svcCodeMap.set(code, materialCode);
                }
              } catch (error) {
                // Ignore
              }
            }),
          );
        }
      }

      // 4. Transform sales to details (Filter out TRUTONKEEP items)
      const detail = await Promise.all(
        allSales
          .filter((sale: any) => {
            // Bỏ qua TRUTONKEEP items
            if (SalesUtils.isTrutonkeepItem(sale.itemCode)) {
              this.logger.log(
                `[Payload] Bỏ qua sale item với itemCode = TRUTONKEEP (docCode: ${orderData.docCode})`,
              );
              return false;
            }
            return true;
          })
          .map((sale: any, index: number) => {
            this.logger.log(
              `[DEBUG] Mapping sale ${index + 1}: ${String(sale.itemCode)}`,
            );
            return this.mapSaleToInvoiceDetail(sale, index, orderData, {
              isNormalOrder,
              stockTransferMap,
              cardSerialMap,
              svcCodeMap,
            });
          }),
      );

      // DEBUG: Log detail count
      this.logger.log(
        `[Payload] Order ${orderData.docCode}: ${detail.length} detail items (NO AGGREGATION)`,
      );

      // 5. Build summary (cbdetail) - NO AGGREGATION
      const cbdetail = this.buildInvoiceCbDetail(detail);

      // 6. Assemble final payload - Using EXPLODED detail (no aggregation)
      return this.assembleInvoicePayload(
        orderData,
        detail, // Use original detail without aggregation
        cbdetail,
        {
          ngayCt,
          ngayLct,
          transDate,
          maBp: detail[0]?.ma_bp || '',
        },
      );
    } catch (error: any) {
      this.logInvoiceError(error, orderData);
      throw new Error(
        `Failed to build invoice data: ${error?.message || error}`,
      );
    }
  }

  /**
   * Build invoice data chỉ cho service lines (productType = 'S')
   */
  async buildFastApiInvoiceDataForServiceLines(
    orderData: any,
    serviceLines: any[],
  ): Promise<any> {
    const docDate = this.parseInvoiceDate(orderData.docDate);
    const ngayCt = this.formatDateKeepLocalDay(docDate);
    const ngayLct = ngayCt;

    // Determine order type
    const { isThuong: isNormalOrder } = InvoiceLogicUtils.getOrderTypes(
      serviceLines[0]?.ordertypeName || serviceLines[0]?.ordertype || '',
    );

    // Map details
    const detail = await Promise.all(
      serviceLines.map((sale, index) =>
        this.mapSaleToInvoiceDetail(sale, index, orderData, {
          isNormalOrder,
          stockTransferMap: new Map(), // Service orders don't use stock transfers usually
          cardSerialMap: new Map(),
          svcCodeMap: new Map(), // Service lines typically use 'service items', but if they have svc_code we could support it. For now empty map.
        }),
      ),
    );

    const cbdetail = this.buildInvoiceCbDetail(detail);

    return this.assembleInvoicePayload(orderData, detail, cbdetail, {
      ngayCt,
      ngayLct,
      transDate: null,
      maBp: detail[0]?.ma_bp || '',
    });
  }

  /**
   * Build GxtInvoice data (Phiếu tạo gộp – xuất tách)
   * - detail: các dòng productType = 'I' (xuất)
   * - ndetail: các dòng productType = 'S' (nhập)
   */
  async buildGxtInvoiceData(
    orderData: any,
    importLines: any[],
    exportLines: any[],
  ): Promise<any> {
    const docDate = this.parseInvoiceDate(orderData.docDate);
    const ngayCt = this.formatDateKeepLocalDay(docDate);
    const ngayLct = ngayCt;

    // Lấy ma_dvcs
    const maDvcs =
      importLines[0]?.department?.ma_dvcs ||
      importLines[0]?.department?.ma_dvcs_ht ||
      exportLines[0]?.department?.ma_dvcs ||
      exportLines[0]?.department?.ma_dvcs_ht ||
      orderData.customer?.brand ||
      orderData.branchCode ||
      '';

    const firstSale = importLines[0] || exportLines[0];

    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      return d.toISOString();
    };

    const limitString = (val: string, max: number) => {
      if (!val) return '';
      return val.toString().slice(0, max);
    };

    const toString = (val: any, def: string) => (val ? String(val) : def);

    // Determine order type for StockTransfer lookup
    const { isThuong: isNormalOrder, isTachThe } =
      InvoiceLogicUtils.getOrderTypes(
        firstSale?.ordertypeName || firstSale?.ordertype || '',
      );

    // Load Stock Transfer Map (Sync logic with SalesInvoice)
    const { stockTransferMap } = await this.getInvoiceStockTransferMap(
      orderData.docCode,
      isNormalOrder,
    );

    // Helper để build detail/ndetail item
    const buildLineItem = async (
      sale: any,
      index: number,
      linkedDong?: number,
    ) => {
      const { qty } = this.calculateInvoiceQty(
        sale,
        orderData.docCode,
        sale.product?.materialCode,
        false, // Not normal order flow for GXT logic here (or simplified)
        new Map(),
      );
      // Simplify logic for GXT lines if needed, or reuse parts of calculateInvoicePrices/Amounts if complexity grows.
      // For now, mapping basic fields as per original requirement:
      const tienHang = this.toNumber(
        sale.tienHang || sale.linetotal || sale.revenue,
        0,
      );
      const giaBan = this.toNumber(sale.giaBan || sale.price, 0);

      const giaNt2 = giaBan > 0 ? giaBan : qty > 0 ? tienHang / qty : 0;
      const tienNt2 = qty * giaNt2;

      // Lấy materialCode từ Loyalty API
      const materialCode =
        SalesUtils.getMaterialCode(sale, sale.product) || sale.itemCode || '';
      const dvt = toString(
        sale.product?.dvt || sale.product?.unit || sale.dvt,
        'Cái',
      );
      const maLo = toString(sale.maLo || sale.ma_lo, '');
      const maBp = toString(
        sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
        '',
      );

      // Resolve ma_kho using standard Invoice Logic (StockTransfer support)
      const lineMaKho = await this.resolveInvoiceMaKho(
        sale,
        materialCode,
        stockTransferMap,
        orderData.docCode,
        maBp,
        isTachThe,
      );

      // Rule đánh số dòng (Dong):
      // - dong: Tự tăng (index + 1) cho cả 2 loại
      // - dong_vt_goc:
      //    + Import Line: 1
      //    + Export Line: Dong của Import Line tương ứng (linkedDong)
      const dong = index + 1;
      const dongVtGoc = linkedDong ? linkedDong : dong;

      return {
        ma_kho_n: lineMaKho,
        ma_kho_x: lineMaKho,
        ma_vt: limitString(materialCode, 16),
        dvt: limitString(dvt, 32),
        ma_lo: limitString(maLo, 16),
        so_luong: Math.abs(qty), // Lấy giá trị tuyệt đối
        gia_nt2: Number(giaNt2),
        tien_nt2: Number(tienNt2),
        ma_nx: 'NX01', // Fix cứng theo yêu cầu
        ma_bp: limitString(maBp, 8),
        dong: dong,
        dong_vt_goc: dongVtGoc, // Link với dòng Import
        _materialCode: materialCode, // Internal use for mapping
        _itemCode: sale.itemCode, // Internal use for mapping
      };
    };

    // 1. Build ndetail (nhập - productType = 'S') FIRST
    const ndetail = await Promise.all(
      importLines.map((sale, index) => buildLineItem(sale, index)),
    );

    // 2. Map Import Lines for Linking: Map both itemCode and materialCode -> Dong
    const svcCodeToDongMap = new Map<string, number>();
    ndetail.forEach((item) => {
      // Map materialCode -> dong
      if (item._materialCode) {
        svcCodeToDongMap.set(item._materialCode, item.dong);
      }
      // Map itemCode -> dong (often the svc code)
      if (item._itemCode) {
        svcCodeToDongMap.set(item._itemCode, item.dong);
      }
    });

    // 3. Build detail (xuất - productType = 'I') matching svc_code
    const detail = await Promise.all(
      exportLines.map(async (sale, index) => {
        // Find corresponding Import line using svc_code
        // sale.svc_code (on Export Item) works as key to find Import Line
        // We match against either materialCode OR itemCode of the Import Line
        let linkedDong = 1;
        if (sale.svc_code && svcCodeToDongMap.has(sale.svc_code)) {
          linkedDong = svcCodeToDongMap.get(sale.svc_code)!;
        } else if (ndetail.length > 0) {
          // Fallback: Default to first line if no match found
          linkedDong = ndetail[0].dong;
        }

        const item = await buildLineItem(sale, index, linkedDong);

        // Remove internal properties before returning
        delete (item as any)._materialCode;
        delete (item as any)._itemCode;
        return item;
      }),
    );

    // Clean up ndetail internal properties
    ndetail.forEach((item) => {
      delete (item as any)._materialCode;
      delete (item as any)._itemCode;
    });

    // Helper resolve cho sale đầu tiên nếu list detail rỗng
    const getFirstMaKho = async (sale: any) => {
      if (!sale) return '';
      const mCode =
        SalesUtils.getMaterialCode(sale, sale.product) || sale.itemCode || '';
      const mBp = toString(
        sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
        '',
      );
      return await this.resolveInvoiceMaKho(
        sale,
        mCode,
        stockTransferMap,
        orderData.docCode,
        mBp,
        isTachThe,
      );
    };

    const maKhoN =
      ndetail.length > 0 ? ndetail[0].ma_kho_n : await getFirstMaKho(firstSale);
    const maKhoX =
      detail.length > 0 ? detail[0].ma_kho_x : await getFirstMaKho(firstSale);

    return {
      ma_dvcs: maDvcs,
      ma_kho_n: maKhoN,
      ma_kho_x: maKhoX,
      ong_ba: orderData.customer?.name || '',
      ma_gd: '1', // 1 = Tạo gộp, 2 = Xuất tách (có thể thay đổi theo rule)
      ngay_ct: ngayCt,
      ngay_lct: ngayLct,
      so_ct: orderData.docCode || '',
      dien_giai: orderData.docCode || '',
      action: 0, // 0: Mới, Sửa; 1: Xóa
      detail: detail,
      ndetail: ndetail,
    };
  }

  /**
   * Build salesReturn data cho Fast API (Hàng bán trả lại)
   * Tương tự như buildFastApiInvoiceData nhưng có thêm các field đặc biệt cho salesReturn
   */
  async buildSalesReturnData(
    orderData: any,
    stockTransfers: StockTransfer[],
  ): Promise<any> {
    try {
      // Sử dụng lại logic từ buildFastApiInvoiceData để build detail
      const invoiceData = await this.buildFastApiInvoiceData(orderData);

      // Format ngày theo ISO 8601
      const formatDateISO = (date: Date | string): string => {
        const d = typeof date === 'string' ? new Date(date) : date;
        if (isNaN(d.getTime())) {
          throw new Error('Invalid date');
        }
        return d.toISOString();
      };

      // Lấy ngày hóa đơn gốc (ngay_ct0) - có thể lấy từ sale đầu tiên hoặc orderData
      const firstSale = orderData.sales?.[0] || {};
      let ngayCt0: string | null = null;
      let soCt0: string | null = null;

      // Tìm hóa đơn gốc từ stock transfer hoặc sale
      // Nếu có stock transfer, có thể lấy từ soCode hoặc docCode
      if (stockTransfers && stockTransfers.length > 0) {
        const firstStockTransfer = stockTransfers.find(
          (stockTransfer) =>
            stockTransfer.doctype === DOC_SOURCE_TYPES.SALE_RETURN,
        );
        // soCode thường là mã đơn hàng gốc
        soCt0 = firstStockTransfer?.soCode || orderData.docCode || null;
        // Ngày có thể lấy từ stock transfer hoặc orderData
        if (firstStockTransfer?.transDate) {
          ngayCt0 = formatDateISO(firstStockTransfer?.transDate);
        } else if (orderData?.docDate) {
          ngayCt0 = formatDateISO(orderData?.docDate);
        }
      } else {
        // Nếu không có stock transfer, lấy từ orderData
        soCt0 = orderData.docCode || null;
        if (orderData?.docDate) {
          ngayCt0 = formatDateISO(orderData.docDate);
        }
      }

      // Format ngày hiện tại
      let docDate: Date;
      if (orderData.docDate instanceof Date) {
        docDate = orderData.docDate;
      } else if (typeof orderData.docDate === 'string') {
        docDate = new Date(orderData.docDate);
        if (isNaN(docDate.getTime())) {
          docDate = new Date();
        }
      } else {
        docDate = new Date();
      }

      const ngayCt = formatDateISO(docDate);
      const ngayLct = formatDateISO(docDate);

      // Lấy ma_dvcs
      const maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';

      // Lấy so_seri
      const soSeri =
        firstSale?.kyHieu ||
        firstSale?.branchCode ||
        orderData.branchCode ||
        'DEFAULT';

      // Gom số lượng trả lại theo mã vật tư
      const stockQtyMap = new Map<string, number>();

      (stockTransfers || [])
        .filter((st) => st.doctype === DOC_SOURCE_TYPES.SALE_RETURN)
        .forEach((st) => {
          const maVt = st.materialCode;
          const qty = Number(st.qty || 0);

          if (!maVt || qty === 0) return;

          stockQtyMap.set(maVt, (stockQtyMap.get(maVt) || 0) + qty);
        });

      // Build detail từ invoiceData.detail, chỉ giữ các field cần thiết cho salesReturn
      const detail = (invoiceData.detail || [])
        .map((item: any, index: number) => {
          const soLuongFromStock = stockQtyMap.get(item.ma_vt) || 0;

          const detailItem: any = {
            // Field bắt buộc
            ma_vt: item.ma_vt,
            dvt: item.dvt,
            ma_kho: item.ma_kho,

            so_luong: soLuongFromStock,

            gia_ban: item.gia_ban,
            tien_hang: item.gia_ban * soLuongFromStock,

            // Field tài khoản
            tk_dt: item.tk_dt || '511',
            tk_gv: item.tk_gv || '632',

            // Field khuyến mãi
            is_reward_line: item.is_reward_line || 0,
            is_bundle_reward_line: item.is_bundle_reward_line || 0,
            km_yn: item.km_yn || 0,

            // CK
            ck01_nt: item.ck01_nt || 0,
            ck02_nt: item.ck02_nt || 0,
            ck03_nt: item.ck03_nt || 0,
            ck04_nt: item.ck04_nt || 0,
            ck05_nt: item.ck05_nt || 0,
            ck06_nt: item.ck06_nt || 0,
            ck07_nt: item.ck07_nt || 0,
            ck08_nt: item.ck08_nt || 0,
            ck09_nt: item.ck09_nt || 0,
            ck10_nt: item.ck10_nt || 0,
            ck11_nt: item.ck11_nt || 0,
            ck12_nt: item.ck12_nt || 0,
            ck13_nt: item.ck13_nt || 0,
            ck14_nt: item.ck14_nt || 0,
            ck15_nt: item.ck15_nt || 0,
            ck16_nt: item.ck16_nt || 0,
            ck17_nt: item.ck17_nt || 0,
            ck18_nt: item.ck18_nt || 0,
            ck19_nt: item.ck19_nt || 0,
            ck20_nt: item.ck20_nt || 0,
            ck21_nt: item.ck21_nt || 0,
            ck22_nt: item.ck22_nt || 0,

            // Thuế
            dt_tg_nt: item.dt_tg_nt || 0,
            ma_thue: item.ma_thue || '',
            tien_thue: item.tien_thue || 0,

            ma_bp: item.ma_bp,
            loai_gd: item.loai_gd || '01',
            dong: index + 1,
            id_goc_so: item.id_goc_so || 0,
            id_goc_ngay: item.id_goc_ngay || formatDateISO(new Date()),
            ma_vt_ref: item.ma_vt_ref || '',
          };

          return detailItem;
        })
        .filter(Boolean); // ❗ bỏ các dòng không có stock transfer

      // Build payload, chỉ thêm các field không null
      const salesReturnPayload: any = {
        ma_dvcs: maDvcs,
        ma_kh: invoiceData.ma_kh,
        ong_ba: invoiceData.ong_ba,
        ma_gd: '1', // Mã giao dịch (mặc định 1 - Hàng bán trả lại)
        tk_co: '131', // Tài khoản có (mặc định 131)
        ngay_lct: ngayLct,
        ngay_ct: ngayCt,
        so_ct: orderData.docCode || '',
        so_seri: soSeri,
        ma_nt: 'VND',
        ty_gia: 1.0,
        ma_kenh: 'ONLINE', // Mã kênh (mặc định ONLINE)
        detail: detail,
      };

      // Chỉ thêm các field optional nếu có giá trị
      if (firstSale?.maCa) {
        salesReturnPayload.ma_ca = firstSale.maCa;
      }
      if (soCt0) {
        salesReturnPayload.so_ct0 = soCt0;
      }
      if (ngayCt0) {
        salesReturnPayload.ngay_ct0 = ngayCt0;
      }
      if (orderData.docCode) {
        salesReturnPayload.dien_giai = orderData.docCode;
      }

      return salesReturnPayload;
    } catch (error: any) {
      this.logger.error(
        `Error building sales return data: ${error?.message || error}`,
      );
      throw new Error(
        `Failed to build sales return data: ${error?.message || error}`,
      );
    }
  }

  /**
   * Build FastAPI stock transfer data từ STOCK_TRANSFER items
   */
  async buildStockTransferData(items: any[], orderData: any): Promise<any> {
    const firstItem = items[0];

    // Lấy ma_dvcs từ order hoặc branch_code
    let maDvcs = '';
    if (orderData) {
      const firstSale = orderData.sales?.[0];
      maDvcs =
        firstSale?.department?.ma_dvcs ||
        firstSale?.department?.ma_dvcs_ht ||
        orderData.customer?.brand ||
        orderData.branchCode ||
        '';
    }
    if (!maDvcs) {
      maDvcs = firstItem.branch_code || '';
    }

    // Lấy ma_kh từ order và normalize (bỏ prefix "NV" nếu có)
    const maKh = SalesUtils.normalizeMaKh(orderData?.customer?.code);

    // Map iotype sang ma_nx (mã nhập xuất)
    // iotype: 'O' = xuất, 'I' = nhập
    // ma_nx: có thể là '1111' cho xuất, '1112' cho nhập (cần xác nhận với FastAPI)
    const getMaNx = (iotype: string): string => {
      if (iotype === 'O') {
        return '1111'; // Xuất nội bộ
      } else if (iotype === 'I') {
        return '1112'; // Nhập nội bộ
      }
      return '1111'; // Default
    };

    // Batch fetch all products BEFORE the loop
    const itemCodes = items.map((item) => item.item_code).filter(Boolean);

    // Fetch from database
    const dbProducts =
      itemCodes.length > 0
        ? await this.productItemRepository.find({
            where: { maERP: In(itemCodes) },
          })
        : [];
    const dbProductMap = new Map(dbProducts.map((p) => [p.maERP, p]));

    // Fetch from Loyalty API (batch)
    const loyaltyProductMap =
      itemCodes.length > 0
        ? await this.loyaltyService.fetchProducts(itemCodes)
        : new Map();

    // Build detail items (now synchronous - no await in loop)
    const detail = items.map((item, index) => {
      // Get product info from pre-fetched maps
      let dvt = 'Cái'; // Default
      let trackSerial: boolean | null = null;
      let trackBatch: boolean | null = null;
      let productTypeFromLoyalty: string | null = null;

      // Check database first
      const dbProduct = dbProductMap.get(item.item_code);
      if (dbProduct?.dvt) {
        dvt = dbProduct.dvt;
      }

      // Then check Loyalty API
      const loyaltyProduct = loyaltyProductMap.get(item.item_code);
      if (loyaltyProduct) {
        if (loyaltyProduct?.unit) {
          dvt = loyaltyProduct.unit;
        }
        trackSerial = loyaltyProduct.trackSerial === true;
        trackBatch = loyaltyProduct.trackBatch === true;
        productTypeFromLoyalty =
          loyaltyProduct?.productType || loyaltyProduct?.producttype || null;
      }

      const productTypeUpper = productTypeFromLoyalty
        ? String(productTypeFromLoyalty).toUpperCase().trim()
        : null;

      // Xác định có dùng ma_lo hay so_serial dựa trên trackSerial và trackBatch từ Loyalty API
      const useBatch = SalesUtils.shouldUseBatch(trackBatch, trackSerial);

      let maLo: string | null = null;
      let soSerial: string | null = null;

      if (useBatch) {
        // trackBatch = true → dùng ma_lo với giá trị batchserial
        const batchSerial = item.batchserial || null;
        if (batchSerial) {
          // Vẫn cần productType để quyết định cắt bao nhiêu ký tự
          if (productTypeUpper === 'TPCN') {
            // Nếu productType là "TPCN", cắt lấy 8 ký tự cuối
            maLo =
              batchSerial.length >= 8 ? batchSerial.slice(-8) : batchSerial;
          } else if (
            productTypeUpper === 'SKIN' ||
            productTypeUpper === 'GIFT'
          ) {
            // Nếu productType là "SKIN" hoặc "GIFT", cắt lấy 4 ký tự cuối
            maLo =
              batchSerial.length >= 4 ? batchSerial.slice(-4) : batchSerial;
          } else {
            // Các trường hợp khác → giữ nguyên toàn bộ
            maLo = batchSerial;
          }
        } else {
          maLo = null;
        }
        soSerial = null;
      } else {
        // trackSerial = true và trackBatch = false → dùng so_serial, không set ma_lo
        maLo = null;
        soSerial = item.batchserial || null;
      }

      return {
        ma_vt: item.item_code,
        dvt: dvt,
        so_serial: soSerial,
        ma_kho: item.stock_code,
        so_luong: Math.abs(item.qty), // Lấy giá trị tuyệt đối
        gia_nt: 0, // Stock transfer thường không có giá
        tien_nt: 0, // Stock transfer thường không có tiền
        ma_lo: maLo,
        px_gia_dd: 0, // Mặc định 0
        ma_nx: getMaNx(item.iotype),
        ma_vv: null,
        ma_bp:
          orderData?.sales?.[0]?.department?.ma_bp || item.branch_code || null,
        so_lsx: null,
        ma_sp: null,
        ma_hd: null,
        ma_phi: null,
        ma_ku: null,
        ma_phi_hh: null,
        ma_phi_ttlk: null,
        tien_hh_nt: 0,
        tien_ttlk_nt: 0,
      };
    });

    // Format date
    const formatDateISO = (date: Date | string): string => {
      const d = typeof date === 'string' ? new Date(date) : date;
      return d.toISOString();
    };

    const transDate = new Date(firstItem.transdate);
    const ngayCt = formatDateISO(transDate);
    // const ngayLct = formatDateISO(transDate);

    // Lấy ma_nx từ item đầu tiên (tất cả items trong cùng 1 phiếu nên có cùng iotype)
    const maNx = getMaNx(firstItem.iotype);

    return {
      action: 0, // Thêm action field giống như salesInvoice
      ma_dvcs: maDvcs,
      ma_kh: maKh,
      ong_ba: orderData?.customer?.name || null,
      ma_gd: '1', // Mã giao dịch: 1
      ma_nx: maNx, // Thêm ma_nx vào header
      ngay_ct: ngayCt,
      so_ct: firstItem.doccode,
      ma_nt: 'VND',
      ty_gia: 1.0,
      dien_giai: firstItem.doc_desc || null,
      detail: detail,
    };
  }

  async cutCode(input: string): Promise<string> {
    return input?.split('-')[0] || '';
  }

  // ==================== HELPERS ====================

  private toNumber(value: any, defaultValue: number = 0): number {
    if (value === null || value === undefined || value === '')
      return defaultValue;
    const num = Number(value);
    return isNaN(num) ? defaultValue : num;
  }

  private toString(value: any, defaultValue: string = ''): string {
    return value === null || value === undefined || value === ''
      ? defaultValue
      : String(value);
  }

  private limitString(value: string, maxLength: number): string {
    if (!value) return '';
    const str = String(value);
    return str.length > maxLength ? str.substring(0, maxLength) : str;
  }

  /**
   * Helper terse wrapper for limitString(toString(value, def), max)
   */
  private val(
    value: any,
    maxLength: number,
    defaultValue: string = '',
  ): string {
    return this.limitString(this.toString(value, defaultValue), maxLength);
  }

  private formatDateISO(date: Date): string {
    if (isNaN(date.getTime())) throw new Error('Invalid date');
    return date.toISOString();
  }

  private formatDateKeepLocalDay(date: Date): string {
    if (isNaN(date.getTime())) throw new Error('Invalid date');

    const pad = (n: number) => String(n).padStart(2, '0');

    return (
      date.getFullYear() +
      '-' +
      pad(date.getMonth() + 1) +
      '-' +
      pad(date.getDate()) +
      'T' +
      pad(date.getHours()) +
      ':' +
      pad(date.getMinutes()) +
      ':' +
      pad(date.getSeconds()) +
      '.000Z'
    );
  }

  private parseInvoiceDate(inputDate: any): Date {
    let docDate: Date;
    if (inputDate instanceof Date) {
      docDate = inputDate;
    } else if (typeof inputDate === 'string') {
      docDate = new Date(inputDate);
      if (isNaN(docDate.getTime())) docDate = new Date();
    } else {
      docDate = new Date();
    }

    const minDate = new Date('1753-01-01T00:00:00');
    const maxDate = new Date('9999-12-31T23:59:59');
    if (docDate < minDate || docDate > maxDate) {
      throw new Error(
        `Date out of range for SQL Server: ${docDate.toISOString()}`,
      );
    }
    return docDate;
  }

  private async getInvoiceStockTransferMap(
    docCode: string,
    isNormalOrder: boolean,
  ) {
    const docCodesForStockTransfer =
      StockTransferUtils.getDocCodesForStockTransfer([docCode]);
    const allStockTransfers = await this.stockTransferRepository.find({
      where: { soCode: In(docCodesForStockTransfer) },
      order: { itemCode: 'ASC', createdAt: 'ASC' },
    });

    const stockTransferMap = new Map<
      string,
      { st?: StockTransfer[]; rt?: StockTransfer[] }
    >();
    let transDate: Date | null = null;

    if (isNormalOrder && allStockTransfers.length > 0) {
      transDate = allStockTransfers[0].transDate || null;

      // ✅ Collect unique item codes for batch fetching
      const itemCodes = Array.from(
        new Set(
          allStockTransfers
            .map((st) => st.itemCode)
            .filter((c): c is string => !!c && c.trim() !== ''),
        ),
      );

      // ✅ Batch fetch all products at once (instead of in loop)
      const loyaltyMap = new Map<string, any>();
      if (itemCodes.length > 0) {
        const products = await this.loyaltyService.fetchProducts(itemCodes);
        products.forEach((product, itemCode) => {
          loyaltyMap.set(itemCode, product);
        });
      }

      allStockTransfers.forEach((st) => {
        const materialCode =
          st.materialCode || loyaltyMap.get(st.itemCode)?.materialCode;
        if (!materialCode) return;
        const key = `${st.soCode || st.docCode || docCode}_${materialCode}`;

        if (!stockTransferMap.has(key)) stockTransferMap.set(key, {});
        const m = stockTransferMap.get(key)!;
        if (st.docCode.startsWith('ST') || Number(st.qty || 0) < 0) {
          if (!m.st) m.st = [];
          m.st.push(st);
        } else if (st.docCode.startsWith('RT') || Number(st.qty || 0) > 0) {
          if (!m.rt) m.rt = [];
          m.rt.push(st);
        }
      });
    }
    return { stockTransferMap, allStockTransfers, transDate };
  }

  private async getInvoiceCardSerialMap(
    docCode: string,
  ): Promise<Map<string, string>> {
    const map = new Map<string, string>();
    const [dataCard] = await this.n8nService.fetchCardData(docCode);

    if (!Array.isArray(dataCard?.data) || dataCard.data.length === 0) {
      return map;
    }

    // Batch fetch instead of N+1 query
    const itemCodes = dataCard.data
      .map((card) => card?.service_item_name)
      .filter(Boolean);

    if (itemCodes.length === 0) {
      return map;
    }

    // Fetch all products in one batch call
    const products = await this.loyaltyService.fetchProducts(itemCodes);

    // Map serial numbers
    for (const card of dataCard.data) {
      if (!card?.service_item_name || !card?.serial) continue;
      const product = products.get(card.service_item_name);
      if (product?.materialCode) {
        map.set(product.materialCode, card.serial);
      }
    }

    return map;
  }

  private async calculateInvoiceAmounts(
    sale: any,
    orderData: any,
    allocationRatio: number,
    isNormalOrder: boolean,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const headerOrderTypes = InvoiceLogicUtils.getOrderTypes(
      orderData.sales?.[0]?.ordertypeName ||
        orderData.sales?.[0]?.ordertype ||
        '',
    );

    const amounts: any = {
      tienThue: this.toNumber(sale.tienThue, 0),
      dtTgNt: this.toNumber(sale.dtTgNt, 0),
      ck01_nt: this.toNumber(
        InvoiceLogicUtils.resolveChietKhauMuaHangGiamGia(
          sale,
          orderTypes.isDoiDiem || headerOrderTypes.isDoiDiem,
        ),
        0,
      ),
      ck02_nt:
        this.toNumber(sale.disc_tm, 0) > 0
          ? this.toNumber(sale.disc_tm, 0)
          : this.toNumber(sale.chietKhauCkTheoChinhSach, 0),
      ck03_nt: this.toNumber(
        sale.chietKhauMuaHangCkVip || sale.grade_discamt,
        0,
      ),
      ck04_nt: this.toNumber(
        sale.chietKhauThanhToanCoupon || sale.chietKhau09,
        0,
      ),
      ck05_nt:
        this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0
          ? this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0)
          : 0,
      ck07_nt: this.toNumber(sale.chietKhauVoucherDp2, 0),
      ck08_nt: this.toNumber(sale.chietKhauVoucherDp3, 0),
    };

    // Fill others with default 0 or from sale fields
    for (let i = 9; i <= 22; i++) {
      if (i === 11) continue; // ck11 handled separately
      const key = `ck${i.toString().padStart(2, '0')}_nt`;
      const saleKey = `chietKhau${i.toString().padStart(2, '0')}`;
      amounts[key] = this.toNumber(sale[saleKey] || sale[key], 0);
    }
    amounts.ck06_nt = 0;

    // ck11 (ECOIN) logic
    let ck11_nt = this.toNumber(
      sale.chietKhauThanhToanTkTienAo || sale.chietKhau11,
      0,
    );
    if (
      ck11_nt === 0 &&
      this.toNumber(sale.paid_by_voucher_ecode_ecoin_bp, 0) > 0 &&
      orderData.cashioData
    ) {
      const ecoin = orderData.cashioData.find(
        (c: any) => c.fop_syscode === 'ECOIN',
      );
      if (ecoin?.total_in) ck11_nt = this.toNumber(ecoin.total_in, 0);
    }
    amounts.ck11_nt = ck11_nt;

    // Allocation
    if (allocationRatio !== 1 && allocationRatio > 0) {
      Object.keys(amounts).forEach((k) => {
        if (k.endsWith('_nt') || k === 'tienThue' || k === 'dtTgNt') {
          amounts[k] *= allocationRatio;
        }
      });
    }

    if (orderTypes.isDoiDiem || headerOrderTypes.isDoiDiem) amounts.ck05_nt = 0;

    // promCode logic
    let promCode = sale.promCode || sale.prom_code || null;

    if (promCode && typeof promCode === 'string' && promCode.trim() !== '') {
      const trimmed = promCode.trim();
      // Special logic for PRMN: transform to RMN, no suffix, no cutCode
      if (trimmed.toUpperCase().startsWith('PRMN')) {
        promCode = trimmed.replace(/^PRMN/i, 'RMN');
      } else {
        // Old logic: cutCode + suffix
        promCode = await this.cutCode(promCode);
        if (sale.productType === 'I') {
          promCode = promCode + '.I';
        } else if (sale.productType === 'S') {
          promCode = promCode + '.S';
        } else if (sale.productType === 'V') {
          promCode = promCode + '.V';
        }
      }
    } else {
      promCode = null;
    }

    amounts.promCode = promCode;

    return amounts;
  }

  private async resolveInvoicePromotionCodes(
    sale: any,
    orderData: any,
    giaBan: number,
    promCode: string | null,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const isTangHang =
      Math.abs(giaBan) < 0.01 &&
      Math.abs(this.toNumber(sale.linetotal || sale.revenue, 0)) < 0.01;
    const maDvcs = this.toString(
      sale.department?.ma_dvcs || sale.department?.ma_dvcs_ht || '',
      '',
    );
    const productType =
      sale.productType ||
      sale.product?.productType ||
      sale.product?.producttype ||
      '';
    const productTypeUpper = String(productType).toUpperCase().trim();

    return InvoiceLogicUtils.resolvePromotionCodes({
      sale,
      orderTypes,
      isTangHang,
      maDvcs,
      productTypeUpper,
      promCode: sale.promCode || sale.prom_code, // Pass RAW code to let Utils handle PRMN logic consistently
    });
  }

  private resolveInvoiceAccounts(
    sale: any,
    loyaltyProduct: any,
    giaBan: number,
    maCk01: string | null,
    maCtkmTangHang: string | null,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    const isTangHang =
      Math.abs(giaBan) < 0.01 &&
      Math.abs(this.toNumber(sale.linetotal || sale.revenue, 0)) < 0.01;

    return InvoiceLogicUtils.resolveAccountingAccounts({
      sale,
      loyaltyProduct,
      orderTypes,
      isTangHang,
      hasMaCtkm: !!(maCk01 || maCtkmTangHang),
      hasMaCtkmTangHang: !!maCtkmTangHang,
    });
  }

  private resolveInvoiceLoaiGd(sale: any, loyaltyProduct: any = null): string {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertype || sale.ordertypeName || '',
    );
    return InvoiceLogicUtils.resolveLoaiGd({
      sale,
      orderTypes,
      loyaltyProduct,
    });
  }

  private async resolveInvoiceBatchSerial(
    sale: any,
    saleMaterialCode: string,
    cardSerialMap: Map<string, string>,
    stockTransferMap: Map<string, any>,
    docCode: string,
    loyaltyProduct: any,
  ) {
    let batchSerial: string | null = null;
    if (saleMaterialCode) {
      const sts = stockTransferMap.get(`${docCode}_${saleMaterialCode}`)?.st;
      if (sts?.[0]?.batchSerial) batchSerial = sts[0].batchSerial;
    }

    // [Fix] Fallback for Voucher (Type 94) - use ma_vt_ref (Ecode) as serial if not found in ST
    if (!batchSerial && loyaltyProduct?.materialType === '94') {
      batchSerial = sale.ma_vt_ref || sale.serial || sale.soSerial;
    }

    return InvoiceLogicUtils.resolveBatchSerial({
      batchSerialFromST: batchSerial,
      trackBatch: loyaltyProduct?.trackBatch === true,
      trackSerial: loyaltyProduct?.trackSerial === true,
    });
  }

  private calculateInvoiceQty(
    sale: any,
    docCode: string,
    saleMaterialCode: string,
    isNormalOrder: boolean,
    stockTransferMap: Map<string, any>,
  ) {
    let qty = this.toNumber(sale.qty, 0);
    const saleQty = this.toNumber(sale.qty, 0);
    let allocationRatio = 1;

    if (saleMaterialCode) {
      const key = `${docCode}_${saleMaterialCode}`;
      const firstSt = stockTransferMap.get(key)?.st?.[0];
      if (firstSt && saleQty !== 0) {
        qty = Math.abs(Number(firstSt.qty || 0));
        allocationRatio = qty / saleQty;
      }
    }
    return { qty, saleQty, allocationRatio };
  }

  private calculateInvoicePrices(
    sale: any,
    qty: number,
    allocationRatio: number,
    isNormalOrder: boolean,
  ) {
    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertypeName || sale.ordertype,
    );
    return InvoiceLogicUtils.calculatePrices({
      sale,
      orderTypes,
      allocationRatio,
      qtyFromStock: qty,
    });
  }

  private resolveInvoiceMaKhHeader(orderData: any): string {
    let maKh = SalesUtils.normalizeMaKh(orderData.customer?.code);
    const firstSale = orderData.sales?.[0];
    const { isTachThe } = InvoiceLogicUtils.getOrderTypes(
      firstSale?.ordertype || firstSale?.ordertypeName || '',
    );

    if (isTachThe && Array.isArray(orderData.sales)) {
      const saleWithIssue =
        orderData.sales.find(
          (s: any) => Number(s.qty || 0) < 0 && s.issuePartnerCode,
        ) || orderData.sales.find((s: any) => s.issuePartnerCode);
      if (saleWithIssue) {
        maKh = SalesUtils.normalizeMaKh(saleWithIssue.issuePartnerCode);
      }
    }
    return maKh;
  }

  private async resolveInvoiceMaKho(
    sale: any,
    saleMaterialCode: string,
    stockTransferMap: Map<string, any>,
    docCode: string,
    maBp: string,
    isTachThe: boolean,
  ): Promise<string> {
    let maKhoFromST: string | null = null;
    if (saleMaterialCode) {
      const sts = stockTransferMap.get(`${docCode}_${saleMaterialCode}`)?.st;
      if (sts?.[0]?.stockCode) maKhoFromST = sts[0].stockCode;
    }

    const orderTypes = InvoiceLogicUtils.getOrderTypes(
      sale.ordertype || sale.ordertypeName || '',
    );

    const maKho = InvoiceLogicUtils.resolveMaKho({
      maKhoFromST,
      maKhoFromSale: sale.maKho || null,
      maBp,
      orderTypes,
    });

    const maKhoMap = await this.categoriesService.mapWarehouseCode(maKho);
    return maKhoMap || maKho || '';
  }

  private fillInvoiceChietKhauFields(
    detailItem: any,
    amounts: any,
    sale: any,
    orderData: any,
    loyaltyProduct: any,
  ) {
    for (let i = 1; i <= 22; i++) {
      const idx = i.toString().padStart(2, '0');
      const key = `ck${idx}_nt`;
      const maKey = `ma_ck${idx}`;
      detailItem[key] = Number(amounts[key] || 0);

      // Special ma_ck logic
      if (i === 2) {
        // 02. Chiết khấu theo chính sách (Bán buôn)
        const isWholesale =
          sale.type_sale === 'WHOLESALE' || sale.type_sale === 'WS';
        const distTm = detailItem.ck02_nt;

        // Bỏ check channel_code vì dữ liệu không có sẵn trong entity
        if (isWholesale && distTm > 0) {
          detailItem[maKey] = this.val(
            InvoiceLogicUtils.resolveWholesalePromotionCode({
              product: loyaltyProduct,
              distTm: distTm,
            }),
            32,
          );
        } else {
          detailItem[maKey] = this.val(sale.maCk02 || '', 32);
        }
      } else if (i === 3) {
        const brand = orderData.customer?.brand || orderData.brand || '';
        detailItem[maKey] = this.val(
          SalesCalculationUtils.calculateMuaHangCkVip(
            sale,
            sale.product,
            brand,
          ),
          32,
        );
      } else if (i === 4) {
        detailItem[maKey] = this.val(
          detailItem.ck04_nt > 0 || sale.thanhToanCoupon
            ? sale.maCk04 || 'COUPON'
            : '',
          32,
        );
      } else if (i === 5) {
        const { isDoiDiem } = InvoiceLogicUtils.getOrderTypes(
          sale.ordertype || sale.ordertypeName,
        );
        const { isDoiDiem: isDoiDiemHeader } = InvoiceLogicUtils.getOrderTypes(
          orderData.sales?.[0]?.ordertype ||
            orderData.sales?.[0]?.ordertypeName ||
            '',
        );

        if (isDoiDiem || isDoiDiemHeader) {
          detailItem[maKey] = '';
        } else if (detailItem.ck05_nt > 0) {
          // Note: using logic from buildFastApiInvoiceData
          detailItem[maKey] = this.val(
            InvoiceLogicUtils.resolveVoucherCode({
              sale: {
                ...sale,
                customer: sale.customer || orderData.customer,
              },
              customer: null, // Resolution happens inside resolveVoucherCode
              brand: orderData.customer?.brand || orderData.brand || '',
            }),
            32,
            sale.maCk05 || 'VOUCHER',
          );
        }
      } else if (i === 7) {
        detailItem[maKey] = this.val(sale.voucherDp2 ? 'VOUCHER_DP2' : '', 32);
      } else if (i === 8) {
        detailItem[maKey] = this.val(sale.voucherDp3 ? 'VOUCHER_DP3' : '', 32);
      } else if (i === 11) {
        detailItem[maKey] = this.val(
          detailItem.ck11_nt > 0 || sale.thanhToanTkTienAo
            ? sale.maCk11 ||
                SalesUtils.generateTkTienAoLabel(
                  orderData.docDate,
                  orderData.customer?.brand ||
                    orderData.sales?.[0]?.customer?.brand,
                )
            : '',
          32,
        );
      } else {
        // Default mapping for other ma_ck fields
        if (i !== 1) {
          const saleMaKey = `maCk${idx}`;
          detailItem[maKey] = this.val(sale[saleMaKey] || '', 32);
        }
      }
    }
  }

  private buildInvoiceCbDetail(detail: any[]) {
    return detail.map((item: any) => {
      let tongChietKhau = 0;
      for (let i = 1; i <= 22; i++) {
        tongChietKhau += Number(
          item[`ck${i.toString().padStart(2, '0')}_nt`] || 0,
        );
      }

      return {
        ma_vt: item.ma_vt || '',
        dvt: item.dvt || '',
        so_luong: Number(item.so_luong || 0),
        ck_nt: Number(tongChietKhau),
        gia_nt: Number(item.gia_ban || 0),
        tien_nt: Number(item.tien_hang || 0),
      };
    });
  }

  private async mapSaleToInvoiceDetail(
    sale: any,
    index: number,
    orderData: any,
    context: any,
  ): Promise<any> {
    const { isNormalOrder, stockTransferMap, cardSerialMap, svcCodeMap } =
      context;
    const saleMaterialCode =
      sale.product?.materialCode ||
      sale.product?.materialCode ||
      sale.product?.maVatTu ||
      sale.product?.maERP;

    // 1. Qty & Allocation
    const { qty, allocationRatio } = this.calculateInvoiceQty(
      sale,
      orderData.docCode,
      saleMaterialCode,
      isNormalOrder,
      stockTransferMap,
    );

    // 2. Prices
    const { giaBan, tienHang, tienHangGoc } = this.calculateInvoicePrices(
      sale,
      qty,
      allocationRatio,
      isNormalOrder,
    );

    // 3. Amounts (Discounts, Tax, Subsidy)
    const amounts = await this.calculateInvoiceAmounts(
      sale,
      orderData,
      allocationRatio,
      isNormalOrder,
    );

    // 4. Resolve Codes & Accounts
    const materialCode =
      (sale.svc_code && svcCodeMap?.get(sale.svc_code)) ||
      SalesUtils.getMaterialCode(sale, sale.product) ||
      sale.itemCode;
    const loyaltyProduct = await this.loyaltyService.checkProduct(materialCode);

    const { maCk01, maCtkmTangHang } = await this.resolveInvoicePromotionCodes(
      sale,
      orderData,
      giaBan,
      amounts.promCode,
    );

    const { tkChietKhau, tkChiPhi, maPhi } = this.resolveInvoiceAccounts(
      sale,
      loyaltyProduct,
      giaBan,
      maCk01,
      maCtkmTangHang,
    );

    // 5. Build Detail Item
    const maBp = this.val(
      sale.department?.ma_bp || sale.branchCode || orderData.branchCode,
      8,
    );
    const loaiGd = this.resolveInvoiceLoaiGd(sale, loyaltyProduct);
    const { maLo, soSerial } = await this.resolveInvoiceBatchSerial(
      sale,
      saleMaterialCode,
      cardSerialMap,
      stockTransferMap,
      orderData.docCode,
      loyaltyProduct,
    );

    const detailItem: any = {
      tk_chiet_khau: this.val(tkChietKhau, 16),
      tk_chi_phi: this.val(tkChiPhi, 16),
      ma_phi: this.val(maPhi, 16),
      tien_hang: Number(sale.qty) * Number(sale.giaBan),
      so_luong: Number(sale.qty),
      ...(soSerial && soSerial.trim() !== ''
        ? { so_serial: this.limitString(soSerial, 64) }
        : maLo && maLo.trim() !== ''
          ? { ma_lo: this.limitString(maLo, 16) }
          : {}),
      ma_kh_i: this.val(sale.issuePartnerCode, 16),
      ma_vt: this.val(
        loyaltyProduct?.materialCode || sale.product?.maVatTu || '',
        16,
      ),
      dvt: this.val(
        sale.product?.dvt || sale.product?.unit || sale.dvt,
        32,
        'Cái',
      ),
      loai: this.val(sale.loai || sale.cat1, 2),
      loai_gd: this.val(loaiGd, 2),
      ma_ctkm_th: this.val(maCtkmTangHang, 32),
    };

    const finalMaKho = await this.resolveInvoiceMaKho(
      sale,
      saleMaterialCode,
      stockTransferMap,
      orderData.docCode,
      maBp,
      loaiGd === '11' || loaiGd === '12',
    );
    if (finalMaKho && finalMaKho.trim() !== '') {
      detailItem.ma_kho = this.limitString(finalMaKho, 16);
    }

    Object.assign(detailItem, {
      gia_ban: Number(giaBan),
      is_reward_line: sale.isRewardLine ? 1 : 0,
      is_bundle_reward_line: sale.isBundleRewardLine ? 1 : 0,
      km_yn:
        maCtkmTangHang === 'TT DAU TU'
          ? 0
          : Math.abs(giaBan) < 0.01 && Math.abs(tienHang) < 0.01
            ? 1
            : 0,
      dong_thuoc_goi: this.val(sale.dongThuocGoi, 32),
      trang_thai: this.val(sale.trangThai, 32),
      barcode: this.val(sale.barcode, 32),
      ma_ck01: this.val(maCk01, 32),
      dt_tg_nt: Number(amounts.dtTgNt),
      tien_thue: Number(amounts.tienThue),
      ma_thue: this.val(sale.maThue, 8, '00'),
      thue_suat: Number(this.toNumber(sale.thueSuat, 0)),
      tk_thue: this.val(sale.tkThueCo, 16),
      tk_cpbh: this.val(sale.tkCpbh, 16),
      ma_bp: maBp,
      ma_the: this.val(
        loyaltyProduct?.materialType === '94' && soSerial
          ? soSerial
          : cardSerialMap.get(saleMaterialCode),
        256,
      ),
      dong: index + 1,
      id_goc_ngay: sale.idGocNgay
        ? this.formatDateISO(new Date(sale.idGocNgay))
        : this.formatDateISO(new Date()),
      id_goc: this.val(sale.idGoc, 70),
      id_goc_ct: this.val(sale.idGocCt, 16),
      id_goc_so: Number(this.toNumber(sale.idGocSo, 0)),
      id_goc_dv: this.val(sale.idGocDv, 8),
      ma_combo: this.val(sale.maCombo, 16),
      ma_nx_st: this.val(sale.ma_nx_st, 32),
      ma_nx_rt: this.val(sale.ma_nx_rt, 32),
      ma_vt_ref: this.val(sale.ma_vt_ref, 32),
    });

    this.fillInvoiceChietKhauFields(
      detailItem,
      amounts,
      sale,
      orderData,
      loyaltyProduct,
    );

    return detailItem;
  }

  private async assembleInvoicePayload(
    orderData: any,
    detail: any[],
    cbdetail: any[],
    context: any,
  ) {
    const { ngayCt, ngayLct, transDate, maBp } = context;
    const firstSale = orderData.sales?.[0];

    const maDvcs = await this.loyaltyService.fetchMaDvcs(maBp);

    // not ma_dvcs_ ???pedding
    return {
      action: 0,
      ma_dvcs: maDvcs,
      ma_kh: this.resolveInvoiceMaKhHeader(orderData),
      ong_ba: orderData.customer?.name || null,
      ma_gd: '1',
      ma_tt: null,
      ma_ca: firstSale?.maCa || null,
      hinh_thuc: '0',
      dien_giai: orderData.docCode || null,
      ngay_lct: ngayLct,
      ngay_ct: ngayCt,
      so_ct: orderData.docCode || '',
      so_seri: orderData.branchCode || 'DEFAULT',
      ma_nt: 'VND',
      ty_gia: 1.0,
      ma_bp: maBp,
      tk_thue_no: '131111',
      ma_kenh: 'ONLINE',
      loai_gd: firstSale ? this.resolveInvoiceLoaiGd(firstSale, null) : '01',
      trans_date: transDate
        ? this.formatDateKeepLocalDay(new Date(transDate))
        : null,
      detail,
      cbdetail,
    };
  }

  private logInvoiceError(error: any, orderData: any) {
    this.logger.error(
      `Error building Fast API invoice data: ${error?.message || error}`,
    );
    this.logger.error(
      `Order data: ${JSON.stringify({
        docCode: orderData?.docCode,
        docDate: orderData?.docDate,
        salesCount: orderData?.sales?.length,
        customer: orderData?.customer
          ? { code: orderData.customer.code, name: orderData.customer.name }
          : null,
      })}`,
    );
  }
}
