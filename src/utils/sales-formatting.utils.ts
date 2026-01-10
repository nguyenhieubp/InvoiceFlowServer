import * as SalesUtils from './sales.utils';
import * as VoucherUtils from './voucher.utils';
import { InvoiceLogicUtils } from './invoice-logic.utils';

/**
 * Sales Formatting Utilities
 */

/**
 * Kiểm tra xem đơn hàng có sử dụng ECOIN không
 */
export function hasEcoin(orderData: any): boolean {
  if (!orderData) return false;
  const chietKhauTkTienAo = orderData.cashioTotalIn ?? 0;
  const isEcoin = orderData.cashioFopSyscode === 'ECOIN';
  return (
    chietKhauTkTienAo > 0 || (isEcoin && (orderData.cashioTotalIn ?? 0) > 0)
  );
}

/**
 * Tính toán các field display phức tạp cho frontend
 */
export async function calculateDisplayFields(
  sale: any,
  order: any,
  loyaltyProduct: any,
  department: any,
  categoriesService: any,
): Promise<{
  thanhToanCouponDisplay: string | null;
  chietKhauThanhToanCouponDisplay: number | null;
  thanhToanVoucherDisplay: string | null;
  thanhToanVoucher: number | null;
  thanhToanTkTienAoDisplay: string | null;
  chietKhauThanhToanTkTienAoDisplay: number | null;
  soSerialDisplay: string | null;
  cucThueDisplay: string | null;
  tkDoanhThuDisplay: string | null;
  tkGiaVonDisplay: string | null;
}> {
  const brand =
    order?.customer?.brand || order?.brand || sale?.customer?.brand || '';
  const orderTypes = InvoiceLogicUtils.getOrderTypes(
    sale.ordertypeName || sale.ordertype,
  );
  const { isDoiDiem } = orderTypes;

  const maCoupon =
    sale.maCk04 ||
    (sale.thanhToanCoupon && sale.thanhToanCoupon > 0 ? 'COUPON' : null);
  const thanhToanCouponDisplay = maCoupon || null;

  const chietKhauCoupon =
    sale.chietKhauThanhToanCoupon ?? sale.chietKhau09 ?? 0;
  const chietKhauThanhToanCouponDisplay =
    chietKhauCoupon > 0 ? chietKhauCoupon : null;

  let thanhToanVoucherDisplay: string | null = null;
  let thanhToanVoucher: number | null = null;

  if (!isDoiDiem && !hasEcoin(order)) {
    const ecommerce = await categoriesService.findActiveEcommerceCustomerByCode(
      sale.partnerCode,
    );
    const paidByVoucher =
      Number(
        sale.paid_by_voucher_ecode_ecoin_bp ??
          sale.chietKhauThanhToanVoucher ??
          0,
      ) || 0;

    if (paidByVoucher > 0) {
      thanhToanVoucher = paidByVoucher;
      thanhToanVoucherDisplay = InvoiceLogicUtils.resolveVoucherCode({
        sale: { ...sale, paid_by_voucher_ecode_ecoin_bp: paidByVoucher },
        customer: ecommerce,
        brand,
      });
    }
  }

  let thanhToanTkTienAoDisplay: string | null = null;
  let chietKhauThanhToanTkTienAoDisplay: number | null = null;
  const chietKhauTkTienAo = sale.chietKhauThanhToanTkTienAo ?? 0;

  if (chietKhauTkTienAo > 0) {
    thanhToanTkTienAoDisplay = SalesUtils.generateTkTienAoLabel(
      order?.docDate,
      brand || sale?.brand,
    );
    chietKhauThanhToanTkTienAoDisplay = chietKhauTkTienAo;
  } else if (
    (sale.paid_by_voucher_ecode_ecoin_bp ?? 0) > 0 &&
    order?.cashioData
  ) {
    const ecoinCashio = order.cashioData.find(
      (c: any) => c.fop_syscode === 'ECOIN',
    );
    if (ecoinCashio?.total_in && parseFloat(String(ecoinCashio.total_in)) > 0) {
      thanhToanTkTienAoDisplay = SalesUtils.generateTkTienAoLabel(
        order?.docDate,
        brand || sale?.brand,
      );
      chietKhauThanhToanTkTienAoDisplay = parseFloat(
        String(ecoinCashio.total_in),
      );
    }
  }

  let soSerialDisplay: string | null = null;
  if (
    sale.serial &&
    sale.serial.indexOf('_') <= 0 &&
    loyaltyProduct?.trackSerial &&
    !loyaltyProduct?.trackBatch
  ) {
    soSerialDisplay = sale.serial;
  }

  const cucThueDisplay =
    sale.cucThue || department?.ma_dvcs || department?.ma_dvcs_ht || null;

  const deptType = department?.type?.toLowerCase()?.trim();
  let tkDoanhThuDisplay = '-';
  let tkGiaVonDisplay = '-';
  if (deptType === 'bán lẻ') {
    tkDoanhThuDisplay = loyaltyProduct?.tkDoanhThuBanLe || '-';
    tkGiaVonDisplay = loyaltyProduct?.tkGiaVonBanLe || '-';
  } else if (deptType === 'bán buôn') {
    tkDoanhThuDisplay = loyaltyProduct?.tkDoanhThuBanBuon || '-';
    tkGiaVonDisplay = loyaltyProduct?.tkGiaVonBanBuon || '-';
  } else {
    tkDoanhThuDisplay =
      loyaltyProduct?.tkDoanhThuBanLe ||
      loyaltyProduct?.tkDoanhThuBanBuon ||
      '-';
    tkGiaVonDisplay =
      loyaltyProduct?.tkGiaVonBanLe || loyaltyProduct?.tkGiaVonBanBuon || '-';
  }

  return {
    thanhToanCouponDisplay,
    chietKhauThanhToanCouponDisplay,
    thanhToanVoucherDisplay,
    thanhToanVoucher,
    thanhToanTkTienAoDisplay,
    chietKhauThanhToanTkTienAoDisplay,
    soSerialDisplay,
    cucThueDisplay,
    tkDoanhThuDisplay,
    tkGiaVonDisplay,
  };
}

/**
 * Format sale object để trả về frontend
 */
export async function formatSaleForFrontend(
  sale: any,
  loyaltyProduct: any,
  department: any,
  calculatedFields: {
    maLo: string | null;
    maCtkmTangHang: string | null;
    muaHangCkVip: string;
    maKho: string | null;
    isTangHang: boolean;
    isDichVu: boolean;
    promCodeDisplay: string | null;
  },
  order: any, // Order object
  categoriesService: any,
  loyaltyService: any,
  stockTransfers?: any[],
): Promise<any> {
  const saleMaterialCode =
    sale.product?.materialCode ||
    sale.product?.maVatTu ||
    sale.product?.maERP ||
    loyaltyProduct?.materialCode;

  const ordertypeName = sale.ordertypeName || sale.ordertype || '';
  const orderTypes = InvoiceLogicUtils.getOrderTypes(ordertypeName);
  const {
    isDoiDiem,
    isDoiVo,
    isDauTu,
    isSinhNhat,
    isThuong,
    isTachThe,
    isDoiDv,
    isDichVu,
    isBanTaiKhoan,
    isSanTmdt,
  } = orderTypes;

  // 1. Resolve Stock Transfer data
  let maKhoFromST: string | null = null;
  let batchSerialFromST: string | null = null;
  let qtyFromST: number | null = null;
  const availableStockTransfers = stockTransfers || order?.stockTransfers;

  if (
    saleMaterialCode &&
    availableStockTransfers &&
    Array.isArray(availableStockTransfers)
  ) {
    const matchingST = availableStockTransfers.find(
      (st: any) => st.materialCode === saleMaterialCode,
    );
    if (matchingST) {
      maKhoFromST = matchingST.stockCode || null;
      batchSerialFromST = matchingST.batchSerial || null;
      qtyFromST = Math.abs(Number(matchingST.qty || 0));
    }
  }

  // 2. Qty & Prices (Allocation Ratio calculation simplified for display)
  // For frontend display, we often use ratio = 1 unless it's a "01.Thường" order with stock transfers
  let allocationRatio = 1;
  const saleQty = Number(sale.qty || 0);
  if (isThuong && qtyFromST !== null && saleQty > 0) {
    allocationRatio = qtyFromST / saleQty;
  }

  const { giaBan, tienHang, tienHangGoc } = InvoiceLogicUtils.calculatePrices({
    sale,
    orderTypes,
    allocationRatio,
    qtyFromStock: qtyFromST ?? undefined,
  });

  // 3. Serial / Batch resolution
  const materialCode = saleMaterialCode || sale.itemCode;
  const loyaltyProductForTracking =
    await loyaltyService.checkProduct(materialCode);
  const trackSerial = loyaltyProductForTracking?.trackSerial === true;
  const trackBatch = loyaltyProductForTracking?.trackBatch === true;

  const { maLo, soSerial } = InvoiceLogicUtils.resolveBatchSerial({
    batchSerialFromST,
    trackBatch,
    trackSerial,
  });

  // 4. Warehouse resolution
  const maBp = department?.ma_bp || '';
  let maKho = InvoiceLogicUtils.resolveMaKho({
    maKhoFromST,
    maKhoFromSale: sale.maKho || calculatedFields.maKho,
    maBp,
    orderTypes,
  });

  // Map warehouse code if categoriesService is provided
  if (maKho && categoriesService && !isTachThe) {
    const mapped = await categoriesService.mapWarehouseCode(maKho);
    if (mapped) maKho = mapped;
  }

  // 5. Codes & Accounts Resolution
  const productType =
    sale.productType ||
    sale.producttype ||
    loyaltyProduct?.producttype ||
    loyaltyProduct?.productType ||
    null;
  const productTypeUpper = productType
    ? String(productType).toUpperCase().trim()
    : null;

  const maDvcs = department?.ma_dvcs || department?.ma_dvcs_ht || '';
  const isTangHang = calculatedFields.isTangHang;

  const { maCk01, maCtkmTangHang } = InvoiceLogicUtils.resolvePromotionCodes({
    sale,
    orderTypes,
    isTangHang,
    maDvcs,
    productTypeUpper,
    promCode: sale.promCode,
  });

  const isGiaBanZero = Math.abs(giaBan) < 0.01;
  const { tkChietKhau, tkChiPhi, maPhi } =
    InvoiceLogicUtils.resolveAccountingAccounts({
      sale,
      loyaltyProduct,
      orderTypes,
      isTangHang,
      isGiaBanZero,
      hasMaCtkm: !!(sale.promCode || maCk01 || maCtkmTangHang),
      hasMaCtkmTangHang: !!maCtkmTangHang,
    });

  const loaiGd = InvoiceLogicUtils.resolveLoaiGd({
    sale,
    orderTypes,
  });

  // 6. Display Fields
  const displayFields = await calculateDisplayFields(
    sale,
    order,
    loyaltyProduct,
    department,
    categoriesService,
  );

  let finalPromCodeDisplay = calculatedFields.promCodeDisplay;
  if (isDoiDiem) finalPromCodeDisplay = '1';

  const other_discamt = isDoiDiem
    ? 0
    : (sale.other_discamt ?? sale.chietKhauMuaHangGiamGia ?? 0);

  return {
    ...sale,
    itemName: sale.itemName || loyaltyProduct?.name || null,
    maKho: maKho,
    maCtkmTangHang: maCtkmTangHang,
    muaHangCkVip: calculatedFields.muaHangCkVip,
    maLo: maLo,
    maSerial: soSerial,
    isTangHang,
    isDichVu: calculatedFields.isDichVu,
    promCodeDisplay: finalPromCodeDisplay,
    muaHangGiamGiaDisplay: maCk01,
    other_discamt: other_discamt,
    chietKhauMuaHangGiamGia: other_discamt,
    giaBan: giaBan,
    tienHang: tienHang,
    linetotal: isDoiDiem ? 0 : (sale.linetotal ?? tienHang),
    promotionDisplayCode: SalesUtils.getPromotionDisplayCode(sale.promCode),
    ordertypeName: ordertypeName,
    loaiGd: loaiGd,
    issuePartnerCode: sale.issuePartnerCode || null,
    partnerCode:
      isTachThe && sale.issuePartnerCode
        ? sale.issuePartnerCode
        : sale.partnerCode || sale.partner_code || null,
    ...displayFields,
    productType: productType,
    product: loyaltyProduct
      ? {
          ...loyaltyProduct,
          productType: productType,
          dvt: loyaltyProduct.unit || null,
          maVatTu: loyaltyProduct.materialCode || sale.itemCode,
          trackInventory: loyaltyProduct.trackInventory ?? null,
          trackSerial: trackSerial,
          trackBatch: trackBatch,
        }
      : null,
    department: department,
    dvt: loyaltyProduct?.unit || sale.dvt || null,
    tkChietKhau,
    tkChiPhi,
    maPhi,
    brand: sale?.brand?.toUpperCase() || null,
    type_sale: sale?.type_sale || null,
  };
}
