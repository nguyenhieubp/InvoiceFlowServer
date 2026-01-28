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
  isEmployee?: boolean, // [NEW] Pre-fetched employee status from API
): Promise<{
  thanhToanCouponDisplay: string | null;
  chietKhauThanhToanCouponDisplay: number | null;
  thanhToanVoucherDisplay: string | null;
  thanhToanVoucher: number | null;
  thanhToanTkTienAoDisplay: string | null;
  chietKhauThanhToanTkTienAoDisplay: number | null;
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

  // Force voucher discount to 0 if order is Point Exchange (isDoiDiem)
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
        maDvcs: department?.ma_dvcs || department?.ma_dvcs_ht,
        isEmployee: isEmployee ?? false, // [API] Use pre-fetched employee status
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
  isPlatformOrderOverride?: boolean, // [NEW]
  platformBrandOverride?: string, // [NEW]
  isEmployee?: boolean, // [NEW] Pre-fetched employee status from API
): Promise<any> {
  const saleMaterialCode =
    sale.product?.materialCode ||
    sale.product?.maVatTu ||
    sale.product?.maERP ||
    loyaltyProduct?.materialCode;

  const ordertypeName = sale.ordertypeName || sale.ordertype || '';
  let orderTypes = InvoiceLogicUtils.getOrderTypes(ordertypeName);

  // [NEW] Override isSanTmdt if detected via OrderFee
  if (isPlatformOrderOverride) {
    orderTypes = {
      ...orderTypes,
      isSanTmdt: true,
    };
  }

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
      (st: any) =>
        st.materialCode === saleMaterialCode ||
        st.itemCode === sale.itemCode ||
        st.materialCode === sale.itemCode,
    );
    if (matchingST) {
      maKhoFromST = matchingST.stockCode || null;
      // [FIX] Map warehouse code immediately to match SalesQueryService logic
      if (maKhoFromST && categoriesService) {
        const mapped = await categoriesService.mapWarehouseCode(maKhoFromST);
        if (mapped) maKhoFromST = mapped;
      }
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

  let { giaBan, tienHang, tienHangGoc } = InvoiceLogicUtils.calculatePrices({
    sale,
    orderTypes,
    allocationRatio,
    qtyFromStock: qtyFromST ?? undefined,
  });

  // Pricing logic for non-normal orders (FIX V7) is now handled inside InvoiceLogicUtils.calculatePrices

  // FIX V7: Re-evaluate isTangHang strict logic
  let isTangHang = giaBan === 0 && tienHang === 0;
  if (isDichVu) isTangHang = false;
  const materialCode = saleMaterialCode || sale.itemCode;

  // OPTIMIZATION: Use passed loyaltyProduct if available and matches
  let loyaltyProductForTracking = loyaltyProduct;
  if (
    !loyaltyProductForTracking ||
    (loyaltyProductForTracking.materialCode !== materialCode &&
      loyaltyProductForTracking.code !== materialCode)
  ) {
    // Only fetch if strictly necessary (different code or missing)
    loyaltyProductForTracking = await loyaltyService.checkProduct(materialCode);
  }

  const trackSerial = loyaltyProductForTracking?.trackSerial === true;
  const trackBatch = loyaltyProductForTracking?.trackBatch === true;

  // 3. Batch/Serial Resolution
  let { maLo, soSerial } = InvoiceLogicUtils.resolveBatchSerial({
    batchSerialFromST,
    trackBatch,
    trackSerial,
  });

  // 4. Warehouse resolution
  const maBp = department?.ma_bp || sale.branchCode || order?.branchCode || '';
  let maKho = InvoiceLogicUtils.resolveMaKho({
    maKhoFromST,
    maKhoFromSale: sale.maKho || calculatedFields.maKho,
    maBp,
    orderTypes,
  });

  // Map warehouse code if categoriesService is provided
  if (maKho && categoriesService) {
    const mapped = await categoriesService.mapWarehouseCode(maKho);
    if (mapped) maKho = mapped;
  }

  // 5. Codes & Accounts Resolution
  const productType = sale?.productType || null;
  const productTypeUpper = productType
    ? String(productType).toUpperCase().trim()
    : null;

  const groupProductType = loyaltyProductForTracking?.productType;
  const loaiVt = loyaltyProductForTracking?.materialType;
  const maHangGiamGia = this.calcCodeDisCount(groupProductType, loaiVt) || '';

  const maDvcs = department?.ma_dvcs || department?.ma_dvcs_ht || '';
  // isTangHang is already calculated above (V7 Fix)

  const { maCk01, maCtkmTangHang } = InvoiceLogicUtils.resolvePromotionCodes({
    sale,
    orderTypes,
    isTangHang,
    maDvcs,
    productTypeUpper,
    promCode: sale.promCode,
    maHangGiamGia: maHangGiamGia,
    isEmployee: isEmployee ?? false, // [API] Use pre-fetched employee status
  });

  const { tkChietKhau, tkChiPhi, maPhi } =
    await InvoiceLogicUtils.resolveAccountingAccounts({
      sale,
      loyaltyProduct,
      orderTypes,
      isTangHang,
      hasMaCtkm: !!(sale.promCode || maCk01 || maCtkmTangHang),
      hasMaCtkmTangHang: !!maCtkmTangHang,
      loyaltyService,
    });

  const loaiGd = InvoiceLogicUtils.resolveLoaiGd({
    sale,
    orderTypes,
    loyaltyProduct,
  });

  // [NEW] Source of Truth Calculation for Amounts
  const amounts = InvoiceLogicUtils.calculateInvoiceAmounts({
    sale: sale,
    orderData: order,
    allocationRatio: allocationRatio,
    isPlatformOrder: isPlatformOrderOverride || isSanTmdt,
    cashioData: order?.cashioData,
  });

  // 7. Display Fields
  const displayFields = await calculateDisplayFields(
    sale,
    order,
    loyaltyProduct,
    department,
    categoriesService,
    isEmployee, // [API] Pass pre-fetched employee status
  );

  let finalPromCodeDisplay = calculatedFields.promCodeDisplay;
  if (isDoiDiem) finalPromCodeDisplay = '1';

  const other_discamt = InvoiceLogicUtils.resolveChietKhauMuaHangGiamGia(
    sale,
    isDoiDiem,
  );

  // [NEW] Resolve mã CTKM cho "Mua hàng giảm giá" (employee discount code)
  const muaHangGiamGiaDisplay = InvoiceLogicUtils.resolveMuaHangGiamGiaCode({
    sale,
    maDvcs,
    productType: productTypeUpper,
    isEmployee: isEmployee ?? false,
  });

  // 8. Wholesale Promotion Code Mapping
  // Áp dụng cho đơn hàng bán buôn khi dist_tm > 0
  let maCkTheoChinhSach;
  const typeSale = (sale.type_sale || '').toUpperCase().trim();
  const distTm = Number(sale.disc_tm || 0);

  // Kiểm tra điều kiện: WHOLESALE và ordertypeName = "Bán buôn kênh Đại lý"
  const isWholesale = typeSale === 'WHOLESALE';
  const isAgencyChannel = ordertypeName.includes('Bán buôn kênh Đại lý');

  if (isWholesale && isAgencyChannel && distTm > 0) {
    // Gọi hàm map mã CTKM cho bán buôn
    const wholesalePromoCode = InvoiceLogicUtils.resolveWholesalePromotionCode({
      product: loyaltyProduct,
      distTm: distTm,
    });

    // Gán mã CTKM đã map vào maCkTheoChinhSach
    if (wholesalePromoCode) {
      maCkTheoChinhSach = wholesalePromoCode;
    }
  }

  // [NEW] Wholesale ECode Logic (FIXED)
  // Nếu bán buôn và là ECode (loaiVt == 94), thì cột Serial lấy giá trị từ Mã thẻ
  // Use loose equality for safety (number vs string)
  if (isWholesale && String(loaiVt) === '94' && sale.maThe) {
    soSerial = sale.maThe; // Override biến soSerial local (data field)
  }

  // [FIX FORCE] Nếu Stock Transfer có batchSerial nhưng không hiển thị (do tracking config),
  // FORCE hiển thị nó ở Serial nếu chưa có giá trị VÀ chưa được gán vào Mã Lô
  // Fix: Chỉ hiển thị ở Serial nếu nó không phải là Mã Lô
  if (!soSerial && !maLo && batchSerialFromST) {
    soSerial = batchSerialFromST;
  }

  return {
    ...sale,
    customer: sale.customer
      ? {
          code: sale.customer.code,
          name: sale.customer.name,
          brand: sale.customer.brand,
          address: sale.customer.address,
          idnumber: sale.customer.idnumber,
          mobile: sale.customer.mobile,
          birthday: sale.customer.birthday,
          sexual: sale.customer.sexual,
        }
      : null,
    itemName: sale.itemName || loyaltyProduct?.name || null,
    maKho: maKho,
    maCtkmTangHang: maCtkmTangHang,
    // [MOVED] muaHangCkVip moved to bottom with explicit check
    maLo: maLo,
    maSerial: batchSerialFromST,
    isTangHang,
    isDichVu: calculatedFields.isDichVu,
    promCodeDisplay: finalPromCodeDisplay,
    // [FIX] Prioritize muaHangGiamGiaDisplay (employee discount) when available
    promotionDisplayCode: maCtkmTangHang
      ? ''
      : muaHangGiamGiaDisplay || // [NEW] Employee discount code takes priority
        maCk01 ||
        SalesUtils.getPromotionDisplayCode(sale.promCode) ||
        (isSanTmdt ? '' : displayFields.thanhToanVoucherDisplay) ||
        '',
    muaHangGiamGiaDisplay: muaHangGiamGiaDisplay, // [NEW] Mã CTKM cho chiết khấu mua hàng NV
    // [FIX] Overwrite fields with Source of Truth Amounts
    // Ensures Frontend matches Fast API Payload exactly
    chietKhauMuaHangGiamGia: amounts.ck01_nt,
    other_discamt: amounts.ck01_nt,
    chietKhauCkTheoChinhSach: amounts.ck02_nt,
    chietKhauMuaHangCkVip: amounts.ck03_nt,
    chietKhauThanhToanCoupon: amounts.ck04_nt,

    // ck05 logic already includes isDoiDiem/isPlatform checks
    paid_by_voucher_ecode_ecoin_bp: amounts.ck05_nt,
    chietKhauThanhToanVoucher: amounts.ck05_nt,

    chietKhauVoucherDp1: amounts.ck06_nt,
    chietKhauVoucherDp2: amounts.ck07_nt,
    chietKhauVoucherDp3: amounts.ck08_nt,
    chietKhauThanhToanTkTienAo: amounts.ck11_nt,

    // [RESTORED] Missing fields
    maCkTheoChinhSach: maCkTheoChinhSach,
    giaBan: giaBan,
    tienHang: giaBan * saleQty,
    linetotal: isDoiDiem ? 0 : (sale.linetotal ?? tienHang),
    ordertypeName: ordertypeName,
    loaiGd: loaiGd,
    issuePartnerCode: SalesUtils.normalizeMaKh(sale.issuePartnerCode || null),
    partnerCode: SalesUtils.normalizeMaKh(
      isTachThe && sale.issuePartnerCode
        ? sale.issuePartnerCode
        : sale.partnerCode || sale.partner_code || null,
    ),

    ...displayFields,

    thanhToanVoucherDisplay: isSanTmdt
      ? null
      : displayFields.thanhToanVoucherDisplay,
    thanhToanVoucher: isSanTmdt ? 0 : displayFields.thanhToanVoucher,

    productType: productType,
    product: loyaltyProduct
      ? {
          productType: productType,
          dvt: loyaltyProduct.unit || null,
          maVatTu: loyaltyProduct.materialCode || sale.itemCode,
          tenVatTu: loyaltyProduct.name || null,
          trackInventory: loyaltyProduct.trackInventory ?? null,
          trackSerial: trackSerial,
          trackBatch: trackBatch,
          tkChietKhau: loyaltyProduct.tkChietKhau || null,
          tkDoanhThuBanLe: loyaltyProduct.tkDoanhThuBanLe || null,
          tkDoanhThuBanBuon: loyaltyProduct.tkDoanhThuBanBuon || null,
          tkGiaVonBanLe: loyaltyProduct.tkGiaVonBanLe || null,
          tkGiaVonBanBuon: loyaltyProduct.tkGiaVonBanBuon || null,
        }
      : null,
    department: department
      ? {
          ma_bp: department.ma_bp || null,
          branchcode: department.branchcode || null,
          ma_dvcs: department.ma_dvcs || null,
          ma_dvcs_ht: department.ma_dvcs_ht || null,
          type: department.type || null,
        }
      : null,
    dvt: loyaltyProduct?.unit || sale.dvt || null,
    tkChietKhau,
    tkChiPhi,
    maPhi,
    brand: (platformBrandOverride || sale?.brand)?.toUpperCase() || null,
    type_sale: sale?.type_sale || null,

    // [FIX] Explicitly return VIP discount fields
    // chietKhauMuaHangCkVip: Number(
    //   sale.chietKhauMuaHangCkVip || sale.grade_discamt || 0,
    // ),
    muaHangCkVip: calculatedFields.muaHangCkVip || null, // Ensure not undefined

    // [FIX] maThe assignment logic:
    // - For type V items in normal orders (01. Thường): use batchSerialFromST if available
    maThe:
      isThuong && productType === 'V'
        ? batchSerialFromST || ''
        : sale.maThe || '',
  };
}

/**
 * Mã mua hàng giảm giá
 */

export function calcCodeDisCount(productType, loaiVt) {
  let isEcode;
  if (loaiVt === '94') {
    isEcode = true;
  } else {
    isEcode = false;
  }

  const ECG = ['10GIFT'];

  if (!isEcode) {
    return 'SP';
  } else {
    if (ECG.includes(productType)) {
      return 'ECG';
    } else {
      return 'E';
    }
  }
}
