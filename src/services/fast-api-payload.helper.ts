/**
 * Helper functions cho Fast API payload building và formatting
 */
export class FastApiPayloadHelper {
  /**
   * Helper: Loại bỏ các field null, undefined, hoặc empty string
   * Giữ lại ma_lo và so_serial ngay cả khi null hoặc empty
   */
  static removeEmptyFields(obj: any, keepMaLoAndSerial: boolean = true): any {
    if (obj === null || obj === undefined) {
      return obj;
    }
    if (Array.isArray(obj)) {
      return obj.map((item) =>
        FastApiPayloadHelper.removeEmptyFields(item, keepMaLoAndSerial),
      );
    }
    if (typeof obj === 'object') {
      const cleaned: any = {};
      for (const [key, value] of Object.entries(obj)) {
        const shouldKeep =
          (value !== null && value !== undefined && value !== '') ||
          (keepMaLoAndSerial && (key === 'ma_lo' || key === 'so_serial'));
        if (shouldKeep) {
          cleaned[key] = FastApiPayloadHelper.removeEmptyFields(
            value,
            keepMaLoAndSerial,
          );
        }
      }
      return cleaned;
    }
    return obj;
  }

  /**
   * Helper: Build clean payload cho salesOrder và salesInvoice
   */
  static buildCleanPayload(
    orderData: any,
    action: number = 0,
    type?: string,
  ): any {
    if (type === 'saleInvoice') {
      return {
        action,
        ma_dvcs: orderData.ma_dvcs,
        ma_kh: orderData.ma_kh,
        ong_ba: orderData.ong_ba ?? null,
        ma_gd: orderData.ma_gd ?? '1',
        ma_tt: orderData.ma_tt ?? null,
        ma_ca: orderData.ma_ca ?? null,
        hinh_thuc: orderData.hinh_thuc ?? '0',
        dien_giai: orderData.dien_giai ?? null,
        //Ưu tiên lấy trans_date nếu có, nếu không thì lấy ngay_lct và ngay_ct
        ngay_lct: orderData.trans_date || orderData.ngay_lct,
        ngay_ct: orderData.trans_date || orderData.ngay_ct,
        so_ct: orderData.so_ct,
        so_seri: orderData.so_seri,
        ma_nt: orderData.ma_nt ?? 'VND',
        ty_gia:
          typeof orderData.ty_gia === 'number'
            ? orderData.ty_gia
            : parseFloat(orderData.ty_gia) || 1.0,
        ma_bp: orderData.ma_bp,
        tk_thue_no: orderData.tk_thue_no ?? '131111',
        ma_kenh: orderData.ma_kenh ?? 'ONLINE',
        detail: (orderData.detail || []).map((item: any) => {
          const { product, ...cleanItem } = item;
          const result: any = { ...cleanItem };
          // Giữ lại ma_lo và so_serial (kể cả null)
          if ('ma_lo' in item) result.ma_lo = item.ma_lo;
          if ('so_serial' in item) result.so_serial = item.so_serial;
          // Giữ lại ma_bp nếu có (không loại bỏ)
          if ('ma_bp' in item) result.ma_bp = item.ma_bp;
          return result;
        }),
        cbdetail: null,
      };
    }

    return {
      action,
      ma_dvcs: orderData.ma_dvcs,
      ma_kh: orderData.ma_kh,
      ong_ba: orderData.ong_ba ?? null,
      ma_gd: orderData.ma_gd ?? '1',
      ma_tt: orderData.ma_tt ?? null,
      ma_ca: orderData.ma_ca ?? null,
      hinh_thuc: orderData.hinh_thuc ?? '0',
      dien_giai: orderData.dien_giai ?? null,
      ngay_lct: orderData.ngay_lct,
      ngay_ct: orderData.ngay_ct,
      so_ct: orderData.so_ct,
      so_seri: orderData.so_seri,
      ma_nt: orderData.ma_nt ?? 'VND',
      ty_gia:
        typeof orderData.ty_gia === 'number'
          ? orderData.ty_gia
          : parseFloat(orderData.ty_gia) || 1.0,
      ma_bp: orderData.ma_bp,
      tk_thue_no: orderData.tk_thue_no ?? '131111',
      ma_kenh: orderData.ma_kenh ?? 'ONLINE',
      detail: (orderData.detail || []).map((item: any) => {
        const { product, ...cleanItem } = item;
        const result: any = { ...cleanItem };
        // Giữ lại ma_lo và so_serial (kể cả null)
        if ('ma_lo' in item) result.ma_lo = item.ma_lo;
        if ('so_serial' in item) result.so_serial = item.so_serial;
        // Giữ lại ma_bp nếu có (không loại bỏ)
        if ('ma_bp' in item) result.ma_bp = item.ma_bp;
        return result;
      }),
      cbdetail: null,
    };
  }

  /**
   * Format date thành ISO string cho Fast API (YYYY-MM-DDTHH:mm:ss)
   */
  static formatDateISO(date: Date | string): string {
    const d = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(d.getTime())) {
      return new Date().toISOString();
    }
    return d.toISOString();
  }

  /**
   * Build payload cho cashReceipt API
   */
  static buildCashReceiptPayload(
    cashioData: any,
    orderData: any,
    invoiceData: any,
  ): any {
    const totalIn = parseFloat(String(cashioData.total_in || '0'));
    const docDate = cashioData.docdate || orderData.docDate || new Date();

    return {
      action: 0,
      ma_dvcs: invoiceData.ma_dvcs || cashioData.branch_code || '',
      ma_kh: invoiceData.ma_kh || cashioData.partner_code || '',
      ong_ba:
        orderData.customer?.name ||
        cashioData.partner_name ||
        invoiceData.ong_ba ||
        '',
      loai_ct: '2', // Mặc định 2 - Thu của khách hàng
      dept_id: invoiceData.ma_bp || cashioData.branch_code || '',
      dien_giai: `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
      ngay_lct: FastApiPayloadHelper.formatDateISO(docDate),
      so_ct: orderData.docCode || invoiceData.so_ct || '',
      so_ct_tc: orderData.docCode || invoiceData.so_ct || '',
      ma_nt: 'VND',
      ty_gia: 1,
      ma_cp1: '',
      ma_cp2: '',
      httt: 'CASH',
      status: '0' as string,
      detail: [
        {
          ma_kh_i: invoiceData.ma_kh || cashioData.partner_code || '',
          tien: totalIn,
          dien_giai:
            cashioData.refno ||
            `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
          ma_bp: invoiceData.ma_bp || cashioData.branch_code || '',
          ma_vv: '',
          ma_hd: '',
          ma_phi: '',
          ma_ku: '',
        },
      ],
    };
  }

  /**
   * Build payload cho creditAdvice API
   */
  static buildCreditAdvicePayload(
    cashioData: any,
    orderData: any,
    invoiceData: any,
    paymentMethod: any,
  ): any {
    const totalIn = parseFloat(String(cashioData.total_in || '0'));
    const docDate = cashioData.docdate || orderData.docDate || new Date();

    return {
      action: 0,
      ma_dvcs: invoiceData.ma_dvcs || cashioData.branch_code || '',
      ma_kh: invoiceData.ma_kh || cashioData.partner_code || '',
      ong_ba:
        orderData.customer?.name ||
        cashioData.partner_name ||
        invoiceData.ong_ba ||
        '',
      loai_ct: '2', // Mặc định 2 - Thu của khách hàng
      dept_id: invoiceData.ma_bp || cashioData.branch_code || '',
      dien_giai: `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
      ngay_lct: FastApiPayloadHelper.formatDateISO(docDate),
      so_ct: orderData.docCode || invoiceData.so_ct || '',
      so_ct_tc: orderData.docCode || invoiceData.so_ct || '',
      ma_nt: 'VND',
      ty_gia: 1,
      ma_cp1: '',
      ma_cp2: '',
      httt: paymentMethod.code || cashioData.fop_syscode || '',
      status: '0',
      detail: [
        {
          ma_kh_i: invoiceData.ma_kh || cashioData.partner_code || '',
          tien: totalIn,
          dien_giai:
            cashioData.refno ||
            paymentMethod.description ||
            `Thu tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`,
          ma_bp: invoiceData.ma_bp || cashioData.branch_code || '',
          ma_vv: '',
          ma_hd: '',
          ma_phi: '',
          ma_ku: '',
        },
      ],
    };
  }

  /**
   * Build payload cho payment API (Phiếu chi tiền mặt)
   */
  static buildPaymentPayload(
    cashioData: any,
    orderData: any,
    invoiceData: any,
    paymentMethod: any,
    loaiCt: string = '2',
  ): any {
    const totalOut = parseFloat(String(cashioData.total_out || '0'));
    const docDate = cashioData.docdate || orderData.docDate || new Date();

    const payload: any = {
      action: 0,
      ma_dvcs: invoiceData.ma_dvcs || cashioData.branch_code || '',
      ma_kh: invoiceData.ma_kh || cashioData.partner_code || '',
      loai_ct: loaiCt, // Mặc định 2 - Chi cho khách hàng
      dept_id: invoiceData.ma_bp || cashioData.branch_code || '',
      ngay_lct: FastApiPayloadHelper.formatDateISO(docDate),
      so_ct: orderData.docCode || invoiceData.so_ct || '',
      httt: paymentMethod?.code || cashioData.fop_syscode || '',
      status: '0',
      detail: [
        {
          tien: totalOut,
          ma_bp: invoiceData.ma_bp || cashioData.branch_code || '',
        },
      ],
    };

    // Optional fields - chỉ thêm nếu có giá trị
    if (
      orderData.customer?.name ||
      cashioData.partner_name ||
      invoiceData.ong_ba
    ) {
      payload.ong_ba =
        orderData.customer?.name ||
        cashioData.partner_name ||
        invoiceData.ong_ba ||
        '';
    }
    if (orderData.customer?.address) {
      payload.dia_chi = orderData.customer.address;
    }
    if (orderData.docCode || invoiceData.so_ct) {
      payload.dien_giai = `Chi tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`;
    }

    // Chi tiết - chỉ thêm ma_kh_i nếu loai_ct = 3 (required trong trường hợp này)
    if (loaiCt === '3') {
      payload.detail[0].ma_kh_i =
        invoiceData.ma_kh || cashioData.partner_code || '';
    }

    return payload;
  }

  /**
   * Build payload cho debitAdvice API (Giấy báo nợ)
   */
  static buildDebitAdvicePayload(
    cashioData: any,
    orderData: any,
    invoiceData: any,
    paymentMethod: any,
    loaiCt: string = '2',
  ): any {
    const totalOut = parseFloat(String(cashioData.total_out || '0'));
    const docDate = cashioData.docdate || orderData.docDate || new Date();

    const payload: any = {
      action: 0,
      ma_dvcs: invoiceData.ma_dvcs || cashioData.branch_code || '',
      ma_kh: invoiceData.ma_kh || cashioData.partner_code || '',
      loai_ct: loaiCt, // Mặc định 2 - Chi cho khách hàng
      dept_id: invoiceData.ma_bp || cashioData.branch_code || '',
      ngay_lct: FastApiPayloadHelper.formatDateISO(docDate),
      so_ct: orderData.docCode || invoiceData.so_ct || '',
      httt: paymentMethod?.code || cashioData.fop_syscode || '',
      status: '0',
      detail: [
        {
          tien: totalOut,
          ma_bp: invoiceData.ma_bp || cashioData.branch_code || '',
        },
      ],
    };

    // Optional fields - chỉ thêm nếu có giá trị
    if (
      orderData.customer?.name ||
      cashioData.partner_name ||
      invoiceData.ong_ba
    ) {
      payload.ong_ba =
        orderData.customer?.name ||
        cashioData.partner_name ||
        invoiceData.ong_ba ||
        '';
    }
    if (orderData.customer?.address) {
      payload.dia_chi = orderData.customer.address;
    }
    if (orderData.docCode || invoiceData.so_ct) {
      payload.dien_giai = `Chi tiền cho chứng từ ${orderData.docCode || invoiceData.so_ct || ''}`;
    }

    // Chi tiết - chỉ thêm ma_kh_i nếu loai_ct = 3 (required trong trường hợp này)
    if (loaiCt === '3') {
      payload.detail[0].ma_kh_i =
        invoiceData.ma_kh || cashioData.partner_code || '';
    }

    return payload;
  }
}
