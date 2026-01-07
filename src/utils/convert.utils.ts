export const convertDate = (input: string): string => {
    if (!input || input.length !== 9) return 'XXX';

    const day = input.slice(0, 2);
    const monthStr = input.slice(2, 5).toUpperCase();
    const year = input.slice(5);

    const monthMap: Record<string, string> = {
        JAN: '01',
        FEB: '02',
        MAR: '03',
        APR: '04',
        MAY: '05',
        JUN: '06',
        JUL: '07',
        AUG: '08',
        SEP: '09',
        OCT: '10',
        NOV: '11',
        DEC: '12',
    };

    const month = monthMap[monthStr];
    if (!month) return 'XXX';

    return `${day}/${month}/${year}`;
}

export const formatDocDate = (input: string): string => {
    // input: "2026-01-07 01:09:54"
    const d = new Date(input.replace(' ', 'T'));

    const dd = String(d.getDate()).padStart(2, '0');
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const yyyy = d.getFullYear();
    const hh = String(d.getHours()).padStart(2, '0');
    const mi = String(d.getMinutes()).padStart(2, '0');

    return `${dd}-${mm}-${yyyy} ${hh}:${mi}`;
}

export const convertOrderToOrderLineFormat = (order: any) => {
    if (!Array.isArray(order?.lines)) return [];
  
    return order.lines.map((line: any, index: number) => ({
      id: null,
      doctype: 'SALE_ORDER',
      code: order.order_name,
      docdate: formatDocDate(order.order_date),
  
      branch_code: order.warehouse?.code || null,
      ordertype_name: null,
      description: order.classify_order || null,
      shift_code: null,
  
      partner_name: order.customer_source?.name || null,
      partner_code: order.customer_source?.ref || null,
      partner_grade: null,
      partner_mobile: order.customer_source?.phone || null,
  
      saleperson_code: null,
  
      itemcode: line.product_code,
      itemname: line.product_name,
      pkg_code: null,
      prom_code: null,
      producttype: 'S',
      serial: null,
  
      // ERP line thường là âm (xuất bán)
      qty: -Math.abs(line.quantity || 0),
      price: line.price_unit || 0,
  
      discamt: line.x_discount_ecom || 0,
      grade_discamt: 0,
      other_discamt: 0,
  
      mn_linetotal: line.subtotal || 0,
      v_paid: 0,
      revenue: line.subtotal || 0,
  
      so_source: order.ecom?.name || null,
      social_page_id: order.ecom?.code || null,
      sp_email: null,
      mvc_serial: order.x_order_dms || null,
      vc_promotion_code: null,
    }));
  };
  