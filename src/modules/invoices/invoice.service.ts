import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Invoice } from '../../entities/invoice.entity';
import { InvoiceItem } from '../../entities/invoice-item.entity';
import { CreateInvoiceDto } from '../../dto/create-invoice.dto';

@Injectable()
export class InvoiceService {
  constructor(
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    @InjectRepository(InvoiceItem)
    private invoiceItemRepository: Repository<InvoiceItem>,
  ) {}

  async createInvoice(dto: CreateInvoiceDto): Promise<Invoice> {
    // Tính toán các giá trị
    const items = dto.items.map((item) => {
      const amount = item.quantity * item.price;
      const discountAmount = (amount * (item.discountRate || 0)) / 100;
      const amountAfterDiscount = amount - discountAmount;
      const taxAmount = (amountAfterDiscount * (item.taxRate || 0)) / 100;

      return {
        ...item,
        amount: amountAfterDiscount,
        discountAmount,
        taxAmount,
        uom: item.uom || 'Pcs',
        taxRate: item.taxRate || 0,
        discountRate: item.discountRate || 0,
      };
    });

    const totalAmount = items.reduce((sum, item) => sum + item.amount, 0);
    const totalDiscount = items.reduce(
      (sum, item) => sum + item.discountAmount,
      0,
    );
    const totalTax = items.reduce((sum, item) => sum + item.taxAmount, 0);
    const totalAmountWithTax = totalAmount + totalTax;

    // Tạo key cho invoice - sử dụng key từ DTO nếu có, nếu không thì tự động tạo
    const invoiceDate = dto.invoiceDate
      ? this.parseDate(dto.invoiceDate)
      : new Date();
    const key =
      dto.key ||
      `INV_${Date.now()}_${dto.customerCode.substring(0, 10)}`.substring(0, 32);

    // Tạo invoice
    const invoice = this.invoiceRepository.create({
      key,
      invoiceDate,
      customerCode: dto.customerCode,
      customerName: dto.customerName,
      customerTaxCode: dto.customerTaxCode || '',
      address: dto.address || '.',
      phoneNumber: dto.phoneNumber || '',
      idCardNo: dto.idCardNo || '',
      currency: 'VND',
      exchangeRate: 1.0,
      amount: totalAmount,
      discountAmount: totalDiscount,
      taxAmount: totalTax,
      totalAmount: totalAmountWithTax,
      amountInWords: this.numberToWords(totalAmountWithTax),
      humanName: 'SYSTEM',
      voucherBook: dto.voucherBook,
      isPrinted: false,
    });

    const savedInvoice = await this.invoiceRepository.save(invoice);

    // Tạo invoice items
    const invoiceItems = items.map((item) =>
      this.invoiceItemRepository.create({
        processType: '1',
        itemCode: item.itemCode,
        itemName: item.itemName,
        uom: item.uom,
        quantity: item.quantity,
        price: item.price,
        amount: item.amount,
        taxRate: item.taxRate,
        taxAmount: item.taxAmount,
        discountRate: item.discountRate,
        discountAmount: item.discountAmount,
        invoiceId: savedInvoice.id,
      }),
    );

    await this.invoiceItemRepository.save(invoiceItems);

    const result = await this.invoiceRepository.findOne({
      where: { id: savedInvoice.id },
      relations: ['items'],
    });

    if (!result) {
      throw new NotFoundException('Invoice not found after creation');
    }

    return result;
  }

  async getInvoice(id: string): Promise<Invoice> {
    const invoice = await this.invoiceRepository.findOne({
      where: { id },
      relations: ['items'],
    });

    if (!invoice) {
      throw new NotFoundException(`Invoice with ID ${id} not found`);
    }

    return invoice;
  }

  async getAllInvoices(): Promise<Invoice[]> {
    return this.invoiceRepository.find({
      relations: ['items'],
      order: { createdAt: 'DESC' },
    });
  }

  async updateInvoice(
    id: string,
    dto: Partial<CreateInvoiceDto>,
  ): Promise<Invoice> {
    const invoice = await this.getInvoice(id);

    if (dto.customerName) invoice.customerName = dto.customerName;
    if (dto.customerTaxCode) invoice.customerTaxCode = dto.customerTaxCode;
    if (dto.address) invoice.address = dto.address;
    if (dto.phoneNumber) invoice.phoneNumber = dto.phoneNumber;
    if (dto.idCardNo) invoice.idCardNo = dto.idCardNo;
    if (dto.voucherBook) invoice.voucherBook = dto.voucherBook;
    if (dto.invoiceDate) invoice.invoiceDate = this.parseDate(dto.invoiceDate);

    // Nếu có items mới, cập nhật lại
    if (dto.items && dto.items.length > 0) {
      // Xóa items cũ
      await this.invoiceItemRepository.delete({ invoiceId: invoice.id });

      // Tính toán lại
      const items = dto.items.map((item) => {
        const amount = item.quantity * item.price;
        const discountAmount = (amount * (item.discountRate || 0)) / 100;
        const amountAfterDiscount = amount - discountAmount;
        const taxAmount = (amountAfterDiscount * (item.taxRate || 0)) / 100;

        return {
          ...item,
          amount: amountAfterDiscount,
          discountAmount,
          taxAmount,
          uom: item.uom || 'Pcs',
          taxRate: item.taxRate || 0,
          discountRate: item.discountRate || 0,
        };
      });

      const totalAmount = items.reduce((sum, item) => sum + item.amount, 0);
      const totalDiscount = items.reduce(
        (sum, item) => sum + item.discountAmount,
        0,
      );
      const totalTax = items.reduce((sum, item) => sum + item.taxAmount, 0);
      const totalAmountWithTax = totalAmount + totalTax;

      invoice.amount = totalAmount;
      invoice.discountAmount = totalDiscount;
      invoice.taxAmount = totalTax;
      invoice.totalAmount = totalAmountWithTax;
      invoice.amountInWords = this.numberToWords(totalAmountWithTax);

      // Tạo items mới
      const invoiceItems = items.map((item) =>
        this.invoiceItemRepository.create({
          processType: '1',
          itemCode: item.itemCode,
          itemName: item.itemName,
          uom: item.uom,
          quantity: item.quantity,
          price: item.price,
          amount: item.amount,
          taxRate: item.taxRate,
          taxAmount: item.taxAmount,
          discountRate: item.discountRate,
          discountAmount: item.discountAmount,
          invoiceId: invoice.id,
        }),
      );

      await this.invoiceItemRepository.save(invoiceItems);
    }

    return this.invoiceRepository.save(invoice);
  }

  private parseDate(dateStr: string): Date {
    // Format: DD/MM/YYYY
    const [day, month, year] = dateStr.split('/');
    return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
  }

  private numberToWords(num: number): string {
    if (num === 0) return 'không';

    let result = '';
    const numStr = Math.floor(num).toString().padStart(9, '0');

    // Triệu
    if (numStr.substring(0, 3) !== '000') {
      result += this.readThreeDigits(numStr.substring(0, 3)) + ' triệu ';
    }

    // Nghìn
    if (numStr.substring(3, 6) !== '000') {
      result += this.readThreeDigits(numStr.substring(3, 6)) + ' nghìn ';
    }

    // Đơn vị
    if (numStr.substring(6, 9) !== '000') {
      result += this.readThreeDigits(numStr.substring(6, 9));
    }

    return result.trim() + ' đồng chẵn';
  }

  private readThreeDigits(str: string): string {
    const ones = [
      '',
      'một',
      'hai',
      'ba',
      'bốn',
      'năm',
      'sáu',
      'bảy',
      'tám',
      'chín',
    ];
    const tens = [
      '',
      'mười',
      'hai mươi',
      'ba mươi',
      'bốn mươi',
      'năm mươi',
      'sáu mươi',
      'bảy mươi',
      'tám mươi',
      'chín mươi',
    ];
    const hundreds = [
      '',
      'một trăm',
      'hai trăm',
      'ba trăm',
      'bốn trăm',
      'năm trăm',
      'sáu trăm',
      'bảy trăm',
      'tám trăm',
      'chín trăm',
    ];

    const h = parseInt(str[0]);
    const t = parseInt(str[1]);
    const o = parseInt(str[2]);

    let result = '';

    if (h > 0) {
      result += hundreds[h] + ' ';
    }

    if (t > 1) {
      result += tens[t] + ' ';
      if (o > 0) result += ones[o];
    } else if (t === 1) {
      result += 'mười ';
      if (o > 0) result += ones[o];
    } else if (o > 0) {
      result += ones[o];
    }

    return result.trim();
  }
}
