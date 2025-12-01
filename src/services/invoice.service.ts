import {
  Injectable,
  Logger,
  NotFoundException,
  BadRequestException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Invoice } from '../entities/invoice.entity';
import { InvoiceItem } from '../entities/invoice-item.entity';
import { Sale } from '../entities/sale.entity';
import { CreateInvoiceDto, InvoiceItemDto } from '../dto/create-invoice.dto';
import { InvoicePrintService } from './invoice-print.service';

@Injectable()
export class InvoiceService {
  private readonly logger = new Logger(InvoiceService.name);

  constructor(
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
    @InjectRepository(InvoiceItem)
    private invoiceItemRepository: Repository<InvoiceItem>,
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    private invoicePrintService: InvoicePrintService,
  ) {}

  async createInvoice(dto: CreateInvoiceDto): Promise<Invoice> {
    // Tính toán các giá trị
    const items = dto.items.map((item) => {
      const amount = item.quantity * item.price;
      const discountAmount =
        (amount * (item.discountRate || 0)) / 100;
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
    const key = dto.key || `INV_${Date.now()}_${dto.customerCode.substring(0, 10)}`.substring(
      0,
      32,
    );

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

  async getInvoice(id: string): Promise<
    Invoice & {
      fastStatus: 'printed' | 'pending' | 'missing';
      fastStatusMessage?: string;
    }
  > {
    const invoice = await this.invoiceRepository.findOne({
      where: { id },
      relations: ['items'],
    });

    if (!invoice) {
      throw new NotFoundException(`Invoice with ID ${id} not found`);
    }

    return this.attachFastStatus(invoice);
  }

  async downloadInvoicePdf(id: string): Promise<{ buffer: Buffer; fileName: string }> {
    const invoice = await this.getInvoice(id);
    if (!invoice.printResponse) {
      throw new BadRequestException('Hóa đơn chưa có dữ liệu in');
    }

    const keySearch = this.extractKeySearch(invoice.printResponse);
    if (!keySearch) {
      throw new BadRequestException('Không tìm thấy thông tin hóa đơn thật (keySearch)');
    }

    const base64Data = await this.invoicePrintService.downloadInvoicePdf(keySearch);
    const buffer = Buffer.from(base64Data, 'base64');

    return {
      buffer,
      fileName: `${invoice.key || invoice.id}.pdf`,
    };
  }

  private extractKeySearch(printResponse: string): string | null {
    try {
      const parsed = JSON.parse(printResponse);
      if (parsed?.Message) {
        try {
          const messageData = JSON.parse(parsed.Message);
          if (Array.isArray(messageData) && messageData[0]?.keySearch) {
            return messageData[0].keySearch;
          }
        } catch (messageParseError) {
          this.logger.warn(`Không parse được Message trong printResponse: ${messageParseError}`);
        }
      }
      if (parsed?.Data && Array.isArray(parsed.Data) && parsed.Data[0]?.keySearch) {
        return parsed.Data[0].keySearch;
      }
    } catch (error) {
      this.logger.warn(`Không parse được printResponse để lấy keySearch: ${error}`);
    }
    return null;
  }

  private async attachFastStatus(
    invoice: Invoice,
  ): Promise<
    Invoice & {
      fastStatus: 'printed' | 'pending' | 'missing';
      fastStatusMessage?: string;
    }
  > {
    try {
      const result = await this.invoicePrintService.checkInvoiceStatus(
        invoice.key,
        invoice.invoiceDate,
      );

      let fastStatus: 'printed' | 'pending' | 'missing' = 'missing';
      if (result.taxStatus === 3) {
        fastStatus = 'printed';
      } else if (result.taxStatus === 0) {
        fastStatus = 'pending';
      } else {
        fastStatus = 'missing';
      }

      return {
        ...invoice,
        fastStatus,
        fastStatusMessage: result.record?.feedbackContent || undefined,
      };
    } catch (error: any) {
      return {
        ...invoice,
        fastStatus: 'missing',
        fastStatusMessage:
          error?.message || 'Không thể kiểm tra trạng thái hóa đơn',
      };
    }
  }

  async getAllInvoices(): Promise<
    Array<
      Invoice & {
        fastStatus: 'printed' | 'pending' | 'missing';
        fastStatusMessage?: string;
      }
    >
  > {
    const invoices = await this.invoiceRepository.find({
      relations: ['items'],
      order: { createdAt: 'DESC' },
    });

    const invoicesWithStatus = await Promise.all(
      invoices.map((invoice) => this.attachFastStatus(invoice)),
    );

    return invoicesWithStatus;
  }

  async updateInvoice(id: string, dto: Partial<CreateInvoiceDto>): Promise<Invoice> {
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
      const totalDiscount = items.reduce((sum, item) => sum + item.discountAmount, 0);
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

  async printInvoice(id: string): Promise<any> {
    const invoice = await this.getInvoice(id);

    if (invoice.isPrinted) {
      this.logger.warn(`Invoice ${id} đã được in rồi`);
    }

    const result = await this.invoicePrintService.printInvoice(invoice);

    // Cập nhật trạng thái
    invoice.isPrinted = true;
    invoice.printResponse = JSON.stringify(result);
    await this.invoiceRepository.save(invoice);

    return result;
  }

  private parseDate(dateStr: string): Date {
    // Format: DD/MM/YYYY
    const [day, month, year] = dateStr.split('/');
    return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
  }

  private numberToWords(num: number): string {
    // Hàm chuyển số thành chữ tiếng Việt (đơn giản)
    // Có thể cải thiện sau
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

