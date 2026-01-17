import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { FastApiInvoice } from '../../entities/fast-api-invoice.entity';
import { Sale } from '../../entities/sale.entity';
import { Invoice } from '../../entities/invoice.entity';

/**
 * InvoicePersistenceService
 * Chịu trách nhiệm: Database persistence operations cho invoices
 */
@Injectable()
export class InvoicePersistenceService {
  private readonly logger = new Logger(InvoicePersistenceService.name);

  constructor(
    @InjectRepository(FastApiInvoice)
    private fastApiInvoiceRepository: Repository<FastApiInvoice>,
    @InjectRepository(Sale)
    private saleRepository: Repository<Sale>,
    @InjectRepository(Invoice)
    private invoiceRepository: Repository<Invoice>,
  ) {}

  /**
   * Lưu hoặc cập nhật FastApiInvoice record
   */
  async saveFastApiInvoice(data: {
    docCode: string;
    maDvcs?: string;
    maKh?: string;
    tenKh?: string;
    ngayCt?: Date;
    status: number;
    message?: string;
    guid?: string | null;
    fastApiResponse?: string;
  }): Promise<FastApiInvoice> {
    try {
      // Kiểm tra xem đã có chưa
      const existing = await this.fastApiInvoiceRepository.findOne({
        where: { docCode: data.docCode },
      });

      if (existing) {
        // Cập nhật record hiện có
        existing.status = data.status;
        existing.message = data.message || existing.message;
        existing.guid = data.guid || existing.guid;
        existing.fastApiResponse =
          data.fastApiResponse || existing.fastApiResponse;
        if (data.maDvcs) existing.maDvcs = data.maDvcs;
        if (data.maKh) existing.maKh = data.maKh;
        if (data.tenKh) existing.tenKh = data.tenKh;
        if (data.ngayCt) existing.ngayCt = data.ngayCt;

        const saved = await this.fastApiInvoiceRepository.save(existing);
        return Array.isArray(saved) ? saved[0] : saved;
      } else {
        // Tạo mới
        const fastApiInvoice = this.fastApiInvoiceRepository.create({
          docCode: data.docCode,
          maDvcs: data.maDvcs ?? null,
          maKh: data.maKh ?? null,
          tenKh: data.tenKh ?? null,
          ngayCt: data.ngayCt ?? new Date(),
          status: data.status,
          message: data.message ?? null,
          guid: data.guid ?? null,
          fastApiResponse: data.fastApiResponse ?? null,
        } as Partial<FastApiInvoice>);

        const saved = await this.fastApiInvoiceRepository.save(fastApiInvoice);
        return Array.isArray(saved) ? saved[0] : saved;
      }
    } catch (error: any) {
      this.logger.error(
        `Error saving FastApiInvoice for ${data.docCode}: ${error?.message || error}`,
      );
      throw error;
    }
  }

  /**
   * Đánh dấu đơn hàng là đã xử lý
   */
  async markOrderAsProcessed(docCode: string): Promise<void> {
    // Tìm tất cả các sale có cùng docCode
    const sales = await this.saleRepository.find({
      where: { docCode },
    });

    // Cập nhật isProcessed = true cho tất cả các sale
    if (sales.length > 0) {
      await this.saleRepository.update({ docCode }, { isProcessed: true });
    }
  }

  /**
   * Đánh dấu lại các đơn hàng đã có invoice là đã xử lý
   * Method này dùng để xử lý các invoice đã được tạo trước đó
   */
  async markProcessedOrdersFromInvoices(): Promise<{
    updated: number;
    message: string;
  }> {
    // Tìm tất cả các invoice đã được in (isPrinted = true)
    const invoices = await this.invoiceRepository.find({
      where: { isPrinted: true },
    });

    let updatedCount = 0;
    const processedDocCodes = new Set<string>();

    // Duyệt qua các invoice và tìm docCode từ key
    // Key có thể là docCode hoặc có format INV_xxx_xxx
    for (const invoice of invoices) {
      let docCode: string | null = null;

      // Thử 1: Key chính là docCode (cho các invoice mới)
      const salesByKey = await this.saleRepository.find({
        where: { docCode: invoice.key },
        take: 1,
      });
      if (salesByKey.length > 0) {
        docCode = invoice.key;
      } else {
        // Thử 2: Tìm trong printResponse xem có docCode không
        try {
          if (invoice.printResponse) {
            const printResponse = JSON.parse(invoice.printResponse);

            // Tìm trong Message (là JSON string chứa array)
            if (printResponse.Message) {
              try {
                const messageData = JSON.parse(printResponse.Message);
                if (Array.isArray(messageData) && messageData.length > 0) {
                  const data = messageData[0];
                  if (data.key) {
                    // Extract docCode từ key (format: SO52.00005808_X -> SO52.00005808)
                    const keyParts = data.key.split('_');
                    if (keyParts.length > 0) {
                      const potentialDocCode = keyParts[0];
                      const salesByPotentialKey =
                        await this.saleRepository.find({
                          where: { docCode: potentialDocCode },
                          take: 1,
                        });
                      if (salesByPotentialKey.length > 0) {
                        docCode = potentialDocCode;
                      }
                    }
                  }
                }
              } catch (msgError) {
                // Message không phải JSON string, bỏ qua
              }
            }

            // Thử tìm trong Data nếu có
            if (
              !docCode &&
              printResponse.Data &&
              Array.isArray(printResponse.Data) &&
              printResponse.Data.length > 0
            ) {
              const data = printResponse.Data[0];
              if (data.key) {
                const keyParts = data.key.split('_');
                if (keyParts.length > 0) {
                  const potentialDocCode = keyParts[0];
                  const salesByPotentialKey = await this.saleRepository.find({
                    where: { docCode: potentialDocCode },
                    take: 1,
                  });
                  if (salesByPotentialKey.length > 0) {
                    docCode = potentialDocCode;
                  }
                }
              }
            }
          }
        } catch (error) {
          // Ignore parse errors
        }
      }

      // Nếu tìm thấy docCode, đánh dấu các sale là đã xử lý
      if (docCode && !processedDocCodes.has(docCode)) {
        const updateResult = await this.saleRepository.update(
          { docCode },
          { isProcessed: true },
        );
        if (updateResult.affected && updateResult.affected > 0) {
          updatedCount += updateResult.affected;
          processedDocCodes.add(docCode);
        }
      }
    }

    return {
      updated: updatedCount,
      message: `Đã đánh dấu ${processedDocCodes.size} đơn hàng là đã xử lý (${updatedCount} sale records)`,
    };
  }
}
