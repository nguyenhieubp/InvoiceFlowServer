import {
  Controller,
  Post,
  Body,
  Res,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import express from 'express';
import { FastApiInvoiceService } from '../fast-api-invoice.service';

interface FastApiStatusPayload {
  docCode: string;
  status: number;
  message?: string;
  fastApiResponse?: any; // The full response from Fast API if available
  payload?: any; // The original payload if passed back
}

@Controller('sales/webhooks')
export class WebhookController {
  private readonly logger = new Logger(WebhookController.name);

  constructor(private readonly fastApiInvoiceService: FastApiInvoiceService) {}

  @Post('fast-api-status')
  async updateFastApiStatus(
    @Body() body: FastApiStatusPayload,
    @Res() res: express.Response,
  ) {
    try {
      this.logger.log(
        `[Webhook] Received status update for ${body.docCode}: Status ${body.status}`,
      );

      if (!body.docCode) {
        return res.status(HttpStatus.BAD_REQUEST).json({
          success: false,
          message: 'docCode is required',
        });
      }

      await this.fastApiInvoiceService.updateFastApiStatus(
        body.docCode,
        body.status,
        body.message,
        body.fastApiResponse,
        body.payload,
      );

      return res.status(HttpStatus.OK).json({
        success: true,
        message: 'Status updated successfully',
      });
    } catch (error: any) {
      this.logger.error(
        `[Webhook] Error processing callback for ${body?.docCode}: ${error?.message}`,
      );
      return res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
        success: false,
        message: 'Internal server error processing webhook',
      });
    }
  }
}
