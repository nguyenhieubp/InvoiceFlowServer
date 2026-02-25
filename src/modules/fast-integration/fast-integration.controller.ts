import { Controller, Post, Body, BadRequestException, Get, Query, Param, Delete } from '@nestjs/common';
import { FastIntegrationService } from './fast-integration.service';

@Controller('fast-integration')
export class FastIntegrationController {
    constructor(
        private readonly fastIntegrationService: FastIntegrationService,
    ) { }

    @Get('audit')
    async getAuditLogs(
        @Query('search') search?: string,
        @Query('dateFrom') dateFrom?: string,
        @Query('dateTo') dateTo?: string,
        @Query('status') status?: string,
    ) {
        return this.fastIntegrationService.getAuditLogs(search, dateFrom, dateTo, status);
    }

    @Post('retry/:id')
    async retrySync(@Param('id') id: string) {
        return this.fastIntegrationService.retrySync(Number(id));
    }

    @Delete('audit/:id')
    async deleteAuditLog(@Param('id') id: string) {
        return this.fastIntegrationService.deleteAuditLog(Number(id));
    }

    @Delete('audit')
    async deleteAuditLogsByDateRange(
        @Query('startDate') startDate: string,
        @Query('endDate') endDate: string,
        @Query('status') status?: string,
    ) {
        if (!startDate || !endDate) {
            throw new BadRequestException('startDate và endDate là bắt buộc');
        }
        return this.fastIntegrationService.deleteAuditLogsByDateRange(startDate, endDate, status);
    }

    @Post('po-charges')
    async syncPOCharges(@Body() payload: any) {
        // Basic validation
        if (!payload.master || !payload.detail) {
            throw new BadRequestException('Payload missing master or detail');
        }

        return this.fastIntegrationService.syncPOCharges(payload);
    }

    @Post('po-charges/batch-sync')
    async batchSyncPOCharges(@Body() payload: { startDate: string; endDate: string; platform?: string }) {
        if (!payload.startDate || !payload.endDate) {
            throw new BadRequestException('startDate and endDate are required');
        }

        return this.fastIntegrationService.batchSyncPOCharges(payload.startDate, payload.endDate, payload.platform);
    }
}

