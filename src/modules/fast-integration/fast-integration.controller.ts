import { Controller, Post, Body, BadRequestException, Get, Query, Param } from '@nestjs/common';
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
    ) {
        return this.fastIntegrationService.getAuditLogs(search, dateFrom, dateTo);
    }

    @Post('retry/:id')
    async retrySync(@Param('id') id: string) {
        return this.fastIntegrationService.retrySync(Number(id));
    }

    @Post('po-charges')
    async syncPOCharges(@Body() payload: any) {
        // Basic validation
        if (!payload.master || !payload.detail) {
            throw new BadRequestException('Payload missing master or detail');
        }

        return this.fastIntegrationService.syncPOCharges(payload);
    }
}
