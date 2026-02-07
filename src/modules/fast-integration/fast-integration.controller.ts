import { Controller, Post, Body, BadRequestException } from '@nestjs/common';
import { FastIntegrationService } from './fast-integration.service';

@Controller('fast-integration')
export class FastIntegrationController {
    constructor(
        private readonly fastIntegrationService: FastIntegrationService,
    ) { }

    @Post('po-charges')
    async syncPOCharges(@Body() payload: any) {
        // Basic validation
        if (!payload.master || !payload.detail) {
            throw new BadRequestException('Payload missing master or detail');
        }

        return this.fastIntegrationService.syncPOCharges(payload);
    }
}
