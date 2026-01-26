import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { PlatformFeeImportService } from './platform-fee-import.service';
import { PlatformFeeImportController } from './platform-fee-import.controller';
import { PlatformFeeImport } from '../../entities/platform-fee-import.entity';

@Module({
  imports: [TypeOrmModule.forFeature([PlatformFeeImport])],
  controllers: [PlatformFeeImportController],
  providers: [PlatformFeeImportService],
  exports: [PlatformFeeImportService],
})
export class PlatformFeeImportModule {}
