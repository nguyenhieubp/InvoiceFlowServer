import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { PlatformFeeService } from './platform-fee.service';
import { PlatformFeeController } from './platform-fee.controller';
import { PlatformFee } from '../../entities/platform-fee.entity';

@Module({
  imports: [TypeOrmModule.forFeature([PlatformFee])],
  controllers: [PlatformFeeController],
  providers: [PlatformFeeService],
  exports: [PlatformFeeService],
})
export class PlatformFeeModule {}
