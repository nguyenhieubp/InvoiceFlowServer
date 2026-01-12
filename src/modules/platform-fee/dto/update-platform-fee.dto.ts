import { PartialType } from '@nestjs/mapped-types';
import { CreatePlatformFeeDto } from './create-platform-fee.dto';

export class UpdatePlatformFeeDto extends PartialType(CreatePlatformFeeDto) {}
