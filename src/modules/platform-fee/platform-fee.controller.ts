import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  Query,
} from '@nestjs/common';
import { PlatformFeeService } from './platform-fee.service';
import { CreatePlatformFeeDto } from './dto/create-platform-fee.dto';
import { UpdatePlatformFeeDto } from './dto/update-platform-fee.dto';

@Controller('platform-fees')
export class PlatformFeeController {
  constructor(private readonly platformFeeService: PlatformFeeService) {}

  @Post()
  create(@Body() createPlatformFeeDto: CreatePlatformFeeDto) {
    return this.platformFeeService.create(createPlatformFeeDto);
  }

  @Get()
  findAll(
    @Query('brand') brand?: string,
    @Query('page') page?: number,
    @Query('limit') limit?: number,
    @Query('search') search?: string,
  ) {
    return this.platformFeeService.findAll({ brand, page, limit, search });
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.platformFeeService.findOne(id);
  }

  @Patch(':id')
  update(
    @Param('id') id: string,
    @Body() updatePlatformFeeDto: UpdatePlatformFeeDto,
  ) {
    return this.platformFeeService.update(id, updatePlatformFeeDto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.platformFeeService.remove(id);
  }
}
