import { TypeOrmModuleOptions } from '@nestjs/typeorm';
import { ConfigService } from '@nestjs/config';

/**
 * Primary Database Configuration (Default)
 * Host: 103.145.79.36
 */
export const getDatabaseConfig = (
  configService: ConfigService,
): TypeOrmModuleOptions => ({
  type: 'postgres',
  host: configService.get('DB_HOST', 'localhost'),
  port: configService.get('DB_PORT', 5432),
  username: configService.get('DB_USERNAME', 'postgres'),
  password: configService.get('DB_PASSWORD', '123456'),
  database: configService.get('DB_NAME', 'postgres'),
  entities: [__dirname + '/../**/*.entity{.ts,.js}'],
  synchronize: true,
  logging: configService.get('NODE_ENV') === 'development',
  name: 'default', // Primary connection
});
/**
 * Secondary Database Configuration
 * Host: 103.145.79.165
 */
export const getSecondaryDatabaseConfig = (
  configService: ConfigService,
): TypeOrmModuleOptions => ({
  type: 'postgres',
  host: configService.get('DB2_HOST', 'localhost'),
  port: configService.get('DB2_PORT', 5432),
  username: configService.get('DB2_USERNAME', 'postgres'),
  password: configService.get('DB2_PASSWORD', '123456'),
  database: configService.get('DB2_NAME', 'postgres'),
  entities: [],
  synchronize: false,
  logging: configService.get('NODE_ENV') === 'development',
  name: 'secondary', // Secondary connection
});

/**
 * Third Database Configuration
 * TODO: Update with actual host information
 * Host: 103.145.79.37
 */
export const getThirdDatabaseConfig = (
  configService: ConfigService,
): TypeOrmModuleOptions => ({
  type: 'postgres',
  host: configService.get('DB3_HOST', 'localhost'),
  port: configService.get('DB3_PORT', 5432),
  username: configService.get('DB3_USERNAME', 'postgres'),
  password: configService.get('DB3_PASSWORD', '123456'),
  database: configService.get('DB3_NAME', 'postgres'),
  entities: [],
  synchronize: false,
  logging: configService.get('NODE_ENV') === 'development',
  name: 'third', // Third connection
});
