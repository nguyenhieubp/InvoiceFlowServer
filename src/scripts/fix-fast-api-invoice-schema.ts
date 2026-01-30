import { DataSource } from 'typeorm';
import { ConfigService } from '@nestjs/config';
import { getDatabaseConfig } from '../config/database.config';
import * as dotenv from 'dotenv';
import * as path from 'path';

// Load env explicitly
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const configService = new ConfigService();
const dbConfig = getDatabaseConfig(configService);

const dataSource = new DataSource(dbConfig as any);

async function fixSchema() {
  try {
    console.log('Connecting to database...');
    await dataSource.initialize();
    console.log('Connected!');

    const queryRunner = dataSource.createQueryRunner();

    console.log(
      'Checking and adding missing columns to "fast_api_invoices"...',
    );

    // Verify columns explicitly
    const columns = await queryRunner.getTable('fast_api_invoices');
    console.log(
      'Current columns in fast_api_invoices:',
      columns?.columns.map((c) => c.name),
    );

    // Check strict existence
    const lastErrorCol = columns?.columns.find(
      (c) => c.name.toLowerCase() === 'lasterrormessage',
    );
    if (lastErrorCol) {
      console.log(`Column found: "${lastErrorCol.name}"`);
    } else {
      console.log('Column "lastErrorMessage" NOT found. Adding...');
      await queryRunner.query(
        `ALTER TABLE "fast_api_invoices" ADD COLUMN "lastErrorMessage" text`,
      );
    }

    // Add payload
    const payloadCol = columns?.columns.find(
      (c) => c.name.toLowerCase() === 'payload',
    );
    if (!payloadCol) {
      console.log('Adding column: payload');
      await queryRunner.query(
        `ALTER TABLE "fast_api_invoices" ADD COLUMN "payload" text`,
      );
    } else {
      console.log('Column payload already exists.');
    }

    // Add type
    const typeCol = columns?.columns.find(
      (c) => c.name.toLowerCase() === 'type',
    );
    if (!typeCol) {
      console.log('Adding column: type');
      await queryRunner.query(
        `ALTER TABLE "fast_api_invoices" ADD COLUMN "type" varchar`,
      );
    } else {
      console.log('Column type already exists.');
    }

    // Add isManuallyCreated
    const manualCol = columns?.columns.find(
      (c) => c.name.toLowerCase() === 'ismanuallycreated',
    ); // Check lower case comparison
    if (!manualCol) {
      console.log('Adding column: isManuallyCreated');
      await queryRunner.query(
        `ALTER TABLE "fast_api_invoices" ADD COLUMN "isManuallyCreated" boolean DEFAULT false`,
      );
    } else {
      console.log('Column isManuallyCreated already exists.');
    }

    console.log('Schema patch completed successfully.');
    await dataSource.destroy();
  } catch (error) {
    console.error('Error updating schema:', error);
    process.exit(1);
  }
}

fixSchema();
