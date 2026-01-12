import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';
import * as express from 'express';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Enable CORS for frontend
  app.enableCors({
    origin: [
      process.env.FRONTEND_URL,
      'http://localhost:3000',
      'https://invoiceflow.vn',
    ],
    credentials: true,
  });

  // Set timeout 5 phút (300000ms) cho route export-orders
  app.use('/sales/export-orders', (req, res, next) => {
    req.setTimeout(300000); // 5 phút
    res.setTimeout(300000); // 5 phút
    next();
  });

  // Enable validation
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
    }),
  );

  const port = process.env.PORT ?? 3000;
  const server = await app.listen(port);

  // Set timeout 5 phút (300000ms) cho tất cả requests
  server.timeout = 300000; // 5 phút
  server.keepAliveTimeout = 300000; // 5 phút

  console.log(`Application is running on: http://localhost:${port}`);
}
bootstrap();
