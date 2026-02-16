import { defineConfig } from "tsup";

export default defineConfig({
  clean: true,
  dts: true,
  entry: ["src/index.ts", "src/core.ts", "src/testing.ts"],
  format: ["cjs", "esm"],
  external: [
    '@nestjs/common',
    '@nestjs/core',
    'reflect-metadata',
    'rxjs',
    '@testcontainers/kafka',
    'testcontainers',
    'kafkajs',
  ],
  sourcemap: true,
  target: 'es2023',
});
