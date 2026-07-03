import { defineConfig } from "tsup";

export default defineConfig({
  clean: true,
  // Declarations are emitted by `tsc -p tsconfig.build.json` (see the build
  // script) — tsup's bundled-dts path injects a deprecated `baseUrl` that
  // TypeScript 6 rejects, so it is not used at all.
  dts: false,
  entry: [
    "src/index.ts",
    "src/core.ts",
    "src/testing.ts",
    "src/otel.ts",
    "src/serde.ts",
    "src/cli/index.ts",
  ],
  format: ["cjs", "esm"],
  external: [
    '@nestjs/common',
    '@nestjs/core',
    'reflect-metadata',
    'rxjs',
    '@testcontainers/kafka',
    'testcontainers',
    'kafkajs',
    '@opentelemetry/api',
    'avsc',
    'protobufjs',
  ],
  sourcemap: true,
  target: 'es2023',
});
