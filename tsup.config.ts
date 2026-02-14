import { defineConfig } from "tsup";

export default defineConfig({
  clean: true,
  dts: true,
  entry: ["src/index.ts", "src/core.ts"],
  format: ["cjs", "esm"],
  external: [
    '@nestjs/common',
    '@nestjs/core',
    'reflect-metadata',
    'rxjs',
  ],
  sourcemap: true,
  target: 'es2023',
});
