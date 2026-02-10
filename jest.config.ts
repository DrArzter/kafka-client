import type { Config } from "jest";

const config: Config = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["<rootDir>/src"],
  testMatch: ["**/*.spec.ts"],
  testPathIgnorePatterns: ["/node_modules/", "/integration/"],
};

export default config;
