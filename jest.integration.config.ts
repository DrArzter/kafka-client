export default {
  preset: "ts-jest",
  testEnvironment: "node",
  rootDir: "src",
  testMatch: ["**/integration/**/*.integration.spec.ts"],
  testTimeout: 180_000,
  // Prevent Jest from using src/__mocks__/ (unit test mocks)
  modulePathIgnorePatterns: ["<rootDir>/__mocks__"],
  globalSetup: "<rootDir>/integration/global-setup.ts",
  globalTeardown: "<rootDir>/integration/global-teardown.ts",
  // Run sequentially — tests share one Kafka container
  maxWorkers: 1,
};
