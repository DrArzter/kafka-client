export default {
  preset: "ts-jest",
  testEnvironment: "node",
  rootDir: "src",
  testMatch: ["**/integration/**/*.spec.ts"],
  testTimeout: 180_000,
  // Prevent Jest from using src/__mocks__/ (unit test mocks)
  modulePathIgnorePatterns: ["<rootDir>/__mocks__"],
};
