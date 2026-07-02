export default {
  preset: "ts-jest",
  testEnvironment: "node",
  rootDir: "src",
  testMatch: ["**/chaos/**/*.chaos.spec.ts"],
  // Chaos tests inject real failures (broker pause/restart, rebalance under
  // load, poison messages, lag throttling). They are deliberately slow and
  // MUST run via `npm run test:chaos`, never as part of the default gate.
  testTimeout: 240_000,
  // Prevent Jest from using src/__mocks__/ (unit test mocks)
  modulePathIgnorePatterns: ["<rootDir>/__mocks__"],
  // Each spec starts and manipulates its OWN Kafka container in beforeAll, so
  // there is no shared global setup. Tests must run serially — several of them
  // pause/restart the broker process, which would break any concurrent suite.
  maxWorkers: 1,
  transform: {
    "^.+\\.tsx?$": ["ts-jest", { tsconfig: "./tsconfig.spec.json" }],
  },
};
