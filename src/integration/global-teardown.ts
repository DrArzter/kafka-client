import * as fs from "fs";
import * as path from "path";

const BROKER_FILE = path.join(__dirname, ".integration-brokers.tmp");

export default async function globalTeardown() {
  const container = (globalThis as any).__KAFKA_CONTAINER__;
  if (container) {
    // Never let a stop() failure keep the process (and the container) alive —
    // orphans are reaped by Testcontainers' ryuk, and `npm run containers:clean`
    // removes anything left from crashed runs.
    try {
      await container.stop();
    } catch (err) {
      console.warn("global-teardown: container.stop() failed:", err);
    }
  }

  // Clean up temp file
  try {
    fs.unlinkSync(BROKER_FILE);
  } catch {
    // ignore
  }
}
