import * as fs from "fs";
import * as path from "path";

const BROKER_FILE = path.join(__dirname, ".integration-brokers.tmp");

export default async function globalTeardown() {
  const container = (globalThis as any).__KAFKA_CONTAINER__;
  if (container) {
    await container.stop();
  }

  // Clean up temp file
  try {
    fs.unlinkSync(BROKER_FILE);
  } catch {
    // ignore
  }
}
