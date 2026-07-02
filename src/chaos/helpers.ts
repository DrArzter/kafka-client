import { KafkaContainer, StartedKafkaContainer } from "@testcontainers/kafka";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { execFile } from "child_process";
import { promisify } from "util";
import { KafkaClient } from "../client/kafka.client";
import type { KafkaClientOptions } from "../client/kafka.client";

const { Kafka, logLevel: KafkaLogLevel } = KafkaJS;
const execFileAsync = promisify(execFile);

// ── Container lifecycle ────────────────────────────────────────────────────
//
// Unlike the integration suite (one shared container via global-setup), each
// chaos spec owns its container so it can pause / restart / kill the broker in
// isolation without breaking neighbouring suites. maxWorkers: 1 keeps them
// serial.

/** Kafka image pinned to match the integration suite. */
const KAFKA_IMAGE = "confluentinc/cp-kafka:7.7.0";

/**
 * Start a fresh single-broker KafkaContainer (KRaft mode), mirroring the
 * integration global-setup configuration. Returns the started container.
 */
export async function startBroker(): Promise<StartedKafkaContainer> {
  const container = await new KafkaContainer(KAFKA_IMAGE)
    .withKraft()
    .withExposedPorts(9093)
    .withEnvironment({
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    })
    .start();
  return container;
}

/** `host:port` broker string for a started container. */
export function brokersOf(container: StartedKafkaContainer): string[] {
  return [`${container.getHost()}:${container.getMappedPort(9093)}`];
}

/**
 * Pre-create topics with the given partition counts using the raw KafkaJS
 * admin (same approach as integration global-setup). Idempotent — ignores
 * "already exists".
 */
export async function createTopics(
  brokers: string[],
  topics: Array<{ topic: string; numPartitions: number }>,
): Promise<void> {
  const kafka = new Kafka({
    kafkaJS: {
      clientId: "chaos-setup",
      brokers,
      logLevel: KafkaLogLevel.NOTHING,
    },
  });
  // The container's start() can resolve before the KRaft controller serves
  // CreateTopics (observed as "Local: Timed out" on testcontainers >= 12) —
  // retry with backoff. "Already exists" is tolerated (idempotent).
  const deadline = Date.now() + 90_000;
  for (;;) {
    const admin = kafka.admin();
    try {
      await admin.connect();
      await admin.createTopics({ topics });
      await admin.disconnect();
      return;
    } catch (err) {
      await admin.disconnect().catch(() => {});
      if (/already exists/i.test(String(err))) return;
      if (Date.now() > deadline) throw err;
      await new Promise((r) => setTimeout(r, 2_000));
    }
  }
}

// ── Docker pause / unpause ──────────────────────────────────────────────────
//
// Testcontainers' StartedTestContainer does NOT expose pause()/unpause(), and
// KafkaContainer.restart() re-runs advertised-listener setup which can change
// the mapped port and desync the advertised listener. `docker pause` freezes
// the broker process WITHOUT touching the port bindings — the cleanest way to
// simulate a broker outage that librdkafka must ride through and reconnect.

/** SIGSTOP the whole container via `docker pause` — the broker stops responding but the port mapping is preserved. */
export async function pauseContainer(
  container: StartedKafkaContainer,
): Promise<void> {
  await execFileAsync("docker", ["pause", container.getId()]);
}

/** SIGCONT the container via `docker unpause` — the broker resumes on the same port. */
export async function unpauseContainer(
  container: StartedKafkaContainer,
): Promise<void> {
  await execFileAsync("docker", ["unpause", container.getId()]);
}

// ── Client factory ──────────────────────────────────────────────────────────

/**
 * Build a KafkaClient bound to the given container. Unique clientId/groupId
 * per call (Date.now suffix) so concurrent groups never collide across a rerun.
 */
export function makeClient<T extends Record<string, any>>(
  container: StartedKafkaContainer,
  name: string,
  options?: KafkaClientOptions,
): KafkaClient<T> {
  const suffix = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  return new KafkaClient<T>(
    `chaos-${name}-${suffix}`,
    `chaos-${name}-group-${suffix}`,
    brokersOf(container),
    options,
  );
}

// ── Capturing logger ─────────────────────────────────────────────────────────

export interface CapturingLogger {
  log(message: string): void;
  warn(message: string, ...args: any[]): void;
  error(message: string, ...args: any[]): void;
  debug(message: string, ...args: any[]): void;
  logs: string[];
  warns: string[];
  errors: string[];
  /** True if any warn message includes `substr`. */
  hasWarn(substr: string): boolean;
}

/** A KafkaLogger that records every message so tests can assert on log output. */
export function capturingLogger(): CapturingLogger {
  const logs: string[] = [];
  const warns: string[] = [];
  const errors: string[] = [];
  return {
    logs,
    warns,
    errors,
    log: (m) => logs.push(m),
    warn: (m) => warns.push(m),
    error: (m) => errors.push(m),
    debug: () => {},
    hasWarn: (substr) => warns.some((w) => w.includes(substr)),
  };
}

// ── Async utilities ──────────────────────────────────────────────────────────

export const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

/** Poll `predicate` every `intervalMs` until true or `timeout` elapses. */
export async function waitUntil(
  predicate: () => boolean,
  timeout = 60_000,
  intervalMs = 200,
): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error("waitUntil: timeout");
    }
    await sleep(intervalMs);
  }
}
