import * as fs from "fs";
import * as path from "path";
import { KafkaClient } from "../client/kafka.client";
import { topic, SchemaLike } from "../client/topic";

// ── Topic types ─────────────────────────────────────────────────────

export type TestTopics = {
  "test.basic": { text: string; seq: number };
  "test.batch": { id: number };
  "test.tx-ok": { orderId: string };
  "test.tx-audit": { action: string };
  "test.tx-abort": { orderId: string };
  "test.retry": { value: string };
  "test.retry.dlq": { value: string };
  "test.intercept": { data: string };
  "test.beginning": { msg: string };
  "test.descriptor": { label: string; num: number };
  "test.retry-error": { value: string };
  "test.retry-error.dlq": { value: string };
  "test.key-order": { seq: number };
  "test.headers": { body: string };
  "test.schema-valid": { name: string; age: number };
  "test.schema-invalid": { name: string; age: number };
  "test.schema-invalid.dlq": { name: string; age: number };
  "test.schema-send": { name: string; age: number };
  "test.batch-consume": { id: number; text: string };
  "test.multi-group": { seq: number };
  "test.health": { probe: boolean };
};

// ── Descriptors ─────────────────────────────────────────────────────

export const TestDescriptor = topic("test.descriptor")<{
  label: string;
  num: number;
}>();

export const personSchema: SchemaLike<{ name: string; age: number }> = {
  parse(data: unknown) {
    const d = data as any;
    if (typeof d?.name !== "string") throw new Error("name must be a string");
    if (typeof d?.age !== "number") throw new Error("age must be a number");
    return { name: d.name, age: d.age };
  },
};

export const SchemaValidTopic = topic("test.schema-valid").schema(personSchema);
export const SchemaInvalidTopic = topic("test.schema-invalid").schema(personSchema);
export const SchemaSendTopic = topic("test.schema-send").schema(personSchema);

// ── Brokers ─────────────────────────────────────────────────────────

const BROKER_FILE = path.join(__dirname, ".integration-brokers.tmp");

export function getBrokers(): string[] {
  const content = fs.readFileSync(BROKER_FILE, "utf-8").trim();
  return [content];
}

// ── Factory ─────────────────────────────────────────────────────────

export function createClient(name: string): KafkaClient<TestTopics> {
  return new KafkaClient<TestTopics>(
    `integration-${name}`,
    `group-${name}-${Date.now()}`,
    getBrokers(),
  );
}

// ── Utilities ───────────────────────────────────────────────────────

export function waitForMessages<T>(
  count: number,
  timeout = 30_000,
): { messages: T[]; promise: Promise<T[]> } {
  const messages: T[] = [];
  const promise = new Promise<T[]>((res, rej) => {
    const timer = setTimeout(
      () => rej(new Error(`Timeout waiting for ${count} messages`)),
      timeout,
    );
    const interval = setInterval(() => {
      if (messages.length >= count) {
        clearTimeout(timer);
        clearInterval(interval);
        res(messages);
      }
    }, 100);
  });
  return { messages, promise };
}

export { KafkaClient } from "../client/kafka.client";
export { KafkaHealthIndicator } from "../nest/kafka.health";
export { KafkaRetryExhaustedError, KafkaValidationError } from "../client/errors";
export type { ConsumerInterceptor } from "../client/kafka.client";
