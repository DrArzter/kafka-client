import * as fs from "fs";
import * as path from "path";
import { KafkaClient } from "../client/kafka.client";
import { topic, SchemaLike, SchemaParseContext } from "../client/message/topic";

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
  // chaos / rebalance tests
  "test.rebalance": { seq: number };
  // retry topic chain tests
  "test.retry-topic": { value: string };
  "test.retry-topic.retry.1": { value: string };
  "test.retry-topic.retry.2": { value: string };
  "test.retry-topic.dlq": { value: string };
  // consumer handle / lag tests
  "test.handle": { seq: number };
  "test.lag": { seq: number };
  // instrumentation / OTel wrap tests
  "test.otel": { value: string };
  // batch retryTopics tests
  "test.batch-retry-topic": { value: string };
  "test.batch-retry-topic.retry.1": { value: string };
  "test.batch-retry-topic.retry.2": { value: string };
  "test.batch-retry-topic.dlq": { value: string };
  // SchemaContext tests
  "test.schema-ctx": { name: string };
  "test.schema-ctx-send": { name: string };
  // graceful shutdown tests
  "test.graceful": { seq: number };
};

// ── Descriptors ─────────────────────────────────────────────────────

export const TestDescriptor = topic("test.descriptor").type<{
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
export const SchemaInvalidTopic = topic("test.schema-invalid").schema(
  personSchema,
);
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
export {
  KafkaRetryExhaustedError,
  KafkaValidationError,
} from "../client/errors";
export type { ConsumerInterceptor } from "../client/kafka.client";
export type { SchemaParseContext } from "../client/message/topic";
