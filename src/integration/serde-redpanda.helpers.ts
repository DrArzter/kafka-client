import { RedpandaContainer, StartedRedpandaContainer } from "@testcontainers/redpanda";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { KafkaClient } from "../client/kafka.client";
import type { KafkaClientOptions } from "../client/kafka.client";

const { Kafka, logLevel: KafkaLogLevel } = KafkaJS;

/**
 * Redpanda image pinned to a known-good version. Redpanda ships the Kafka API
 * AND a Confluent-compatible Schema Registry in one process — apache/kafka
 * (used by the shared integration global-setup) has NO registry, so the Avro /
 * Protobuf serde specs own their own Redpanda container instead.
 */
export const REDPANDA_IMAGE = "redpandadata/redpanda:v24.2.7";

/**
 * Start a single-node Redpanda container in dev-container mode. The
 * `@testcontainers/redpanda` module renders the dev-container bootstrap config,
 * exposes the Kafka API (9092) and the Schema Registry (8081), and waits until
 * both are ready.
 */
export async function startRedpanda(): Promise<StartedRedpandaContainer> {
  return new RedpandaContainer(REDPANDA_IMAGE).start();
}

/** `host:port` broker list for a started Redpanda container. */
export function brokersOf(rp: StartedRedpandaContainer): string[] {
  return [rp.getBootstrapServers()];
}

/**
 * Build a KafkaClient bound to a Redpanda container with a unique
 * clientId/groupId per call so reruns never collide on group state.
 */
export function makeClient<T extends Record<string, any>>(
  rp: StartedRedpandaContainer,
  name: string,
  options?: KafkaClientOptions,
): KafkaClient<T> {
  const suffix = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  return new KafkaClient<T>(
    `serde-${name}-${suffix}`,
    `serde-${name}-group-${suffix}`,
    brokersOf(rp),
    { autoCreateTopics: true, ...options },
  );
}

/** Unique topic name per test run — avoids offset/schema bleed across reruns. */
export function uniqueTopic(base: string): string {
  return `${base}.${Date.now()}.${Math.floor(Math.random() * 1e6)}`;
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

/**
 * Read the FIRST message on `topic` with a RAW `@confluentinc/kafka-javascript`
 * consumer, returning the value as raw bytes only. This is the interop proof:
 * a client that knows nothing about this library's serde must be able to read
 * the exact wire bytes an external (Java/Go) consumer would see.
 */
export async function readFirstRawValue(
  brokers: string[],
  topic: string,
  timeoutMs = 30_000,
): Promise<Buffer> {
  const kafka = new Kafka({
    kafkaJS: {
      clientId: `raw-reader-${Date.now()}`,
      brokers,
      logLevel: KafkaLogLevel.NOTHING,
    },
  });
  const consumer = kafka.consumer({
    kafkaJS: {
      groupId: `raw-reader-group-${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
      fromBeginning: true,
    },
  });

  await consumer.connect();
  await consumer.subscribe({ topics: [topic] });

  const value = await new Promise<Buffer>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`readFirstRawValue: timed out on ${topic}`)),
      timeoutMs,
    );
    void consumer
      .run({
        eachMessage: async ({ message }) => {
          if (message.value == null) return;
          const buf = Buffer.isBuffer(message.value)
            ? message.value
            : Buffer.from(message.value);
          clearTimeout(timer);
          resolve(buf);
        },
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
  });

  await consumer.disconnect().catch(() => {});
  return value;
}

/**
 * Fetch the schema-registry-assigned id for `<topic>-value` via the REST API,
 * bypassing this library's client, so the interop assertion compares the
 * on-wire id against an INDEPENDENT lookup.
 */
export async function fetchLatestSchemaId(
  registryUrl: string,
  subject: string,
): Promise<{ id: number; schema: string; schemaType?: string }> {
  const res = await fetch(
    `${registryUrl.replace(/\/$/, "")}/subjects/${encodeURIComponent(subject)}/versions/latest`,
  );
  if (!res.ok) {
    throw new Error(
      `fetchLatestSchemaId(${subject}) failed: ${res.status} ${res.statusText}`,
    );
  }
  const body = (await res.json()) as {
    id: number;
    schema: string;
    schemaType?: string;
  };
  return body;
}

/** Fetch a schema string by its registry id via the REST API (independent lookup). */
export async function fetchSchemaById(
  registryUrl: string,
  id: number,
): Promise<{ schema: string; schemaType?: string }> {
  const res = await fetch(
    `${registryUrl.replace(/\/$/, "")}/schemas/ids/${id}`,
  );
  if (!res.ok) {
    throw new Error(
      `fetchSchemaById(${id}) failed: ${res.status} ${res.statusText}`,
    );
  }
  return (await res.json()) as { schema: string; schemaType?: string };
}
