import { KafkaContainer } from "@testcontainers/kafka";
import { Kafka, logLevel as KafkaLogLevel } from "kafkajs";
import * as fs from "fs";
import * as path from "path";

const ALL_TOPICS = [
  "test.basic",
  "test.batch",
  "test.tx-ok",
  "test.tx-audit",
  "test.tx-abort",
  "test.retry",
  "test.retry.dlq",
  "test.intercept",
  "test.beginning",
  "test.descriptor",
  "test.retry-error",
  "test.retry-error.dlq",
  "test.key-order",
  "test.headers",
  "test.schema-valid",
  "test.schema-invalid",
  "test.schema-invalid.dlq",
  "test.schema-send",
  "test.batch-consume",
  "test.multi-group",
];

const BROKER_FILE = path.join(__dirname, ".integration-brokers.tmp");

export default async function globalSetup() {
  const container = await new KafkaContainer("confluentinc/cp-kafka:7.7.0")
    .withKraft()
    .withExposedPorts(9093)
    .withEnvironment({
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    })
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(9093);
  const brokers = `${host}:${port}`;

  // Store container reference for teardown
  (globalThis as any).__KAFKA_CONTAINER__ = container;

  // Write brokers to temp file so test files can read it
  fs.writeFileSync(BROKER_FILE, brokers, "utf-8");

  const kafka = new Kafka({
    clientId: "setup",
    brokers: [brokers],
    logLevel: KafkaLogLevel.NOTHING,
  });

  // Pre-create topics
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: ALL_TOPICS.map((topic) => ({ topic, numPartitions: 1 })),
  });
  await admin.disconnect();

  // Warmup: trigger transaction coordinator initialization
  const warmupKafka = new Kafka({
    clientId: "warmup",
    brokers: [brokers],
    logLevel: KafkaLogLevel.NOTHING,
    retry: { retries: 30, initialRetryTime: 500, maxRetryTime: 30000 },
  });
  const txProducer = warmupKafka.producer({
    transactionalId: "warmup-tx",
    idempotent: true,
    maxInFlightRequests: 1,
  });
  await txProducer.connect();
  const tx = await txProducer.transaction();
  await tx.abort();
  await txProducer.disconnect();

  // Warmup: trigger group coordinator initialization so consumers don't hit
  // "coordinator is loading" on first join
  const warmupConsumer = warmupKafka.consumer({ groupId: "warmup-group" });
  await warmupConsumer.connect();
  await warmupConsumer.subscribe({ topics: [ALL_TOPICS[0]], fromBeginning: false });
  await warmupConsumer.run({ eachMessage: async () => {} });
  // Give the coordinator time to fully initialize
  await new Promise((r) => setTimeout(r, 2000));
  await warmupConsumer.stop();
  await warmupConsumer.disconnect();
}
