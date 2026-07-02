import { KafkaContainer } from "@testcontainers/kafka";
import { KafkaJS } from "@confluentinc/kafka-javascript";
const { Kafka, logLevel: KafkaLogLevel } = KafkaJS;
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
  "test.health",
  // retry topic chain tests
  "test.retry-topic",
  "test.retry-topic.retry.1",
  "test.retry-topic.retry.2",
  "test.retry-topic.dlq",
  // consumer handle / lag tests
  "test.handle",
  "test.lag",
  // instrumentation / OTel wrap tests
  "test.otel",
  // batch retryTopics tests
  "test.batch-retry-topic",
  "test.batch-retry-topic.retry.1",
  "test.batch-retry-topic.retry.2",
  "test.batch-retry-topic.dlq",
  // SchemaContext tests
  "test.schema-ctx",
  "test.schema-ctx-send",
  // graceful shutdown tests
  "test.graceful",
  // Lamport Clock deduplication tests
  "test.dedup",
  "test.dedup.duplicates",
  "test.dedup.dlq",
];

// Topics that need more than 1 partition (for rebalance tests)
const MULTI_PARTITION_TOPICS: Record<string, number> = {
  "test.rebalance": 2,
};

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
    kafkaJS: {
      clientId: "setup",
      brokers: [brokers],
      logLevel: KafkaLogLevel.NOTHING,
    },
  });

  // Pre-create topics. The container's start() can resolve before the KRaft
  // controller is fully ready to serve CreateTopics (observed as
  // "Local: Timed out" with testcontainers >= 12) — retry with backoff.
  const allTopicConfigs = [
    ...ALL_TOPICS.map((topic) => ({ topic, numPartitions: 1 })),
    ...Object.entries(MULTI_PARTITION_TOPICS).map(([topic, numPartitions]) => ({
      topic,
      numPartitions,
    })),
  ];
  const deadline = Date.now() + 90_000;
  for (;;) {
    const admin = kafka.admin();
    try {
      await admin.connect();
      await admin.createTopics({ topics: allTopicConfigs });
      await admin.disconnect();
      break;
    } catch (err) {
      await admin.disconnect().catch(() => {});
      if (Date.now() > deadline) throw err;
      await new Promise((r) => setTimeout(r, 2_000));
    }
  }

  // Warmup: trigger transaction coordinator initialization
  const warmupKafka = new Kafka({
    kafkaJS: {
      clientId: "warmup",
      brokers: [brokers],
      logLevel: KafkaLogLevel.NOTHING,
    },
  });
  const txProducer = warmupKafka.producer({
    kafkaJS: {
      transactionalId: "warmup-tx",
      idempotent: true,
      maxInFlightRequests: 1,
    },
  });
  await txProducer.connect();
  const tx = await txProducer.transaction();
  await tx.abort();
  await txProducer.disconnect();
}
