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

  // Pre-create topics
  const admin = kafka.admin();
  await admin.connect();
  const allTopicConfigs = [
    ...ALL_TOPICS.map((topic) => ({ topic, numPartitions: 1 })),
    ...Object.entries(MULTI_PARTITION_TOPICS).map(([topic, numPartitions]) => ({
      topic,
      numPartitions,
    })),
  ];
  await admin.createTopics({ topics: allTopicConfigs });
  await admin.disconnect();

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
