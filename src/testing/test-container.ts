import {
  KafkaContainer,
  type StartedKafkaContainer,
} from "@testcontainers/kafka";
import { KafkaJS } from "@confluentinc/kafka-javascript";
const { Kafka, logLevel: KafkaLogLevel } = KafkaJS;

/** Options for `KafkaTestContainer`. */
export interface KafkaTestContainerOptions {
  /** Docker image. Default: `"confluentinc/cp-kafka:7.7.0"`. */
  image?: string;
  /** Warm up the transactional coordinator on start. Default: `true`. */
  transactionWarmup?: boolean;
  /** Topics to pre-create. Each entry can be a string (1 partition) or `{ topic, numPartitions }`. */
  topics?: Array<string | { topic: string; numPartitions?: number }>;
}

/**
 * Thin wrapper around `@testcontainers/kafka` that starts a single-node
 * KRaft Kafka container and exposes `brokers` for use with `KafkaClient`.
 *
 * Handles common setup pain points:
 * - Transaction coordinator warmup (avoids transactional producer hangs)
 * - Topic pre-creation (avoids race conditions)
 *
 * @example
 * ```ts
 * const container = new KafkaTestContainer({ topics: ['orders', 'payments'] });
 * const brokers = await container.start();
 *
 * const kafka = new KafkaClient('test', 'test-group', brokers);
 * // ... run tests ...
 *
 * await container.stop();
 * ```
 *
 * @example Jest lifecycle
 * ```ts
 * let container: KafkaTestContainer;
 * let brokers: string[];
 *
 * beforeAll(async () => {
 *   container = new KafkaTestContainer({ topics: ['orders'] });
 *   brokers = await container.start();
 * }, 120_000);
 *
 * afterAll(() => container.stop());
 * ```
 */
export class KafkaTestContainer {
  private container: StartedKafkaContainer | undefined;
  private readonly image: string;
  private readonly transactionWarmup: boolean;
  private readonly topics: Array<
    string | { topic: string; numPartitions?: number }
  >;

  constructor(options?: KafkaTestContainerOptions) {
    this.image = options?.image ?? "confluentinc/cp-kafka:7.7.0";
    this.transactionWarmup = options?.transactionWarmup ?? true;
    this.topics = options?.topics ?? [];
  }

  /**
   * Start the Kafka container, pre-create topics, and optionally warm up
   * the transaction coordinator.
   *
   * @returns Broker connection strings, e.g. `["localhost:55123"]`.
   */
  async start(): Promise<string[]> {
    this.container = await new KafkaContainer(this.image)
      .withKraft()
      .withExposedPorts(9093)
      .withEnvironment({
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
      })
      .start();

    const host = this.container.getHost();
    const port = this.container.getMappedPort(9093);
    const brokers = [`${host}:${port}`];

    const kafka = new Kafka({
      kafkaJS: {
        clientId: "test-container-setup",
        brokers,
        logLevel: KafkaLogLevel.NOTHING,
      },
    });

    if (this.topics.length > 0) {
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: this.topics.map((t) =>
          typeof t === "string"
            ? { topic: t, numPartitions: 1 }
            : { topic: t.topic, numPartitions: t.numPartitions ?? 1 },
        ),
      });
      await admin.disconnect();
    }

    if (this.transactionWarmup) {
      const warmupKafka = new Kafka({
        kafkaJS: {
          clientId: "test-container-warmup",
          brokers,
          logLevel: KafkaLogLevel.NOTHING,
        },
      });
      const txProducer = warmupKafka.producer({
        kafkaJS: {
          transactionalId: "test-container-warmup-tx",
          idempotent: true,
          maxInFlightRequests: 1,
        },
      });
      await txProducer.connect();
      const tx = await txProducer.transaction();
      await tx.abort();
      await txProducer.disconnect();
    }

    return brokers;
  }

  /** Stop and remove the container. */
  async stop(): Promise<void> {
    await this.container?.stop();
    this.container = undefined;
  }

  /** Broker connection strings. Throws if container is not started. */
  get brokers(): string[] {
    if (!this.container) {
      throw new Error(
        "KafkaTestContainer is not started. Call start() first.",
      );
    }
    const host = this.container.getHost();
    const port = this.container.getMappedPort(9093);
    return [`${host}:${port}`];
  }
}
