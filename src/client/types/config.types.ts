import type { KafkaLogger } from "./common";
import type {
  KafkaInstrumentation,
  MessageLostContext,
  TtlExpiredContext,
} from "./consumer.types";

/**
 * Options for `KafkaClient` constructor.
 *
 * @example
 * ```ts
 * const kafka = new KafkaClient(kafkaConfig, 'my-service', {
 *   transactionalId: `my-service-tx-${replicaIndex}`,
 *   lagThrottle: { maxLag: 10_000, pollIntervalMs: 3_000 },
 *   clockRecovery: { topics: ['orders.created'] },
 *   onMessageLost: (ctx) => alerting.fire('kafka.message-lost', ctx),
 *   instrumentation: [otelInstrumentation()],
 * });
 * ```
 */
export interface KafkaClientOptions {
  /** Auto-create topics via admin before the first `sendMessage`, `sendBatch`, or `transaction` for each topic. Useful for development — not recommended in production. */
  autoCreateTopics?: boolean;
  /** When `true`, string topic keys are validated against any schema previously registered via a TopicDescriptor. Default: `true`. */
  strictSchemas?: boolean;
  /** Custom logger. Defaults to console with `[KafkaClient:<clientId>]` prefix. */
  logger?: KafkaLogger;
  /** Number of partitions for auto-created topics. Default: `1`. */
  numPartitions?: number;
  /** Client-wide instrumentation hooks (e.g. OTel). Applied to both send and consume paths. */
  instrumentation?: KafkaInstrumentation[];
  /**
   * Pluggable serialization for message payloads. Defaults to `JsonSerde`
   * (`JSON.stringify` / `JSON.parse`), which is byte-identical to the client's
   * historical behaviour.
   *
   * A per-topic override declared via `topic(...).serde(mySerde)` wins over
   * this client-wide serde for that topic. Serde only touches the message
   * value — envelope headers are unaffected.
   *
   * @example
   * ```ts
   * const kafka = new KafkaClient(id, group, brokers, {
   *   serde: new JsonSerde(), // or a custom Avro/Protobuf serde
   * });
   * ```
   */
  serde?: import("../message/serde").MessageSerde;
  /**
   * Override the transactional producer ID used by `transaction()`.
   * Defaults to `${clientId}-tx`.
   *
   * The transactional ID must be **unique per producer instance** across the
   * entire Kafka cluster. Two `KafkaClient` instances with the same ID will
   * cause Kafka to fence one of the producers — the fenced producer will fail
   * on the next `transaction()` call. Set a distinct value per replica when
   * running multiple instances of the same service.
   */
  transactionalId?: string;
  /**
   * Called when a message is dropped without being sent to a DLQ.
   * Fires when the handler throws after all retries, or schema validation fails — and `dlq` is not enabled.
   * Use this to alert, log to external systems, or trigger fallback logic.
   */
  onMessageLost?: (ctx: MessageLostContext) => void | Promise<void>;
  /**
   * Called when a message is dropped due to TTL expiration (`messageTtlMs`).
   * Fires instead of `onMessageLost` for expired messages when `dlq` is not enabled.
   * When `dlq: true`, expired messages go to the DLQ and this callback is NOT called.
   *
   * **Client-wide fallback**: if `ConsumerOptions.onTtlExpired` is set on the consumer,
   * it takes precedence over this client-level callback.
   */
  onTtlExpired?: (ctx: TtlExpiredContext) => void | Promise<void>;
  /**
   * Called whenever a consumer group rebalance occurs.
   * - `'assign'` — new partitions were granted to this instance.
   * - `'revoke'` — partitions were taken away (e.g. another consumer joined).
   *
   * Applied to every consumer created by this client. If you need per-consumer
   * rebalance handling, use separate `KafkaClient` instances.
   */
  onRebalance?: (
    type: "assign" | "revoke",
    partitions: Array<{ topic: string; partition: number }>,
  ) => void;
  /**
   * Recover the Lamport clock from the last message in the given topics on `connectProducer()`.
   *
   * On startup the producer creates a short-lived consumer, seeks each partition to its
   * last message (`highWatermark − 1`), reads the `x-lamport-clock` header, then
   * initialises `_lamportClock` to the maximum value found. This guarantees monotonic
   * clock values across restarts without an external store.
   *
   * Topics that do not exist or are empty are silently skipped.
   */
  clockRecovery?: {
    /** Topic names to scan for the highest Lamport clock. */
    topics: string[];
    /**
     * Max time to wait for recovery before proceeding with a partial result.
     * Guards against partitions whose last message was compacted or trimmed
     * away between the offset fetch and the seek. Default: `30000`.
     */
    timeoutMs?: number;
  };
  /**
   * Delay `sendMessage` / `sendBatch` / `sendTombstone` when the observed lag of a
   * consumer group exceeds `maxLag`. Resumes immediately when lag drops below the threshold.
   *
   * Lag is polled via `getConsumerLag()` every `pollIntervalMs` in the background;
   * no admin call is made on each individual send.
   *
   * When `maxWaitMs` is exceeded the send is unblocked with a warning — this is
   * best-effort throttling, not hard back-pressure.
   *
   * Requires `connectProducer()` to have been called to start the polling loop.
   */
  lagThrottle?: {
    /** Consumer group whose lag is monitored. Defaults to the client's default group. */
    groupId?: string;
    /** Lag threshold (number of messages) above which sends are delayed. */
    maxLag: number;
    /** How often to poll `getConsumerLag()`. Default: `5000` ms. */
    pollIntervalMs?: number;
    /**
     * Maximum time (ms) a send will wait while throttled before proceeding anyway.
     * Default: `30_000` ms.
     */
    maxWaitMs?: number;
  };
  /**
   * Custom transport implementation.
   *
   * By default `KafkaClient` uses `ConfluentTransport` which wraps
   * `@confluentinc/kafka-javascript` (librdkafka). Inject a different
   * `KafkaTransport` to target an alternative broker library, or to supply
   * a deterministic fake in unit tests without mocking the confluentinc module.
   *
   * @example
   * ```ts
   * // In tests — no jest.mock() needed
   * const kafka = new KafkaClient('svc', 'grp', [], {
   *   transport: new FakeTransport(),
   * });
   * ```
   */
  transport?: import("../transport/transport.interface").KafkaTransport;
  /**
   * Transport security: TLS and SASL authentication.
   *
   * Secure by default without getting in the way:
   * - `sasl` configured with `ssl` unset → TLS is enabled automatically.
   * - No security at all against non-local brokers → a one-time warning
   *   (silence with `allowInsecure: true` if intentional).
   *
   * Supports `plain`, `scram-sha-256/512`, and `oauthbearer` — the latter
   * enables AWS MSK IAM (`awsMskIamProvider`) and Google Cloud Managed Kafka
   * (`gcpAccessTokenProvider`).
   *
   * @example
   * ```ts
   * security: {
   *   sasl: { mechanism: 'scram-sha-512', username: 'svc', password: secret },
   *   // ssl: true is implied
   * }
   * ```
   */
  security?: import("../security/security.types").KafkaSecurityOptions;
}
