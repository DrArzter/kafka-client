import { TopicDescriptor, SchemaLike } from "./message/topic";
import type { EventEnvelope } from "./message/envelope";

/**
 * Mapping of topic names to their message types.
 * Define this interface to get type-safe publish/subscribe across your app.
 *
 * @example
 * ```ts
 * // with explicit extends (IDE hints for values)
 * interface MyTopics extends TTopicMessageMap {
 *   "orders.created": { orderId: string; amount: number };
 *   "users.updated": { userId: string; name: string };
 * }
 *
 * // or plain interface / type — works the same
 * interface MyTopics {
 *   "orders.created": { orderId: string; amount: number };
 * }
 * ```
 */
export type TTopicMessageMap = {
  [topic: string]: Record<string, any>;
};

/**
 * Generic constraint for topic-message maps.
 * Works with both `type` aliases and `interface` declarations.
 */
export type TopicMapConstraint<T> = { [K in keyof T]: Record<string, any> };

export type ClientId = string;
export type GroupId = string;

export type MessageHeaders = Record<string, string>;

/** Options for sending a single message. */
export interface SendOptions {
  /** Partition key for message routing. */
  key?: string;
  /** Custom headers attached to the message (merged with auto-generated envelope headers). */
  headers?: MessageHeaders;
  /** Override the auto-propagated correlation ID (default: inherited from ALS context or new UUID). */
  correlationId?: string;
  /** Schema version for the payload. Default: `1`. */
  schemaVersion?: number;
  /** Override the auto-generated event ID (UUID v4). */
  eventId?: string;
}

/** Shape of each item in a `sendBatch` call. */
export interface BatchMessageItem<V> {
  value: V;
  key?: string;
  headers?: MessageHeaders;
  correlationId?: string;
  schemaVersion?: number;
  eventId?: string;
}

/** Metadata exposed to batch consumer handlers. */
export interface BatchMeta {
  /** Partition number for this batch. */
  partition: number;
  /**
   * Highest offset available on the broker for this partition.
   * `null` when the message is being replayed via a retry topic consumer —
   * in that path the broker high-watermark is not accessible without an admin
   * call. Do not use this for lag calculation in the retry path.
   */
  highWatermark: string | null;
  /** Send a heartbeat to the broker to prevent session timeout. */
  heartbeat(): Promise<void>;
  /** Mark an offset as processed (for manual offset management). */
  resolveOffset(offset: string): void;
  /** Commit offsets if the auto-commit threshold has been reached. */
  commitOffsetsIfNecessary(): Promise<void>;
}

/**
 * Options for Lamport Clock-based message deduplication.
 *
 * The producer stamps every outgoing message with a monotonically increasing
 * `x-lamport-clock` header. The consumer tracks the last processed value per
 * `topic:partition` and skips any message whose clock is not strictly greater
 * than that value.
 *
 * Messages that arrive without the `x-lamport-clock` header are passed through
 * unchanged (backwards-compatible with producers that don't use this library).
 *
 * **In-session limitation**: deduplication state lives in memory and is reset
 * whenever the process restarts or `stopConsumer` is called. After a restart,
 * previously processed messages with `clock ≤ N` will be re-processed until
 * their offsets catch up to the high-watermark again. The same applies after a
 * rebalance: the instance that receives the partition begins with empty state.
 * This is a fundamental limitation of the in-memory Lamport clock approach —
 * it provides deduplication only within a single process session.
 */
export interface DeduplicationOptions {
  /**
   * What to do with detected duplicate messages:
   * - `'drop'`  — silently discard. No routing, no callback. **(default)**
   * - `'dlq'`   — forward to `<topic>.dlq` with reason metadata headers.
   *               Requires `dlq: true` on the consumer options.
   * - `'topic'` — forward to `<topic>.duplicates` (or `duplicatesTopic` if set).
   */
  strategy?: "dlq" | "topic" | "drop";
  /**
   * Custom destination topic for `strategy: 'topic'`.
   * Defaults to `<consumedTopic>.duplicates`.
   */
  duplicatesTopic?: string;
}

/**
 * Options for the per-partition circuit breaker.
 *
 * The circuit breaker tracks recent message outcomes in a **sliding window** and
 * opens (pauses the partition) when too many failures accumulate:
 *
 * - **CLOSED** — normal operation. Each DLQ route or successful delivery is recorded
 *   in the window. When `failuresInWindow >= threshold` the circuit opens.
 * - **OPEN** — the partition is paused. After `recoveryMs` the circuit moves to HALF-OPEN.
 * - **HALF-OPEN** — the partition is resumed. If the next `halfOpenSuccesses` messages
 *   succeed the circuit closes; a new failure immediately re-opens it.
 */
export interface CircuitBreakerOptions {
  /**
   * Number of failures within the sliding window required to open the circuit.
   * A failure is any message that ends up in the DLQ.
   * Default: `5`.
   */
  threshold?: number;
  /**
   * Time (ms) to keep the circuit OPEN before attempting recovery (HALF_OPEN).
   * Default: `30_000` (30 s).
   */
  recoveryMs?: number;
  /**
   * Number of outcomes (successes + failures) to keep in the sliding window.
   * Default: `threshold * 2` (minimum `10`).
   */
  windowSize?: number;
  /**
   * Number of consecutive successes in HALF-OPEN state required to close the
   * circuit. Default: `1`.
   */
  halfOpenSuccesses?: number;
}

/** Options for configuring a Kafka consumer. */
export interface ConsumerOptions<
  T extends TopicMapConstraint<T> = TTopicMessageMap,
> {
  /** Override the default consumer group ID from the constructor. */
  groupId?: string;
  /** Start reading from earliest offset. Default: `false`. */
  fromBeginning?: boolean;
  /** Automatically commit offsets. Default: `true`. */
  autoCommit?: boolean;
  /** Retry policy for failed message processing. */
  retry?: RetryOptions;
  /** Send failed messages to a Dead Letter Queue (`<topic>.dlq`). */
  dlq?: boolean;
  /** Interceptors called before/after each message. */
  interceptors?: ConsumerInterceptor<T>[];
  /** @internal Schema map populated by @SubscribeTo when descriptors have schemas. */
  schemas?: Map<string, SchemaLike>;
  /** Retry config for `consumer.subscribe()` when the topic doesn't exist yet. */
  subscribeRetry?: SubscribeRetryOptions;
  /**
   * Route failed messages through a Kafka retry topic (`<topic>.retry`) instead of sleeping
   * in-process between retry attempts. Requires `retry` to be set.
   *
   * Benefits over in-process retry:
   * - Retry messages survive consumer restarts (durable)
   * - Original consumer is not blocked during retry delay
   *
   * A companion consumer on `<topic>.retry` is auto-started with group `<groupId>-retry`.
   * On exhaustion, messages go to `<topic>.dlq` (if `dlq: true`) or `onMessageLost`.
   */
  retryTopics?: boolean;
  /**
   * Timeout (ms) for waiting until each retry level consumer receives partition
   * assignments after `startConsumer` connects. Default: `10000`.
   * Increase this when the broker is slow to rebalance.
   */
  retryTopicAssignmentTimeoutMs?: number;
  /**
   * Log a warning if the message handler has not resolved within this window (ms).
   * The handler is not cancelled — this is a diagnostic aid to surface stuck handlers
   * before they starve a partition.
   */
  handlerTimeoutMs?: number;
  /**
   * Enable Lamport Clock deduplication.
   * Requires the producer to stamp messages with `x-lamport-clock` headers
   * (done automatically when using this library's send methods).
   * Messages without the header are passed through unchanged.
   */
  deduplication?: DeduplicationOptions;
  /**
   * Drop messages older than this threshold, measured in milliseconds from
   * the `x-timestamp` header set by the producer.
   *
   * Expired messages are routed to `{topic}.dlq` when `dlq: true`, otherwise
   * `onTtlExpired` is called. The handler is never invoked for an expired message.
   */
  messageTtlMs?: number;
  /**
   * Automatically pause a partition when it accumulates too many consecutive
   * failures, then resume after a recovery window.
   *
   * See `CircuitBreakerOptions` for the sliding-window semantics.
   */
  circuitBreaker?: CircuitBreakerOptions;
  /**
   * Max number of messages buffered in the `consume()` iterator queue before
   * the partition is paused. The partition resumes when the queue drains below 50%.
   * Only applies to `consume()`. Default: unbounded.
   */
  queueHighWaterMark?: number;
}

/** Configuration for consumer retry behavior. */
export interface RetryOptions {
  /** Maximum number of retry attempts before giving up. */
  maxRetries: number;
  /** Base delay for exponential backoff in ms. Default: `1000`. */
  backoffMs?: number;
  /** Maximum delay cap for exponential backoff in ms. Default: `30000`. */
  maxBackoffMs?: number;
}

/**
 * Interceptor hooks for consumer message processing.
 * All methods are optional — implement only what you need.
 *
 * Interceptors are per-consumer. For client-wide hooks (e.g. OTel),
 * use `KafkaInstrumentation` instead.
 */
export interface ConsumerInterceptor<
  T extends TopicMapConstraint<T> = TTopicMessageMap,
> {
  /** Called before the message handler. */
  before?(envelope: EventEnvelope<T[keyof T]>): Promise<void> | void;
  /** Called after the message handler succeeds. */
  after?(envelope: EventEnvelope<T[keyof T]>): Promise<void> | void;
  /** Called when the message handler throws. */
  onError?(
    envelope: EventEnvelope<T[keyof T]>,
    error: Error,
  ): Promise<void> | void;
}

/**
 * Return value of `KafkaInstrumentation.beforeConsume`.
 *
 * - `() => void` — legacy form: a cleanup function called after the handler.
 * - Object form:
 *   - `cleanup?()` — called after the handler (same as the legacy function form).
 *   - `wrap?(fn)` — wraps the handler execution; call `fn()` inside the desired
 *     async context (e.g. `context.with(spanCtx, fn)` for OpenTelemetry). Multiple
 *     wraps from different instrumentations are composed in declaration order,
 *     so the first instrumentation's wrap is the outermost.
 */
export type BeforeConsumeResult =
  | (() => void)
  | { cleanup?(): void; wrap?(fn: () => Promise<void>): Promise<void> };

/**
 * Reason a message was sent to the DLQ.
 * - `'handler-error'` — the consumer handler threw after all retry attempts.
 * - `'validation-error'` — schema validation failed before the handler ran.
 * - `'lamport-clock-duplicate'` — message was identified as a Lamport-clock duplicate
 *   and `deduplication.strategy` is `'dlq'`.
 * - `'ttl-expired'` — message age exceeded `messageTtlMs` before the handler ran.
 */
export type DlqReason =
  | "handler-error"
  | "validation-error"
  | "lamport-clock-duplicate"
  | "ttl-expired";

/** Options for `replayDlq`. */
export interface DlqReplayOptions {
  /**
   * Override the target topic to re-publish to.
   * Default: reads the `x-dlq-original-topic` header from each DLQ message.
   */
  targetTopic?: string;
  /**
   * Dry-run mode — log what would be replayed without actually sending.
   * Increments the `replayed` counter so you can see what would happen.
   */
  dryRun?: boolean;
  /**
   * Optional filter — return `false` to skip a message.
   * @param headers All headers on the DLQ message (including `x-dlq-*` metadata).
   * @param value Raw message value (JSON string).
   */
  filter?: (headers: MessageHeaders, value: string) => boolean;
}

/**
 * Snapshot of internal event counters accumulated since client creation
 * (or since the last `resetMetrics()` call).
 */
export interface KafkaMetrics {
  /** Total messages successfully processed by the consumer handler. */
  processedCount: number;
  /** Total retry attempts routed — covers both in-process retries and retry-topic hops. */
  retryCount: number;
  /** Total messages sent to a DLQ topic. */
  dlqCount: number;
  /** Total duplicate messages detected by the Lamport clock. */
  dedupCount: number;
}

/**
 * Client-wide instrumentation hooks for both send and consume paths.
 * Use this for cross-cutting concerns like tracing and metrics.
 *
 * @see `otelInstrumentation()` from `@drarzter/kafka-client/otel`
 */
export interface KafkaInstrumentation {
  /** Called before sending — can mutate `headers` (e.g. inject `traceparent`). */
  beforeSend?(topic: string, headers: MessageHeaders): void;
  /** Called after a successful send. */
  afterSend?(topic: string): void;
  /**
   * Called before the consumer handler.
   * Return a cleanup function (legacy) or a `BeforeConsumeResult` object with
   * optional `cleanup` and `wrap`. Use `wrap` to run the handler inside a
   * specific async context (e.g. an active OpenTelemetry span).
   */
  beforeConsume?(envelope: EventEnvelope<any>): BeforeConsumeResult | void;
  /** Called when the consumer handler throws. */
  onConsumeError?(envelope: EventEnvelope<any>, error: Error): void;
  /**
   * Called when a message is queued for retry.
   * Fires for both in-process retries (before the backoff sleep) and
   * retry-topic routing (EOS and non-EOS paths).
   */
  onRetry?(
    envelope: EventEnvelope<any>,
    attempt: number,
    maxRetries: number,
  ): void;
  /** Called when a message is routed to a DLQ topic. */
  onDlq?(envelope: EventEnvelope<any>, reason: DlqReason): void;
  /**
   * Called when a duplicate message is detected via the Lamport clock.
   * Fires regardless of the configured `deduplication.strategy`.
   */
  onDuplicate?(
    envelope: EventEnvelope<any>,
    strategy: "drop" | "dlq" | "topic",
  ): void;
  /**
   * Called after the consumer handler successfully processes a message.
   * Use this as a success counter for error-rate calculations.
   * Fires for both single-message and batch consumers (once per envelope).
   */
  onMessage?(envelope: EventEnvelope<any>): void;
  /** Called when a partition circuit opens (consumer paused due to DLQ failures). */
  onCircuitOpen?(topic: string, partition: number): void;
  /** Called when the circuit moves to half-open (partition resumed for a probe). */
  onCircuitHalfOpen?(topic: string, partition: number): void;
  /** Called when the circuit closes (normal operation restored). */
  onCircuitClose?(topic: string, partition: number): void;
}

/** Context passed to the `transaction()` callback with type-safe send methods. */
export interface TransactionContext<T extends TopicMapConstraint<T>> {
  send<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;
  send<D extends TopicDescriptor<string & keyof T, T[string & keyof T]>>(
    descriptor: D,
    message: D["__type"],
    options?: SendOptions,
  ): Promise<void>;

  sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<BatchMessageItem<T[K]>>,
  ): Promise<void>;
  sendBatch<D extends TopicDescriptor<string & keyof T, T[string & keyof T]>>(
    descriptor: D,
    messages: Array<BatchMessageItem<D["__type"]>>,
  ): Promise<void>;
}

/** Handle returned by `startConsumer` / `startBatchConsumer`. */
export interface ConsumerHandle {
  /** The consumer group ID this consumer is running under. */
  groupId: string;
  /** Stop this consumer. Equivalent to calling `client.stopConsumer(groupId)`. */
  stop(): Promise<void>;
}

/** Result returned by `KafkaClient.checkStatus()`. */
export type KafkaHealthResult =
  | { status: "up"; clientId: string; topics: string[] }
  | { status: "down"; clientId: string; error: string };

/** Interface describing all public methods of the Kafka client. */
export interface IKafkaClient<T extends TopicMapConstraint<T>> {
  checkStatus(): Promise<KafkaHealthResult>;

  /**
   * Query the consumer group lag per partition using the admin API.
   * Lag = (broker high-watermark offset) − (last committed offset).
   * - A committed offset of `-1` (no offset committed yet) counts as full lag.
   * - Defaults to the client's default `groupId` when none is provided.
   */
  getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>>;

  startConsumer<K extends Array<keyof T>>(
    topics: K,
    handleMessage: (envelope: EventEnvelope<T[K[number]]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  startConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleMessage: (envelope: EventEnvelope<D["__type"]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  startBatchConsumer<K extends Array<keyof T>>(
    topics: K,
    handleBatch: (
      envelopes: EventEnvelope<T[K[number]]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  startBatchConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleBatch: (
      envelopes: EventEnvelope<D["__type"]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * Stop consumer(s).
   * - `stopConsumer(groupId)` — disconnect and remove the consumer for a specific group.
   * - `stopConsumer()` — disconnect and remove all consumers.
   */
  stopConsumer(groupId?: string): Promise<void>;

  sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;

  sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<BatchMessageItem<T[K]>>,
  ): Promise<void>;

  transaction(fn: (ctx: TransactionContext<T>) => Promise<void>): Promise<void>;

  getClientId(): ClientId;

  /**
   * Return a snapshot of internal event counters (retry / DLQ / dedup).
   * - `getMetrics()` — aggregate across all topics.
   * - `getMetrics(topic)` — counters for a specific topic only; returns all-zero
   *   if no events have been observed for that topic yet.
   *
   * Counters accumulate since client creation or the last `resetMetrics()` call.
   */
  getMetrics(topic?: string): Readonly<KafkaMetrics>;

  /**
   * Reset internal event counters to zero.
   * - `resetMetrics()` — reset all topics.
   * - `resetMetrics(topic)` — reset a single topic only.
   */
  resetMetrics(topic?: string): void;

  /**
   * Consume all messages currently in `{topic}.dlq`, strip the `x-dlq-*` metadata
   * headers, and re-publish each message to its original topic (or `options.targetTopic`).
   *
   * A temporary consumer group is created and torn down automatically. The DLQ topic
   * itself is not modified — messages remain there after replay.
   *
   * @returns `{ replayed, skipped }` — counts of re-published vs skipped messages.
   */
  replayDlq(
    topic: string,
    options?: DlqReplayOptions,
  ): Promise<{ replayed: number; skipped: number }>;

  /**
   * Reset committed offsets for a consumer group to the earliest or latest position.
   *
   * The consumer group must be inactive (no running consumers) — Kafka does not
   * allow offset resets while members are actively consuming. Call
   * `stopConsumer(groupId)` first.
   *
   * @param groupId Consumer group to reset. Defaults to the client's default groupId.
   * @param topic Topic to reset.
   * @param position `'earliest'` seeks to the first available offset; `'latest'`
   *   seeks past the last message (consumer will only see new messages).
   */
  resetOffsets(
    groupId: string | undefined,
    topic: string,
    position: "earliest" | "latest",
  ): Promise<void>;

  /**
   * Seek specific partitions to explicit offsets.
   * More granular than `resetOffsets` — each partition can target a different offset.
   *
   * The consumer group must be inactive. Assignments for different topics are batched
   * into one admin call per topic.
   *
   * @param groupId Consumer group to seek. Defaults to the client's default groupId.
   * @param assignments Array of `{ topic, partition, offset }` tuples.
   */
  seekToOffset(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; offset: string }>,
  ): Promise<void>;

  /**
   * Seek specific partitions to the offset nearest to a given Unix timestamp (ms).
   * Uses `admin.fetchTopicOffsetsByTime` under the hood. Falls back to `-1` (end of
   * topic) when no offset exists at the requested timestamp (e.g. empty partition or
   * future timestamp).
   *
   * The consumer group must be inactive. Call `stopConsumer(groupId)` first.
   *
   * @param groupId Consumer group to seek. Defaults to the client's default groupId.
   * @param assignments Array of `{ topic, partition, timestamp }` tuples (Unix ms).
   */
  seekToTimestamp(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; timestamp: number }>,
  ): Promise<void>;

  /**
   * Returns the current circuit breaker state for a specific topic partition.
   * Returns `undefined` when no circuit state exists — either `circuitBreaker` is not
   * configured for the group, or the circuit has never been tripped.
   *
   * @param topic Topic name.
   * @param partition Partition index.
   * @param groupId Consumer group. Defaults to the client's default groupId.
   */
  getCircuitState(
    topic: string,
    partition: number,
    groupId?: string,
  ): { status: "closed" | "open" | "half-open"; failures: number; windowSize: number } | undefined;

  /**
   * Consume messages as an async iterator. Useful for scripts, migrations, and
   * one-off processing where the full `startConsumer` lifecycle is unnecessary.
   *
   * Breaking out of the loop (or calling `return()` on the iterator) stops the
   * underlying consumer automatically.
   *
   * @example
   * ```ts
   * for await (const envelope of kafka.consume('orders')) {
   *   await process(envelope);
   * }
   * ```
   */
  consume<K extends keyof T & string>(
    topic: K,
    options?: ConsumerOptions<T>,
  ): AsyncIterableIterator<EventEnvelope<T[K]>>;

  /**
   * Pause message delivery for specific topic-partitions on a consumer group.
   * The consumer remains connected and its committed offsets are preserved —
   * only polling is suspended. Call `resumeConsumer` to restart delivery.
   *
   * @param groupId Consumer group to pause. Defaults to the client's default groupId.
   * @param assignments Topic-partition pairs to pause.
   */
  pauseConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void;

  /**
   * Resume message delivery for previously paused topic-partitions.
   *
   * @param groupId Consumer group to resume. Defaults to the client's default groupId.
   * @param assignments Topic-partition pairs to resume.
   */
  resumeConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void;

  /**
   * Drain in-flight handlers, then disconnect all producers, consumers, and admin.
   * @param drainTimeoutMs Max ms to wait for in-flight handlers (default 30 000).
   */
  disconnect(drainTimeoutMs?: number): Promise<void>;

  /**
   * Register SIGTERM / SIGINT signal handlers that drain in-flight messages before
   * disconnecting. Call once after constructing the client in non-NestJS apps.
   * NestJS apps get drain automatically via `onModuleDestroy` → `disconnect()`.
   */
  enableGracefulShutdown(
    signals?: NodeJS.Signals[],
    drainTimeoutMs?: number,
  ): void;
}

/**
 * Logger interface for KafkaClient.
 * Compatible with NestJS Logger, console, winston, pino, or any custom logger.
 *
 * `debug` is optional — omit it to suppress debug output in production.
 */
export interface KafkaLogger {
  log(message: string): void;
  warn(message: string, ...args: any[]): void;
  error(message: string, ...args: any[]): void;
  debug?(message: string, ...args: any[]): void;
}

/**
 * Context passed to `onTtlExpired` when a message is dropped because it
 * exceeded `messageTtlMs` and `dlq` is not enabled.
 */
export interface TtlExpiredContext {
  /** Topic the message was consumed from. */
  topic: string;
  /** Actual age of the message in ms at the time it was dropped. */
  ageMs: number;
  /** The configured TTL threshold (`messageTtlMs`). */
  messageTtlMs: number;
  /** Original Kafka message headers (correlationId, traceparent, etc.). */
  headers: MessageHeaders;
}

/**
 * Context passed to `onMessageLost` when a message is silently dropped
 * (handler threw and `dlq` is not enabled).
 */
export interface MessageLostContext {
  /** Topic the message was consumed from. */
  topic: string;
  /** Error that caused the message to be dropped. */
  error: Error;
  /** Number of processing attempts (0 = validation failure, before handler ran). */
  attempt: number;
  /** Original Kafka message headers (correlationId, traceparent, etc.). */
  headers: MessageHeaders;
}

/** Options for `KafkaClient` constructor. */
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
}

/** Options for consumer subscribe retry when topic doesn't exist yet. */
export interface SubscribeRetryOptions {
  /** Maximum number of subscribe attempts. Default: `5`. */
  retries?: number;
  /** Delay between retries in ms. Default: `5000`. */
  backoffMs?: number;
}
