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

/**
 * Message compression codec.
 * Maps directly to the underlying librdkafka codec values.
 * - `'none'` — no compression (default)
 * - `'gzip'` — widely supported, moderate compression ratio
 * - `'snappy'` — fast, moderate compression ratio
 * - `'lz4'` — fastest, slightly lower ratio
 * - `'zstd'` — best ratio, slightly slower
 */
export type CompressionType = "none" | "gzip" | "snappy" | "lz4" | "zstd";

/**
 * Options for sending a single message.
 *
 * @example
 * ```ts
 * await kafka.sendMessage('orders.created', { orderId: '123', amount: 99 }, {
 *   key: 'order-123',
 *   headers: { 'x-source': 'checkout-service' },
 *   compression: 'snappy',
 * });
 * ```
 */
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
  /**
   * Compression codec for this message.
   * Applied at the producer record level — all messages in a single `send` call share the same codec.
   * Default: `'none'`.
   */
  compression?: CompressionType;
}

/**
 * Shape of each item in a `sendBatch` call.
 *
 * @example
 * ```ts
 * await kafka.sendBatch('orders.created', [
 *   { value: { orderId: '1', amount: 10 }, key: 'order-1' },
 *   { value: { orderId: '2', amount: 20 }, key: 'order-2', headers: { 'x-priority': 'high' } },
 * ]);
 * ```
 */
export interface BatchMessageItem<V> {
  value: V;
  /**
   * Kafka partition key for this message.
   * Kafka hashes the key to deterministically route the message to a partition.
   * Messages with the same key always land on the same partition — use this to
   * guarantee ordering per entity (e.g. `userId`, `orderId`).
   */
  key?: string;
  headers?: MessageHeaders;
  correlationId?: string;
  schemaVersion?: number;
  eventId?: string;
}

/**
 * Options for a `sendBatch` call (applies to all messages in the batch).
 *
 * @example
 * ```ts
 * await kafka.sendBatch('metrics', messages, { compression: 'zstd' });
 * ```
 */
export interface BatchSendOptions {
  /**
   * Compression codec for this batch.
   * Applied at the producer record level — all messages in the batch share the same codec.
   * Default: `'none'`.
   */
  compression?: CompressionType;
}

/**
 * Metadata exposed to batch consumer handlers.
 *
 * @example
 * ```ts
 * await kafka.startBatchConsumer(['events'], async (envelopes, meta) => {
 *   console.log(`Partition ${meta.partition}, HWM ${meta.highWatermark}`);
 *   await db.insertMany(envelopes.map(e => e.payload));
 *   meta.resolveOffset(envelopes.at(-1)!.offset);
 *   await meta.commitOffsetsIfNecessary();
 * }, { autoCommit: false });
 * ```
 */
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
 * @example
 * ```ts
 * await kafka.startConsumer(['payments'], handler, {
 *   deduplication: { strategy: 'dlq' },
 *   dlq: true,
 * });
 * ```
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
 * @example
 * ```ts
 * await kafka.startConsumer(['payments'], handler, {
 *   circuitBreaker: {
 *     threshold: 5,
 *     recoveryMs: 30_000,
 *     windowSize: 20,
 *     halfOpenSuccesses: 2,
 *   },
 *   dlq: true,
 * });
 * ```
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

/**
 * Options for configuring a Kafka consumer.
 *
 * @example
 * ```ts
 * await kafka.startConsumer(['orders.created'], handler, {
 *   groupId: 'billing-service',
 *   retry: { maxRetries: 5, backoffMs: 1000 },
 *   dlq: true,
 *   deduplication: { strategy: 'drop' },
 *   circuitBreaker: { threshold: 3, recoveryMs: 60_000 },
 *   interceptors: [loggingInterceptor],
 * });
 * ```
 */
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
  /**
   * Kafka partition assignment strategy for this consumer group.
   * - `'cooperative-sticky'` — **(default)** partitions move as little as possible on rebalance.
   *   Best for horizontal scaling: only the partitions that need to be reassigned are moved.
   * - `'roundrobin'` — distributes partitions as evenly as possible across consumers.
   * - `'range'` — assigns contiguous partition ranges; can cause uneven distribution with multiple topics.
   */
  partitionAssigner?: "roundrobin" | "range" | "cooperative-sticky";
  /**
   * Called when a message is dropped due to TTL expiration (`messageTtlMs`).
   * Fires instead of `onMessageLost` for expired messages when `dlq` is not enabled.
   * When `dlq: true`, expired messages go to the DLQ and this callback is NOT called.
   *
   * **Per-consumer override**: takes precedence over `KafkaClientOptions.onTtlExpired`
   * when both are set. Use this to handle TTL expiry differently per consumer group.
   */
  onTtlExpired?: (ctx: TtlExpiredContext) => void | Promise<void>;
}

/**
 * Configuration for consumer retry behavior.
 *
 * @example
 * ```ts
 * await kafka.startConsumer(['orders'], handler, {
 *   retry: { maxRetries: 5, backoffMs: 500, maxBackoffMs: 10_000 },
 * });
 * ```
 */
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
 *
 * @example
 * ```ts
 * const logger: ConsumerInterceptor = {
 *   async before(envelope) { console.log('Processing', envelope.eventId); },
 *   async after(envelope)  { console.log('Done',       envelope.eventId); },
 *   async onError(envelope, err) { console.error('Failed', err.message); },
 * };
 *
 * await kafka.startConsumer(['orders'], handler, { interceptors: [logger] });
 * ```
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
 *
 * @example
 * ```ts
 * // Cleanup-only (legacy form):
 * beforeConsume(envelope) {
 *   const timer = startTimer();
 *   return () => timer.end();
 * }
 *
 * // Wrap form — run handler inside an OTel span context:
 * beforeConsume(envelope) {
 *   const ctx = propagator.extract(ROOT_CONTEXT, envelope.headers);
 *   const span = tracer.startSpan(envelope.topic, {}, ctx);
 *   return {
 *     wrap: (fn) => context.with(trace.setSpan(ctx, span), fn),
 *     cleanup: () => span.end(),
 *   };
 * }
 * ```
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

/**
 * Options for `readSnapshot`.
 *
 * @example
 * ```ts
 * const snapshot = await kafka.readSnapshot('users.state', {
 *   schema: UserSchema,
 *   onTombstone: (key) => console.log(`Key ${key} was compacted away`),
 * });
 * ```
 */
export interface ReadSnapshotOptions {
  /**
   * Schema to validate each message payload against (Zod, Valibot, ArkType, or any `.parse()` shape).
   * Messages that fail validation are skipped with a warning log — they do not throw.
   */
  schema?: SchemaLike;
  /**
   * Called when a tombstone record (null-value message) is encountered.
   * The corresponding key is removed from the snapshot automatically.
   * Use this for auditing or logging which keys were compacted away.
   */
  onTombstone?: (key: string) => void;
}

/** A single partition offset entry stored in a checkpoint record. */
export interface CheckpointEntry {
  topic: string;
  partition: number;
  offset: string;
}

/** Result returned by a successful `checkpointOffsets` call. */
export interface CheckpointResult {
  /** Consumer group whose offsets were saved. */
  groupId: string;
  /** Topics included in the checkpoint. */
  topics: string[];
  /** Total number of topic-partition pairs saved. */
  partitionCount: number;
  /** Unix timestamp (ms) when the checkpoint was created. */
  savedAt: number;
}

/** Options for `restoreFromCheckpoint`. */
export interface RestoreCheckpointOptions {
  /**
   * Target Unix timestamp (ms). The newest checkpoint whose `savedAt` is **≤ this value**
   * is selected. Defaults to the latest available checkpoint when omitted.
   */
  timestamp?: number;
}

/** Result returned by a successful `restoreFromCheckpoint` call. */
export interface CheckpointRestoreResult {
  /** Consumer group that was repositioned. */
  groupId: string;
  /** The committed offsets restored from the checkpoint. */
  offsets: CheckpointEntry[];
  /** Unix timestamp (ms) recorded when the checkpoint was originally saved. */
  restoredAt: number;
  /** Age of the restored checkpoint in milliseconds (now − `restoredAt`). */
  checkpointAge: number;
}

/**
 * Options for `replayDlq`.
 *
 * @example
 * ```ts
 * await kafka.replayDlq('orders.created', {
 *   targetTopic: 'orders.retry-manual',
 *   dryRun: false,
 *   filter: (headers, value) =>
 *     headers['x-dlq-reason'] === 'handler-error' &&
 *     JSON.parse(value).amount > 0,
 * });
 * ```
 */
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
 *
 * @example
 * ```ts
 * const tracing: KafkaInstrumentation = {
 *   beforeSend(topic, headers) {
 *     headers['traceparent'] = getCurrentTraceId();
 *   },
 *   beforeConsume(envelope) {
 *     const span = tracer.startSpan(envelope.topic);
 *     return { cleanup: () => span.end() };
 *   },
 *   onDlq(envelope, reason) {
 *     metrics.increment('kafka.dlq', { topic: envelope.topic, reason });
 *   },
 * };
 *
 * const kafka = new KafkaClient(config, groupId, { instrumentation: [tracing] });
 * ```
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

/**
 * Context passed to the `transaction()` callback with type-safe send methods.
 *
 * @example
 * ```ts
 * await kafka.transaction(async (tx) => {
 *   await tx.send('inventory.reserved', { itemId: 'a', qty: 1 });
 *   await tx.sendBatch('audit.log', [
 *     { value: { action: 'reserve', itemId: 'a' } },
 *   ]);
 * });
 * ```
 */
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
    options?: BatchSendOptions,
  ): Promise<void>;
  sendBatch<D extends TopicDescriptor<string & keyof T, T[string & keyof T]>>(
    descriptor: D,
    messages: Array<BatchMessageItem<D["__type"]>>,
    options?: BatchSendOptions,
  ): Promise<void>;
}

/**
 * Transactional context passed to each `startTransactionalConsumer` handler invocation.
 *
 * Every call to `send` or `sendBatch` on this object is part of the same Kafka
 * transaction as the source message offset commit. Either all sends and the offset
 * commit succeed atomically, or the transaction is aborted and the source message
 * is redelivered.
 *
 * @example
 * ```ts
 * await kafka.startTransactionalConsumer(['orders.created'], async (envelope, tx) => {
 *   await tx.send('inventory.reserved', { orderId: envelope.payload.orderId, qty: 1 });
 *   await tx.sendBatch('audit.log', [{ value: { action: 'reserve', orderId: envelope.payload.orderId } }]);
 * });
 * ```
 */
export interface TransactionalHandlerContext<T extends TopicMapConstraint<T>> {
  /**
   * Send a message as part of this message's transaction.
   * The send is staged — it only becomes visible to consumers after the transaction commits.
   */
  send<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;

  /**
   * Send multiple messages as part of this message's transaction.
   * All messages are staged together and become visible only after commit.
   */
  sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<BatchMessageItem<T[K]>>,
    options?: BatchSendOptions,
  ): Promise<void>;
}

/**
 * Routing configuration for `startRoutedConsumer`.
 *
 * Messages are dispatched to the handler whose key matches the value of `header`.
 * Unmatched messages are forwarded to `fallback` if provided, or silently skipped.
 *
 * @example
 * ```ts
 * await kafka.startRoutedConsumer(['events'], {
 *   header: 'x-event-type',
 *   routes: {
 *     'order.created':   async (e) => handleOrderCreated(e.payload),
 *     'order.cancelled': async (e) => handleOrderCancelled(e.payload),
 *   },
 *   fallback: async (e) => logger.warn('Unknown event type', e.headers),
 * });
 * ```
 */
export interface RoutingOptions<E> {
  /** Header whose value determines which handler is invoked. */
  header: string;
  /**
   * Map of header value → handler.
   * The handler with the matching key is called for each message.
   */
  routes: Record<string, (envelope: EventEnvelope<E>) => Promise<void>>;
  /**
   * Called when no route matches the header value (or the header is absent).
   * If omitted, unmatched messages are silently skipped.
   */
  fallback?: (envelope: EventEnvelope<E>) => Promise<void>;
}

/**
 * Metadata passed to the `startWindowConsumer` handler on each flush.
 *
 * @example
 * ```ts
 * await kafka.startWindowConsumer('events', async (batch, meta) => {
 *   console.log(`Flush: ${batch.length} events, trigger=${meta.trigger}`);
 *   console.log(`Window: ${meta.windowEnd - meta.windowStart} ms`);
 *   await db.insertMany(batch.map(e => e.payload));
 * }, { maxMessages: 100, maxMs: 5_000 });
 * ```
 */
export interface WindowMeta {
  /** What triggered this flush: accumulated `maxMessages` messages, or the `maxMs` timer. */
  trigger: "size" | "time";
  /** Unix timestamp (ms) of the first message that entered the current window. */
  windowStart: number;
  /** Unix timestamp (ms) when the flush was initiated. */
  windowEnd: number;
}

/**
 * Options for `startWindowConsumer`.
 *
 * Extends `ConsumerOptions` — all standard consumer settings apply.
 * `retryTopics`, `retryTopicAssignmentTimeoutMs`, and `queueHighWaterMark`
 * are excluded: they are incompatible with windowed accumulation.
 */
export interface WindowConsumerOptions<
  T extends TopicMapConstraint<T> = TTopicMessageMap,
> extends Omit<
  ConsumerOptions<T>,
  "retryTopics" | "retryTopicAssignmentTimeoutMs" | "queueHighWaterMark"
> {
  /**
   * Maximum number of messages to accumulate before flushing.
   * When the buffer reaches this size the handler is called immediately,
   * regardless of how much time has elapsed since the first message.
   */
  maxMessages: number;
  /**
   * Maximum time (ms) to wait after the first message before flushing.
   * When this timer expires the handler is called with whatever messages
   * have accumulated so far — even if the buffer is not full.
   */
  maxMs: number;
}

/**
 * Handle returned by `startConsumer` / `startBatchConsumer`.
 *
 * @example
 * ```ts
 * const handle = await kafka.startConsumer(['orders'], handler);
 * // later, on shutdown:
 * await handle.stop();
 * ```
 */
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

/** Summary of a consumer group returned by `listConsumerGroups`. */
export interface ConsumerGroupSummary {
  /** Consumer group ID. */
  groupId: string;
  /**
   * Current broker-reported state of the group.
   * Common values: `'Empty'`, `'Stable'`, `'PreparingRebalance'`, `'CompletingRebalance'`, `'Dead'`.
   */
  state: string;
}

/** Partition-level metadata for a topic. */
export interface TopicPartitionInfo {
  /** Partition index (0-based). */
  partition: number;
  /** Node ID of the partition leader broker. */
  leader: number;
  /** Node IDs of all replica brokers. */
  replicas: number[];
  /** Node IDs of in-sync replicas. */
  isr: number[];
}

/** Topic metadata returned by `describeTopics`. */
export interface TopicDescription {
  /** Topic name. */
  name: string;
  /** Per-partition metadata. */
  partitions: TopicPartitionInfo[];
}

/** Interface describing all public methods of the Kafka client. */
export interface IKafkaClient<T extends TopicMapConstraint<T>> {
  /**
   * @example
   * ```ts
   * const status = await kafka.checkStatus();
   * if (status.status === 'down') console.error(status.error);
   * ```
   */
  checkStatus(): Promise<KafkaHealthResult>;

  /**
   * List all consumer groups known to the broker.
   * @returns Array of `{ groupId, state }` summaries.
   *
   * @example
   * ```ts
   * const groups = await kafka.listConsumerGroups();
   * console.log(groups.map(g => `${g.groupId}: ${g.state}`));
   * ```
   */
  listConsumerGroups(): Promise<ConsumerGroupSummary[]>;

  /**
   * Describe topics — returns partition layout, leader, replicas, and ISR for each topic.
   * @param topics Topic names to describe. Omit to describe all topics visible to this client.
   *
   * @example
   * ```ts
   * const [desc] = await kafka.describeTopics(['orders.created']);
   * console.log(desc.partitions.length); // number of partitions
   * ```
   */
  describeTopics(topics?: string[]): Promise<TopicDescription[]>;

  /**
   * Delete records from a topic up to (but not including) the specified offsets.
   * All messages with offsets **before** the given offset are deleted.
   * Useful for purging expired or sensitive data.
   *
   * @param topic Topic name.
   * @param partitions Array of `{ partition, offset }` — records before each offset are deleted.
   *
   * @example
   * ```ts
   * await kafka.deleteRecords('orders.created', [
   *   { partition: 0, offset: '1000' },
   *   { partition: 1, offset: '500'  },
   * ]);
   * ```
   */
  deleteRecords(
    topic: string,
    partitions: Array<{ partition: number; offset: string }>,
  ): Promise<void>;

  /**
   * Query the consumer group lag per partition using the admin API.
   * Lag = (broker high-watermark offset) − (last committed offset).
   * - A committed offset of `-1` (no offset committed yet) counts as full lag.
   * - Defaults to the client's default `groupId` when none is provided.
   *
   * @example
   * ```ts
   * const lag = await kafka.getConsumerLag();
   * const total = lag.reduce((sum, p) => sum + p.lag, 0);
   * console.log(`Total lag: ${total} messages`);
   * ```
   */
  getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>>;

  /**
   * @example
   * ```ts
   * const handle = await kafka.startConsumer(['orders.created'], async (envelope) => {
   *   await processOrder(envelope.payload);
   * }, { retry: { maxRetries: 3 }, dlq: true });
   *
   * // on shutdown:
   * await handle.stop();
   * ```
   */
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

  /**
   * Subscribe using regex topic patterns (or a mix of strings and patterns).
   * Note: type-safety is reduced to the union of all topic payloads when using regex.
   * Incompatible with `retryTopics: true`.
   */
  startConsumer(
    topics: (string | RegExp)[],
    handleMessage: (envelope: EventEnvelope<T[keyof T]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * @example
   * ```ts
   * await kafka.startBatchConsumer(['metrics'], async (envelopes, meta) => {
   *   await db.insertMany(envelopes.map(e => e.payload));
   *   meta.resolveOffset(envelopes.at(-1)!.offset);
   * }, { autoCommit: false });
   * ```
   */
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
   * Subscribe using regex topic patterns (or a mix of strings and patterns).
   * Note: type-safety is reduced to the union of all topic payloads when using regex.
   * Incompatible with `retryTopics: true`.
   */
  startBatchConsumer(
    topics: (string | RegExp)[],
    handleBatch: (
      envelopes: EventEnvelope<T[keyof T]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * Subscribe to a topic and accumulate messages into a window, flushing the handler
   * when **either** `maxMessages` messages have accumulated **or** `maxMs` milliseconds
   * have elapsed since the first message in the current window — whichever fires first.
   *
   * This is semantically different from `startBatchConsumer`, which delivers
   * broker-sized batches of unpredictable size. A window consumer gives you control
   * over both the size and the latency of each batch — useful for micro-batching
   * writes to a database, aggregating events before processing, or rate-limiting a
   * downstream API.
   *
   * On `handle.stop()`, any messages remaining in the buffer are flushed before the
   * consumer disconnects, so no messages are lost on clean shutdown.
   *
   * @param topic Topic to consume.
   * @param handler Called with each flushed window and a `WindowMeta` describing the trigger.
   * @param options Window size/timeout and standard consumer options.
   *
   * @example
   * ```ts
   * await kafka.startWindowConsumer('events', async (batch, meta) => {
   *   console.log(`Flushing ${batch.length} events (trigger: ${meta.trigger})`);
   *   await db.insertMany(batch.map(e => e.payload));
   * }, { maxMessages: 100, maxMs: 5_000 });
   * ```
   */
  startWindowConsumer<K extends keyof T & string>(
    topic: K,
    handler: (
      envelopes: EventEnvelope<T[K]>[],
      meta: WindowMeta,
    ) => Promise<void>,
    options: WindowConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * Subscribe to one or more topics and dispatch each message to a handler based
   * on the value of a specific Kafka header. Eliminates boilerplate `if/switch`
   * statements inside a catch-all handler when one topic carries multiple event types.
   *
   * Messages whose header value does not match any route key are forwarded to `fallback`
   * if provided, or silently skipped otherwise.
   *
   * Accepts the same `ConsumerOptions` as `startConsumer` — retry, DLQ, deduplication,
   * circuit breaker, interceptors, etc. all apply to every route.
   *
   * @param topics Array of topic keys or `RegExp` patterns.
   * @param routing Header name, route map, and optional fallback handler.
   * @param options Standard consumer options.
   * @returns A handle with `{ groupId, stop() }`.
   *
   * @example
   * ```ts
   * await kafka.startRoutedConsumer(['domain.events'], {
   *   header: 'x-event-type',
   *   routes: {
   *     'order.created':   async (e) => handleOrderCreated(e.payload),
   *     'order.cancelled': async (e) => handleOrderCancelled(e.payload),
   *   },
   *   fallback: async (e) => logger.warn('Unknown event type', e.headers),
   * });
   * ```
   */
  startRoutedConsumer<K extends Array<keyof T>>(
    topics: K,
    routing: RoutingOptions<T[K[number]]>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * Subscribe to topics and consume messages with **exactly-once semantics** for
   * read-process-write pipelines.
   *
   * Each message is processed inside a Kafka transaction: the handler receives a
   * `TransactionalHandlerContext` with `send` / `sendBatch` methods that stage
   * outgoing messages inside the transaction. When the handler resolves, the source
   * offset commit and all staged sends are committed atomically. A handler error
   * aborts the transaction and the source message is redelivered — no sends become
   * visible to downstream consumers.
   *
   * Incompatible with `retryTopics: true` — throws at startup if set.
   * `autoCommit` is always `false` (managed by the transaction).
   *
   * @param topics Array of topic keys.
   * @param handler Called for each message with the decoded envelope and a transaction context.
   * @param options Standard consumer options (`retry`, `dlq`, `deduplication`, etc.).
   * @returns A handle with `{ groupId, stop() }`.
   *
   * @example
   * ```ts
   * await kafka.startTransactionalConsumer(['orders.created'], async (envelope, tx) => {
   *   await tx.send('inventory.reserved', { orderId: envelope.payload.orderId, qty: 1 });
   * });
   * ```
   */
  startTransactionalConsumer<K extends Array<keyof T>>(
    topics: K,
    handler: (
      envelope: EventEnvelope<T[K[number]]>,
      tx: TransactionalHandlerContext<T>,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * Stop consumer(s).
   * - `stopConsumer(groupId)` — disconnect and remove the consumer for a specific group.
   * - `stopConsumer()` — disconnect and remove all consumers.
   */
  stopConsumer(groupId?: string): Promise<void>;

  /**
   * @example
   * ```ts
   * await kafka.sendMessage('orders.created', { orderId: '123', amount: 99 });
   * ```
   */
  sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;

  /**
   * Send a null-value (tombstone) message to a topic.
   * Tombstones are used with log-compacted topics to signal that a key's record
   * should be removed during the next compaction cycle.
   *
   * Unlike `sendMessage`, tombstones carry no payload, no envelope headers, and
   * skip schema validation. Only the partition `key` and optional custom `headers`
   * are forwarded to Kafka.
   *
   * @param topic Topic name or descriptor.
   * @param key Partition key identifying the record to tombstone.
   * @param headers Optional custom Kafka headers.
   *
   * @example
   * ```ts
   * await kafka.sendTombstone('users.state', 'user-42');
   * ```
   */
  sendTombstone(
    topic: string,
    key: string,
    headers?: MessageHeaders,
  ): Promise<void>;

  /**
   * @example
   * ```ts
   * await kafka.sendBatch('orders.created', [
   *   { value: { orderId: '1', amount: 10 }, key: 'order-1' },
   *   { value: { orderId: '2', amount: 20 }, key: 'order-2' },
   * ]);
   * ```
   */
  sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<BatchMessageItem<T[K]>>,
    options?: BatchSendOptions,
  ): Promise<void>;

  /**
   * @example
   * ```ts
   * await kafka.transaction(async (tx) => {
   *   await tx.send('orders.created',    { orderId: '123', amount: 99 });
   *   await tx.send('inventory.reserved', { itemId: 'a',  qty: 1 });
   * });
   * ```
   */
  transaction(fn: (ctx: TransactionContext<T>) => Promise<void>): Promise<void>;

  /**
   * @example
   * ```ts
   * const id = kafka.getClientId(); // e.g. 'my-service'
   * ```
   */
  getClientId(): ClientId;

  /**
   * Return a snapshot of internal event counters (retry / DLQ / dedup).
   * - `getMetrics()` — aggregate across all topics.
   * - `getMetrics(topic)` — counters for a specific topic only; returns all-zero
   *   if no events have been observed for that topic yet.
   *
   * Counters accumulate since client creation or the last `resetMetrics()` call.
   *
   * @example
   * ```ts
   * const { processedCount, dlqCount, retryCount } = kafka.getMetrics();
   * console.log(`Processed: ${processedCount}, DLQ: ${dlqCount}`);
   *
   * const topicMetrics = kafka.getMetrics('orders.created');
   * ```
   */
  getMetrics(topic?: string): Readonly<KafkaMetrics>;

  /**
   * Reset internal event counters to zero.
   * - `resetMetrics()` — reset all topics.
   * - `resetMetrics(topic)` — reset a single topic only.
   *
   * @example
   * ```ts
   * kafka.resetMetrics();                   // reset all
   * kafka.resetMetrics('orders.created');   // reset one topic
   * ```
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
   *
   * @example
   * ```ts
   * const { replayed, skipped } = await kafka.replayDlq('orders.created');
   * console.log(`Replayed ${replayed}, skipped ${skipped}`);
   *
   * // dry-run with filter:
   * await kafka.replayDlq('orders.created', {
   *   dryRun: true,
   *   filter: (headers) => headers['x-dlq-reason'] === 'handler-error',
   * });
   * ```
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
   *
   * @example
   * ```ts
   * await kafka.stopConsumer('billing-service');
   * await kafka.resetOffsets('billing-service', 'orders.created', 'earliest');
   * ```
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
   *
   * @example
   * ```ts
   * await kafka.seekToOffset('billing-service', [
   *   { topic: 'orders.created', partition: 0, offset: '1000' },
   *   { topic: 'orders.created', partition: 1, offset: '500'  },
   * ]);
   * ```
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
   *
   * @example
   * ```ts
   * const midnight = new Date('2025-01-01').getTime();
   * await kafka.seekToTimestamp('billing-service', [
   *   { topic: 'orders.created', partition: 0, timestamp: midnight },
   * ]);
   * ```
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
   *
   * @example
   * ```ts
   * const state = kafka.getCircuitState('orders.created', 0);
   * if (state?.status === 'open') console.warn('Circuit open!', state.failures);
   * ```
   */
  getCircuitState(
    topic: string,
    partition: number,
    groupId?: string,
  ):
    | {
        status: "closed" | "open" | "half-open";
        failures: number;
        windowSize: number;
      }
    | undefined;

  /**
   * Read a compacted topic from the beginning to its current high-watermark and
   * return a `Map<key, EventEnvelope<T>>` with the **latest** value per key.
   *
   * Tombstone records (null-value messages) remove the key from the map —
   * consistent with log-compaction semantics.
   *
   * Useful for bootstrapping in-memory state at service startup without an external cache:
   * ```ts
   * const orders = await kafka.readSnapshot('orders.state');
   * // orders.get('order-123') → EventEnvelope with the latest payload for that key
   * ```
   *
   * @param topic Topic to read. Must be a log-compacted topic (or any topic you want a key → latest-value index for).
   * @param options Optional schema validation and tombstone callback.
   * @returns Map keyed by the Kafka message key string; value is the last seen `EventEnvelope`.
   */
  readSnapshot<K extends keyof T & string>(
    topic: K,
    options?: ReadSnapshotOptions,
  ): Promise<Map<string, EventEnvelope<T[K]>>>;

  /**
   * Snapshot the current committed offsets of a consumer group into a Kafka topic.
   *
   * Each call appends a new checkpoint record keyed by `groupId`. The checkpoint topic
   * acts as an append-only audit log — use a non-compacted topic to retain history.
   * Use `restoreFromCheckpoint` to rewind the group to any saved point in time.
   *
   * Requires `connectProducer()` to have been called.
   *
   * @param groupId Consumer group whose offsets to checkpoint. Defaults to the client's default group.
   * @param checkpointTopic Topic where checkpoint records are written.
   * @returns Summary of the saved checkpoint.
   *
   * @example
   * ```ts
   * const result = await kafka.checkpointOffsets(undefined, 'checkpoints');
   * console.log(`Saved ${result.partitionCount} offsets at ${result.savedAt}`);
   * ```
   */
  checkpointOffsets(
    groupId: string | undefined,
    checkpointTopic: string,
  ): Promise<CheckpointResult>;

  /**
   * Restore a consumer group's committed offsets from the nearest checkpoint stored in `checkpointTopic`.
   *
   * Reads all checkpoint records for the group, selects the newest checkpoint whose `savedAt`
   * timestamp is ≤ `options.timestamp` (or the latest checkpoint if no timestamp is given),
   * then calls `admin.setOffsets` for every topic-partition in that checkpoint.
   *
   * **The consumer group must be stopped before calling this method** — throws if any consumer
   * in the group is currently running.
   *
   * @param groupId Consumer group to reposition. Defaults to the client's default group.
   * @param checkpointTopic Topic where checkpoints were written by `checkpointOffsets`.
   * @param options.timestamp Target Unix ms. Omit to restore the latest checkpoint.
   * @throws If no checkpoint exists for the group, or if the group is still running.
   *
   * @example
   * ```ts
   * await kafka.stopConsumer('billing-service');
   * const result = await kafka.restoreFromCheckpoint(undefined, 'checkpoints');
   * console.log(`Restored from checkpoint saved ${result.checkpointAge}ms ago`);
   *
   * // restore to the state before a specific deployment:
   * await kafka.restoreFromCheckpoint(undefined, 'checkpoints', {
   *   timestamp: new Date('2025-06-01T00:00:00Z').getTime(),
   * });
   * ```
   */
  restoreFromCheckpoint(
    groupId: string | undefined,
    checkpointTopic: string,
    options?: RestoreCheckpointOptions,
  ): Promise<CheckpointRestoreResult>;

  /**
   * Consume messages as an async iterator. Useful for scripts, migrations, and
   * one-off processing where the full `startConsumer` lifecycle is unnecessary.
   *
   * Breaking out of the loop (or calling `return()` on the iterator) stops the
   * underlying consumer automatically.
   *
   * @remarks
   * **Limitations vs `startConsumer`:**
   * - Does **not** support `retryTopics: true` (EOS retry chains). Use `startConsumer` for that.
   * - Does not support regex topic patterns.
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
   *
   * @example
   * ```ts
   * kafka.pauseConsumer(undefined, [{ topic: 'orders.created', partitions: [0, 1] }]);
   * // ... do some backpressure work ...
   * kafka.resumeConsumer(undefined, [{ topic: 'orders.created', partitions: [0, 1] }]);
   * ```
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
   *
   * @example
   * ```ts
   * kafka.resumeConsumer(undefined, [{ topic: 'orders.created', partitions: [0, 1] }]);
   * ```
   */
  resumeConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void;

  /**
   * Drain in-flight handlers, then disconnect all producers, consumers, and admin.
   * @param drainTimeoutMs Max ms to wait for in-flight handlers (default 30 000).
   *
   * @example
   * ```ts
   * await kafka.disconnect();
   * ```
   */
  disconnect(drainTimeoutMs?: number): Promise<void>;

  /**
   * Register SIGTERM / SIGINT signal handlers that drain in-flight messages before
   * disconnecting. Call once after constructing the client in non-NestJS apps.
   * NestJS apps get drain automatically via `onModuleDestroy` → `disconnect()`.
   *
   * @example
   * ```ts
   * kafka.enableGracefulShutdown(['SIGTERM', 'SIGINT'], 30_000);
   * ```
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
 *
 * @example
 * ```ts
 * // Pass a NestJS logger:
 * const kafka = new KafkaClient(config, groupId, { logger: this.logger });
 *
 * // Or a minimal pino wrapper:
 * const kafka = new KafkaClient(config, groupId, {
 *   logger: {
 *     log:   (msg) => pino.info(msg),
 *     warn:  (msg, ...a) => pino.warn(msg, ...a),
 *     error: (msg, ...a) => pino.error(msg, ...a),
 *     debug: (msg, ...a) => pino.debug(msg, ...a),
 *   },
 * });
 * ```
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
 *
 * @example
 * ```ts
 * const kafka = new KafkaClient(config, groupId, {
 *   onTtlExpired: ({ topic, ageMs, messageTtlMs }) => {
 *     console.warn(`Message on ${topic} expired: age=${ageMs}ms, ttl=${messageTtlMs}ms`);
 *   },
 * });
 * ```
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
 *
 * @example
 * ```ts
 * const kafka = new KafkaClient(config, groupId, {
 *   onMessageLost: ({ topic, error, attempt }) => {
 *     alerting.fire('kafka.message-lost', { topic, error: error.message, attempt });
 *   },
 * });
 * ```
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
}

/**
 * Options for consumer subscribe retry when topic doesn't exist yet.
 *
 * @example
 * ```ts
 * await kafka.startConsumer(['orders.created'], handler, {
 *   subscribeRetry: { retries: 10, backoffMs: 2_000 },
 * });
 * ```
 */
export interface SubscribeRetryOptions {
  /** Maximum number of subscribe attempts. Default: `5`. */
  retries?: number;
  /** Delay between retries in ms. Default: `5000`. */
  backoffMs?: number;
}
