import type { TTopicMessageMap, TopicMapConstraint, MessageHeaders } from "./common";
import type { SendOptions, BatchMessageItem, BatchSendOptions } from "./producer.types";
import type { DedupStore } from "./dedup.types";
import type { TopicDescriptor, SchemaLike } from "../message/topic";
import type { EventEnvelope } from "../message/envelope";

// ── Batch consumer ────────────────────────────────────────────────────────────

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

// ── Retry / dedup / circuit breaker ──────────────────────────────────────────

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
 * **In-session limitation (default store)**: with the built-in in-memory store,
 * deduplication state is reset whenever the process restarts or `stopConsumer`
 * is called. After a restart, previously processed messages with `clock ≤ N`
 * will be re-processed until their offsets catch up to the high-watermark again.
 * The same applies after a rebalance: the instance that receives the partition
 * begins with empty state. Provide a persistent `store` (e.g. Redis-backed) to
 * make deduplication survive both restarts and rebalances.
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
  /**
   * Pluggable backing store for the per-partition last-processed clock.
   *
   * When omitted, an in-memory store is used — state is per-client, keyed by
   * `groupId`, and cleared on `stopConsumer` / `disconnect`. Provide a
   * persistent `DedupStore` (e.g. Redis-backed) so deduplication survives
   * process restarts and consumer group rebalances.
   *
   * Store errors are handled fail-open: on a thrown/rejected read or write the
   * error is logged and the message is treated as not a duplicate.
   */
  store?: DedupStore;
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

// ── Interceptor / instrumentation ────────────────────────────────────────────

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

// ── Callback contexts ─────────────────────────────────────────────────────────

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

// ── ConsumerOptions & SubscribeRetryOptions ───────────────────────────────────

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
   * Static group membership id (`group.instance.id`).
   *
   * A member that restarts within the broker's `session.timeout.ms` rejoins
   * the group with the same partitions and **no rebalance** — useful for k8s
   * rolling restarts and short redeploys. Must be unique per member within
   * the consumer group (e.g. derive from the pod ordinal / hostname).
   *
   * Not propagated to retry-chain companion consumers (they run in their own
   * groups and rebalance independently).
   *
   * @example
   * ```ts
   * await kafka.startConsumer(['orders'], handler, {
   *   groupInstanceId: `orders-svc-${process.env.HOSTNAME}`,
   * });
   * ```
   */
  groupInstanceId?: string;
  /**
   * Called when a message is dropped due to TTL expiration (`messageTtlMs`).
   * Fires instead of `onMessageLost` for expired messages when `dlq` is not enabled.
   * When `dlq: true`, expired messages go to the DLQ and this callback is NOT called.
   *
   * **Per-consumer override**: takes precedence over `KafkaClientOptions.onTtlExpired`
   * when both are set. Use this to handle TTL expiry differently per consumer group.
   */
  onTtlExpired?: (ctx: TtlExpiredContext) => void | Promise<void>;
  /**
   * Called when a message is silently dropped after all retries are exhausted
   * and `dlq` is not enabled.
   *
   * **Per-consumer override**: takes precedence over `KafkaClientOptions.onMessageLost`
   * when both are set. Use this for consumer-specific alerting or dead-message logging.
   */
  onMessageLost?: (ctx: MessageLostContext) => void | Promise<void>;
  /**
   * Called before each retry attempt (both in-process and retry-topic paths).
   *
   * **Per-consumer override**: fires in addition to any global instrumentation hooks.
   * Use this for per-consumer retry metrics or structured logging.
   */
  onRetry?: (
    envelope: EventEnvelope<any>,
    attempt: number,
    maxRetries: number,
  ) => void | Promise<void>;
}

// ── ConsumerHandle ────────────────────────────────────────────────────────────

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
  /**
   * Resolves once the consumer has received its first partition assignment from
   * the broker (i.e. it has joined the group and is ready to receive messages).
   *
   * Use this in tests and startup probes instead of a fixed `setTimeout` delay:
   *
   * @example
   * ```ts
   * const handle = await kafka.startConsumer(['orders'], handler);
   * await handle.ready(); // wait for partition assignment — no fixed sleep needed
   * await kafka.sendMessage('orders', payload);
   * ```
   */
  ready(): Promise<void>;
}

// ── Specialized consumer types ────────────────────────────────────────────────

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
