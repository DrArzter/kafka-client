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
  /** Highest offset available on the broker for this partition. */
  highWatermark: string;
  /** Send a heartbeat to the broker to prevent session timeout. */
  heartbeat(): Promise<void>;
  /** Mark an offset as processed (for manual offset management). */
  resolveOffset(offset: string): void;
  /** Commit offsets if the auto-commit threshold has been reached. */
  commitOffsetsIfNecessary(): Promise<void>;
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
  /** Called before the consumer handler. Return a cleanup function called after the handler. */
  beforeConsume?(envelope: EventEnvelope<any>): (() => void) | void;
  /** Called when the consumer handler throws. */
  onConsumeError?(envelope: EventEnvelope<any>, error: Error): void;
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

  getClientId: () => ClientId;

  disconnect(): Promise<void>;
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
   * Called when a message is dropped without being sent to a DLQ.
   * Fires when the handler throws after all retries, or schema validation fails — and `dlq` is not enabled.
   * Use this to alert, log to external systems, or trigger fallback logic.
   */
  onMessageLost?: (ctx: MessageLostContext) => void | Promise<void>;
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
