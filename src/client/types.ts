import { TopicDescriptor, SchemaLike } from "./topic";

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
  /** Custom headers attached to the message. */
  headers?: MessageHeaders;
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
}

/** Configuration for consumer retry behavior. */
export interface RetryOptions {
  /** Maximum number of retry attempts before giving up. */
  maxRetries: number;
  /** Base delay between retries in ms (multiplied by attempt number). Default: `1000`. */
  backoffMs?: number;
}

/**
 * Interceptor hooks for consumer message processing.
 * All methods are optional — implement only what you need.
 */
export interface ConsumerInterceptor<
  T extends TopicMapConstraint<T> = TTopicMessageMap,
> {
  /** Called before the message handler. */
  before?(message: T[keyof T], topic: string): Promise<void> | void;
  /** Called after the message handler succeeds. */
  after?(message: T[keyof T], topic: string): Promise<void> | void;
  /** Called when the message handler throws. */
  onError?(
    message: T[keyof T],
    topic: string,
    error: Error,
  ): Promise<void> | void;
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
    messages: Array<{ value: T[K]; key?: string; headers?: MessageHeaders }>,
  ): Promise<void>;
  sendBatch<D extends TopicDescriptor<string & keyof T, T[string & keyof T]>>(
    descriptor: D,
    messages: Array<{
      value: D["__type"];
      key?: string;
      headers?: MessageHeaders;
    }>,
  ): Promise<void>;
}

/** Interface describing all public methods of the Kafka client. */
export interface IKafkaClient<T extends TopicMapConstraint<T>> {
  checkStatus(): Promise<{ topics: string[] }>;

  startConsumer<K extends Array<keyof T>>(
    topics: K,
    handleMessage: (message: T[K[number]], topic: K[number]) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;

  startConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleMessage: (message: D["__type"], topic: D["__topic"]) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;

  startBatchConsumer<K extends Array<keyof T>>(
    topics: K,
    handleBatch: (
      messages: T[K[number]][],
      topic: K[number],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;

  startBatchConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleBatch: (
      messages: D["__type"][],
      topic: D["__topic"],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;

  stopConsumer(): Promise<void>;

  sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;

  sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<{ value: T[K]; key?: string; headers?: MessageHeaders }>,
  ): Promise<void>;

  transaction(fn: (ctx: TransactionContext<T>) => Promise<void>): Promise<void>;

  getClientId: () => ClientId;

  disconnect(): Promise<void>;
}

/** Options for `KafkaClient` constructor. */
export interface KafkaClientOptions {
  /** Auto-create topics via admin before the first `sendMessage`, `sendBatch`, or `transaction` for each topic. Useful for development — not recommended in production. */
  autoCreateTopics?: boolean;
  /** When `true`, string topic keys are validated against any schema previously registered via a TopicDescriptor. Default: `true`. */
  strictSchemas?: boolean;
}

/** Options for consumer subscribe retry when topic doesn't exist yet. */
export interface SubscribeRetryOptions {
  /** Maximum number of subscribe attempts. Default: `5`. */
  retries?: number;
  /** Delay between retries in ms. Default: `5000`. */
  backoffMs?: number;
}
