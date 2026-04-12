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
