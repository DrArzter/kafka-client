import type { CompressionType, MessageHeaders } from "./common";

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
