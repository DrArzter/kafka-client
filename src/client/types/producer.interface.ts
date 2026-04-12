import type { TopicMapConstraint, MessageHeaders } from "./common";
import type { SendOptions, BatchMessageItem, BatchSendOptions } from "./producer.types";
import type { TransactionContext } from "./consumer.types";
import type { TopicDescriptor } from "../message/topic";

/** Producer methods of `IKafkaClient`. */
export interface IKafkaProducer<T extends TopicMapConstraint<T>> {
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

  sendMessage<D extends TopicDescriptor<string & keyof T, T[string & keyof T]>>(
    descriptor: D,
    message: D["__type"],
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

  sendBatch<D extends TopicDescriptor<string & keyof T, T[string & keyof T]>>(
    descriptor: D,
    messages: Array<BatchMessageItem<D["__type"]>>,
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
}
