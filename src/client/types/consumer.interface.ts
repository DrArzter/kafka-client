import type { TopicMapConstraint } from "./common";
import type {
  ConsumerOptions,
  ConsumerHandle,
  BatchMeta,
  TransactionalHandlerContext,
  RoutingOptions,
  WindowConsumerOptions,
  WindowMeta,
} from "./consumer.types";
import type { TopicDescriptor } from "../message/topic";
import type { EventEnvelope } from "../message/envelope";

/** Consumer methods of `IKafkaClient`. */
export interface IKafkaConsumer<T extends TopicMapConstraint<T>> {
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
   * have elapsed — whichever fires first. On `handle.stop()`, any remaining buffered
   * messages are flushed before the consumer disconnects.
   */
  startWindowConsumer<K extends keyof T & string>(
    topic: K,
    handler: (
      batch: EventEnvelope<T[K]>[],
      meta: WindowMeta,
    ) => Promise<void>,
    options: WindowConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * Subscribe to topics and dispatch each message to a handler based on a Kafka header value.
   * Messages whose header is absent or doesn't match any route key are forwarded to `fallback`
   * (or silently skipped when no fallback is set).
   */
  startRoutedConsumer<K extends Array<keyof T>>(
    topics: K,
    routing: RoutingOptions<T[K[number]]>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;

  /**
   * Subscribe to topics and consume messages with **exactly-once semantics** for
   * read-process-write pipelines. Each message is processed inside a Kafka transaction;
   * on handler success the source offset commit and all staged sends are committed
   * atomically. Incompatible with `retryTopics: true`.
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
   * Consume messages as an async iterator. Useful for scripts, migrations, and
   * one-off processing. Breaking out of the loop stops the consumer automatically.
   * Does **not** support `retryTopics: true`.
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
   * Pause message delivery for specific topic-partitions. The consumer remains
   * connected; only polling is suspended. Call `resumeConsumer` to restart.
   *
   * @example
   * ```ts
   * kafka.pauseConsumer(undefined, [{ topic: 'orders.created', partitions: [0, 1] }]);
   * ```
   */
  pauseConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void;

  /**
   * Resume message delivery for previously paused topic-partitions.
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
}
