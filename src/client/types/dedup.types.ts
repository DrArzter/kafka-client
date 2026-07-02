/**
 * Pluggable backing store for Lamport-clock deduplication.
 *
 * By default deduplication state is held in memory (`InMemoryDedupStore`) and is
 * reset on process restart or rebalance. Provide a custom `DedupStore` — backed
 * by Redis, a database, or any external system — to make deduplication survive
 * restarts and rebalances.
 *
 * The store is keyed by consumer `groupId` and `"topic:partition"`. Both methods
 * may run synchronously or return a promise; `applyDeduplication` always awaits
 * the result.
 *
 * **Failure semantics (fail-open):** if `getLastClock` or `setLastClock` throws
 * or rejects, the error is logged and the message is treated as **not** a
 * duplicate. This biases towards at-least-once delivery — a transient store
 * outage never silently drops messages, it only weakens deduplication until the
 * store recovers.
 *
 * @example
 * ```ts
 * // Minimal Redis-backed store (generic redis client pseudocode).
 * class RedisDedupStore implements DedupStore {
 *   constructor(private readonly redis: RedisClient) {}
 *
 *   private key(groupId: string, topicPartition: string): string {
 *     return `dedup:${groupId}:${topicPartition}`;
 *   }
 *
 *   async getLastClock(groupId: string, topicPartition: string): Promise<number | undefined> {
 *     const raw = await this.redis.get(this.key(groupId, topicPartition));
 *     return raw === null ? undefined : Number(raw);
 *   }
 *
 *   async setLastClock(groupId: string, topicPartition: string, clock: number): Promise<void> {
 *     await this.redis.set(this.key(groupId, topicPartition), String(clock));
 *   }
 * }
 *
 * await kafka.startConsumer(['payments'], handler, {
 *   deduplication: { strategy: 'drop', store: new RedisDedupStore(redis) },
 * });
 * ```
 */
export interface DedupStore {
  /** Return the last processed Lamport clock for a group+topic:partition, or undefined if none. */
  getLastClock(
    groupId: string,
    topicPartition: string,
  ): number | undefined | Promise<number | undefined>;
  /** Persist the last processed Lamport clock for a group+topic:partition. */
  setLastClock(
    groupId: string,
    topicPartition: string,
    clock: number,
  ): void | Promise<void>;
}
