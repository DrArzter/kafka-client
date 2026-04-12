import type { TopicMapConstraint } from "./common";
import type {
  KafkaHealthResult,
  ConsumerGroupSummary,
  TopicDescription,
  ReadSnapshotOptions,
  CheckpointResult,
  RestoreCheckpointOptions,
  CheckpointRestoreResult,
  DlqReplayOptions,
} from "./admin.types";
import type { EventEnvelope } from "../message/envelope";

/** Admin, offset-management, and state methods of `IKafkaClient`. */
export interface IKafkaAdmin<T extends TopicMapConstraint<T>> {
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
   * Omit `topics` to describe all topics visible to this client.
   *
   * @example
   * ```ts
   * const [desc] = await kafka.describeTopics(['orders.created']);
   * console.log(desc.partitions.length);
   * ```
   */
  describeTopics(topics?: string[]): Promise<TopicDescription[]>;

  /**
   * Delete records from a topic up to (but not including) the specified offsets.
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
   * Query the consumer group lag per partition.
   * Lag = (broker high-watermark offset) − (last committed offset).
   *
   * @example
   * ```ts
   * const lag = await kafka.getConsumerLag();
   * const total = lag.reduce((sum, p) => sum + p.lag, 0);
   * ```
   */
  getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>>;

  /**
   * Consume all messages in `{topic}.dlq` and re-publish each to its original topic
   * (or `options.targetTopic`). The DLQ topic itself is not modified.
   *
   * @returns `{ replayed, skipped }` counts.
   *
   * @example
   * ```ts
   * const { replayed } = await kafka.replayDlq('orders.created');
   * ```
   */
  replayDlq(
    topic: string,
    options?: DlqReplayOptions,
  ): Promise<{ replayed: number; skipped: number }>;

  /**
   * Reset committed offsets to `'earliest'` or `'latest'`.
   * The consumer group must be inactive — call `stopConsumer(groupId)` first.
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
   * The consumer group must be inactive.
   *
   * @example
   * ```ts
   * await kafka.seekToOffset('billing-service', [
   *   { topic: 'orders.created', partition: 0, offset: '1000' },
   * ]);
   * ```
   */
  seekToOffset(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; offset: string }>,
  ): Promise<void>;

  /**
   * Seek partitions to the offset nearest to a given Unix timestamp (ms).
   * The consumer group must be inactive.
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
   * Returns the current circuit breaker state for a topic partition.
   * Returns `undefined` when `circuitBreaker` is not configured or never tripped.
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
   * Read a compacted topic from the beginning to its high-watermark and return a
   * `Map<key, EventEnvelope<T>>` with the latest value per key.
   * Tombstone records remove the key from the map.
   *
   * @example
   * ```ts
   * const orders = await kafka.readSnapshot('orders.state');
   * // orders.get('order-123') → latest EventEnvelope for that key
   * ```
   */
  readSnapshot<K extends keyof T & string>(
    topic: K,
    options?: ReadSnapshotOptions,
  ): Promise<Map<string, EventEnvelope<T[K]>>>;

  /**
   * Snapshot the current committed offsets of a consumer group into a Kafka topic.
   * Requires `connectProducer()` to have been called.
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
   * Restore a consumer group's committed offsets from the nearest checkpoint.
   * The consumer group must be stopped before calling this method.
   *
   * @example
   * ```ts
   * await kafka.stopConsumer('billing-service');
   * const result = await kafka.restoreFromCheckpoint(undefined, 'checkpoints');
   * ```
   */
  restoreFromCheckpoint(
    groupId: string | undefined,
    checkpointTopic: string,
    options?: RestoreCheckpointOptions,
  ): Promise<CheckpointRestoreResult>;
}
