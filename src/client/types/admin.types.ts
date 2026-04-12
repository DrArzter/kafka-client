import type { MessageHeaders } from "./common";
import type { SchemaLike } from "../message/topic";

// ── Snapshot ──────────────────────────────────────────────────────────────────

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

// ── Checkpoint ────────────────────────────────────────────────────────────────

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

// ── DLQ replay ────────────────────────────────────────────────────────────────

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
  /**
   * Seek to the earliest available offset before consuming, regardless of any
   * previously committed offsets for the replay consumer group.
   * Default: `true` — full replay of all DLQ messages on every call.
   * Set to `false` to replay only messages added since the previous `replayDlq` call.
   */
  fromBeginning?: boolean;
}

// ── Admin / health ────────────────────────────────────────────────────────────

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
