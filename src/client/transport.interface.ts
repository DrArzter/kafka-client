// ── Transport abstraction ─────────────────────────────────────────────────────
//
// These interfaces decouple KafkaClient from @confluentinc/kafka-javascript.
// The only concrete implementation is ConfluentTransport (confluent-transport.ts).
// Tests can inject a FakeTransport; future adapters can target other brokers.
//

// ── Shared primitives ─────────────────────────────────────────────────────────

/** A topic-partition pair. */
export type ITopicPartition = { topic: string; partition: number };

/** A topic-partition pair with an absolute offset string. */
export type ITopicPartitionOffset = {
  topic: string;
  partition: number;
  offset: string;
};

/** Pause / resume assignment shape: one topic + its partition list. */
export type ITopicPartitions = { topic: string; partitions: number[] };

// ── Producer ──────────────────────────────────────────────────────────────────

/** A single message in a produce request. */
export type IProducerMessage = {
  value: string | null;
  key?: string | null;
  headers?: Record<string, string | Buffer | string[]>;
};

/** Produce request payload for one topic. */
export type IProducerRecord = {
  topic: string;
  messages: IProducerMessage[];
};

/** Options for creating a producer. */
export type IProducerCreationOptions = {
  /** When set, the producer uses idempotent + exactly-once semantics. */
  transactionalId?: string;
  /** Enable idempotent writes (required for `transactionalId`). */
  idempotent?: boolean;
};

/** An open Kafka transaction. */
export interface ITransaction {
  send(record: IProducerRecord): Promise<void>;
  /**
   * Atomically commit offsets for `consumer` as part of this transaction.
   * The `consumer` parameter must be the `IConsumer` whose offsets are being committed.
   */
  sendOffsets(options: {
    consumer: IConsumer;
    topics: Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: string }>;
    }>;
  }): Promise<void>;
  commit(): Promise<void>;
  abort(): Promise<void>;
}

/** A Kafka producer. */
export interface IProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: IProducerRecord): Promise<void>;
  transaction(): Promise<ITransaction>;
}

// ── Consumer ──────────────────────────────────────────────────────────────────

/** A single message in an `eachMessage` callback. */
export type IMessage = {
  value: Buffer | null;
  /** Header map as returned by librdkafka — values may be arrays. */
  headers: Record<string, any>;
  offset: string;
  key: Buffer | null;
};

/** Payload passed to the `eachMessage` handler. */
export type IEachMessagePayload = {
  topic: string;
  partition: number;
  message: IMessage;
};

/** A batch of messages from one topic-partition. */
export type IMessageBatch = {
  topic: string;
  partition: number;
  messages: IMessage[];
  highWatermark: string;
};

/** Payload passed to the `eachBatch` handler. */
export type IEachBatchPayload = {
  batch: IMessageBatch;
  /** Send a heartbeat to the broker to prevent session timeout. */
  heartbeat: () => Promise<void>;
  /** Mark `offset` as processed (without committing). */
  resolveOffset: (offset: string) => void;
  /** Commit if the auto-commit threshold has been reached. */
  commitOffsetsIfNecessary: () => Promise<void>;
};

/** Configuration passed to `IConsumer.run()`. */
export type IConsumerRunConfig = {
  eachMessage?: (payload: IEachMessagePayload) => Promise<void>;
  eachBatch?: (payload: IEachBatchPayload) => Promise<void>;
};

/** Options for creating a consumer. */
export type IConsumerCreationOptions = {
  groupId: string;
  fromBeginning?: boolean;
  autoCommit?: boolean;
  partitionAssigner?: "cooperative-sticky" | "roundrobin" | "range";
  /** Fired on every partition assign/revoke. */
  onRebalance?: (
    type: "assign" | "revoke",
    assignments: ITopicPartition[],
  ) => void;
};

/** A Kafka consumer. */
export interface IConsumer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(options: { topics: (string | RegExp)[] }): Promise<void>;
  run(config: IConsumerRunConfig): Promise<void>;
  pause(assignments: ITopicPartitions[]): void;
  resume(assignments: ITopicPartitions[]): void;
  /** Seek a partition to an explicit offset. */
  seek(options: ITopicPartitionOffset): void;
  /** Current partition assignment for this consumer. */
  assignment(): ITopicPartition[];
  commitOffsets(offsets: ITopicPartitionOffset[]): Promise<void>;
  /** Stop processing (alias for disconnect in some usages). */
  stop(): Promise<void>;
}

// ── Admin ─────────────────────────────────────────────────────────────────────

/** Low/current/high watermark offsets for one partition. */
export type IPartitionWatermarks = {
  partition: number;
  low: string;
  high: string;
};

/** A partition → offset pair. */
export type IPartitionOffset = { partition: number; offset: string };

/** Committed offsets for a group's topic. */
export type IGroupTopicOffsets = {
  topic: string;
  partitions: IPartitionOffset[];
};

/** A consumer group descriptor. */
export type IGroupDescription = { groupId: string; state?: string };

/** Partition metadata. */
export type IPartitionMetadata = {
  partitionId?: number;
  partition?: number;
  leader?: number;
  replicas?: (number | { nodeId: number })[];
  isr?: (number | { nodeId: number })[];
};

/** Topic metadata. */
export type ITopicMetadata = {
  name: string;
  partitions: IPartitionMetadata[];
};

/** A Kafka admin client. */
export interface IAdmin {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  createTopics(options: {
    topics: Array<{ topic: string; numPartitions: number }>;
  }): Promise<void>;
  fetchTopicOffsets(topic: string): Promise<IPartitionWatermarks[]>;
  fetchTopicOffsetsByTimestamp(
    topic: string,
    timestamp: number,
  ): Promise<IPartitionOffset[]>;
  fetchOffsets(options: { groupId: string }): Promise<IGroupTopicOffsets[]>;
  setOffsets(options: {
    groupId: string;
    topic: string;
    partitions: IPartitionOffset[];
  }): Promise<void>;
  listTopics(): Promise<string[]>;
  listGroups(): Promise<{ groups: IGroupDescription[] }>;
  fetchTopicMetadata(options?: {
    topics?: string[];
  }): Promise<{ topics: ITopicMetadata[] }>;
  deleteGroups(groupIds: string[]): Promise<void>;
  deleteTopicRecords(options: {
    topic: string;
    partitions: IPartitionOffset[];
  }): Promise<void>;
}

// ── KafkaTransport ────────────────────────────────────────────────────────────

/**
 * Factory that creates connected Kafka primitives.
 * The default implementation wraps `@confluentinc/kafka-javascript` via
 * `ConfluentTransport`. Inject a custom transport (e.g. a fake) via
 * `KafkaClientOptions.transport` for testing or alternative broker support.
 */
export interface KafkaTransport {
  producer(options?: IProducerCreationOptions): IProducer;
  consumer(options: IConsumerCreationOptions): IConsumer;
  admin(): IAdmin;
}
