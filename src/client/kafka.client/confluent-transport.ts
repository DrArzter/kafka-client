import { KafkaJS } from "@confluentinc/kafka-javascript";
const { Kafka: KafkaClass, logLevel: KafkaLogLevel, PartitionAssigners } = KafkaJS;

import type {
  KafkaTransport,
  IProducer,
  IConsumer,
  IAdmin,
  ITransaction,
  IProducerRecord,
  IProducerCreationOptions,
  IConsumerCreationOptions,
  ITopicPartition,
  ITopicPartitions,
  ITopicPartitionOffset,
  IConsumerRunConfig,
  IPartitionWatermarks,
  IPartitionOffset,
  IGroupTopicOffsets,
  IGroupDescription,
  ITopicMetadata,
} from "../transport";

// ── ConfluentTransaction ──────────────────────────────────────────────────────

class ConfluentTransaction implements ITransaction {
  constructor(private readonly tx: KafkaJS.Transaction) {}

  async send(record: IProducerRecord): Promise<void> {
    await this.tx.send(record as any);
  }

  async sendOffsets(options: {
    consumer: IConsumer;
    topics: Array<{
      topic: string;
      partitions: Array<{ partition: number; offset: string }>;
    }>;
  }): Promise<void> {
    // Unwrap the ConfluentConsumer to get the native KafkaJS.Consumer for sendOffsets
    const nativeConsumer = (options.consumer as ConfluentConsumer).getNative();
    await this.tx.sendOffsets({
      consumer: nativeConsumer,
      topics: options.topics,
    } as any);
  }

  async commit(): Promise<void> {
    await this.tx.commit();
  }

  async abort(): Promise<void> {
    await this.tx.abort();
  }
}

// ── ConfluentProducer ─────────────────────────────────────────────────────────

class ConfluentProducer implements IProducer {
  constructor(private readonly producer: KafkaJS.Producer) {}

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  async send(record: IProducerRecord): Promise<void> {
    await this.producer.send(record as any);
  }

  async transaction(): Promise<ITransaction> {
    const tx = await this.producer.transaction();
    return new ConfluentTransaction(tx);
  }
}

// ── ConfluentConsumer ─────────────────────────────────────────────────────────

export class ConfluentConsumer implements IConsumer {
  constructor(private readonly consumer: KafkaJS.Consumer) {}

  /** Returns the underlying KafkaJS.Consumer — used by ConfluentTransaction.sendOffsets. */
  getNative(): KafkaJS.Consumer {
    return this.consumer;
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
  }

  async subscribe(options: { topics: (string | RegExp)[] }): Promise<void> {
    await this.consumer.subscribe(options as any);
  }

  async run(config: IConsumerRunConfig): Promise<void> {
    await this.consumer.run(config as any);
  }

  pause(assignments: ITopicPartitions[]): void {
    this.consumer.pause(assignments as any);
  }

  resume(assignments: ITopicPartitions[]): void {
    this.consumer.resume(assignments as any);
  }

  seek(options: ITopicPartitionOffset): void {
    this.consumer.seek(options as any);
  }

  assignment(): ITopicPartition[] {
    return (this.consumer as any).assignment() as ITopicPartition[];
  }

  async commitOffsets(offsets: ITopicPartitionOffset[]): Promise<void> {
    await this.consumer.commitOffsets(offsets as any);
  }

  async stop(): Promise<void> {
    await (this.consumer as any).stop?.();
  }
}

// ── ConfluentAdmin ────────────────────────────────────────────────────────────

class ConfluentAdmin implements IAdmin {
  constructor(private readonly admin: KafkaJS.Admin) {}

  async connect(): Promise<void> {
    await this.admin.connect();
  }

  async disconnect(): Promise<void> {
    await this.admin.disconnect();
  }

  async createTopics(options: {
    topics: Array<{ topic: string; numPartitions: number }>;
  }): Promise<void> {
    await this.admin.createTopics(options as any);
  }

  async fetchTopicOffsets(topic: string): Promise<IPartitionWatermarks[]> {
    return this.admin.fetchTopicOffsets(topic) as any;
  }

  async fetchTopicOffsetsByTimestamp(
    topic: string,
    timestamp: number,
  ): Promise<IPartitionOffset[]> {
    return (this.admin as any).fetchTopicOffsetsByTime(topic, timestamp);
  }

  async fetchOffsets(options: {
    groupId: string;
  }): Promise<IGroupTopicOffsets[]> {
    return this.admin.fetchOffsets(options as any) as any;
  }

  async setOffsets(options: {
    groupId: string;
    topic: string;
    partitions: IPartitionOffset[];
  }): Promise<void> {
    await (this.admin as any).setOffsets(options);
  }

  async listTopics(): Promise<string[]> {
    return this.admin.listTopics();
  }

  async listGroups(): Promise<{ groups: IGroupDescription[] }> {
    return this.admin.listGroups() as any;
  }

  async fetchTopicMetadata(options?: {
    topics?: string[];
  }): Promise<{ topics: ITopicMetadata[] }> {
    return (this.admin as any).fetchTopicMetadata(options);
  }

  async deleteGroups(groupIds: string[]): Promise<void> {
    await (this.admin as any).deleteGroups(groupIds);
  }

  async deleteTopicRecords(options: {
    topic: string;
    partitions: IPartitionOffset[];
  }): Promise<void> {
    await this.admin.deleteTopicRecords(options as any);
  }
}

// ── ConfluentTransport ────────────────────────────────────────────────────────

/**
 * `KafkaTransport` implementation backed by `@confluentinc/kafka-javascript`.
 * Wraps the KafkaJS-compatibility layer from librdkafka.
 */
export class ConfluentTransport implements KafkaTransport {
  private readonly kafka: KafkaJS.Kafka;

  constructor(clientId: string, brokers: string[]) {
    this.kafka = new KafkaClass({
      kafkaJS: { clientId, brokers, logLevel: KafkaLogLevel.ERROR },
    });
  }

  producer(options?: IProducerCreationOptions): IProducer {
    const native = this.kafka.producer({
      kafkaJS: {
        acks: -1,
        ...(options?.idempotent !== undefined && {
          idempotent: options.idempotent,
        }),
        ...(options?.transactionalId !== undefined && {
          transactionalId: options.transactionalId,
          maxInFlightRequests: 1,
        }),
      },
    });
    return new ConfluentProducer(native);
  }

  consumer(options: IConsumerCreationOptions): IConsumer {
    const assigner =
      options.partitionAssigner === "roundrobin"
        ? PartitionAssigners.roundRobin
        : options.partitionAssigner === "range"
          ? PartitionAssigners.range
          : PartitionAssigners.cooperativeSticky;

    const config: any = {
      kafkaJS: {
        groupId: options.groupId,
        fromBeginning: options.fromBeginning,
        autoCommit: options.autoCommit,
        partitionAssigners: [assigner],
      },
    };

    if (options.onRebalance) {
      const cb = options.onRebalance;
      // err.code -175 = ERR__ASSIGN_PARTITIONS, -174 = ERR__REVOKE_PARTITIONS
      config.rebalance_cb = (err: any, assignment: any[]) => {
        const type = err.code === -175 ? "assign" : "revoke";
        cb(
          type,
          assignment.map((p) => ({ topic: p.topic, partition: p.partition })),
        );
      };
    }

    return new ConfluentConsumer(this.kafka.consumer(config));
  }

  admin(): IAdmin {
    return new ConfluentAdmin(this.kafka.admin());
  }
}
