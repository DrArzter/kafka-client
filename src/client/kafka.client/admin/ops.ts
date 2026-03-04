import { KafkaJS } from "@confluentinc/kafka-javascript";
type Admin = KafkaJS.Admin;
import type {
  ClientId,
  ConsumerGroupSummary,
  KafkaHealthResult,
  KafkaLogger,
  TopicDescription,
} from "../../types";

export type AdminOpsDeps = {
  admin: Admin;
  logger: KafkaLogger;
  runningConsumers: Map<string, "eachMessage" | "eachBatch">;
  defaultGroupId: string;
  clientId: ClientId;
};

export class AdminOps {
  private isConnected = false;

  constructor(private readonly deps: AdminOpsDeps) {}

  /** Underlying admin client — used by index.ts for topic validation. */
  get admin(): Admin {
    return this.deps.admin;
  }

  /** Whether the admin client is currently connected. */
  get connected(): boolean {
    return this.isConnected;
  }

  /**
   * Connect the admin client if not already connected.
   * The flag is only set to `true` after a successful connect — if `admin.connect()`
   * throws the flag remains `false` so the next call will retry the connection.
   */
  async ensureConnected(): Promise<void> {
    if (this.isConnected) return;
    try {
      await this.deps.admin.connect();
      this.isConnected = true;
    } catch (err) {
      this.isConnected = false;
      throw err;
    }
  }

  /** Disconnect admin if connected. Resets the connected flag. */
  async disconnect(): Promise<void> {
    if (!this.isConnected) return;
    await this.deps.admin.disconnect();
    this.isConnected = false;
  }

  public async resetOffsets(
    groupId: string | undefined,
    topic: string,
    position: "earliest" | "latest",
  ): Promise<void> {
    const gid = groupId ?? this.deps.defaultGroupId;
    if (this.deps.runningConsumers.has(gid)) {
      throw new Error(
        `resetOffsets: consumer group "${gid}" is still running. ` +
          `Call stopConsumer("${gid}") before resetting offsets.`,
      );
    }
    await this.ensureConnected();
    const partitionOffsets = await this.deps.admin.fetchTopicOffsets(topic);
    const partitions = partitionOffsets.map(({ partition, low, high }) => ({
      partition,
      offset: position === "earliest" ? low : high,
    }));
    await (this.deps.admin as any).setOffsets({ groupId: gid, topic, partitions });
    this.deps.logger.log(
      `Offsets reset to ${position} for group "${gid}" on topic "${topic}"`,
    );
  }

  /**
   * Seek specific topic-partition pairs to explicit offsets for a stopped consumer group.
   * Throws if the group is still running — call `stopConsumer(groupId)` first.
   * Assignments are grouped by topic and committed via `admin.setOffsets`.
   */
  public async seekToOffset(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; offset: string }>,
  ): Promise<void> {
    const gid = groupId ?? this.deps.defaultGroupId;
    if (this.deps.runningConsumers.has(gid)) {
      throw new Error(
        `seekToOffset: consumer group "${gid}" is still running. ` +
          `Call stopConsumer("${gid}") before seeking offsets.`,
      );
    }
    await this.ensureConnected();
    const byTopic = new Map<string, Array<{ partition: number; offset: string }>>();
    for (const { topic, partition, offset } of assignments) {
      const list = byTopic.get(topic) ?? [];
      list.push({ partition, offset });
      byTopic.set(topic, list);
    }
    for (const [topic, partitions] of byTopic) {
      await (this.deps.admin as any).setOffsets({ groupId: gid, topic, partitions });
      this.deps.logger.log(
        `Offsets set for group "${gid}" on "${topic}": ${JSON.stringify(partitions)}`,
      );
    }
  }

  /**
   * Seek specific topic-partition pairs to the offset nearest to a given timestamp
   * (in milliseconds) for a stopped consumer group.
   * Throws if the group is still running — call `stopConsumer(groupId)` first.
   * Assignments are grouped by topic and committed via `admin.setOffsets`.
   * If no offset exists at the requested timestamp (e.g. empty partition or
   * future timestamp), the partition falls back to `-1` (end of topic — new messages only).
   */
  public async seekToTimestamp(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; timestamp: number }>,
  ): Promise<void> {
    const gid = groupId ?? this.deps.defaultGroupId;
    if (this.deps.runningConsumers.has(gid)) {
      throw new Error(
        `seekToTimestamp: consumer group "${gid}" is still running. ` +
          `Call stopConsumer("${gid}") before seeking offsets.`,
      );
    }
    await this.ensureConnected();
    const byTopic = new Map<string, Array<{ partition: number; timestamp: number }>>();
    for (const { topic, partition, timestamp } of assignments) {
      const list = byTopic.get(topic) ?? [];
      list.push({ partition, timestamp });
      byTopic.set(topic, list);
    }
    for (const [topic, parts] of byTopic) {
      const offsets: Array<{ partition: number; offset: string }> =
        await Promise.all(
          parts.map(async ({ partition, timestamp }) => {
            const results = await (this.deps.admin as any).fetchTopicOffsetsByTime(
              topic,
              timestamp,
            );
            const found = (results as Array<{ partition: number; offset: string }>).find(
              (r) => r.partition === partition,
            );
            return { partition, offset: found?.offset ?? "-1" };
          }),
        );
      await (this.deps.admin as any).setOffsets({ groupId: gid, topic, partitions: offsets });
      this.deps.logger.log(
        `Offsets set by timestamp for group "${gid}" on "${topic}": ${JSON.stringify(offsets)}`,
      );
    }
  }

  /**
   * Query consumer group lag per partition.
   * Lag = broker high-watermark − last committed offset.
   * A committed offset of -1 (nothing committed yet) counts as full lag.
   *
   * Returns an empty array when the consumer group has never committed any
   * offsets (freshly created group, `autoCommit: false` with no manual commits,
   * or group not yet assigned). This is a Kafka protocol limitation:
   * `fetchOffsets` only returns data for topic-partitions that have at least one
   * committed offset. Use `checkStatus()` to verify broker connectivity in that case.
   */
  public async getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>> {
    const gid = groupId ?? this.deps.defaultGroupId;
    await this.ensureConnected();

    const committedByTopic = await this.deps.admin.fetchOffsets({ groupId: gid });

    const brokerOffsetsAll = await Promise.all(
      committedByTopic.map(({ topic }) => this.deps.admin.fetchTopicOffsets(topic)),
    );

    const result: Array<{ topic: string; partition: number; lag: number }> = [];

    for (let i = 0; i < committedByTopic.length; i++) {
      const { topic, partitions } = committedByTopic[i];
      const brokerOffsets = brokerOffsetsAll[i];

      for (const { partition, offset } of partitions) {
        const broker = brokerOffsets.find((o) => o.partition === partition);
        if (!broker) continue;

        const committed = parseInt(offset, 10);
        const high = parseInt(broker.high, 10);
        // committed === -1 means the group has never committed for this partition
        const lag = committed === -1 ? high : Math.max(0, high - committed);
        result.push({ topic, partition, lag });
      }
    }

    return result;
  }

  /** Check broker connectivity. Never throws — returns a discriminated union. */
  public async checkStatus(): Promise<KafkaHealthResult> {
    try {
      await this.ensureConnected();
      const topics = await this.deps.admin.listTopics();
      return { status: "up", clientId: this.deps.clientId, topics };
    } catch (error) {
      return {
        status: "down",
        clientId: this.deps.clientId,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * List all consumer groups known to the broker.
   * Useful for monitoring which groups are active and their current state.
   */
  public async listConsumerGroups(): Promise<ConsumerGroupSummary[]> {
    await this.ensureConnected();
    const result = await this.deps.admin.listGroups();
    return result.groups.map((g: any) => ({
      groupId: g.groupId,
      state: g.state ?? "Unknown",
    }));
  }

  /**
   * Describe topics — returns partition layout, leader, replicas, and ISR.
   * @param topics Topic names to describe. Omit to describe all topics.
   */
  public async describeTopics(topics?: string[]): Promise<TopicDescription[]> {
    await this.ensureConnected();
    const result = await (this.deps.admin as any).fetchTopicMetadata(
      topics ? { topics } : undefined,
    );
    return (result.topics as any[]).map((t: any) => ({
      name: t.name,
      partitions: (t.partitions as any[]).map((p: any) => ({
        partition: p.partitionId ?? p.partition,
        leader: p.leader,
        replicas: (p.replicas as any[]).map((r: any) =>
          typeof r === "number" ? r : r.nodeId,
        ),
        isr: (p.isr as any[]).map((r: any) =>
          typeof r === "number" ? r : r.nodeId,
        ),
      })),
    }));
  }

  /**
   * Delete records from a topic up to (but not including) the given offsets.
   * All messages with offsets **before** the given offset are deleted.
   */
  public async deleteRecords(
    topic: string,
    partitions: Array<{ partition: number; offset: string }>,
  ): Promise<void> {
    await this.ensureConnected();
    await this.deps.admin.deleteTopicRecords({ topic, partitions });
  }

  /**
   * When `retryTopics: true` and `autoCreateTopics: false`, verify that every
   * `<topic>.retry.<level>` topic already exists. Throws a clear error at startup
   * rather than silently discovering missing topics on the first handler failure.
   */
  public async validateRetryTopicsExist(
    topicNames: string[],
    maxRetries: number,
  ): Promise<void> {
    await this.ensureConnected();
    const existing = new Set(await this.deps.admin.listTopics());
    const missing: string[] = [];
    for (const t of topicNames) {
      for (let level = 1; level <= maxRetries; level++) {
        const retryTopic = `${t}.retry.${level}`;
        if (!existing.has(retryTopic)) missing.push(retryTopic);
      }
    }
    if (missing.length > 0) {
      throw new Error(
        `retryTopics: true but the following retry topics do not exist: ${missing.join(", ")}. ` +
          `Create them manually or set autoCreateTopics: true.`,
      );
    }
  }

  /**
   * When `autoCreateTopics` is disabled, verify that `<topic>.dlq` exists for every
   * consumed topic. Throws a clear error at startup rather than silently discovering
   * missing DLQ topics on the first handler failure.
   */
  public async validateDlqTopicsExist(topicNames: string[]): Promise<void> {
    await this.ensureConnected();
    const existing = new Set(await this.deps.admin.listTopics());
    const missing = topicNames
      .filter((t) => !existing.has(`${t}.dlq`))
      .map((t) => `${t}.dlq`);
    if (missing.length > 0) {
      throw new Error(
        `dlq: true but the following DLQ topics do not exist: ${missing.join(", ")}. ` +
          `Create them manually or set autoCreateTopics: true.`,
      );
    }
  }

  /**
   * When `deduplication.strategy: 'topic'` and `autoCreateTopics: false`, verify
   * that every `<topic>.duplicates` destination topic already exists. Throws a
   * clear error at startup rather than silently dropping duplicates on first hit.
   */
  public async validateDuplicatesTopicsExist(
    topicNames: string[],
    customDestination: string | undefined,
  ): Promise<void> {
    await this.ensureConnected();
    const existing = new Set(await this.deps.admin.listTopics());
    const toCheck = customDestination
      ? [customDestination]
      : topicNames.map((t) => `${t}.duplicates`);
    const missing = toCheck.filter((t) => !existing.has(t));
    if (missing.length > 0) {
      throw new Error(
        `deduplication.strategy: 'topic' but the following duplicate-routing topics do not exist: ${missing.join(", ")}. ` +
          `Create them manually or set autoCreateTopics: true.`,
      );
    }
  }
}
