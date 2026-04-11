import type {
  KafkaTransport,
  IProducer,
  IConsumer,
  IAdmin,
  ITransaction,
  IProducerRecord,
  IProducerCreationOptions,
  IConsumerCreationOptions,
  IConsumerRunConfig,
  ITopicPartition,
  ITopicPartitions,
  ITopicPartitionOffset,
  IPartitionWatermarks,
  IPartitionOffset,
  IGroupTopicOffsets,
  IGroupDescription,
  ITopicMetadata,
  IMessage,
} from "../client/transport";

// ── FakeTransaction ───────────────────────────────────────────────────────────

/**
 * An in-memory Kafka transaction.
 * Staged sends are visible in `staged`; committed sends are flushed
 * to the owning `FakeProducer.sent` on `commit()`.
 */
export class FakeTransaction implements ITransaction {
  /** Records staged within this transaction (not yet committed). */
  readonly staged: IProducerRecord[] = [];
  /** True after `commit()` was called. */
  committed = false;
  /** True after `abort()` was called. */
  aborted = false;
  /** sendOffsets calls (for EOS assertions). */
  readonly offsetsCommitted: Array<{
    consumer: IConsumer;
    topics: Array<{ topic: string; partitions: IPartitionOffset[] }>;
  }> = [];

  constructor(private readonly producer: FakeProducer) {}

  async send(record: IProducerRecord): Promise<void> {
    this.staged.push(record);
  }

  async sendOffsets(options: {
    consumer: IConsumer;
    topics: Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }>;
  }): Promise<void> {
    this.offsetsCommitted.push(options);
  }

  async commit(): Promise<void> {
    if (this.aborted) throw new Error("FakeTransaction: already aborted");
    this.committed = true;
    for (const record of this.staged) {
      this.producer.sent.push(record);
    }
  }

  async abort(): Promise<void> {
    if (this.committed) throw new Error("FakeTransaction: already committed");
    this.aborted = true;
    this.staged.length = 0;
  }
}

// ── FakeProducer ──────────────────────────────────────────────────────────────

/**
 * In-memory producer. All `send()` calls are captured in `sent`.
 * Transactions are backed by `FakeTransaction`.
 */
export class FakeProducer implements IProducer {
  /** All records delivered via `send()` (direct + committed transactions). */
  readonly sent: IProducerRecord[] = [];
  /** All transactions opened via `transaction()`. */
  readonly transactions: FakeTransaction[] = [];

  readonly options: IProducerCreationOptions | undefined;
  connected = false;

  constructor(options?: IProducerCreationOptions) {
    this.options = options;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }

  async send(record: IProducerRecord): Promise<void> {
    this.sent.push(record);
  }

  async transaction(): Promise<ITransaction> {
    const tx = new FakeTransaction(this);
    this.transactions.push(tx);
    return tx;
  }

  /** Return the last committed transaction, or throw if none exist. */
  get lastTransaction(): FakeTransaction {
    const tx = this.transactions.at(-1);
    if (!tx) throw new Error("FakeProducer: no transactions opened yet");
    return tx;
  }

  /** All topic names that received at least one message. */
  sentTopics(): string[] {
    return [...new Set(this.sent.map((r) => r.topic))];
  }

  /** All messages sent to a specific topic. */
  sentTo(topic: string): IProducerRecord["messages"] {
    return this.sent.filter((r) => r.topic === topic).flatMap((r) => r.messages);
  }
}

// ── FakeConsumer ──────────────────────────────────────────────────────────────

/**
 * In-memory consumer.
 * Call `deliver(topic, message)` from your test to push messages through
 * the `eachMessage` handler without a real broker.
 */
export class FakeConsumer implements IConsumer {
  readonly groupId: string;
  readonly fromBeginning: boolean;

  /** Topics subscribed via `subscribe()`. */
  readonly subscribed: string[] = [];

  private _runConfig: IConsumerRunConfig | undefined;
  private _assignments: ITopicPartition[] = [];
  readonly pausedTopics = new Set<string>();
  connected = false;

  private readonly onRebalance: IConsumerCreationOptions["onRebalance"];

  constructor(options: IConsumerCreationOptions) {
    this.groupId = options.groupId;
    this.fromBeginning = options.fromBeginning ?? false;
    this.onRebalance = options.onRebalance;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }

  async subscribe(options: { topics: (string | RegExp)[] }): Promise<void> {
    for (const t of options.topics) {
      if (typeof t === "string") this.subscribed.push(t);
    }
    // Auto-assign one partition per subscribed topic and immediately fire the
    // rebalance callback so that handle.ready() resolves without a real broker.
    this._assignments = this.subscribed.map((topic) => ({ topic, partition: 0 }));
    this.onRebalance?.("assign", this._assignments);
  }

  async run(config: IConsumerRunConfig): Promise<void> {
    this._runConfig = config;
  }

  pause(assignments: ITopicPartitions[]): void {
    for (const { topic } of assignments) this.pausedTopics.add(topic);
  }

  resume(assignments: ITopicPartitions[]): void {
    for (const { topic } of assignments) this.pausedTopics.delete(topic);
  }

  seek(_options: ITopicPartitionOffset): void {}

  assignment(): ITopicPartition[] {
    return this._assignments;
  }

  async commitOffsets(_offsets: ITopicPartitionOffset[]): Promise<void> {}

  async stop(): Promise<void> {
    this.connected = false;
  }

  // ── Test helpers ─────────────────────────────────────────────────────

  /**
   * Push a message through the `eachMessage` handler.
   * Throws if `run()` has not been called yet.
   */
  async deliver(
    topic: string,
    message: Partial<IMessage> & { value: Buffer | null },
    partition = 0,
    offset = "0",
  ): Promise<void> {
    if (!this._runConfig?.eachMessage) {
      throw new Error(
        `FakeConsumer(${this.groupId}): run() with eachMessage not called yet`,
      );
    }
    await this._runConfig.eachMessage({
      topic,
      partition,
      message: {
        value: message.value,
        headers: message.headers ?? {},
        offset,
        key: message.key,
      },
    });
  }

  /**
   * Simulate a partition-assign rebalance event.
   * Useful for testing onRebalance callbacks.
   */
  triggerRebalance(
    type: "assign" | "revoke",
    assignments: ITopicPartition[],
  ): void {
    this.onRebalance?.(type, assignments);
  }

  /** Whether `run()` has been called (consumer is active). */
  get isRunning(): boolean {
    return this._runConfig !== undefined;
  }
}

// ── FakeAdmin ─────────────────────────────────────────────────────────────────

/**
 * In-memory admin client.
 * Pre-populate `topicOffsets`, `groupOffsets`, and `existingTopics`
 * to control what admin queries return.
 */
export class FakeAdmin implements IAdmin {
  /** Topics returned by `listTopics()`. Add to this from your test. */
  readonly existingTopics: string[] = [];

  /** Per-topic partition watermarks returned by `fetchTopicOffsets()`. */
  readonly topicOffsets = new Map<string, IPartitionWatermarks[]>();

  /** Per-groupId committed offsets returned by `fetchOffsets()`. */
  readonly groupOffsets = new Map<string, IGroupTopicOffsets[]>();

  /** Calls captured by `setOffsets()` — inspect in tests. */
  readonly setOffsetsCalls: Array<{
    groupId: string;
    topic: string;
    partitions: IPartitionOffset[];
  }> = [];

  /** Group IDs deleted via `deleteGroups()`. */
  readonly deletedGroups: string[] = [];

  /** Records deleted via `deleteTopicRecords()`. */
  readonly deletedRecords: Array<{
    topic: string;
    partitions: IPartitionOffset[];
  }> = [];

  connected = false;

  async connect(): Promise<void> {
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }

  async createTopics(options: {
    topics: Array<{ topic: string; numPartitions: number }>;
  }): Promise<void> {
    for (const { topic } of options.topics) {
      if (!this.existingTopics.includes(topic)) this.existingTopics.push(topic);
    }
  }

  async fetchTopicOffsets(topic: string): Promise<IPartitionWatermarks[]> {
    return this.topicOffsets.get(topic) ?? [{ partition: 0, low: "0", high: "0" }];
  }

  async fetchTopicOffsetsByTimestamp(
    _topic: string,
    _timestamp: number,
  ): Promise<IPartitionOffset[]> {
    return [];
  }

  async fetchOffsets(options: {
    groupId: string;
  }): Promise<IGroupTopicOffsets[]> {
    return this.groupOffsets.get(options.groupId) ?? [];
  }

  async setOffsets(options: {
    groupId: string;
    topic: string;
    partitions: IPartitionOffset[];
  }): Promise<void> {
    this.setOffsetsCalls.push(options);
  }

  async listTopics(): Promise<string[]> {
    return this.existingTopics;
  }

  async listGroups(): Promise<{ groups: IGroupDescription[] }> {
    return { groups: [] };
  }

  async fetchTopicMetadata(_options?: {
    topics?: string[];
  }): Promise<{ topics: ITopicMetadata[] }> {
    return { topics: [] };
  }

  async deleteGroups(groupIds: string[]): Promise<void> {
    this.deletedGroups.push(...groupIds);
  }

  async deleteTopicRecords(options: {
    topic: string;
    partitions: IPartitionOffset[];
  }): Promise<void> {
    this.deletedRecords.push(options);
  }
}

// ── FakeTransport ─────────────────────────────────────────────────────────────

/**
 * In-memory `KafkaTransport` for unit testing.
 *
 * Inject into `KafkaClient` via `KafkaClientOptions.transport` to test
 * producer/consumer logic without `jest.mock('@confluentinc/kafka-javascript')`.
 *
 * @example
 * ```ts
 * const transport = new FakeTransport();
 * const client = new KafkaClient('svc', 'grp', [], { transport });
 *
 * await client.connectProducer();
 * await client.sendMessage('orders', { id: '1' });
 *
 * expect(transport.mainProducer.sentTo('orders')).toHaveLength(1);
 * ```
 */
export class FakeTransport implements KafkaTransport {
  private readonly _producers: FakeProducer[] = [];
  private readonly _consumers: FakeConsumer[] = [];
  private readonly _admin = new FakeAdmin();

  producer(options?: IProducerCreationOptions): IProducer {
    const p = new FakeProducer(options);
    this._producers.push(p);
    return p;
  }

  consumer(options: IConsumerCreationOptions): IConsumer {
    const c = new FakeConsumer(options);
    this._consumers.push(c);
    return c;
  }

  admin(): IAdmin {
    return this._admin;
  }

  // ── Convenience accessors ─────────────────────────────────────────

  /** The admin client shared across all admin() calls. */
  get fakeAdmin(): FakeAdmin {
    return this._admin;
  }

  /**
   * The first (default) producer — the non-transactional producer
   * created during `KafkaClient` construction.
   */
  get mainProducer(): FakeProducer {
    const p = this._producers[0];
    if (!p) throw new Error("FakeTransport: no producers created yet");
    return p;
  }

  /** All producers created so far (main + transactional). */
  get producers(): readonly FakeProducer[] {
    return this._producers;
  }

  /** All consumers created so far. */
  get consumers(): readonly FakeConsumer[] {
    return this._consumers;
  }

  /**
   * Find the consumer for a given group ID.
   * Throws if no consumer with that group exists.
   */
  consumerFor(groupId: string): FakeConsumer {
    const c = this._consumers.find(
      (c) => c.groupId === groupId || c.groupId.startsWith(groupId),
    );
    if (!c)
      throw new Error(
        `FakeTransport: no consumer for group "${groupId}". ` +
          `Available: ${this._consumers.map((c) => c.groupId).join(", ") || "(none)"}`,
      );
    return c;
  }

  /**
   * Deliver a JSON-serialized message to the first consumer subscribed to `topic`.
   * Simulates a broker dispatching a message to the consumer handler.
   */
  async deliver<T>(
    topic: string,
    payload: T,
    options: {
      key?: string;
      headers?: Record<string, string>;
      partition?: number;
      offset?: string;
    } = {},
  ): Promise<void> {
    const consumer = this._consumers.find((c) => c.subscribed.includes(topic));
    if (!consumer) {
      throw new Error(
        `FakeTransport: no consumer subscribed to "${topic}". ` +
          `Subscribed topics: ${this._consumers.flatMap((c) => c.subscribed).join(", ") || "(none)"}`,
      );
    }
    await consumer.deliver(
      topic,
      {
        value: Buffer.from(JSON.stringify(payload)),
        headers: options.headers
          ? Object.fromEntries(
              Object.entries(options.headers).map(([k, v]) => [k, [v]]),
            )
          : {},
        key: options.key !== undefined ? Buffer.from(options.key) : undefined,
      },
      options.partition ?? 0,
      options.offset ?? "0",
    );
  }
}
