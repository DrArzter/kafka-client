import { KafkaJS } from "@confluentinc/kafka-javascript";
type Kafka = KafkaJS.Kafka;
type Producer = KafkaJS.Producer;
type Consumer = KafkaJS.Consumer;
type Admin = KafkaJS.Admin;
const { Kafka: KafkaClass, logLevel: KafkaLogLevel } = KafkaJS;
import { TopicDescriptor, SchemaLike } from "../message/topic";
import type { EventEnvelope } from "../message/envelope";
import type {
  ClientId,
  GroupId,
  SendOptions,
  BatchMessageItem,
  ConsumerOptions,
  ConsumerHandle,
  TransactionContext,
  TopicMapConstraint,
  IKafkaClient,
  KafkaClientOptions,
  KafkaInstrumentation,
  KafkaLogger,
  KafkaMetrics,
  DlqReason,
  BatchMeta,
} from "../types";

/**
 * Process-level registry of active transactional producer IDs.
 * Used to detect same-process `transactionalId` conflicts before Kafka silently
 * fences one of the producers. Cross-process conflicts cannot be detected here —
 * they surface as fencing errors from the broker.
 */
const _activeTransactionalIds = new Set<string>();

// Re-export all types so existing `import { ... } from './kafka.client'` keeps working
export * from "../types";

import {
  buildSendPayload,
  registerSchema,
  resolveTopicName,
  type BuildSendPayloadDeps,
} from "./producer-ops";
import {
  getOrCreateConsumer,
  buildSchemaMap,
  type ConsumerOpsDeps,
} from "./consumer-ops";
import { handleEachMessage, handleEachBatch } from "./message-handler";
import type {
  MessageHandlerDeps,
  DeduplicationContext,
} from "./message-handler";
import { startRetryTopicConsumers } from "./retry-topic";
import { subscribeWithRetry } from "../consumer/subscribe-retry";
import { toError } from "../consumer/pipeline";

/**
 * Type-safe Kafka client.
 * Wraps @confluentinc/kafka-javascript (librdkafka) with JSON serialization,
 * retries, DLQ, transactions, and interceptors.
 *
 * @typeParam T - Topic-to-message type mapping for compile-time safety.
 */
export class KafkaClient<
  T extends TopicMapConstraint<T>,
> implements IKafkaClient<T> {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private txProducer: Producer | undefined;
  private txProducerInitPromise: Promise<Producer> | undefined;
  /** Maps transactionalId → Producer for each active retry level consumer. */
  private readonly retryTxProducers = new Map<string, Producer>();
  private readonly consumers = new Map<string, Consumer>();
  private readonly admin: Admin;
  private readonly logger: KafkaLogger;
  private readonly autoCreateTopicsEnabled: boolean;
  private readonly strictSchemasEnabled: boolean;
  private readonly numPartitions: number;
  private readonly ensuredTopics = new Set<string>();
  /** Pending topic-creation promises keyed by topic name. Prevents duplicate createTopics calls. */
  private readonly ensureTopicPromises = new Map<string, Promise<void>>();
  private readonly defaultGroupId: string;
  private readonly schemaRegistry = new Map<string, SchemaLike>();
  private readonly runningConsumers = new Map<
    string,
    "eachMessage" | "eachBatch"
  >();
  private readonly consumerCreationOptions = new Map<
    string,
    { fromBeginning: boolean; autoCommit: boolean }
  >();
  /** Maps each main consumer groupId to its companion retry level groupIds. */
  private readonly companionGroupIds = new Map<string, string[]>();
  private readonly instrumentation: KafkaInstrumentation[];
  private readonly onMessageLost: KafkaClientOptions["onMessageLost"];
  private readonly onRebalance: KafkaClientOptions["onRebalance"];
  /** Transactional producer ID — configurable via `KafkaClientOptions.transactionalId`. */
  private readonly txId: string;
  /** Internal event counters exposed via `getMetrics()`. */
  private readonly _metrics: KafkaMetrics = {
    processedCount: 0,
    retryCount: 0,
    dlqCount: 0,
    dedupCount: 0,
  };

  /** Monotonically increasing Lamport clock stamped on every outgoing message. */
  private _lamportClock = 0;
  /** Per-groupId deduplication state: `"topic:partition"` → last processed clock. */
  private readonly dedupStates = new Map<string, Map<string, number>>();

  private isAdminConnected = false;
  private inFlightTotal = 0;
  private readonly drainResolvers: Array<() => void> = [];
  public readonly clientId: ClientId;

  constructor(
    clientId: ClientId,
    groupId: GroupId,
    brokers: string[],
    options?: KafkaClientOptions,
  ) {
    this.clientId = clientId;
    this.defaultGroupId = groupId;
    this.logger = options?.logger ?? {
      log: (msg) => console.log(`[KafkaClient:${clientId}] ${msg}`),
      warn: (msg, ...args) =>
        console.warn(`[KafkaClient:${clientId}] ${msg}`, ...args),
      error: (msg, ...args) =>
        console.error(`[KafkaClient:${clientId}] ${msg}`, ...args),
      debug: (msg, ...args) =>
        console.debug(`[KafkaClient:${clientId}] ${msg}`, ...args),
    };
    this.autoCreateTopicsEnabled = options?.autoCreateTopics ?? false;
    this.strictSchemasEnabled = options?.strictSchemas ?? true;
    this.numPartitions = options?.numPartitions ?? 1;
    this.instrumentation = options?.instrumentation ?? [];
    this.onMessageLost = options?.onMessageLost;
    this.onRebalance = options?.onRebalance;
    this.txId = options?.transactionalId ?? `${clientId}-tx`;

    this.kafka = new KafkaClass({
      kafkaJS: {
        clientId: this.clientId,
        brokers,
        logLevel: KafkaLogLevel.ERROR,
      },
    });
    this.producer = this.kafka.producer({
      kafkaJS: {
        acks: -1,
      },
    });
    this.admin = this.kafka.admin();
  }

  // ── Send ─────────────────────────────────────────────────────────

  /** Send a single typed message. Accepts a topic key or a TopicDescriptor. */
  public async sendMessage<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(descriptor: D, message: D["__type"], options?: SendOptions): Promise<void>;
  public async sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;
  public async sendMessage(
    topicOrDesc: any,
    message: any,
    options: SendOptions = {},
  ): Promise<void> {
    const payload = await this.preparePayload(topicOrDesc, [
      {
        value: message,
        key: options.key,
        headers: options.headers,
        correlationId: options.correlationId,
        schemaVersion: options.schemaVersion,
        eventId: options.eventId,
      },
    ]);
    await this.producer.send(payload);
    this.notifyAfterSend(payload.topic, payload.messages.length);
  }

  /** Send multiple typed messages in one call. Accepts a topic key or a TopicDescriptor. */
  public async sendBatch<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    descriptor: D,
    messages: Array<BatchMessageItem<D["__type"]>>,
  ): Promise<void>;
  public async sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<BatchMessageItem<T[K]>>,
  ): Promise<void>;
  public async sendBatch(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
  ): Promise<void> {
    const payload = await this.preparePayload(topicOrDesc, messages);
    await this.producer.send(payload);
    this.notifyAfterSend(payload.topic, payload.messages.length);
  }

  /** Execute multiple sends atomically. Commits on success, aborts on error. */
  public async transaction(
    fn: (ctx: TransactionContext<T>) => Promise<void>,
  ): Promise<void> {
    if (!this.txProducerInitPromise) {
      // Guarded by a promise (not just a flag) so that concurrent transaction()
      // calls do not each create their own producer — only the first call creates
      // the promise; subsequent concurrent calls await the same promise.
      // On connect failure the promise is cleared so the next call can retry.
      if (_activeTransactionalIds.has(this.txId)) {
        this.logger.warn(
          `transactionalId "${this.txId}" is already in use by another KafkaClient in this process. ` +
            `Kafka will fence one of the producers. ` +
            `Set a unique \`transactionalId\` (or distinct \`clientId\`) per instance.`,
        );
      }
      const initPromise: Promise<Producer> = (async () => {
        const p = this.kafka.producer({
          kafkaJS: {
            acks: -1,
            idempotent: true,
            transactionalId: this.txId,
            maxInFlightRequests: 1,
          },
        });
        await p.connect();
        _activeTransactionalIds.add(this.txId);
        return p;
      })();
      this.txProducerInitPromise = initPromise.catch((err) => {
        this.txProducerInitPromise = undefined;
        throw err;
      });
    }
    this.txProducer = await this.txProducerInitPromise;
    const tx = await this.txProducer.transaction();
    try {
      const ctx: TransactionContext<T> = {
        send: async (
          topicOrDesc: any,
          message: any,
          options: SendOptions = {},
        ) => {
          const payload = await this.preparePayload(topicOrDesc, [
            {
              value: message,
              key: options.key,
              headers: options.headers,
              correlationId: options.correlationId,
              schemaVersion: options.schemaVersion,
              eventId: options.eventId,
            },
          ]);
          await tx.send(payload);
          this.notifyAfterSend(payload.topic, payload.messages.length);
        },
        sendBatch: async (
          topicOrDesc: any,
          messages: BatchMessageItem<any>[],
        ) => {
          const payload = await this.preparePayload(topicOrDesc, messages);
          await tx.send(payload);
          this.notifyAfterSend(payload.topic, payload.messages.length);
        },
      };
      await fn(ctx);
      await tx.commit();
    } catch (error) {
      try {
        await tx.abort();
      } catch (abortError) {
        this.logger.error(
          "Failed to abort transaction:",
          toError(abortError).message,
        );
      }
      throw error;
    }
  }

  // ── Producer lifecycle ───────────────────────────────────────────

  /**
   * Connect the idempotent producer. Called automatically by `KafkaModule.register()`.
   * @internal Not part of `IKafkaClient` — use `disconnect()` for full teardown.
   */
  public async connectProducer(): Promise<void> {
    await this.producer.connect();
    this.logger.log("Producer connected");
  }

  /**
   * @internal Not part of `IKafkaClient` — use `disconnect()` for full teardown.
   */
  public async disconnectProducer(): Promise<void> {
    await this.producer.disconnect();
    this.logger.log("Producer disconnected");
  }

  // ── Consumer: eachMessage ────────────────────────────────────────

  /** Subscribe to topics and start consuming messages with the given handler. */
  public async startConsumer<K extends Array<keyof T>>(
    topics: K,
    handleMessage: (envelope: EventEnvelope<T[K[number]]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleMessage: (envelope: EventEnvelope<D["__type"]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startConsumer(
    topics: any[],
    handleMessage: (envelope: EventEnvelope<any>) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<ConsumerHandle> {
    if (options.retryTopics && !options.retry) {
      throw new Error(
        "retryTopics requires retry to be configured — set retry.maxRetries to enable the retry topic chain",
      );
    }

    const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachMessage", options);

    const deps = this.messageDeps;
    const timeoutMs = options.handlerTimeoutMs;
    const deduplication = this.resolveDeduplicationContext(
      gid,
      options.deduplication,
    );

    await consumer.run({
      eachMessage: (payload) =>
        this.trackInFlight(() =>
          handleEachMessage(
            payload,
            {
              schemaMap,
              handleMessage,
              interceptors,
              dlq,
              retry,
              retryTopics: options.retryTopics,
              timeoutMs,
              wrapWithTimeout: this.wrapWithTimeoutWarning.bind(this),
              deduplication,
            },
            deps,
          ),
        ),
    });

    this.runningConsumers.set(gid, "eachMessage");

    if (options.retryTopics && retry) {
      if (!this.autoCreateTopicsEnabled) {
        await this.validateRetryTopicsExist(topicNames, retry.maxRetries);
      }
      const companions = await startRetryTopicConsumers(
        topicNames,
        gid,
        handleMessage,
        retry,
        dlq,
        interceptors,
        schemaMap,
        this.retryTopicDeps,
        options.retryTopicAssignmentTimeoutMs,
      );
      this.companionGroupIds.set(gid, companions);
    }

    return { groupId: gid, stop: () => this.stopConsumer(gid) };
  }

  // ── Consumer: eachBatch ──────────────────────────────────────────

  /** Subscribe to topics and consume messages in batches. */
  public async startBatchConsumer<K extends Array<keyof T>>(
    topics: K,
    handleBatch: (
      envelopes: EventEnvelope<T[K[number]]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startBatchConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleBatch: (
      envelopes: EventEnvelope<D["__type"]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startBatchConsumer(
    topics: any[],
    handleBatch: (
      envelopes: EventEnvelope<any>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<ConsumerHandle> {
    if (options.retryTopics && !options.retry) {
      throw new Error(
        "retryTopics requires retry to be configured — set retry.maxRetries to enable the retry topic chain",
      );
    }

    if (options.autoCommit !== false) {
      this.logger.debug?.(
        `startBatchConsumer: autoCommit is enabled (default true). ` +
          `If your handler calls resolveOffset() or commitOffsetsIfNecessary(), set autoCommit: false to avoid offset conflicts.`,
      );
    }

    const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachBatch", options);

    const deps = this.messageDeps;
    const timeoutMs = options.handlerTimeoutMs;
    const deduplication = this.resolveDeduplicationContext(
      gid,
      options.deduplication,
    );

    await consumer.run({
      eachBatch: (payload) =>
        this.trackInFlight(() =>
          handleEachBatch(
            payload,
            {
              schemaMap,
              handleBatch,
              interceptors,
              dlq,
              retry,
              retryTopics: options.retryTopics,
              timeoutMs,
              wrapWithTimeout: this.wrapWithTimeoutWarning.bind(this),
              deduplication,
            },
            deps,
          ),
        ),
    });

    this.runningConsumers.set(gid, "eachBatch");

    if (options.retryTopics && retry) {
      if (!this.autoCreateTopicsEnabled) {
        await this.validateRetryTopicsExist(topicNames, retry.maxRetries);
      }
      // Wrap batch handler as single-message handler for retry consumers.
      // Retry consumers use eachMessage (not eachBatch), so the broker
      // high-watermark is not available. `highWatermark` is set to `null` —
      // handlers must guard against null before doing lag calculations.
      const handleMessageForRetry = (env: EventEnvelope<any>) =>
        handleBatch([env], {
          partition: env.partition,
          highWatermark: null,
          heartbeat: async () => {},
          resolveOffset: () => {},
          commitOffsetsIfNecessary: async () => {},
        });
      const companions = await startRetryTopicConsumers(
        topicNames,
        gid,
        handleMessageForRetry,
        retry,
        dlq,
        interceptors,
        schemaMap,
        this.retryTopicDeps,
        options.retryTopicAssignmentTimeoutMs,
      );
      this.companionGroupIds.set(gid, companions);
    }

    return { groupId: gid, stop: () => this.stopConsumer(gid) };
  }

  // ── Consumer lifecycle ───────────────────────────────────────────

  public async stopConsumer(groupId?: string): Promise<void> {
    if (groupId !== undefined) {
      const consumer = this.consumers.get(groupId);
      if (!consumer) {
        this.logger.warn(
          `stopConsumer: no active consumer for group "${groupId}"`,
        );
        return;
      }
      await consumer
        .disconnect()
        .catch((e) =>
          this.logger.warn(
            `Error disconnecting consumer "${groupId}":`,
            toError(e).message,
          ),
        );
      this.consumers.delete(groupId);
      this.runningConsumers.delete(groupId);
      this.consumerCreationOptions.delete(groupId);
      this.dedupStates.delete(groupId);
      this.logger.log(`Consumer disconnected: group "${groupId}"`);

      // Stop all companion retry level consumers and their tx producers
      const companions = this.companionGroupIds.get(groupId) ?? [];
      for (const cGroupId of companions) {
        const cConsumer = this.consumers.get(cGroupId);
        if (cConsumer) {
          await cConsumer
            .disconnect()
            .catch((e) =>
              this.logger.warn(
                `Error disconnecting retry consumer "${cGroupId}":`,
                toError(e).message,
              ),
            );
          this.consumers.delete(cGroupId);
          this.runningConsumers.delete(cGroupId);
          this.consumerCreationOptions.delete(cGroupId);
          this.logger.log(`Retry consumer disconnected: group "${cGroupId}"`);
        }
        // Disconnect the EOS tx producer for this retry level
        const txId = `${cGroupId}-tx`;
        const txProducer = this.retryTxProducers.get(txId);
        if (txProducer) {
          await txProducer
            .disconnect()
            .catch((e) =>
              this.logger.warn(
                `Error disconnecting retry tx producer "${txId}":`,
                toError(e).message,
              ),
            );
          _activeTransactionalIds.delete(txId);
          this.retryTxProducers.delete(txId);
        }
      }
      this.companionGroupIds.delete(groupId);
    } else {
      const tasks: Promise<void>[] = [
        ...Array.from(this.consumers.values()).map((c) =>
          c.disconnect().catch(() => {}),
        ),
        ...Array.from(this.retryTxProducers.values()).map((p) =>
          p.disconnect().catch(() => {}),
        ),
      ];
      await Promise.allSettled(tasks);
      this.consumers.clear();
      this.runningConsumers.clear();
      this.consumerCreationOptions.clear();
      this.companionGroupIds.clear();
      this.retryTxProducers.clear();
      this.dedupStates.clear();
      this.logger.log("All consumers disconnected");
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
    const gid = groupId ?? this.defaultGroupId;
    await this.ensureAdminConnected();

    const committedByTopic = await this.admin.fetchOffsets({ groupId: gid });

    const brokerOffsetsAll = await Promise.all(
      committedByTopic.map(({ topic }) => this.admin.fetchTopicOffsets(topic)),
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
  public async checkStatus(): Promise<import("../types").KafkaHealthResult> {
    try {
      await this.ensureAdminConnected();
      const topics = await this.admin.listTopics();
      return { status: "up", clientId: this.clientId, topics };
    } catch (error) {
      return {
        status: "down",
        clientId: this.clientId,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  public getClientId(): ClientId {
    return this.clientId;
  }

  public getMetrics(): Readonly<KafkaMetrics> {
    return { ...this._metrics };
  }

  public resetMetrics(): void {
    this._metrics.processedCount = 0;
    this._metrics.retryCount = 0;
    this._metrics.dlqCount = 0;
    this._metrics.dedupCount = 0;
  }

  /** Gracefully disconnect producer, all consumers, and admin. */
  public async disconnect(drainTimeoutMs = 30_000): Promise<void> {
    await this.waitForDrain(drainTimeoutMs);
    const tasks: Promise<void>[] = [this.producer.disconnect()];
    if (this.txProducer) {
      tasks.push(this.txProducer.disconnect());
      _activeTransactionalIds.delete(this.txId);
      this.txProducer = undefined;
      this.txProducerInitPromise = undefined;
    }
    for (const txId of this.retryTxProducers.keys()) {
      _activeTransactionalIds.delete(txId);
    }
    for (const p of this.retryTxProducers.values()) {
      tasks.push(p.disconnect());
    }
    this.retryTxProducers.clear();
    for (const consumer of this.consumers.values()) {
      tasks.push(consumer.disconnect());
    }
    if (this.isAdminConnected) {
      tasks.push(this.admin.disconnect());
      this.isAdminConnected = false;
    }
    await Promise.allSettled(tasks);
    this.consumers.clear();
    this.runningConsumers.clear();
    this.consumerCreationOptions.clear();
    this.companionGroupIds.clear();
    this.logger.log("All connections closed");
  }

  // ── Graceful shutdown ────────────────────────────────────────────

  /**
   * NestJS lifecycle hook — called automatically when the host module is torn down.
   * Drains in-flight handlers and disconnects all producers, consumers, and admin.
   * `KafkaModule` relies on this method; no separate destroy provider is needed.
   */
  public async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }

  /**
   * Register SIGTERM / SIGINT handlers that drain in-flight messages before
   * disconnecting. Call this once after constructing the client in non-NestJS apps.
   * NestJS apps get drain for free via `onModuleDestroy` → `disconnect()`.
   */
  public enableGracefulShutdown(
    signals: NodeJS.Signals[] = ["SIGTERM", "SIGINT"],
    drainTimeoutMs = 30_000,
  ): void {
    const handler = () => {
      this.logger.log(
        "Shutdown signal received — draining in-flight handlers...",
      );
      this.disconnect(drainTimeoutMs).catch((err) =>
        this.logger.error(
          "Error during graceful shutdown:",
          toError(err).message,
        ),
      );
    };
    for (const signal of signals) {
      process.once(signal, handler);
    }
  }

  private trackInFlight<R>(fn: () => Promise<R>): Promise<R> {
    this.inFlightTotal++;
    return fn().finally(() => {
      this.inFlightTotal--;
      if (this.inFlightTotal === 0) {
        this.drainResolvers.splice(0).forEach((r) => r());
      }
    });
  }

  private waitForDrain(timeoutMs: number): Promise<void> {
    if (this.inFlightTotal === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      let handle: ReturnType<typeof setTimeout>;
      const onDrain = () => {
        clearTimeout(handle);
        resolve();
      };
      this.drainResolvers.push(onDrain);
      handle = setTimeout(() => {
        const idx = this.drainResolvers.indexOf(onDrain);
        if (idx !== -1) this.drainResolvers.splice(idx, 1);
        this.logger.warn(
          `Drain timed out after ${timeoutMs}ms — ${this.inFlightTotal} handler(s) still in flight`,
        );
        resolve();
      }, timeoutMs);
    });
  }

  // ── Private helpers ──────────────────────────────────────────────

  private async preparePayload(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
  ) {
    registerSchema(topicOrDesc, this.schemaRegistry, this.logger);
    const payload = await buildSendPayload(
      topicOrDesc,
      messages,
      this.producerOpsDeps,
    );
    await this.ensureTopic(payload.topic);
    return payload;
  }

  // afterSend is called once per message — symmetric with beforeSend in buildSendPayload.
  private notifyAfterSend(topic: string, count: number): void {
    for (let i = 0; i < count; i++) {
      for (const inst of this.instrumentation) {
        inst.afterSend?.(topic);
      }
    }
  }

  private notifyRetry(
    envelope: import("../message/envelope").EventEnvelope<any>,
    attempt: number,
    maxRetries: number,
  ): void {
    this._metrics.retryCount++;
    for (const inst of this.instrumentation) {
      inst.onRetry?.(envelope, attempt, maxRetries);
    }
  }

  private notifyDlq(
    envelope: import("../message/envelope").EventEnvelope<any>,
    reason: DlqReason,
  ): void {
    this._metrics.dlqCount++;
    for (const inst of this.instrumentation) {
      inst.onDlq?.(envelope, reason);
    }
  }

  private notifyDuplicate(
    envelope: import("../message/envelope").EventEnvelope<any>,
    strategy: "drop" | "dlq" | "topic",
  ): void {
    this._metrics.dedupCount++;
    for (const inst of this.instrumentation) {
      inst.onDuplicate?.(envelope, strategy);
    }
  }

  private notifyMessage(
    envelope: import("../message/envelope").EventEnvelope<any>,
  ): void {
    this._metrics.processedCount++;
    for (const inst of this.instrumentation) {
      inst.onMessage?.(envelope);
    }
  }

  /**
   * Start a timer that logs a warning if `fn` hasn't resolved within `timeoutMs`.
   * The handler itself is not cancelled — the warning is diagnostic only.
   */
  private wrapWithTimeoutWarning<R>(
    fn: () => Promise<R>,
    timeoutMs: number,
    topic: string,
  ): Promise<R> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const promise = fn().finally(() => {
      if (timer !== undefined) clearTimeout(timer);
    });
    timer = setTimeout(() => {
      this.logger.warn(
        `Handler for topic "${topic}" has not resolved after ${timeoutMs}ms — possible stuck handler`,
      );
    }, timeoutMs);
    return promise;
  }

  /**
   * When `retryTopics: true` and `autoCreateTopics: false`, verify that every
   * `<topic>.retry.<level>` topic already exists. Throws a clear error at startup
   * rather than silently discovering missing topics on the first handler failure.
   */
  private async validateRetryTopicsExist(
    topicNames: string[],
    maxRetries: number,
  ): Promise<void> {
    await this.ensureAdminConnected();
    const existing = new Set(await this.admin.listTopics());
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
  private async validateDlqTopicsExist(topicNames: string[]): Promise<void> {
    await this.ensureAdminConnected();
    const existing = new Set(await this.admin.listTopics());
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
  private async validateDuplicatesTopicsExist(
    topicNames: string[],
    customDestination: string | undefined,
  ): Promise<void> {
    await this.ensureAdminConnected();
    const existing = new Set(await this.admin.listTopics());
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

  /**
   * Connect the admin client if not already connected.
   * The flag is only set to `true` after a successful connect — if `admin.connect()`
   * throws the flag remains `false` so the next call will retry the connection.
   */
  private async ensureAdminConnected(): Promise<void> {
    if (this.isAdminConnected) return;
    try {
      await this.admin.connect();
      this.isAdminConnected = true;
    } catch (err) {
      this.isAdminConnected = false;
      throw err;
    }
  }

  /**
   * Create and connect a transactional producer for EOS retry routing.
   * Each retry level consumer gets its own producer with a unique `transactionalId`
   * so Kafka can fence stale producers on restart without affecting other levels.
   */
  private async createRetryTxProducer(
    transactionalId: string,
  ): Promise<Producer> {
    if (_activeTransactionalIds.has(transactionalId)) {
      this.logger.warn(
        `transactionalId "${transactionalId}" is already in use by another KafkaClient in this process. ` +
          `Kafka will fence one of the producers. ` +
          `Set a unique \`transactionalId\` (or distinct \`clientId\`) per instance.`,
      );
    }
    const p = this.kafka.producer({
      kafkaJS: {
        acks: -1,
        idempotent: true,
        transactionalId,
        maxInFlightRequests: 1,
      },
    });
    await p.connect();
    _activeTransactionalIds.add(transactionalId);
    this.retryTxProducers.set(transactionalId, p);
    return p;
  }

  private async ensureTopic(topic: string): Promise<void> {
    if (!this.autoCreateTopicsEnabled || this.ensuredTopics.has(topic)) return;
    // Deduplicate concurrent calls for the same topic so that parallel sends
    // (or consumer setup + send) don't each race to call createTopics.
    let p = this.ensureTopicPromises.get(topic);
    if (!p) {
      p = (async () => {
        await this.ensureAdminConnected();
        await this.admin.createTopics({
          topics: [{ topic, numPartitions: this.numPartitions }],
        });
        this.ensuredTopics.add(topic);
      })().finally(() => this.ensureTopicPromises.delete(topic));
      this.ensureTopicPromises.set(topic, p);
    }
    await p;
  }

  /** Shared consumer setup: groupId check, schema map, connect, subscribe. */
  private async setupConsumer(
    topics: any[],
    mode: "eachMessage" | "eachBatch",
    options: ConsumerOptions<T>,
  ) {
    const {
      groupId: optGroupId,
      fromBeginning = false,
      retry,
      dlq = false,
      interceptors = [],
      schemas: optionSchemas,
    } = options;

    const gid = optGroupId || this.defaultGroupId;
    const existingMode = this.runningConsumers.get(gid);
    const oppositeMode = mode === "eachMessage" ? "eachBatch" : "eachMessage";
    if (existingMode === oppositeMode) {
      throw new Error(
        `Cannot use ${mode} on consumer group "${gid}" — it is already running with ${oppositeMode}. ` +
          `Use a different groupId for this consumer.`,
      );
    }
    if (existingMode === mode) {
      const callerName =
        mode === "eachMessage" ? "startConsumer" : "startBatchConsumer";
      throw new Error(
        `${callerName}("${gid}") called twice — this group is already consuming. ` +
          `Call stopConsumer("${gid}") first or pass a different groupId.`,
      );
    }

    const consumer = getOrCreateConsumer(
      gid,
      fromBeginning,
      options.autoCommit ?? true,
      this.consumerOpsDeps,
    );
    const schemaMap = buildSchemaMap(
      topics,
      this.schemaRegistry,
      optionSchemas,
      this.logger,
    );

    const topicNames = (topics as any[]).map((t: any) => resolveTopicName(t));

    // Ensure topics exist before subscribing — librdkafka errors on unknown topics
    for (const t of topicNames) {
      await this.ensureTopic(t);
    }
    if (dlq) {
      for (const t of topicNames) {
        await this.ensureTopic(`${t}.dlq`);
      }
      if (!this.autoCreateTopicsEnabled) {
        await this.validateDlqTopicsExist(topicNames);
      }
    }

    if (options.deduplication?.strategy === "topic") {
      const dest = options.deduplication.duplicatesTopic;
      if (this.autoCreateTopicsEnabled) {
        for (const t of topicNames) {
          await this.ensureTopic(dest ?? `${t}.duplicates`);
        }
      } else {
        await this.validateDuplicatesTopicsExist(topicNames, dest);
      }
    }

    await consumer.connect();
    await subscribeWithRetry(
      consumer,
      topicNames,
      this.logger,
      options.subscribeRetry,
    );

    this.logger.log(
      `${mode === "eachBatch" ? "Batch consumer" : "Consumer"} subscribed to topics: ${topicNames.join(", ")}`,
    );

    return { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry };
  }

  /** Create or retrieve the deduplication context for a consumer group. */
  private resolveDeduplicationContext(
    groupId: string,
    options: import("../types").DeduplicationOptions | undefined,
  ): DeduplicationContext | undefined {
    if (!options) return undefined;
    if (!this.dedupStates.has(groupId)) {
      this.dedupStates.set(groupId, new Map());
    }
    return { options, state: this.dedupStates.get(groupId)! };
  }

  // ── Deps object getters ──────────────────────────────────────────

  private get producerOpsDeps(): BuildSendPayloadDeps {
    return {
      schemaRegistry: this.schemaRegistry,
      strictSchemasEnabled: this.strictSchemasEnabled,
      instrumentation: this.instrumentation,
      logger: this.logger,
      nextLamportClock: () => ++this._lamportClock,
    };
  }

  private get consumerOpsDeps(): ConsumerOpsDeps {
    return {
      consumers: this.consumers,
      consumerCreationOptions: this.consumerCreationOptions,
      kafka: this.kafka,
      onRebalance: this.onRebalance,
      logger: this.logger,
    };
  }

  private get messageDeps(): MessageHandlerDeps {
    return {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
      onRetry: this.notifyRetry.bind(this),
      onDlq: this.notifyDlq.bind(this),
      onDuplicate: this.notifyDuplicate.bind(this),
      onMessage: this.notifyMessage.bind(this),
    };
  }

  private get retryTopicDeps() {
    return {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
      onRetry: this.notifyRetry.bind(this),
      onDlq: this.notifyDlq.bind(this),
      onMessage: this.notifyMessage.bind(this),
      ensureTopic: (t: string) => this.ensureTopic(t),
      getOrCreateConsumer: (gid: string, fb: boolean, ac: boolean) =>
        getOrCreateConsumer(gid, fb, ac, this.consumerOpsDeps),
      runningConsumers: this.runningConsumers,
      createRetryTxProducer: (txId: string) => this.createRetryTxProducer(txId),
    };
  }
}
