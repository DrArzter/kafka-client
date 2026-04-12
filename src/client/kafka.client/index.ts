import type { KafkaTransport } from "../transport.interface";
import { ConfluentTransport } from "./confluent-transport";
import type { TopicDescriptor, SchemaLike } from "../message/topic";
import type { EventEnvelope } from "../message/envelope";
import type {
  ClientId,
  GroupId,
  SendOptions,
  BatchMessageItem,
  BatchSendOptions,
  ConsumerOptions,
  ConsumerHandle,
  TransactionContext,
  TopicMapConstraint,
  IKafkaClient,
  KafkaClientOptions,
  BatchMeta,
  DlqReplayOptions,
  ConsumerGroupSummary,
  TopicDescription,
  MessageHeaders,
  ReadSnapshotOptions,
  CheckpointResult,
  CheckpointRestoreResult,
  RestoreCheckpointOptions,
  WindowConsumerOptions,
  WindowMeta,
  RoutingOptions,
  TransactionalHandlerContext,
} from "../types";

// Re-export all types so existing `import { ... } from './kafka.client'` keeps working
export * from "../types";

import { getOrCreateConsumer } from "./consumer/ops";
import { AdminOps } from "./admin/ops";
import { replayDlqTopic } from "./consumer/dlq-replay";
import { MetricsManager } from "./infra/metrics.manager";
import { InFlightTracker } from "./infra/inflight.tracker";
import { CircuitBreakerManager } from "./infra/circuit-breaker.manager";
import { AsyncQueue } from "./consumer/queue";
import { toError } from "./consumer/pipeline";

import type { KafkaClientContext } from "./context";
import { connectProducerImpl, disconnectImpl } from "./producer/lifecycle";
import {
  sendMessageImpl,
  sendBatchImpl,
  sendTombstoneImpl,
  transactionImpl,
} from "./producer/send";
import { buildRetryTopicDeps } from "./consumer/setup";
import { startConsumerImpl, startBatchConsumerImpl, startTransactionalConsumerImpl } from "./consumer/start";
import { startWindowConsumerImpl } from "./consumer/window";
import { startRoutedConsumerImpl } from "./consumer/routed";
import {
  stopConsumerImpl,
  pauseConsumerImpl,
  resumeConsumerImpl,
  pauseTopicAllPartitions,
  resumeTopicAllPartitions,
} from "./consumer/stop";
import {
  readSnapshotImpl,
  checkpointOffsetsImpl,
  restoreFromCheckpointImpl,
} from "./consumer/snapshot";

/** DLQ header keys added by the pipeline — stripped before re-publishing. */
const DLQ_HEADER_KEYS = new Set([
  "x-dlq-original-topic",
  "x-dlq-failed-at",
  "x-dlq-error-message",
  "x-dlq-error-stack",
  "x-dlq-attempt-count",
]);

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
  public readonly clientId: ClientId;
  private readonly ctx: KafkaClientContext<T>;

  /**
   * Create a new KafkaClient.
   * @param clientId Unique client identifier (used in Kafka metadata and logs).
   * @param groupId Default consumer group ID for this client.
   * @param brokers Array of broker addresses, e.g. `['localhost:9092']`.
   * @param options Optional client-wide configuration.
   * @example
   * ```ts
   * const kafka = new KafkaClient('my-service', 'my-service-group', ['localhost:9092'], {
   *   lagThrottle: { maxLag: 10_000 },
   *   onMessageLost: (ctx) => logger.error('Message lost', ctx),
   * });
   * await kafka.connectProducer();
   * ```
   */
  constructor(
    clientId: ClientId,
    groupId: GroupId,
    brokers: string[],
    options?: KafkaClientOptions,
  ) {
    this.clientId = clientId;

    const logger = options?.logger ?? {
      log: (msg: string) => console.log(`[KafkaClient:${clientId}] ${msg}`),
      warn: (msg: string, ...args: any[]) =>
        console.warn(`[KafkaClient:${clientId}] ${msg}`, ...args),
      error: (msg: string, ...args: any[]) =>
        console.error(`[KafkaClient:${clientId}] ${msg}`, ...args),
      debug: (msg: string, ...args: any[]) =>
        console.debug(`[KafkaClient:${clientId}] ${msg}`, ...args),
    };

    const transport: KafkaTransport =
      options?.transport ?? new ConfluentTransport(clientId, brokers);
    const producer = transport.producer();

    const runningConsumers = new Map<string, "eachMessage" | "eachBatch">();
    const consumers = new Map();
    const consumerCreationOptions = new Map<string, { fromBeginning: boolean; autoCommit: boolean }>();
    const schemaRegistry = new Map<string, SchemaLike>();

    const adminOps = new AdminOps({
      admin: transport.admin(),
      logger,
      runningConsumers,
      defaultGroupId: groupId,
      clientId,
    });

    const circuitBreaker = new CircuitBreakerManager({
      pauseConsumer: (gid, assignments) => pauseConsumerImpl(this.ctx, gid, assignments),
      resumeConsumer: (gid, assignments) => resumeConsumerImpl(this.ctx, gid, assignments),
      logger,
      instrumentation: options?.instrumentation ?? [],
    });

    const metrics = new MetricsManager({
      instrumentation: options?.instrumentation ?? [],
      onCircuitFailure: (envelope, gid) => circuitBreaker.onFailure(envelope, gid),
      onCircuitSuccess: (envelope, gid) => circuitBreaker.onSuccess(envelope, gid),
    });

    const inFlight = new InFlightTracker((msg) => logger.warn(msg));

    // Partially construct context — retryTopicDeps is patched below
    const ctx = {
      clientId,
      defaultGroupId: groupId,
      logger,
      autoCreateTopicsEnabled: options?.autoCreateTopics ?? false,
      strictSchemasEnabled: options?.strictSchemas ?? true,
      numPartitions: options?.numPartitions ?? 1,
      txId: options?.transactionalId ?? `${clientId}-tx`,
      clockRecoveryTopics: options?.clockRecovery?.topics ?? [],
      lagThrottleOpts: options?.lagThrottle,
      instrumentation: options?.instrumentation ?? [],
      onMessageLost: options?.onMessageLost,
      onTtlExpired: options?.onTtlExpired,
      onRebalance: options?.onRebalance,
      transport,
      producer,
      txProducer: undefined as any,
      txProducerInitPromise: undefined as any,
      retryTxProducers: new Map(),
      consumers,
      runningConsumers,
      consumerCreationOptions,
      companionGroupIds: new Map<string, string[]>(),
      dedupStates: new Map<string, Map<string, number>>(),
      ensuredTopics: new Set<string>(),
      ensureTopicPromises: new Map<string, Promise<void>>(),
      schemaRegistry,
      _lagThrottled: false,
      _lagThrottleTimer: undefined as any,
      _lamportClock: 0,
      circuitBreaker,
      adminOps,
      metrics,
      inFlight,
      producerOpsDeps: {
        schemaRegistry,
        strictSchemasEnabled: options?.strictSchemas ?? true,
        instrumentation: options?.instrumentation ?? [],
        logger,
        nextLamportClock: () => 0, // patched below
      },
      consumerOpsDeps: {
        consumers,
        consumerCreationOptions,
        transport,
        onRebalance: options?.onRebalance,
        logger,
      },
      retryTopicDeps: {} as any, // patched below
    } satisfies KafkaClientContext<T>;

    // Patch mutable closures that need the fully constructed ctx
    ctx.producerOpsDeps = {
      schemaRegistry,
      strictSchemasEnabled: options?.strictSchemas ?? true,
      instrumentation: options?.instrumentation ?? [],
      logger,
      nextLamportClock: () => ++ctx._lamportClock,
    };
    ctx.retryTopicDeps = buildRetryTopicDeps(ctx);

    this.ctx = ctx;
  }

  // ── Send ──────────────────────────────────────────────────────────

  /** @inheritDoc */
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
    return sendMessageImpl(this.ctx, topicOrDesc, message, options);
  }

  /** @inheritDoc */
  public async sendTombstone(
    topic: string,
    key: string,
    headers?: MessageHeaders,
  ): Promise<void> {
    return sendTombstoneImpl(this.ctx, topic, key, headers);
  }

  /** @inheritDoc */
  public async sendBatch<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    descriptor: D,
    messages: Array<BatchMessageItem<D["__type"]>>,
    options?: BatchSendOptions,
  ): Promise<void>;
  public async sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<BatchMessageItem<T[K]>>,
    options?: BatchSendOptions,
  ): Promise<void>;
  public async sendBatch(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
    options?: BatchSendOptions,
  ): Promise<void> {
    return sendBatchImpl(this.ctx, topicOrDesc, messages, options);
  }

  /** @inheritDoc */
  public async transaction(
    fn: (ctx: TransactionContext<T>) => Promise<void>,
  ): Promise<void> {
    return transactionImpl(this.ctx, fn);
  }

  // ── Producer lifecycle ────────────────────────────────────────────

  /** @inheritDoc */
  public async connectProducer(): Promise<void> {
    return connectProducerImpl(this.ctx);
  }

  /** @internal */
  public async disconnectProducer(): Promise<void> {
    if (this.ctx._lagThrottleTimer) {
      clearInterval(this.ctx._lagThrottleTimer);
      this.ctx._lagThrottleTimer = undefined;
    }
    await this.ctx.producer.disconnect();
    this.ctx.logger.log("Producer disconnected");
  }

  // ── Consumer: eachMessage ─────────────────────────────────────────

  /** @inheritDoc */
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
    return startConsumerImpl(this.ctx, topics, handleMessage, options);
  }

  // ── Consumer: eachBatch ───────────────────────────────────────────

  /** @inheritDoc */
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
    return startBatchConsumerImpl(this.ctx, topics, handleBatch, options);
  }

  // ── Consumer: AsyncIterableIterator ──────────────────────────────

  /** @inheritDoc */
  public consume<K extends keyof T & string>(
    topic: K,
    options?: ConsumerOptions<T>,
  ): AsyncIterableIterator<EventEnvelope<T[K]>> {
    if (options?.retryTopics) {
      throw new Error(
        "consume() does not support retryTopics (EOS retry chains). " +
          "Use startConsumer() with retryTopics: true for guaranteed retry delivery.",
      );
    }
    const gid = options?.groupId ?? this.ctx.defaultGroupId;
    const queue = new AsyncQueue<EventEnvelope<T[K]>>(
      options?.queueHighWaterMark,
      () => pauseTopicAllPartitions(this.ctx, gid, topic as string),
      () => resumeTopicAllPartitions(this.ctx, gid, topic as string),
    );
    const handlePromise = this.startConsumer(
      [topic as any],
      async (envelope) => {
        queue.push(envelope as EventEnvelope<T[K]>);
      },
      options,
    );
    handlePromise.catch((err: Error) => queue.fail(err));
    return {
      [Symbol.asyncIterator]() {
        return this;
      },
      next: () => queue.next(),
      return: async () => {
        queue.close();
        const handle = await handlePromise;
        await handle.stop();
        return { value: undefined as any, done: true as const };
      },
    };
  }

  // ── Consumer: windowed ────────────────────────────────────────────

  /** @inheritDoc */
  public startWindowConsumer<K extends keyof T & string>(
    topic: K,
    handler: (
      envelopes: EventEnvelope<T[K]>[],
      meta: WindowMeta,
    ) => Promise<void>,
    options: WindowConsumerOptions<T>,
  ): Promise<ConsumerHandle> {
    return startWindowConsumerImpl(this.ctx, topic, handler, options);
  }

  // ── Consumer: header routing ──────────────────────────────────────

  /** @inheritDoc */
  public startRoutedConsumer<K extends Array<keyof T>>(
    topics: K,
    routing: RoutingOptions<T[K[number]]>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle> {
    return startRoutedConsumerImpl(this.ctx, topics, routing, options);
  }

  // ── Consumer: transactional EOS ───────────────────────────────────

  /** @inheritDoc */
  public async startTransactionalConsumer<K extends Array<keyof T>>(
    topics: K,
    handler: (
      envelope: EventEnvelope<T[K[number]]>,
      tx: TransactionalHandlerContext<T>,
    ) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<ConsumerHandle> {
    return startTransactionalConsumerImpl(this.ctx, topics, handler, options);
  }

  // ── Consumer lifecycle ────────────────────────────────────────────

  /** @inheritDoc */
  public async stopConsumer(groupId?: string): Promise<void> {
    return stopConsumerImpl(this.ctx, groupId);
  }

  /** @inheritDoc */
  public pauseConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void {
    return pauseConsumerImpl(this.ctx, groupId, assignments);
  }

  /** @inheritDoc */
  public resumeConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void {
    return resumeConsumerImpl(this.ctx, groupId, assignments);
  }

  // ── DLQ replay ────────────────────────────────────────────────────

  /** @inheritDoc */
  public async replayDlq(
    topic: string,
    options: DlqReplayOptions = {},
  ): Promise<{ replayed: number; skipped: number }> {
    await this.ctx.adminOps.ensureConnected();
    return replayDlqTopic(
      topic,
      {
        logger: this.ctx.logger,
        fetchTopicOffsets: (t) => this.ctx.adminOps.admin.fetchTopicOffsets(t),
        send: async (t, messages) => {
          await this.ctx.producer.send({ topic: t, messages });
        },
        createConsumer: (gid, fromBeginning) =>
          getOrCreateConsumer(gid, fromBeginning, true, this.ctx.consumerOpsDeps),
        cleanupConsumer: (consumer, gid, deleteGroup) => {
          consumer
            .disconnect()
            .catch(() => {})
            .finally(() => {
              this.ctx.consumers.delete(gid);
              this.ctx.runningConsumers.delete(gid);
              this.ctx.consumerCreationOptions.delete(gid);
              if (deleteGroup) {
                this.ctx.adminOps.deleteGroups([gid]).catch(() => {});
              }
            });
        },
        dlqHeaderKeys: DLQ_HEADER_KEYS,
      },
      options,
    );
  }

  // ── Snapshot & checkpoint ─────────────────────────────────────────

  /** @inheritDoc */
  public async readSnapshot<K extends keyof T & string>(
    topic: K,
    options: ReadSnapshotOptions = {},
  ): Promise<Map<string, EventEnvelope<T[K]>>> {
    return readSnapshotImpl(this.ctx, topic, options);
  }

  /** @inheritDoc */
  public async checkpointOffsets(
    groupId: string | undefined,
    checkpointTopic: string,
  ): Promise<CheckpointResult> {
    return checkpointOffsetsImpl(this.ctx, groupId, checkpointTopic);
  }

  /** @inheritDoc */
  public async restoreFromCheckpoint(
    groupId: string | undefined,
    checkpointTopic: string,
    options: RestoreCheckpointOptions = {},
  ): Promise<CheckpointRestoreResult> {
    return restoreFromCheckpointImpl(this.ctx, groupId, checkpointTopic, options);
  }

  // ── Admin ─────────────────────────────────────────────────────────

  /** @inheritDoc */
  public async resetOffsets(
    groupId: string | undefined,
    topic: string,
    position: "earliest" | "latest",
  ): Promise<void> {
    return this.ctx.adminOps.resetOffsets(groupId, topic, position);
  }

  /** @inheritDoc */
  public async seekToOffset(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; offset: string }>,
  ): Promise<void> {
    return this.ctx.adminOps.seekToOffset(groupId, assignments);
  }

  /** @inheritDoc */
  public async seekToTimestamp(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; timestamp: number }>,
  ): Promise<void> {
    return this.ctx.adminOps.seekToTimestamp(groupId, assignments);
  }

  /** @inheritDoc */
  public async getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>> {
    return this.ctx.adminOps.getConsumerLag(groupId);
  }

  /** @inheritDoc */
  public async checkStatus(): Promise<import("../types").KafkaHealthResult> {
    return this.ctx.adminOps.checkStatus();
  }

  /** @inheritDoc */
  public async listConsumerGroups(): Promise<ConsumerGroupSummary[]> {
    return this.ctx.adminOps.listConsumerGroups();
  }

  /** @inheritDoc */
  public async describeTopics(topics?: string[]): Promise<TopicDescription[]> {
    return this.ctx.adminOps.describeTopics(topics);
  }

  /** @inheritDoc */
  public async deleteRecords(
    topic: string,
    partitions: Array<{ partition: number; offset: string }>,
  ): Promise<void> {
    return this.ctx.adminOps.deleteRecords(topic, partitions);
  }

  // ── Circuit breaker ───────────────────────────────────────────────

  /** @inheritDoc */
  public getCircuitState(
    topic: string,
    partition: number,
    groupId?: string,
  ):
    | { status: "closed" | "open" | "half-open"; failures: number; windowSize: number }
    | undefined {
    return this.ctx.circuitBreaker.getState(
      topic,
      partition,
      groupId ?? this.ctx.defaultGroupId,
    );
  }

  // ── Metrics ───────────────────────────────────────────────────────

  /** @inheritDoc */
  public getMetrics(topic?: string): Readonly<import("../types").KafkaMetrics> {
    return this.ctx.metrics.getMetrics(topic);
  }

  /** @inheritDoc */
  public resetMetrics(topic?: string): void {
    this.ctx.metrics.resetMetrics(topic);
  }

  public getClientId(): ClientId {
    return this.clientId;
  }

  // ── Lifecycle ─────────────────────────────────────────────────────

  /** @inheritDoc */
  public async disconnect(drainTimeoutMs = 30_000): Promise<void> {
    return disconnectImpl(this.ctx, drainTimeoutMs);
  }

  /** NestJS lifecycle hook — called automatically on module teardown. */
  public async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }

  /** @inheritDoc */
  public enableGracefulShutdown(
    signals: NodeJS.Signals[] = ["SIGTERM", "SIGINT"],
    drainTimeoutMs = 30_000,
  ): void {
    const handler = () => {
      this.ctx.logger.log(
        "Shutdown signal received — draining in-flight handlers...",
      );
      this.disconnect(drainTimeoutMs)
        .catch((err) =>
          this.ctx.logger.error(
            "Error during graceful shutdown:",
            toError(err).message,
          ),
        )
        .finally(() => process.exit(0));
    };
    for (const signal of signals) process.once(signal, handler);
  }
}
