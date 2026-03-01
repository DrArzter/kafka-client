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
  BatchSendOptions,
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
  DlqReplayOptions,
  CircuitBreakerOptions,
  ConsumerGroupSummary,
  TopicDescription,
  MessageHeaders,
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
import { decodeHeaders } from "../message/envelope";

/** Push-to-pull async queue used by consume() to bridge Kafka's push model to AsyncIterableIterator. */
class AsyncQueue<V> {
  private readonly items: V[] = [];
  private readonly waiting: Array<{
    resolve: (r: IteratorResult<V>) => void;
    reject: (err: Error) => void;
  }> = [];
  private closed = false;
  private error?: Error;
  private paused = false;

  constructor(
    private readonly highWaterMark = Infinity,
    private readonly onFull: () => void = () => {},
    private readonly onDrained: () => void = () => {},
  ) {}

  push(item: V): void {
    if (this.waiting.length > 0) {
      this.waiting.shift()!.resolve({ value: item, done: false });
    } else {
      this.items.push(item);
      if (!this.paused && this.items.length >= this.highWaterMark) {
        this.paused = true;
        this.onFull();
      }
    }
  }

  fail(err: Error): void {
    this.closed = true;
    this.error = err;
    for (const { reject } of this.waiting.splice(0)) reject(err);
  }

  close(): void {
    this.closed = true;
    for (const { resolve } of this.waiting.splice(0))
      resolve({ value: undefined as any, done: true });
  }

  next(): Promise<IteratorResult<V>> {
    if (this.error) return Promise.reject(this.error);
    if (this.items.length > 0) {
      const value = this.items.shift()!;
      if (
        this.paused &&
        this.items.length <= Math.floor(this.highWaterMark / 2)
      ) {
        this.paused = false;
        this.onDrained();
      }
      return Promise.resolve({ value, done: false });
    }
    if (this.closed) return Promise.resolve({ value: undefined as any, done: true });
    return new Promise((resolve, reject) => this.waiting.push({ resolve, reject }));
  }
}

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
  private readonly onTtlExpired: KafkaClientOptions["onTtlExpired"];
  private readonly onRebalance: KafkaClientOptions["onRebalance"];
  /** Transactional producer ID — configurable via `KafkaClientOptions.transactionalId`. */
  private readonly txId: string;
  /** Per-topic event counters, lazily created on first event. Aggregated by `getMetrics()`. */
  private readonly _topicMetrics = new Map<string, KafkaMetrics>();

  /** Monotonically increasing Lamport clock stamped on every outgoing message. */
  private _lamportClock = 0;
  /** Per-groupId deduplication state: `"topic:partition"` → last processed clock. */
  private readonly dedupStates = new Map<string, Map<string, number>>();

  /** Circuit breaker state per `"${gid}:${topic}:${partition}"` key. */
  private readonly circuitStates = new Map<
    string,
    {
      status: "closed" | "open" | "half-open";
      window: boolean[];
      successes: number;
      timer?: ReturnType<typeof setTimeout>;
    }
  >();
  /** Circuit breaker config per groupId, set at startConsumer/startBatchConsumer time. */
  private readonly circuitConfigs = new Map<string, CircuitBreakerOptions>();

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
    this.onTtlExpired = options?.onTtlExpired;
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

  /**
   * Send a single typed message. Accepts a topic key or a `TopicDescriptor`.
   *
   * @param topic Topic key from the `TopicMessageMap` or a `TopicDescriptor` object.
   * @param message Message payload — validated against the topic schema when one is registered.
   * @param options Optional per-send settings: `key`, `headers`, `correlationId`, `compression`, etc.
   */
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
    const payload = await this.preparePayload(
      topicOrDesc,
      [
        {
          value: message,
          key: options.key,
          headers: options.headers,
          correlationId: options.correlationId,
          schemaVersion: options.schemaVersion,
          eventId: options.eventId,
        },
      ],
      options.compression,
    );
    await this.producer.send(payload);
    this.notifyAfterSend(payload.topic, payload.messages.length);
  }

  /**
   * Send a null-value (tombstone) message. Used with log-compacted topics to signal
   * that a key's record should be removed during the next compaction cycle.
   *
   * Tombstones skip envelope headers, schema validation, and Lamport clock stamping.
   * Both `beforeSend` and `afterSend` instrumentation hooks are still called so tracing works correctly.
   *
   * @param topic Topic name.
   * @param key Partition key identifying the record to tombstone.
   * @param headers Optional custom Kafka headers.
   */
  public async sendTombstone(
    topic: string,
    key: string,
    headers?: MessageHeaders,
  ): Promise<void> {
    const hdrs: MessageHeaders = { ...headers };
    for (const inst of this.instrumentation) {
      inst.beforeSend?.(topic, hdrs);
    }
    await this.ensureTopic(topic);
    await this.producer.send({
      topic,
      messages: [{ value: null, key, headers: hdrs }],
    });
    for (const inst of this.instrumentation) {
      inst.afterSend?.(topic);
    }
  }

  /**
   * Send multiple typed messages in a single Kafka produce request. Accepts a topic key or a `TopicDescriptor`.
   *
   * Each item in `messages` can carry its own `key`, `headers`, `correlationId`, and `schemaVersion`.
   * The `key` is used for partition routing — messages with the same key always land on the same partition.
   *
   * @param topic Topic key from the `TopicMessageMap` or a `TopicDescriptor` object.
   * @param messages Array of messages to send.
   * @param options Optional batch-level settings: `compression` codec.
   */
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
    const payload = await this.preparePayload(
      topicOrDesc,
      messages,
      options?.compression,
    );
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
  /**
   * Send multiple messages in a single call to the topic.
   * All messages in the batch will be sent atomically.
   * If any message fails to send, the entire batch will be aborted.
   * @param topicOrDesc - topic name or TopicDescriptor
   * @param messages - array of messages to send with optional key, headers, correlationId, schemaVersion, and eventId
   */
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

  /**
   * Subscribe to one or more topics and start consuming messages one at a time.
   *
   * Each message is delivered to `handleMessage` as a fully-decoded `EventEnvelope`.
   * The call blocks until the consumer is connected and the subscription is set up,
   * then returns a `ConsumerHandle` with a `stop()` method for clean shutdown.
   *
   * @param topics Array of topic keys, `TopicDescriptor` objects, or `RegExp` patterns.
   *   Regex patterns cannot be combined with `retryTopics: true`.
   * @param handleMessage Async handler called for every message. Throw to trigger retries.
   * @param options Consumer configuration — `groupId`, `retry`, `dlq`, `circuitBreaker`, etc.
   * @returns A handle with `{ groupId, stop() }` for managing the consumer lifecycle.
   */
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
    const hasRegexTopics = topics.some((t) => t instanceof RegExp);
    if (options.retryTopics && hasRegexTopics) {
      throw new Error(
        "retryTopics is incompatible with regex topic patterns — retry topics require a fixed topic name to build the retry chain.",
      );
    }

    // When retryTopics is enabled, the main consumer must run with autoCommit: false
    // so that offset commits can be coordinated with EOS routing transactions.
    const setupOptions = options.retryTopics
      ? { ...options, autoCommit: false as const }
      : options;

    const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachMessage", setupOptions);

    if (options.circuitBreaker)
      this.circuitConfigs.set(gid, options.circuitBreaker);
    const deps = this.messageDepsFor(gid);
    const timeoutMs = options.handlerTimeoutMs;
    const deduplication = this.resolveDeduplicationContext(
      gid,
      options.deduplication,
    );

    // Create EOS transactional producer for atomic main → retry.1 routing.
    let eosMainContext:
      | import("./message-handler").EachMessageOpts["eosMainContext"]
      | undefined;
    if (options.retryTopics && retry) {
      const mainTxId = `${gid}-main-tx`;
      const txProducer = await this.createRetryTxProducer(mainTxId);
      eosMainContext = { txProducer, consumer };
    }

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
              messageTtlMs: options.messageTtlMs,
              onTtlExpired: options.onTtlExpired,
              eosMainContext,
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

  /**
   * Subscribe to one or more topics and consume messages in batches.
   *
   * `handleBatch` receives an array of decoded `EventEnvelope` objects together with
   * batch metadata (topic, partition, high-watermark offset). Prefer this over
   * `startConsumer` when throughput matters more than per-message latency.
   *
   * Set `autoCommit: false` in options when the handler calls `resolveOffset()` or
   * `commitOffsetsIfNecessary()` directly, to avoid offset conflicts.
   *
   * @param topics Array of topic keys, `TopicDescriptor` objects, or `RegExp` patterns.
   * @param handleBatch Async handler called with each batch of decoded messages.
   * @param options Consumer configuration — `groupId`, `retry`, `dlq`, `autoCommit`, etc.
   * @returns A handle with `{ groupId, stop() }` for managing the consumer lifecycle.
   */
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
    const hasRegexTopics = topics.some((t) => t instanceof RegExp);
    if (options.retryTopics && hasRegexTopics) {
      throw new Error(
        "retryTopics is incompatible with regex topic patterns — retry topics require a fixed topic name to build the retry chain.",
      );
    }

    if (options.retryTopics) {
      // When retryTopics is enabled, autoCommit: false is required for EOS routing.
      // Suppress the manual-offset diagnostic that would otherwise fire.
    } else if (options.autoCommit !== false) {
      this.logger.debug?.(
        `startBatchConsumer: autoCommit is enabled (default true). ` +
          `If your handler calls resolveOffset() or commitOffsetsIfNecessary(), set autoCommit: false to avoid offset conflicts.`,
      );
    }

    const setupOptions = options.retryTopics
      ? { ...options, autoCommit: false as const }
      : options;

    const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachBatch", setupOptions);

    if (options.circuitBreaker)
      this.circuitConfigs.set(gid, options.circuitBreaker);
    const deps = this.messageDepsFor(gid);
    const timeoutMs = options.handlerTimeoutMs;
    const deduplication = this.resolveDeduplicationContext(
      gid,
      options.deduplication,
    );

    let eosMainContext:
      | import("./message-handler").EachBatchOpts["eosMainContext"]
      | undefined;
    if (options.retryTopics && retry) {
      const mainTxId = `${gid}-main-tx`;
      const txProducer = await this.createRetryTxProducer(mainTxId);
      eosMainContext = { txProducer, consumer };
    }

    await consumer.run({
    /**
     * eachBatch: called by the consumer for each batch of messages.
     * Called with the `payload` argument, which is an object containing the
     * batch of messages and a `BatchMeta` object with offset management controls.
     *
     * The function is wrapped with `trackInFlight` and `handleEachBatch` to provide
     * error handling and offset management.
     *
     * @param payload - an object containing the batch of messages and a `BatchMeta` object.
     */
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
              messageTtlMs: options.messageTtlMs,
              onTtlExpired: options.onTtlExpired,
              eosMainContext,
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

  /**
   * Consume messages from a topic as an AsyncIterableIterator.
   * Use with `for await` — breaking out of the loop automatically stops the consumer.
   *
   * @example
   * for await (const envelope of kafka.consume('my.topic')) {
   *   console.log(envelope.data);
   * }
   */
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
    const gid = options?.groupId ?? this.defaultGroupId;
    const queue = new AsyncQueue<EventEnvelope<T[K]>>(
      options?.queueHighWaterMark,
      () => this.pauseTopicAllPartitions(gid, topic as string),
      () => this.resumeTopicAllPartitions(gid, topic as string),
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

  // ── Consumer lifecycle ───────────────────────────────────────────

  /**
   * Stop all consumers or a specific group.
   *
   * If `groupId` is unspecified, all active consumers are stopped.
   * If `groupId` is specified, only the consumer with that group ID is stopped.
   *
   * @throws {Error} if the consumer fails to disconnect.
   */
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
      // Clean up circuit breaker state for this group
      for (const key of [...this.circuitStates.keys()]) {
        if (key.startsWith(`${groupId}:`)) {
          clearTimeout(this.circuitStates.get(key)!.timer);
          this.circuitStates.delete(key);
        }
      }
      this.circuitConfigs.delete(groupId);
      this.logger.log(`Consumer disconnected: group "${groupId}"`);

      // Clean up the main consumer's EOS tx producer (present when retryTopics: true)
      const mainTxId = `${groupId}-main-tx`;
      const mainTxProducer = this.retryTxProducers.get(mainTxId);
      if (mainTxProducer) {
        await mainTxProducer
          .disconnect()
          .catch((e) =>
            this.logger.warn(
              `Error disconnecting main tx producer "${mainTxId}":`,
              toError(e).message,
            ),
          );
        _activeTransactionalIds.delete(mainTxId);
        this.retryTxProducers.delete(mainTxId);
      }

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
      for (const state of this.circuitStates.values())
        clearTimeout(state.timer);
      this.circuitStates.clear();
      this.circuitConfigs.clear();
      this.logger.log("All consumers disconnected");
    }
  }

  /**
   * Temporarily stop delivering messages from specific partitions without disconnecting the consumer.
   *
   * @param groupId Consumer group to pause. Defaults to the client's default groupId.
   * @param assignments Topic-partition pairs to pause.
   */
  public pauseConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void {
    const gid = groupId ?? this.defaultGroupId;
    const consumer = this.consumers.get(gid);
    if (!consumer) {
      this.logger.warn(`pauseConsumer: no active consumer for group "${gid}"`);
      return;
    }
    consumer.pause(
      assignments.flatMap(({ topic, partitions }) =>
        partitions.map((p) => ({ topic, partitions: [p] })),
      ),
    );
  }

  /**
   * Resume message delivery for previously paused topic-partitions.
   *
   * @param {string|undefined} groupId Consumer group to resume. Defaults to the client's default groupId.
   * @param {Array<{ topic: string; partitions: number[] }>} assignments Topic-partition pairs to resume.
   */
  public resumeConsumer(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partitions: number[] }>,
  ): void {
    const gid = groupId ?? this.defaultGroupId;
    const consumer = this.consumers.get(gid);
    if (!consumer) {
      this.logger.warn(`resumeConsumer: no active consumer for group "${gid}"`);
      return;
    }
    consumer.resume(
      assignments.flatMap(({ topic, partitions }) =>
        partitions.map((p) => ({ topic, partitions: [p] })),
      ),
    );
  }

  /** Pause all assigned partitions of a topic for a consumer group (used for queue backpressure). */
  private pauseTopicAllPartitions(gid: string, topic: string): void {
    const consumer = this.consumers.get(gid);
    if (!consumer) return;
    const assignment: Array<{ topic: string; partition: number }> =
      (consumer as any).assignment?.() ?? [];
    const partitions = assignment
      .filter((a) => a.topic === topic)
      .map((a) => a.partition);
    if (partitions.length > 0)
      consumer.pause(partitions.map((p) => ({ topic, partitions: [p] })));
  }

  /** Resume all assigned partitions of a topic for a consumer group (used for queue backpressure). */
  private resumeTopicAllPartitions(gid: string, topic: string): void {
    const consumer = this.consumers.get(gid);
    if (!consumer) return;
    const assignment: Array<{ topic: string; partition: number }> =
      (consumer as any).assignment?.() ?? [];
    const partitions = assignment
      .filter((a) => a.topic === topic)
      .map((a) => a.partition);
    if (partitions.length > 0)
      consumer.resume(partitions.map((p) => ({ topic, partitions: [p] })));
  }

  /** DLQ header keys added by `sendToDlq` — stripped before re-publishing. */
  private static readonly DLQ_HEADER_KEYS = new Set([
    "x-dlq-original-topic",
    "x-dlq-failed-at",
    "x-dlq-error-message",
    "x-dlq-error-stack",
    "x-dlq-attempt-count",
  ]);

  /**
   * Re-publish messages from a dead letter queue back to the original topic.
   *
   * Messages are consumed from `<topic>.dlq` and re-published to `<topic>`.
   * The original topic is determined by the `x-dlq-original-topic` header.
   * The `x-dlq-*` headers are stripped before re-publishing.
   *
   * @param topic - The topic to replay from `<topic>.dlq`
   * @param options - Options for replay
   * @returns { replayed: number; skipped: number } - counts of re-published vs skipped messages
   */
  public async replayDlq(
    topic: string,
    options: DlqReplayOptions = {},
  ): Promise<{ replayed: number; skipped: number }> {
    const dlqTopic = `${topic}.dlq`;
    await this.ensureAdminConnected();

    const partitionOffsets = await this.admin.fetchTopicOffsets(dlqTopic);
    const activePartitions = partitionOffsets.filter(
      (p) => parseInt(p.high, 10) > 0,
    );
    if (activePartitions.length === 0) {
      this.logger.log(`replayDlq: "${dlqTopic}" is empty — nothing to replay`);
      return { replayed: 0, skipped: 0 };
    }

    const highWatermarks = new Map(
      activePartitions.map(({ partition, high }) => [
        partition,
        parseInt(high, 10),
      ]),
    );
    const processedOffsets = new Map<number, number>();

    let replayed = 0;
    let skipped = 0;

    const tempGroupId = `${dlqTopic}-replay-${Date.now()}`;

    await new Promise<void>((resolve, reject) => {
      const consumer = getOrCreateConsumer(
        tempGroupId,
        true,
        true,
        this.consumerOpsDeps,
      );

      const cleanup = () => {
        consumer
          .disconnect()
          .catch(() => {})
          .finally(() => {
            this.consumers.delete(tempGroupId);
            this.runningConsumers.delete(tempGroupId);
            this.consumerCreationOptions.delete(tempGroupId);
          });
      };

      consumer
        .connect()
        .then(() => subscribeWithRetry(consumer, [dlqTopic], this.logger))
        .then(() =>
          consumer.run({
            eachMessage: async ({ partition, message }) => {
              if (!message.value) return;

              const offset = parseInt(message.offset, 10);
              processedOffsets.set(partition, offset);

              const headers = decodeHeaders(message.headers);
              const targetTopic =
                options.targetTopic ?? headers["x-dlq-original-topic"];
              const originalHeaders = Object.fromEntries(
                Object.entries(headers).filter(
                  ([k]) => !KafkaClient.DLQ_HEADER_KEYS.has(k),
                ),
              );
              const value = message.value.toString();
              const shouldProcess =
                !options.filter || options.filter(headers, value);

              if (!targetTopic || !shouldProcess) {
                skipped++;
              } else if (options.dryRun) {
                this.logger.log(
                  `[DLQ replay dry-run] Would replay to "${targetTopic}"`,
                );
                replayed++;
              } else {
                await this.producer.send({
                  topic: targetTopic,
                  messages: [{ value, headers: originalHeaders }],
                });
                replayed++;
              }

              // Stop when all active partitions have reached their high watermark
              const allDone = Array.from(highWatermarks.entries()).every(
                ([p, hwm]) => (processedOffsets.get(p) ?? -1) >= hwm - 1,
              );
              if (allDone) {
                cleanup();
                resolve();
              }
            },
          }),
        )
        .catch((err) => {
          cleanup();
          reject(err);
        });
    });

    this.logger.log(
      `replayDlq: replayed ${replayed}, skipped ${skipped} from "${dlqTopic}"`,
    );
    return { replayed, skipped };
  }

  public async resetOffsets(
    groupId: string | undefined,
    topic: string,
    position: "earliest" | "latest",
  ): Promise<void> {
    const gid = groupId ?? this.defaultGroupId;
    if (this.runningConsumers.has(gid)) {
      throw new Error(
        `resetOffsets: consumer group "${gid}" is still running. ` +
          `Call stopConsumer("${gid}") before resetting offsets.`,
      );
    }
    await this.ensureAdminConnected();
    const partitionOffsets = await this.admin.fetchTopicOffsets(topic);
    const partitions = partitionOffsets.map(({ partition, low, high }) => ({
      partition,
      offset: position === "earliest" ? low : high,
    }));
    await (this.admin as any).setOffsets({ groupId: gid, topic, partitions });
    this.logger.log(
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
    const gid = groupId ?? this.defaultGroupId;
    if (this.runningConsumers.has(gid)) {
      throw new Error(
        `seekToOffset: consumer group "${gid}" is still running. ` +
          `Call stopConsumer("${gid}") before seeking offsets.`,
      );
    }
    await this.ensureAdminConnected();
    const byTopic = new Map<
      string,
      Array<{ partition: number; offset: string }>
    >();
    for (const { topic, partition, offset } of assignments) {
      const list = byTopic.get(topic) ?? [];
      list.push({ partition, offset });
      byTopic.set(topic, list);
    }
    for (const [topic, partitions] of byTopic) {
      await (this.admin as any).setOffsets({ groupId: gid, topic, partitions });
      this.logger.log(
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
    const gid = groupId ?? this.defaultGroupId;
    if (this.runningConsumers.has(gid)) {
      throw new Error(
        `seekToTimestamp: consumer group "${gid}" is still running. ` +
          `Call stopConsumer("${gid}") before seeking offsets.`,
      );
    }
    await this.ensureAdminConnected();
    const byTopic = new Map<
      string,
      Array<{ partition: number; timestamp: number }>
    >();
    for (const { topic, partition, timestamp } of assignments) {
      const list = byTopic.get(topic) ?? [];
      list.push({ partition, timestamp });
      byTopic.set(topic, list);
    }
    for (const [topic, parts] of byTopic) {
      const offsets: Array<{ partition: number; offset: string }> =
        await Promise.all(
          parts.map(async ({ partition, timestamp }) => {
            const results = await (this.admin as any).fetchTopicOffsetsByTime(
              topic,
              timestamp,
            );
            const found = (results as Array<{ partition: number; offset: string }>).find(
              (r) => r.partition === partition,
            );
            return { partition, offset: found?.offset ?? "-1" };
          }),
        );
      await (this.admin as any).setOffsets({ groupId: gid, topic, partitions: offsets });
      this.logger.log(
        `Offsets set by timestamp for group "${gid}" on "${topic}": ${JSON.stringify(offsets)}`,
      );
    }
  }

  /**
   * Returns the current circuit breaker state for a specific topic partition.
   * Returns `undefined` when no circuit state exists — either `circuitBreaker` is not
   * configured for the group, or the circuit has never been tripped.
   *
   * @param topic Topic name.
   * @param partition Partition index.
   * @param groupId Consumer group. Defaults to the client's default groupId.
   *
   * @returns `{ status, failures, windowSize }` snapshot for a given partition or `undefined` if no state exists.
   */
  public getCircuitState(
    topic: string,
    partition: number,
    groupId?: string,
  ): { status: "closed" | "open" | "half-open"; failures: number; windowSize: number } | undefined {
    const gid = groupId ?? this.defaultGroupId;
    const state = this.circuitStates.get(`${gid}:${topic}:${partition}`);
    if (!state) return undefined;
    return {
      status: state.status,
      failures: state.window.filter((v) => !v).length,
      windowSize: state.window.length,
    };
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

  /**
   * List all consumer groups known to the broker.
   * Useful for monitoring which groups are active and their current state.
   */
  public async listConsumerGroups(): Promise<ConsumerGroupSummary[]> {
    await this.ensureAdminConnected();
    const result = await this.admin.listGroups();
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
    await this.ensureAdminConnected();
    const result = await (this.admin as any).fetchTopicMetadata(
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
    await this.ensureAdminConnected();
    await this.admin.deleteTopicRecords({ topic, partitions });
  }

  /** Return the client ID provided during `KafkaClient` construction. */
  public getClientId(): ClientId {
    return this.clientId;
  }

  /**
   * Return a snapshot of internal event counters accumulated since client creation
   * (or since the last `resetMetrics()` call).
   *
   * @param topic Topic name to scope the snapshot to. When omitted, counters are
   *   aggregated across all topics. If the topic has no recorded events yet, returns
   *   a zero-valued snapshot.
   * @returns Read-only `KafkaMetrics` snapshot: `processedCount`, `retryCount`, `dlqCount`, `dedupCount`.
   */
  public getMetrics(topic?: string): Readonly<KafkaMetrics> {
    if (topic !== undefined) {
      const m = this._topicMetrics.get(topic);
      return m
        ? { ...m }
        : { processedCount: 0, retryCount: 0, dlqCount: 0, dedupCount: 0 };
    }
    // Aggregate across all topics
    const agg: KafkaMetrics = {
      processedCount: 0,
      retryCount: 0,
      dlqCount: 0,
      dedupCount: 0,
    };
    for (const m of this._topicMetrics.values()) {
      agg.processedCount += m.processedCount;
      agg.retryCount += m.retryCount;
      agg.dlqCount += m.dlqCount;
      agg.dedupCount += m.dedupCount;
    }
    return agg;
  }

  /**
   * Reset internal event counters to zero.
   *
   * @param topic Topic name to reset. When omitted, all topics are reset.
   */
  public resetMetrics(topic?: string): void {
    if (topic !== undefined) {
      this._topicMetrics.delete(topic);
      return;
    }
    this._topicMetrics.clear();
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
    for (const state of this.circuitStates.values()) clearTimeout(state.timer);
    this.circuitStates.clear();
    this.circuitConfigs.clear();
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

  /**
   * Increment the in-flight handler count and return a promise that calls the given handler.
   * When the promise resolves or rejects, decrement the in flight handler count.
   * If the in flight handler count reaches 0, call all previously registered drain resolvers.
   * @param fn The handler to call when the promise is resolved or rejected.
   * @returns A promise that resolves or rejects with the result of calling the handler.
   */
  private trackInFlight<R>(fn: () => Promise<R>): Promise<R> {
    this.inFlightTotal++;
    return fn().finally(() => {
      this.inFlightTotal--;
      if (this.inFlightTotal === 0) {
        this.drainResolvers.splice(0).forEach((r) => r());
      }
    });
  }

  /**
   * Waits for all in-flight handlers to complete or for a given timeout, whichever comes first.
   * @param timeoutMs Maximum time to wait in milliseconds.
   * @returns A promise that resolves when all handlers have completed or the timeout is reached.
   * @private
   */
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

  /**
   * Prepare a send payload by registering the topic's schema and then building the payload.
   * @param topicOrDesc - topic name or topic descriptor
   * @param messages - batch of messages to send
   * @returns - prepared payload
   */
  private async preparePayload(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
    compression?: import("../types").CompressionType,
  ) {
    registerSchema(topicOrDesc, this.schemaRegistry, this.logger);
    const payload = await buildSendPayload(
      topicOrDesc,
      messages,
      this.producerOpsDeps,
      compression,
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

  /**
   * Returns the KafkaMetrics for a given topic.
   * If the topic hasn't seen any events, initializes a zero-valued snapshot.
   * @param topic - name of the topic to get the metrics for
   * @returns - KafkaMetrics for the given topic
   */
  private metricsFor(topic: string): KafkaMetrics {
    let m = this._topicMetrics.get(topic);
    if (!m) {
      m = { processedCount: 0, retryCount: 0, dlqCount: 0, dedupCount: 0 };
      this._topicMetrics.set(topic, m);
    }
    return m;
  }

/**
 * Notifies instrumentation hooks of a retry event.
 * @param envelope The original message envelope that triggered the retry.
 * @param attempt The current retry attempt (1-indexed).
 * @param maxRetries The maximum number of retries configured for this topic.
 */
  private notifyRetry(
    envelope: import("../message/envelope").EventEnvelope<any>,
    attempt: number,
    maxRetries: number,
  ): void {
    this.metricsFor(envelope.topic).retryCount++;
    for (const inst of this.instrumentation) {
      inst.onRetry?.(envelope, attempt, maxRetries);
    }
  }

/**
 * Called whenever a message is routed to the dead letter queue.
 * @param envelope The original message envelope.
 * @param reason The reason for routing to the dead letter queue.
 * @param gid The group ID of the consumer that triggered the circuit breaker, if any.
 */
  private notifyDlq(
    envelope: import("../message/envelope").EventEnvelope<any>,
    reason: DlqReason,
    gid?: string,
  ): void {
    this.metricsFor(envelope.topic).dlqCount++;
    for (const inst of this.instrumentation) {
      inst.onDlq?.(envelope, reason);
    }

    if (!gid) return;
    const cfg = this.circuitConfigs.get(gid);
    if (!cfg) return;

    const threshold = cfg.threshold ?? 5;
    const recoveryMs = cfg.recoveryMs ?? 30_000;

    const stateKey = `${gid}:${envelope.topic}:${envelope.partition}`;
    let state = this.circuitStates.get(stateKey);
    if (!state) {
      state = { status: "closed", window: [], successes: 0 };
      this.circuitStates.set(stateKey, state);
    }
    if (state.status === "open") return; // already tripped — skip window update

    const openCircuit = () => {
      state!.status = "open";
      state!.window = [];
      state!.successes = 0;
      clearTimeout(state!.timer);
      for (const inst of this.instrumentation)
        inst.onCircuitOpen?.(envelope.topic, envelope.partition);
      this.pauseConsumer(gid, [
        { topic: envelope.topic, partitions: [envelope.partition] },
      ]);
      state!.timer = setTimeout(() => {
        state!.status = "half-open";
        state!.successes = 0;
        this.logger.log(
          `[CircuitBreaker] HALF-OPEN — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition}`,
        );
        for (const inst of this.instrumentation)
          inst.onCircuitHalfOpen?.(envelope.topic, envelope.partition);
        this.resumeConsumer(gid, [
          { topic: envelope.topic, partitions: [envelope.partition] },
        ]);
      }, recoveryMs);
    };

    if (state.status === "half-open") {
      // Any failure in half-open immediately re-opens the circuit.
      clearTimeout(state.timer);
      this.logger.warn(
        `[CircuitBreaker] OPEN (half-open failure) — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition}`,
      );
      openCircuit();
      return;
    }

    // CLOSED: update sliding window
    const windowSize = cfg.windowSize ?? Math.max(threshold * 2, 10);
    state.window = [...state.window, false];
    if (state.window.length > windowSize) {
      state.window = state.window.slice(state.window.length - windowSize);
    }
    const failures = state.window.filter((v) => !v).length;

    if (failures >= threshold) {
      this.logger.warn(
        `[CircuitBreaker] OPEN — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition} ` +
          `(${failures}/${state.window.length} failures, threshold=${threshold})`,
      );
      openCircuit();
    }
  }

  /**
   * Notify all instrumentation hooks about a duplicate message detection.
   * Invoked by the consumer after a message has been successfully processed
   * and the Lamport clock detected a duplicate.
   * @param envelope The processed message envelope.
   * @param strategy The duplicate detection strategy used.
   */
  private notifyDuplicate(
    envelope: import("../message/envelope").EventEnvelope<any>,
    strategy: "drop" | "dlq" | "topic",
  ): void {
    this.metricsFor(envelope.topic).dedupCount++;
    for (const inst of this.instrumentation) {
      inst.onDuplicate?.(envelope, strategy);
    }
  }

  /**
   * Notify all instrumentation hooks about a successfully processed message.
   * Invoked by the consumer after a message has been successfully processed
   * by the handler.
   * @param envelope The processed message envelope.
   * @param gid The optional consumer group ID.
   */
  private notifyMessage(
    envelope: import("../message/envelope").EventEnvelope<any>,
    gid?: string,
  ): void {
    this.metricsFor(envelope.topic).processedCount++;
    for (const inst of this.instrumentation) {
      inst.onMessage?.(envelope);
    }

    if (!gid) return;
    const cfg = this.circuitConfigs.get(gid);
    if (!cfg) return;

    const stateKey = `${gid}:${envelope.topic}:${envelope.partition}`;
    const state = this.circuitStates.get(stateKey);
    if (!state) return;

    const halfOpenSuccesses = cfg.halfOpenSuccesses ?? 1;

    if (state.status === "half-open") {
      state.successes++;
      if (state.successes >= halfOpenSuccesses) {
        clearTimeout(state.timer);
        state.timer = undefined;
        state.status = "closed";
        state.window = [];
        state.successes = 0;
        this.logger.log(
          `[CircuitBreaker] CLOSED — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition}`,
        );
        for (const inst of this.instrumentation)
          inst.onCircuitClose?.(envelope.topic, envelope.partition);
      }
    } else if (state.status === "closed") {
      const threshold = cfg.threshold ?? 5;
      const windowSize = cfg.windowSize ?? Math.max(threshold * 2, 10);
      state.window = [...state.window, true];
      if (state.window.length > windowSize) {
        state.window = state.window.slice(state.window.length - windowSize);
      }
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

  /**
   * Ensure that a topic exists by creating it if it doesn't already exist.
   * If `autoCreateTopics` is disabled, this method will not create the topic and
   * will return immediately.
   * If multiple concurrent calls are made to `ensureTopic` for the same topic,
   * they are deduplicated to prevent multiple calls to `admin.createTopics()`.
   * @param topic - The topic to ensure exists.
   * @returns A promise that resolves when the topic has been created or already exists.
   */
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

    // Separate string topic names from regex patterns.
    // Schema registration, topic-ensure, and retry chains only apply to string topics.
    const stringTopics: any[] = topics.filter((t) => !(t instanceof RegExp));
    const regexTopics: RegExp[] = topics.filter((t) => t instanceof RegExp);
    const hasRegex = regexTopics.length > 0;

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
      options.partitionAssigner,
    );

    // Only build schema map for string topics (regex patterns have no fixed name to look up).
    const schemaMap = buildSchemaMap(
      stringTopics,
      this.schemaRegistry,
      optionSchemas,
      this.logger,
    );

    const topicNames = stringTopics.map((t: any) => resolveTopicName(t));
    // All topics (strings + regex) passed to subscribe.
    const subscribeTopics: (string | RegExp)[] = [
      ...topicNames,
      ...regexTopics,
    ];

    // Ensure topics exist before subscribing — librdkafka errors on unknown topics.
    // Regex patterns are skipped (we can't create or validate a pattern).
    for (const t of topicNames) {
      await this.ensureTopic(t);
    }
    if (dlq) {
      for (const t of topicNames) {
        await this.ensureTopic(`${t}.dlq`);
      }
      if (!this.autoCreateTopicsEnabled && topicNames.length > 0) {
        await this.validateDlqTopicsExist(topicNames);
      }
    }

    if (options.deduplication?.strategy === "topic") {
      const dest = options.deduplication.duplicatesTopic;
      if (this.autoCreateTopicsEnabled) {
        for (const t of topicNames) {
          await this.ensureTopic(dest ?? `${t}.duplicates`);
        }
      } else if (topicNames.length > 0) {
        await this.validateDuplicatesTopicsExist(topicNames, dest);
      }
    }

    await consumer.connect();
    await subscribeWithRetry(
      consumer,
      subscribeTopics,
      this.logger,
      options.subscribeRetry,
    );

    const displayTopics = subscribeTopics
      .map((t) => (t instanceof RegExp ? t.toString() : t))
      .join(", ");
    this.logger.log(
      `${mode === "eachBatch" ? "Batch consumer" : "Consumer"} subscribed to topics: ${displayTopics}`,
    );

    return { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry, hasRegex };
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

  /**
   * An object containing the necessary dependencies for building a send payload.
   *
   * @property {Map<string, SchemaLike>} schemaRegistry - A map of topic names to their schemas.
   * @property {boolean} strictSchemasEnabled - Whether strict schema validation is enabled.
   * @property {KafkaInstrumentation} instrumentation - An object for creating a span for instrumentation.
   * @property {KafkaLogger} logger - A logger for logging messages.
   * @property {() => number} nextLamportClock - A function that returns the next value of the logical clock.
   */
  private get producerOpsDeps(): BuildSendPayloadDeps {
    return {
      schemaRegistry: this.schemaRegistry,
      strictSchemasEnabled: this.strictSchemasEnabled,
      instrumentation: this.instrumentation,
      logger: this.logger,
      nextLamportClock: () => ++this._lamportClock,
    };
  }

  /**
   * ConsumerOpsDeps object properties:
   *
   * @property {Map<string, Consumer>} consumers - A map of consumer group IDs to their corresponding consumer instances.
   * @property {Map<string, { fromBeginning: boolean; autoCommit: boolean }>} consumerCreationOptions - A map of consumer group IDs to their creation options.
   * @property {Kafka} kafka - The Kafka client instance.
   * @property {function(string, Partition[]): void} onRebalance - An optional callback function called when a consumer group is rebalanced.
   * @property {KafkaLogger} logger - The logger instance used for logging consumer operations.
   */
  private get consumerOpsDeps(): ConsumerOpsDeps {
    return {
      consumers: this.consumers,
      consumerCreationOptions: this.consumerCreationOptions,
      kafka: this.kafka,
      onRebalance: this.onRebalance,
      logger: this.logger,
    };
  }

  /** Build MessageHandlerDeps with circuit breaker callbacks bound to the given groupId. */
  private messageDepsFor(gid: string): MessageHandlerDeps {
    return {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
      onTtlExpired: this.onTtlExpired,
      onRetry: this.notifyRetry.bind(this),
      onDlq: (envelope, reason) => this.notifyDlq(envelope, reason, gid),
      onDuplicate: this.notifyDuplicate.bind(this),
      onMessage: (envelope) => this.notifyMessage(envelope, gid),
    };
  }

  /**
   * The dependencies object passed to the retry topic consumers.
   *
   * `logger`: The logger instance passed to the retry topic consumers.
   * `producer`: The producer instance passed to the retry topic consumers.
   * `instrumentation`: The instrumentation instance passed to the retry topic consumers.
   * `onMessageLost`: The callback function passed to the retry topic consumers for tracking lost messages.
   * `onRetry`: The callback function passed to the retry topic consumers for tracking retry attempts.
   * `onDlq`: The callback function passed to the retry topic consumers for tracking dead-letter queue routing.
   * `onMessage`: The callback function passed to the retry topic consumers for tracking message delivery.
   * `ensureTopic`: A function that ensures a topic exists before subscribing to it.
   * `getOrCreateConsumer`: A function that creates or retrieves a consumer instance.
   * `runningConsumers`: A map of consumer group IDs to their corresponding consumer instances.
   * `createRetryTxProducer`: A function that creates a retry transactional producer instance.
   */
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
