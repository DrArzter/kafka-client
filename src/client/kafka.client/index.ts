import { KafkaJS } from "@confluentinc/kafka-javascript";
type Kafka = KafkaJS.Kafka;
type Producer = KafkaJS.Producer;
type Consumer = KafkaJS.Consumer;
const { Kafka: KafkaClass, logLevel: KafkaLogLevel } = KafkaJS;
import { TopicDescriptor, SchemaLike } from "../message/topic";
import type { EventEnvelope } from "../message/envelope";
import {
  HEADER_LAMPORT_CLOCK,
  decodeHeaders,
  extractEnvelope,
  runWithEnvelopeContext,
} from "../message/envelope";
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
  BatchMeta,
  DlqReplayOptions,
  RetryOptions,
  ConsumerInterceptor,
  ConsumerGroupSummary,
  TopicDescription,
  MessageHeaders,
  ReadSnapshotOptions,
  CheckpointResult,
  CheckpointRestoreResult,
  RestoreCheckpointOptions,
  CheckpointEntry,
  WindowConsumerOptions,
  WindowMeta,
  RoutingOptions,
  TransactionalHandlerContext,
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
} from "./producer/ops";
import {
  getOrCreateConsumer,
  buildSchemaMap,
  type ConsumerOpsDeps,
} from "./consumer/ops";
import {
  handleEachMessage,
  handleEachBatch,
  parseSingleMessage,
} from "./consumer/handler";
import type {
  MessageHandlerDeps,
  DeduplicationContext,
} from "./consumer/handler";
import { startRetryTopicConsumers } from "./consumer/retry-topic";
import { subscribeWithRetry } from "./consumer/subscribe-retry";
import { toError } from "./consumer/pipeline";
import { AsyncQueue } from "./consumer/queue";
import { CircuitBreakerManager } from "./infra/circuit-breaker";
import { AdminOps } from "./admin/ops";
import { replayDlqTopic } from "./consumer/dlq-replay";
import { MetricsManager } from "./infra/metrics-manager";
import { InFlightTracker } from "./infra/inflight-tracker";

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
  /** Monotonically increasing Lamport clock stamped on every outgoing message. */
  private _lamportClock = 0;
  /** Topics to scan for the highest Lamport clock value on `connectProducer()`. */
  private readonly clockRecoveryTopics: string[];
  /** Lag-throttle configuration — set when `lagThrottle` is configured. */
  private readonly lagThrottleOpts: KafkaClientOptions["lagThrottle"];
  /** `true` while the observed consumer group lag exceeds `lagThrottle.maxLag`. */
  private _lagThrottled = false;
  /** Background polling timer for lag throttle. */
  private _lagThrottleTimer: ReturnType<typeof setInterval> | undefined;
  /** Per-groupId deduplication state: `"topic:partition"` → last processed clock. */
  private readonly dedupStates = new Map<string, Map<string, number>>();

  private readonly circuitBreaker: CircuitBreakerManager;
  private readonly adminOps: AdminOps;
  private readonly metrics: MetricsManager;
  private readonly inFlight: InFlightTracker;
  public readonly clientId: ClientId;

  private readonly _producerOpsDeps: BuildSendPayloadDeps;
  private readonly _consumerOpsDeps: ConsumerOpsDeps;
  private readonly _retryTopicDeps: ReturnType<
    KafkaClient<T>["buildRetryTopicDeps"]
  >;

  /** DLQ header keys added by the pipeline — stripped before re-publishing. */
  private static readonly DLQ_HEADER_KEYS = new Set([
    "x-dlq-original-topic",
    "x-dlq-failed-at",
    "x-dlq-error-message",
    "x-dlq-error-stack",
    "x-dlq-attempt-count",
  ]);

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
    this.clockRecoveryTopics = options?.clockRecovery?.topics ?? [];
    this.lagThrottleOpts = options?.lagThrottle;

    this.kafka = new KafkaClass({
      kafkaJS: {
        clientId: this.clientId,
        brokers,
        logLevel: KafkaLogLevel.ERROR,
      },
    });
    this.producer = this.kafka.producer({ kafkaJS: { acks: -1 } });

    this.adminOps = new AdminOps({
      admin: this.kafka.admin(),
      logger: this.logger,
      runningConsumers: this.runningConsumers,
      defaultGroupId: this.defaultGroupId,
      clientId: this.clientId,
    });

    this.circuitBreaker = new CircuitBreakerManager({
      pauseConsumer: (gid, assignments) => this.pauseConsumer(gid, assignments),
      resumeConsumer: (gid, assignments) =>
        this.resumeConsumer(gid, assignments),
      logger: this.logger,
      instrumentation: this.instrumentation,
    });

    this.metrics = new MetricsManager({
      instrumentation: this.instrumentation,
      onCircuitFailure: (envelope, gid) =>
        this.circuitBreaker.onFailure(envelope, gid),
      onCircuitSuccess: (envelope, gid) =>
        this.circuitBreaker.onSuccess(envelope, gid),
    });

    this.inFlight = new InFlightTracker((msg) => this.logger.warn(msg));

    this._producerOpsDeps = {
      schemaRegistry: this.schemaRegistry,
      strictSchemasEnabled: this.strictSchemasEnabled,
      instrumentation: this.instrumentation,
      logger: this.logger,
      nextLamportClock: () => ++this._lamportClock,
    };

    this._consumerOpsDeps = {
      consumers: this.consumers,
      consumerCreationOptions: this.consumerCreationOptions,
      kafka: this.kafka,
      onRebalance: this.onRebalance,
      logger: this.logger,
    };

    this._retryTopicDeps = this.buildRetryTopicDeps();
  }

  // ── Send ─────────────────────────────────────────────────────────

  /**
   * Send a single typed message. Accepts a topic key or a `TopicDescriptor`.
   *
   * @param topic Topic key from the `TopicMessageMap` or a `TopicDescriptor` object.
   * @param message Message payload — validated against the topic schema when one is registered.
   * @param options Optional per-send settings: `key`, `headers`, `correlationId`, `compression`, etc.
   * @example
   * ```ts
   * await kafka.sendMessage('orders.created', { orderId: '123', amount: 99 });
   * ```
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
    await this.waitIfThrottled();
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
    this.metrics.notifyAfterSend(payload.topic, payload.messages.length);
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
   * @example
   * ```ts
   * await kafka.sendTombstone('users.state', 'user-42');
   * ```
   */
  public async sendTombstone(
    topic: string,
    key: string,
    headers?: MessageHeaders,
  ): Promise<void> {
    await this.waitIfThrottled();
    const hdrs: MessageHeaders = { ...headers };
    for (const inst of this.instrumentation) inst.beforeSend?.(topic, hdrs);
    await this.ensureTopic(topic);
    await this.producer.send({
      topic,
      messages: [{ value: null, key, headers: hdrs }],
    });
    for (const inst of this.instrumentation) inst.afterSend?.(topic);
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
   * @example
   * ```ts
   * await kafka.sendBatch('orders.created', [
   *   { value: { orderId: '1', amount: 10 }, key: 'order-1' },
   * ]);
   * ```
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
    await this.waitIfThrottled();
    const payload = await this.preparePayload(
      topicOrDesc,
      messages,
      options?.compression,
    );
    await this.producer.send(payload);
    this.metrics.notifyAfterSend(payload.topic, payload.messages.length);
  }

  /**
   * Execute multiple sends atomically. Commits on success, aborts on error.
   * @example
   * ```ts
   * await kafka.transaction(async (tx) => {
   *   await tx.send('orders.created', { orderId: '123' });
   *   await tx.send('inventory.reserved', { itemId: 'a', qty: 1 });
   * });
   * ```
   */
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
          this.metrics.notifyAfterSend(payload.topic, payload.messages.length);
        },
        sendBatch: async (
          topicOrDesc: any,
          messages: BatchMessageItem<any>[],
        ) => {
          const payload = await this.preparePayload(topicOrDesc, messages);
          await tx.send(payload);
          this.metrics.notifyAfterSend(payload.topic, payload.messages.length);
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
    await this.recoverLamportClock(this.clockRecoveryTopics);
    if (this.lagThrottleOpts) this.startLagThrottlePoller();
    this.logger.log("Producer connected");
  }

  /** Start the background lag-polling loop for producer throttling. */
  private startLagThrottlePoller(): void {
    const opts = this.lagThrottleOpts!;
    const { maxLag, pollIntervalMs = 5_000 } = opts;
    const groupId = opts.groupId; // undefined → default group

    const poll = async () => {
      try {
        const lags = await this.getConsumerLag(groupId);
        const total = lags.reduce((sum, e) => sum + e.lag, 0);
        if (total > maxLag && !this._lagThrottled) {
          this._lagThrottled = true;
          this.logger.warn(
            `lagThrottle: lag ${total} > ${maxLag} — producer sends will be delayed`,
          );
        } else if (total <= maxLag && this._lagThrottled) {
          this._lagThrottled = false;
          this.logger.log(
            `lagThrottle: lag ${total} ≤ ${maxLag} — producer sends resumed`,
          );
        }
      } catch {
        // Poll errors do not block sends — silently ignore
      }
    };

    this._lagThrottleTimer = setInterval(() => {
      void poll();
    }, pollIntervalMs);

    // Don't prevent clean process exit when no other work is pending
    this._lagThrottleTimer.unref?.();
  }

  /** Wait until lag drops below the threshold (or maxWaitMs is exceeded). */
  private async waitIfThrottled(): Promise<void> {
    if (!this._lagThrottled) return;
    const maxWait = this.lagThrottleOpts?.maxWaitMs ?? 30_000;
    const start = Date.now();
    while (this._lagThrottled) {
      if (Date.now() - start >= maxWait) {
        this.logger.warn(
          `lagThrottle: maxWaitMs (${maxWait} ms) exceeded — sending anyway`,
        );
        return;
      }
      await new Promise<void>((r) => setTimeout(r, 100));
    }
  }

  /**
   * Recover the Lamport clock from the last message across the given topics.
   *
   * For each topic, fetches partition high-watermarks via admin, creates a
   * short-lived consumer, seeks every non-empty partition to its last offset
   * (`highWatermark − 1`), reads one message per partition, and extracts the
   * maximum `x-lamport-clock` header value. On completion `_lamportClock` is
   * set to that maximum so the next `++_lamportClock` yields a strictly greater
   * value than any previously sent clock.
   *
   * Topics that are empty or missing are silently skipped.
   */
  private async recoverLamportClock(topics: string[]): Promise<void> {
    if (topics.length === 0) return;

    this.logger.log(
      `Clock recovery: scanning ${topics.length} topic(s) for Lamport clock...`,
    );
    await this.adminOps.ensureConnected();

    // Collect non-empty (topic, partition, lastOffset) tuples
    const partitionsToRead: Array<{
      topic: string;
      partition: number;
      lastOffset: string;
    }> = [];
    for (const t of topics) {
      let offsets: Array<{ partition: number; low: string; high: string }>;
      try {
        offsets = await this.adminOps.admin.fetchTopicOffsets(t);
      } catch {
        this.logger.warn(
          `Clock recovery: could not fetch offsets for "${t}", skipping`,
        );
        continue;
      }
      for (const { partition, high, low } of offsets) {
        if (Number.parseInt(high, 10) > Number.parseInt(low, 10)) {
          partitionsToRead.push({
            topic: t,
            partition,
            lastOffset: String(Number.parseInt(high, 10) - 1),
          });
        }
      }
    }

    if (partitionsToRead.length === 0) {
      this.logger.log(
        "Clock recovery: all topics empty — keeping Lamport clock at 0",
      );
      return;
    }

    const recoveryGroupId = `${this.clientId}-clock-recovery-${Date.now()}`;
    let maxClock = -1;

    await new Promise<void>((resolve, reject) => {
      const consumer = this.kafka.consumer({
        kafkaJS: { groupId: recoveryGroupId },
      });
      const remaining = new Set(
        partitionsToRead.map((p) => `${p.topic}:${p.partition}`),
      );

      const cleanup = () => {
        consumer.disconnect().catch(() => {});
      };

      consumer
        .connect()
        .then(async () => {
          const uniqueTopics = [
            ...new Set(partitionsToRead.map((p) => p.topic)),
          ];
          await consumer.subscribe({ topics: uniqueTopics });
          for (const { topic: t, partition, lastOffset } of partitionsToRead) {
            consumer.seek({ topic: t, partition, offset: lastOffset });
          }
        })
        .then(() =>
          consumer.run({
            eachMessage: async ({ topic: t, partition, message }) => {
              const key = `${t}:${partition}`;
              if (!remaining.has(key)) return;
              remaining.delete(key);

              const clockHeader = message.headers?.[HEADER_LAMPORT_CLOCK];
              if (clockHeader !== undefined) {
                const raw = Buffer.isBuffer(clockHeader)
                  ? clockHeader.toString()
                  : String(clockHeader);
                const clock = Number(raw);
                if (!Number.isNaN(clock) && clock > maxClock) maxClock = clock;
              }

              if (remaining.size === 0) {
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

    if (maxClock >= 0) {
      this._lamportClock = maxClock;
      this.logger.log(
        `Clock recovery: Lamport clock restored — next clock will be ${maxClock + 1}`,
      );
    } else {
      this.logger.log(
        "Clock recovery: no x-lamport-clock headers found — keeping clock at 0",
      );
    }
  }

  /**
   * @internal Not part of `IKafkaClient` — use `disconnect()` for full teardown.
   */
  public async disconnectProducer(): Promise<void> {
    if (this._lagThrottleTimer) {
      clearInterval(this._lagThrottleTimer);
      this._lagThrottleTimer = undefined;
    }
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
   * @example
   * ```ts
   * const handle = await kafka.startConsumer(['orders.created'], async (envelope) => {
   *   await processOrder(envelope.payload);
   * }, { retry: { maxRetries: 3 }, dlq: true });
   * await handle.stop();
   * ```
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
    this.validateTopicConsumerOpts(topics, options);
    const setupOptions = options.retryTopics
      ? { ...options, autoCommit: false as const }
      : options;
    const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachMessage", setupOptions);

    if (options.circuitBreaker)
      this.circuitBreaker.setConfig(gid, options.circuitBreaker);
    const deps = this.messageDepsFor(gid);
    const eosMainContext = await this.makeEosMainContext(
      gid,
      consumer,
      options,
    );

    await consumer.run({
      eachMessage: (payload) =>
        this.inFlight.track(() =>
          handleEachMessage(
            payload,
            {
              schemaMap,
              handleMessage,
              interceptors,
              dlq,
              retry,
              retryTopics: options.retryTopics,
              timeoutMs: options.handlerTimeoutMs,
              wrapWithTimeout: this.wrapWithTimeoutWarning.bind(this),
              deduplication: this.resolveDeduplicationContext(
                gid,
                options.deduplication,
              ),
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
      await this.launchRetryChain(gid, topicNames, handleMessage, {
        retry,
        dlq,
        interceptors,
        schemaMap,
        assignmentTimeoutMs: options.retryTopicAssignmentTimeoutMs,
      });
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
   * @example
   * ```ts
   * await kafka.startBatchConsumer(['metrics'], async (envelopes, meta) => {
   *   await db.insertMany(envelopes.map(e => e.payload));
   *   meta.resolveOffset(envelopes.at(-1)!.offset);
   * }, { autoCommit: false });
   * ```
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
    this.validateTopicConsumerOpts(topics, options);
    if (!options.retryTopics && options.autoCommit !== false) {
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
      this.circuitBreaker.setConfig(gid, options.circuitBreaker);
    const deps = this.messageDepsFor(gid);
    const eosMainContext = await this.makeEosMainContext(
      gid,
      consumer,
      options,
    );

    await consumer.run({
      eachBatch: (payload) =>
        this.inFlight.track(() =>
          handleEachBatch(
            payload,
            {
              schemaMap,
              handleBatch,
              interceptors,
              dlq,
              retry,
              retryTopics: options.retryTopics,
              timeoutMs: options.handlerTimeoutMs,
              wrapWithTimeout: this.wrapWithTimeoutWarning.bind(this),
              deduplication: this.resolveDeduplicationContext(
                gid,
                options.deduplication,
              ),
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
      // Retry consumers use eachMessage — wrap the batch handler for single-message delivery.
      // `highWatermark` is null (broker HWM not available); handlers must guard before lag calculations.
      const handleMessageForRetry = (env: EventEnvelope<any>) =>
        handleBatch([env], {
          partition: env.partition,
          highWatermark: null,
          heartbeat: async () => {},
          resolveOffset: () => {},
          commitOffsetsIfNecessary: async () => {},
        });
      await this.launchRetryChain(gid, topicNames, handleMessageForRetry, {
        retry,
        dlq,
        interceptors,
        schemaMap,
        assignmentTimeoutMs: options.retryTopicAssignmentTimeoutMs,
      });
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

  // ── Windowed batch consumer ──────────────────────────────────────

  /**
   * Accumulate messages into a window and flush the handler when either
   * `maxMessages` is reached or `maxMs` has elapsed — whichever fires first.
   * Remaining messages are flushed before the consumer disconnects on `stop()`.
   * @example
   * ```ts
   * await kafka.startWindowConsumer('events', async (batch, meta) => {
   *   await db.insertMany(batch.map(e => e.payload));
   * }, { maxMessages: 100, maxMs: 5_000 });
   * ```
   */
  public async startWindowConsumer<K extends keyof T & string>(
    topic: K,
    handler: (
      envelopes: EventEnvelope<T[K]>[],
      meta: WindowMeta,
    ) => Promise<void>,
    options: WindowConsumerOptions<T>,
  ): Promise<ConsumerHandle> {
    const { maxMessages, maxMs, ...consumerOptions } = options;

    if (maxMessages <= 0)
      throw new Error("startWindowConsumer: maxMessages must be > 0");
    if (maxMs <= 0) throw new Error("startWindowConsumer: maxMs must be > 0");
    if ((consumerOptions as ConsumerOptions<T>).retryTopics) {
      throw new Error(
        "startWindowConsumer() does not support retryTopics. " +
          "Use startConsumer() with retryTopics: true for guaranteed retry delivery.",
      );
    }

    const buffer: EventEnvelope<T[K]>[] = [];
    let flushTimer: ReturnType<typeof setTimeout> | null = null;
    let windowStart = 0;

    const flush = async (trigger: "size" | "time"): Promise<void> => {
      if (flushTimer !== null) {
        clearTimeout(flushTimer);
        flushTimer = null;
      }
      if (buffer.length === 0) return;
      const envelopes = buffer.splice(0);
      const windowEnd = Date.now();
      await handler(envelopes, { trigger, windowStart, windowEnd });
    };

    const scheduleFlush = (): void => {
      if (flushTimer !== null) return;
      flushTimer = setTimeout(() => {
        flushTimer = null;
        flush("time").catch((err) => {
          this.logger.warn(
            `startWindowConsumer: time-triggered flush error — ${toError(err).message}`,
          );
        });
      }, maxMs);
    };

    const handle = await this.startConsumer(
      [topic as any],
      async (envelope) => {
        if (buffer.length === 0) windowStart = Date.now();
        buffer.push(envelope as EventEnvelope<T[K]>);
        scheduleFlush();
        if (buffer.length >= maxMessages) {
          await flush("size");
        }
      },
      consumerOptions as ConsumerOptions<T>,
    );

    const originalStop = handle.stop.bind(handle);
    handle.stop = async (): Promise<void> => {
      if (flushTimer !== null) {
        clearTimeout(flushTimer);
        flushTimer = null;
      }
      if (buffer.length > 0) {
        const envelopes = buffer.splice(0);
        await handler(envelopes, {
          trigger: "time",
          windowStart,
          windowEnd: Date.now(),
        }).catch((err) => {
          this.logger.warn(
            `startWindowConsumer: shutdown flush error — ${toError(err).message}`,
          );
        });
      }
      return originalStop();
    };

    return handle;
  }

  // ── Consumer: header routing ─────────────────────────────────────

  /**
   * Subscribe to topics and dispatch each message to a handler based on the value
   * of a specific Kafka header. A thin, zero-overhead wrapper over `startConsumer`.
   *
   * All `ConsumerOptions` (retry, DLQ, deduplication, circuit breaker, etc.) apply
   * uniformly across every route.
   * @example
   * ```ts
   * await kafka.startRoutedConsumer(['domain.events'], {
   *   header: 'x-event-type',
   *   routes: {
   *     'order.created': async (e) => handleOrderCreated(e.payload),
   *   },
   * });
   * ```
   */
  public async startRoutedConsumer<K extends Array<keyof T>>(
    topics: K,
    routing: RoutingOptions<T[K[number]]>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle> {
    const { header, routes, fallback } = routing;

    const handleMessage = async (
      envelope: EventEnvelope<T[K[number]]>,
    ): Promise<void> => {
      const headerValue = envelope.headers[header];
      const handler =
        headerValue !== undefined ? routes[headerValue] : undefined;
      if (handler) {
        await handler(envelope);
      } else {
        await fallback?.(envelope);
      }
    };

    return this.startConsumer(topics, handleMessage, options);
  }

  // ── Consumer: transactional EOS ──────────────────────────────────

  /**
   * Subscribe to topics and consume messages with exactly-once semantics for
   * read-process-write pipelines.
   *
   * Each message is handled inside a dedicated Kafka transaction.
   * The handler receives a `TransactionalHandlerContext` whose `send` / `sendBatch`
   * methods stage outgoing messages inside that transaction. On handler success the
   * source offset commit and all staged sends are committed atomically. On handler
   * failure the transaction is aborted and the source message is redelivered — no
   * partial writes become visible to downstream consumers.
   *
   * Incompatible with `retryTopics: true` — throws at startup if set.
   * @example
   * ```ts
   * await kafka.startTransactionalConsumer(['orders.created'], async (envelope, tx) => {
   *   await tx.send('inventory.reserved', { orderId: envelope.payload.orderId, qty: 1 });
   * });
   * ```
   */
  public async startTransactionalConsumer<K extends Array<keyof T>>(
    topics: K,
    handler: (
      envelope: EventEnvelope<T[K[number]]>,
      tx: TransactionalHandlerContext<T>,
    ) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<ConsumerHandle> {
    if (options.retryTopics) {
      throw new Error(
        "startTransactionalConsumer: retryTopics is not supported. " +
          "EOS is already guaranteed by the transaction — redelivery on failure is handled automatically.",
      );
    }

    const setupOptions = { ...options, autoCommit: false as const };
    const { consumer, schemaMap, gid } = await this.setupConsumer(
      topics,
      "eachMessage",
      setupOptions,
    );

    const txProducer = await this.createRetryTxProducer(`${gid}-txc`);
    const deps = this.messageDepsFor(gid);

    await consumer.run({
      eachMessage: ({ topic, partition, message }) =>
        this.inFlight.track(async () => {
          const envelope = await parseSingleMessage(
            message,
            topic,
            partition,
            schemaMap,
            options.interceptors ?? [],
            false, // no DLQ inside the tx handler
            deps,
          );

          const nextOffset = String(Number.parseInt(message.offset, 10) + 1);

          if (envelope === null) {
            // Invalid/empty message — commit and skip; no tx needed
            await consumer.commitOffsets([
              { topic, partition, offset: nextOffset },
            ]);
            return;
          }

          const tx = await txProducer.transaction();

          const txCtx: TransactionalHandlerContext<T> = {
            send: async (t: any, msg: any, sendOpts?: SendOptions) => {
              const payload = await this.preparePayload(
                t,
                [
                  {
                    value: msg,
                    key: sendOpts?.key,
                    headers: sendOpts?.headers,
                    correlationId: sendOpts?.correlationId,
                    schemaVersion: sendOpts?.schemaVersion,
                    eventId: sendOpts?.eventId,
                  },
                ],
                sendOpts?.compression,
              );
              await tx.send(payload);
            },
            sendBatch: async (
              t: any,
              msgs: any[],
              batchOpts?: BatchSendOptions,
            ) => {
              const payload = await this.preparePayload(
                t,
                msgs,
                batchOpts?.compression,
              );
              await tx.send(payload);
            },
          };

          try {
            await runWithEnvelopeContext(
              {
                correlationId: envelope.correlationId,
                traceparent: envelope.traceparent,
              },
              () => handler(envelope, txCtx),
            );
            await tx.sendOffsets({
              consumer,
              topics: [
                { topic, partitions: [{ partition, offset: nextOffset }] },
              ],
            });
            await tx.commit();
            deps.onMessage?.(envelope);
          } catch (err) {
            try {
              await tx.abort();
            } catch {}
            this.logger.warn(
              `startTransactionalConsumer: handler failed on ${topic}[${partition}]@${message.offset} — ` +
                `tx aborted, message will be redelivered (${toError(err).message})`,
            );
            throw err;
          }
        }),
    });

    this.runningConsumers.set(gid, "eachMessage");
    return { groupId: gid, stop: () => this.stopConsumer(gid) };
  }

  // ── Consumer lifecycle ───────────────────────────────────────────

  /**
   * Stop all consumers or a specific group.
   *
   * If `groupId` is unspecified, all active consumers are stopped.
   * If `groupId` is specified, only the consumer with that group ID is stopped.
   *
   * @throws {Error} if the consumer fails to disconnect.
   * @example
   * ```ts
   * await kafka.stopConsumer('billing-service'); // stop one group
   * await kafka.stopConsumer();                  // stop all
   * ```
   */
  public async stopConsumer(groupId?: string): Promise<void> {
    if (groupId === undefined) {
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
      this.circuitBreaker.clear();
      this.logger.log("All consumers disconnected");
      return;
    }

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
    this.circuitBreaker.removeGroup(groupId);
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
  }

  /** @inheritDoc */
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

  /** @inheritDoc */
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
   * @example
   * ```ts
   * const { replayed, skipped } = await kafka.replayDlq('orders.created');
   * ```
   */
  public async replayDlq(
    topic: string,
    options: DlqReplayOptions = {},
  ): Promise<{ replayed: number; skipped: number }> {
    await this.adminOps.ensureConnected();
    return replayDlqTopic(topic, options, {
      logger: this.logger,
      fetchTopicOffsets: (t) => this.adminOps.admin.fetchTopicOffsets(t),
      send: async (t, messages) => {
        await this.producer.send({ topic: t, messages });
      },
      createConsumer: (gid) =>
        getOrCreateConsumer(gid, true, true, this._consumerOpsDeps),
      cleanupConsumer: (consumer, gid) => {
        consumer
          .disconnect()
          .catch(() => {})
          .finally(() => {
            this.consumers.delete(gid);
            this.runningConsumers.delete(gid);
            this.consumerCreationOptions.delete(gid);
          });
      },
      dlqHeaderKeys: KafkaClient.DLQ_HEADER_KEYS,
    });
  }

  /**
   * Read a compacted topic from the beginning to its current high-watermark.
   * Returns a `Map<key, EventEnvelope>` with the latest value per key.
   * Tombstone messages (null value) remove the key from the map.
   * @example
   * ```ts
   * const snapshot = await kafka.readSnapshot('users.state');
   * const user = snapshot.get('user-42')?.payload;
   * ```
   */
  public async readSnapshot<K extends keyof T & string>(
    topic: K,
    options: ReadSnapshotOptions = {},
  ): Promise<Map<string, EventEnvelope<T[K]>>> {
    await this.adminOps.ensureConnected();

    let offsets: Array<{ partition: number; low: string; high: string }>;
    try {
      offsets = await this.adminOps.admin.fetchTopicOffsets(topic);
    } catch {
      this.logger.warn(
        `readSnapshot: could not fetch offsets for "${String(topic)}", returning empty snapshot`,
      );
      return new Map();
    }

    // Build targets: partition → last offset to consume (highWatermark − 1)
    const targets = new Map<number, number>();
    for (const { partition, high, low } of offsets) {
      const highN = Number.parseInt(high, 10);
      const lowN = Number.parseInt(low, 10);
      if (highN > lowN) targets.set(partition, highN - 1);
    }

    if (targets.size === 0) {
      this.logger.debug?.(
        `readSnapshot: topic "${String(topic)}" is empty — returning empty snapshot`,
      );
      return new Map();
    }

    const snapshot = new Map<string, EventEnvelope<T[K]>>();
    const remaining = new Set(targets.keys());
    const snapshotGroupId = `${this.clientId}-snapshot-${Date.now()}`;

    await new Promise<void>((resolve, reject) => {
      const consumer = this.kafka.consumer({
        kafkaJS: { groupId: snapshotGroupId, fromBeginning: true },
      });
      const cleanup = () => {
        consumer.disconnect().catch(() => {});
      };

      consumer
        .connect()
        .then(() => consumer.subscribe({ topics: [topic] }))
        .then(() =>
          consumer.run({
            eachMessage: async ({ topic: t, partition, message }) => {
              if (!remaining.has(partition)) return;

              const msgOffsetN = Number.parseInt(message.offset, 10);
              this.applySnapshotMessage(
                snapshot,
                options,
                t,
                partition,
                message,
              );

              if (msgOffsetN >= targets.get(partition)!) {
                remaining.delete(partition);
                if (remaining.size === 0) {
                  cleanup();
                  resolve();
                }
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
      `readSnapshot: ${snapshot.size} key(s) from "${String(topic)}"`,
    );
    return snapshot;
  }

  private applySnapshotMessage<K extends keyof T & string>(
    snapshot: Map<string, EventEnvelope<T[K]>>,
    options: ReadSnapshotOptions,
    t: string,
    partition: number,
    message: {
      key: Buffer | null;
      value: Buffer | null;
      offset: string;
      headers?: any;
    },
  ): void {
    let key: string | null = null;
    if (message.key) {
      key = Buffer.isBuffer(message.key)
        ? message.key.toString()
        : String(message.key);
    }

    if (message.value === null || message.value === undefined) {
      if (key !== null) {
        snapshot.delete(key);
        options.onTombstone?.(key);
      }
      return;
    }

    if (key === null) return;

    const rawValue = Buffer.isBuffer(message.value)
      ? message.value.toString()
      : String(message.value);
    try {
      const jsonValue = JSON.parse(rawValue);
      const headers = decodeHeaders(message.headers);
      const parsed: T[K] = options.schema
        ? options.schema.parse(jsonValue)
        : jsonValue;
      snapshot.set(
        key,
        extractEnvelope<T[K]>(parsed, headers, t, partition, message.offset),
      );
    } catch (err) {
      this.logger.warn(
        `readSnapshot: skipping ${t}:${partition}@${message.offset} — ${toError(err).message}`,
      );
    }
  }

  // ── Offset checkpointing ─────────────────────────────────────────

  /**
   * Snapshot the current committed offsets of a consumer group into a Kafka topic.
   * Each call appends a new record — the checkpoint topic is an append-only audit log.
   * @example
   * ```ts
   * const result = await kafka.checkpointOffsets(undefined, 'checkpoints');
   * console.log(`Saved ${result.partitionCount} offsets`);
   * ```
   */
  public async checkpointOffsets(
    groupId: string | undefined,
    checkpointTopic: string,
  ): Promise<CheckpointResult> {
    const gid = groupId ?? this.defaultGroupId;
    await this.adminOps.ensureConnected();

    const committed = await this.adminOps.admin.fetchOffsets({ groupId: gid });

    const offsets: CheckpointEntry[] = [];
    for (const { topic, partitions } of committed) {
      for (const { partition, offset } of partitions) {
        offsets.push({ topic, partition, offset });
      }
    }

    const savedAt = Date.now();
    const payload = JSON.stringify({ groupId: gid, offsets, savedAt });

    await this.producer.send({
      topic: checkpointTopic,
      messages: [
        {
          key: gid,
          value: payload,
          headers: {
            "x-checkpoint-group-id": [gid],
            "x-checkpoint-timestamp": [String(savedAt)],
          },
        },
      ],
    });

    const topics = [...new Set(offsets.map((e) => e.topic))];
    this.logger.log(
      `checkpointOffsets: saved ${offsets.length} partition(s) for group "${gid}" → "${checkpointTopic}"`,
    );
    return { groupId: gid, topics, partitionCount: offsets.length, savedAt };
  }

  /**
   * Restore a consumer group's committed offsets from the nearest checkpoint in `checkpointTopic`.
   * Requires the consumer group to be stopped.
   * @example
   * ```ts
   * await kafka.stopConsumer();
   * await kafka.restoreFromCheckpoint(undefined, 'checkpoints');
   * ```
   */
  public async restoreFromCheckpoint(
    groupId: string | undefined,
    checkpointTopic: string,
    options: RestoreCheckpointOptions = {},
  ): Promise<CheckpointRestoreResult> {
    const gid = groupId ?? this.defaultGroupId;

    if (this.runningConsumers.has(gid)) {
      throw new Error(
        `restoreFromCheckpoint: consumer group "${gid}" is still running. ` +
          `Call stopConsumer("${gid}") before restoring offsets.`,
      );
    }

    await this.adminOps.ensureConnected();

    // Read all checkpoint records from the topic (beginning → HWM)
    const checkpoints: Array<{ savedAt: number; offsets: CheckpointEntry[] }> =
      [];

    let hwmOffsets: Array<{ partition: number; low: string; high: string }>;
    try {
      hwmOffsets = await this.adminOps.admin.fetchTopicOffsets(checkpointTopic);
    } catch {
      throw new Error(
        `restoreFromCheckpoint: could not fetch offsets for "${checkpointTopic}" — does the topic exist?`,
      );
    }

    const targets = new Map<number, number>();
    for (const { partition, high, low } of hwmOffsets) {
      const highN = Number.parseInt(high, 10);
      if (highN > Number.parseInt(low, 10)) targets.set(partition, highN - 1);
    }

    if (targets.size > 0) {
      const snapshotGroupId = `${this.clientId}-checkpoint-restore-${Date.now()}`;
      await new Promise<void>((resolve, reject) => {
        const consumer = this.kafka.consumer({
          kafkaJS: { groupId: snapshotGroupId, fromBeginning: true },
        });
        const cleanup = () => {
          consumer.disconnect().catch(() => {});
        };
        const remaining = new Set(targets.keys());

        consumer
          .connect()
          .then(() => consumer.subscribe({ topics: [checkpointTopic] }))
          .then(() =>
            consumer.run({
              eachMessage: async ({ partition, message }) => {
                if (!remaining.has(partition)) return;

                let msgKey: string | null = null;
                if (message.key) {
                  msgKey = Buffer.isBuffer(message.key)
                    ? message.key.toString()
                    : String(message.key);
                }

                if (msgKey === gid && message.value) {
                  try {
                    const raw = Buffer.isBuffer(message.value)
                      ? message.value.toString()
                      : String(message.value);
                    const parsed = JSON.parse(raw) as {
                      groupId: string;
                      offsets: CheckpointEntry[];
                      savedAt: number;
                    };
                    checkpoints.push({
                      savedAt: parsed.savedAt,
                      offsets: parsed.offsets,
                    });
                  } catch {
                    this.logger.warn(
                      `restoreFromCheckpoint: skipping malformed checkpoint at partition ${partition}@${message.offset}`,
                    );
                  }
                }

                if (
                  Number.parseInt(message.offset, 10) >= targets.get(partition)!
                ) {
                  remaining.delete(partition);
                  if (remaining.size === 0) {
                    cleanup();
                    resolve();
                  }
                }
              },
            }),
          )
          .catch((err) => {
            cleanup();
            reject(err);
          });
      });
    }

    if (checkpoints.length === 0) {
      throw new Error(
        `restoreFromCheckpoint: no checkpoints found for group "${gid}" in "${checkpointTopic}"`,
      );
    }

    // Pick the newest checkpoint whose savedAt ≤ options.timestamp, or the overall latest
    const target = options.timestamp;
    let best: (typeof checkpoints)[number];
    if (target === undefined) {
      best = checkpoints.reduce(
        (acc, c) => (c.savedAt > acc.savedAt ? c : acc),
        checkpoints[0],
      );
    } else {
      const candidates = checkpoints.filter((c) => c.savedAt <= target);
      if (candidates.length > 0) {
        // Newest checkpoint at or before the target timestamp
        best = candidates.reduce(
          (acc, c) => (c.savedAt > acc.savedAt ? c : acc),
          candidates[0],
        );
      } else {
        // All checkpoints are newer than the target — pick the oldest one
        best = checkpoints.reduce(
          (acc, c) => (c.savedAt < acc.savedAt ? c : acc),
          checkpoints[0],
        );
        this.logger.warn(
          `restoreFromCheckpoint: no checkpoint at or before ${new Date(target).toISOString()} — ` +
            `using oldest available (${new Date(best.savedAt).toISOString()})`,
        );
      }
    }

    await this.adminOps.seekToOffset(gid, best.offsets);

    const checkpointAge = Date.now() - best.savedAt;
    this.logger.log(
      `restoreFromCheckpoint: restored ${best.offsets.length} partition(s) for group "${gid}" ` +
        `from checkpoint at ${new Date(best.savedAt).toISOString()} (age: ${checkpointAge}ms)`,
    );

    return {
      groupId: gid,
      offsets: best.offsets,
      restoredAt: best.savedAt,
      checkpointAge,
    };
  }

  /** @inheritDoc */
  public async resetOffsets(
    groupId: string | undefined,
    topic: string,
    position: "earliest" | "latest",
  ): Promise<void> {
    return this.adminOps.resetOffsets(groupId, topic, position);
  }

  /** @inheritDoc */
  public async seekToOffset(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; offset: string }>,
  ): Promise<void> {
    return this.adminOps.seekToOffset(groupId, assignments);
  }

  /** @inheritDoc */
  public async seekToTimestamp(
    groupId: string | undefined,
    assignments: Array<{ topic: string; partition: number; timestamp: number }>,
  ): Promise<void> {
    return this.adminOps.seekToTimestamp(groupId, assignments);
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
   * @example
   * ```ts
   * const state = kafka.getCircuitState('orders.created', 0);
   * if (state?.status === 'open') console.warn('Circuit open!');
   * ```
   */
  public getCircuitState(
    topic: string,
    partition: number,
    groupId?: string,
  ):
    | {
        status: "closed" | "open" | "half-open";
        failures: number;
        windowSize: number;
      }
    | undefined {
    return this.circuitBreaker.getState(
      topic,
      partition,
      groupId ?? this.defaultGroupId,
    );
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
   * @example
   * ```ts
   * const lag = await kafka.getConsumerLag();
   * const total = lag.reduce((sum, p) => sum + p.lag, 0);
   * ```
   */
  public async getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>> {
    return this.adminOps.getConsumerLag(groupId);
  }

  /** @inheritDoc */
  public async checkStatus(): Promise<import("../types").KafkaHealthResult> {
    return this.adminOps.checkStatus();
  }

  /** @inheritDoc */
  public async listConsumerGroups(): Promise<ConsumerGroupSummary[]> {
    return this.adminOps.listConsumerGroups();
  }

  /** @inheritDoc */
  public async describeTopics(topics?: string[]): Promise<TopicDescription[]> {
    return this.adminOps.describeTopics(topics);
  }

  /** @inheritDoc */
  public async deleteRecords(
    topic: string,
    partitions: Array<{ partition: number; offset: string }>,
  ): Promise<void> {
    return this.adminOps.deleteRecords(topic, partitions);
  }

  /**
   * Return the client ID provided during `KafkaClient` construction.
   * @example
   * ```ts
   * const id = kafka.getClientId(); // e.g. 'my-service'
   * ```
   */
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
   * @example
   * ```ts
   * const { processedCount, dlqCount } = kafka.getMetrics();
   * const topicMetrics = kafka.getMetrics('orders.created');
   * ```
   */
  public getMetrics(topic?: string): Readonly<KafkaMetrics> {
    return this.metrics.getMetrics(topic);
  }

  /** @inheritDoc */
  public resetMetrics(topic?: string): void {
    this.metrics.resetMetrics(topic);
  }

  /** @inheritDoc */
  public async disconnect(drainTimeoutMs = 30_000): Promise<void> {
    if (this._lagThrottleTimer) {
      clearInterval(this._lagThrottleTimer);
      this._lagThrottleTimer = undefined;
    }
    await this.inFlight.waitForDrain(drainTimeoutMs);
    const tasks: Promise<void>[] = [this.producer.disconnect()];
    if (this.txProducer) {
      tasks.push(this.txProducer.disconnect());
      _activeTransactionalIds.delete(this.txId);
      this.txProducer = undefined;
      this.txProducerInitPromise = undefined;
    }
    for (const txId of this.retryTxProducers.keys())
      _activeTransactionalIds.delete(txId);
    for (const p of this.retryTxProducers.values()) tasks.push(p.disconnect());
    this.retryTxProducers.clear();
    for (const consumer of this.consumers.values())
      tasks.push(consumer.disconnect());
    tasks.push(this.adminOps.disconnect());
    await Promise.allSettled(tasks);
    this.consumers.clear();
    this.runningConsumers.clear();
    this.consumerCreationOptions.clear();
    this.companionGroupIds.clear();
    this.circuitBreaker.clear();
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

  /** @inheritDoc */
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
    for (const signal of signals) process.once(signal, handler);
  }

  // ── Private helpers ──────────────────────────────────────────────

  private async preparePayload(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
    compression?: import("../types").CompressionType,
  ) {
    registerSchema(topicOrDesc, this.schemaRegistry, this.logger);
    const payload = await buildSendPayload(
      topicOrDesc,
      messages,
      this._producerOpsDeps,
      compression,
    );
    await this.ensureTopic(payload.topic);
    return payload;
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
   * If `autoCreateTopics` is disabled, returns immediately.
   * Concurrent calls for the same topic are deduplicated.
   */
  private async ensureTopic(topic: string): Promise<void> {
    if (!this.autoCreateTopicsEnabled || this.ensuredTopics.has(topic)) return;
    let p = this.ensureTopicPromises.get(topic);
    if (!p) {
      p = (async () => {
        await this.adminOps.ensureConnected();
        await this.adminOps.admin.createTopics({
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
      this._consumerOpsDeps,
      options.partitionAssigner,
    );
    const schemaMap = buildSchemaMap(
      stringTopics,
      this.schemaRegistry,
      optionSchemas,
      this.logger,
    );
    const topicNames = stringTopics.map((t: any) => resolveTopicName(t));
    const subscribeTopics: (string | RegExp)[] = [
      ...topicNames,
      ...regexTopics,
    ];

    await this.ensureConsumerTopics(topicNames, dlq, options.deduplication);

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

    return {
      consumer,
      schemaMap,
      topicNames,
      gid,
      dlq,
      interceptors,
      retry,
      hasRegex,
    };
  }

  /** Create or retrieve the deduplication context for a consumer group. */
  private resolveDeduplicationContext(
    groupId: string,
    options: import("../types").DeduplicationOptions | undefined,
  ): DeduplicationContext | undefined {
    if (!options) return undefined;
    if (!this.dedupStates.has(groupId))
      this.dedupStates.set(groupId, new Map());
    return { options, state: this.dedupStates.get(groupId)! };
  }

  // ── Shared consumer setup helpers ────────────────────────────────

  /** Guard checks shared by startConsumer and startBatchConsumer. */
  private validateTopicConsumerOpts(
    topics: any[],
    options: ConsumerOptions<T>,
  ): void {
    if (options.retryTopics && !options.retry) {
      throw new Error(
        "retryTopics requires retry to be configured — set retry.maxRetries to enable the retry topic chain",
      );
    }
    if (options.retryTopics && topics.some((t) => t instanceof RegExp)) {
      throw new Error(
        "retryTopics is incompatible with regex topic patterns — retry topics require a fixed topic name to build the retry chain.",
      );
    }
  }

  /** Ensure all required topics exist for a consumer: base, DLQ, and dedup topics. */
  private async ensureConsumerTopics(
    topicNames: string[],
    dlq: boolean,
    deduplication: import("../types").DeduplicationOptions | undefined,
  ): Promise<void> {
    for (const t of topicNames) await this.ensureTopic(t);
    if (dlq) {
      for (const t of topicNames) await this.ensureTopic(`${t}.dlq`);
      if (!this.autoCreateTopicsEnabled && topicNames.length > 0) {
        await this.adminOps.validateDlqTopicsExist(topicNames);
      }
    }
    if (deduplication?.strategy === "topic") {
      const dest = deduplication.duplicatesTopic;
      if (this.autoCreateTopicsEnabled) {
        for (const t of topicNames)
          await this.ensureTopic(dest ?? `${t}.duplicates`);
      } else if (topicNames.length > 0) {
        await this.adminOps.validateDuplicatesTopicsExist(topicNames, dest);
      }
    }
  }

  /** Create EOS transactional producer context for atomic main → retry.1 routing. */
  private async makeEosMainContext(
    gid: string,
    consumer: Consumer,
    options: ConsumerOptions<T>,
  ): Promise<{ txProducer: Producer; consumer: Consumer } | undefined> {
    if (!options.retryTopics || !options.retry) return undefined;
    const txProducer = await this.createRetryTxProducer(`${gid}-main-tx`);
    return { txProducer, consumer };
  }

  /** Start companion retry-level consumers and register them under the main groupId. */
  private async launchRetryChain(
    gid: string,
    topicNames: string[],
    handleMessage: (env: EventEnvelope<any>) => Promise<void>,
    opts: {
      retry: RetryOptions;
      dlq: boolean;
      interceptors: ConsumerInterceptor<T>[];
      schemaMap: Map<string, SchemaLike>;
      assignmentTimeoutMs?: number;
    },
  ): Promise<void> {
    const { retry, dlq, interceptors, schemaMap, assignmentTimeoutMs } = opts;
    if (!this.autoCreateTopicsEnabled) {
      await this.adminOps.validateRetryTopicsExist(
        topicNames,
        retry.maxRetries,
      );
    }
    const companions = await startRetryTopicConsumers(
      topicNames,
      gid,
      handleMessage,
      retry,
      dlq,
      interceptors,
      schemaMap,
      this._retryTopicDeps,
      assignmentTimeoutMs,
    );
    this.companionGroupIds.set(gid, companions);
  }

  // ── Deps object builders ─────────────────────────────────────────

  /** Build MessageHandlerDeps with circuit breaker callbacks bound to the given groupId. */
  private messageDepsFor(gid: string): MessageHandlerDeps {
    return {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
      onTtlExpired: this.onTtlExpired,
      onRetry: this.metrics.notifyRetry.bind(this.metrics),
      onDlq: (envelope, reason) =>
        this.metrics.notifyDlq(envelope, reason, gid),
      onDuplicate: this.metrics.notifyDuplicate.bind(this.metrics),
      onMessage: (envelope) => this.metrics.notifyMessage(envelope, gid),
    };
  }

  /** Build the deps object passed to retry topic consumers. */
  private buildRetryTopicDeps() {
    return {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
      onRetry: this.metrics.notifyRetry.bind(this.metrics),
      onDlq: this.metrics.notifyDlq.bind(this.metrics),
      onMessage: this.metrics.notifyMessage.bind(this.metrics),
      ensureTopic: (t: string) => this.ensureTopic(t),
      getOrCreateConsumer: (gid: string, fb: boolean, ac: boolean) =>
        getOrCreateConsumer(gid, fb, ac, this._consumerOpsDeps),
      runningConsumers: this.runningConsumers,
      createRetryTxProducer: (txId: string) => this.createRetryTxProducer(txId),
    };
  }
}
