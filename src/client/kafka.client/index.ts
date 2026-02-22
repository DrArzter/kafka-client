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
  BatchMeta,
} from "../types";

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
import type { MessageHandlerDeps } from "./message-handler";
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
  private readonly consumers = new Map<string, Consumer>();
  private readonly admin: Admin;
  private readonly logger: KafkaLogger;
  private readonly autoCreateTopicsEnabled: boolean;
  private readonly strictSchemasEnabled: boolean;
  private readonly numPartitions: number;
  private readonly ensuredTopics = new Set<string>();
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

  private isAdminConnected = false;
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
    };
    this.autoCreateTopicsEnabled = options?.autoCreateTopics ?? false;
    this.strictSchemasEnabled = options?.strictSchemas ?? true;
    this.numPartitions = options?.numPartitions ?? 1;
    this.instrumentation = options?.instrumentation ?? [];
    this.onMessageLost = options?.onMessageLost;
    this.onRebalance = options?.onRebalance;

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
    if (!this.txProducer) {
      // transactionalId must be unique per producer instance across the cluster.
      // Two KafkaClient instances sharing the same clientId will share this id,
      // causing Kafka to fence one of the producers. Use distinct clientIds when
      // running multiple instances of the same service.
      this.txProducer = this.kafka.producer({
        kafkaJS: {
          acks: -1,
          idempotent: true,
          transactionalId: `${this.clientId}-tx`,
          maxInFlightRequests: 1,
        },
      });
      await this.txProducer.connect();
    }
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
        },
        sendBatch: async (
          topicOrDesc: any,
          messages: BatchMessageItem<any>[],
        ) => {
          await tx.send(await this.preparePayload(topicOrDesc, messages));
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

  /** Connect the idempotent producer. Called automatically by `KafkaModule.register()`. */
  public async connectProducer(): Promise<void> {
    await this.producer.connect();
    this.logger.log("Producer connected");
  }

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

    await consumer.run({
      eachMessage: (payload) =>
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
          },
          deps,
        ),
    });

    this.runningConsumers.set(gid, "eachMessage");

    if (options.retryTopics && retry) {
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
    const { consumer, schemaMap, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachBatch", options);

    const deps = this.messageDeps;
    const timeoutMs = options.handlerTimeoutMs;

    await consumer.run({
      eachBatch: (payload) =>
        handleEachBatch(
          payload,
          {
            schemaMap,
            handleBatch,
            interceptors,
            dlq,
            retry,
            timeoutMs,
            wrapWithTimeout: this.wrapWithTimeoutWarning.bind(this),
          },
          deps,
        ),
    });

    this.runningConsumers.set(gid, "eachBatch");

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
      await consumer.disconnect().catch(() => {});
      this.consumers.delete(groupId);
      this.runningConsumers.delete(groupId);
      this.consumerCreationOptions.delete(groupId);
      this.logger.log(`Consumer disconnected: group "${groupId}"`);

      // Stop all companion retry level consumers started for this group
      const companions = this.companionGroupIds.get(groupId) ?? [];
      for (const cGroupId of companions) {
        const cConsumer = this.consumers.get(cGroupId);
        if (cConsumer) {
          await cConsumer.disconnect().catch(() => {});
          this.consumers.delete(cGroupId);
          this.runningConsumers.delete(cGroupId);
          this.consumerCreationOptions.delete(cGroupId);
          this.logger.log(`Retry consumer disconnected: group "${cGroupId}"`);
        }
      }
      this.companionGroupIds.delete(groupId);
    } else {
      const tasks = Array.from(this.consumers.values()).map((c) =>
        c.disconnect().catch(() => {}),
      );
      await Promise.allSettled(tasks);
      this.consumers.clear();
      this.runningConsumers.clear();
      this.consumerCreationOptions.clear();
      this.companionGroupIds.clear();
      this.logger.log("All consumers disconnected");
    }
  }

  /**
   * Query consumer group lag per partition.
   * Lag = broker high-watermark − last committed offset.
   * A committed offset of -1 (nothing committed yet) counts as full lag.
   */
  public async getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>> {
    const gid = groupId ?? this.defaultGroupId;
    if (!this.isAdminConnected) {
      await this.admin.connect();
      this.isAdminConnected = true;
    }

    const committedByTopic = await this.admin.fetchOffsets({ groupId: gid });
    const result: Array<{ topic: string; partition: number; lag: number }> = [];

    for (const { topic, partitions } of committedByTopic) {
      const brokerOffsets = await this.admin.fetchTopicOffsets(topic);

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
      if (!this.isAdminConnected) {
        await this.admin.connect();
        this.isAdminConnected = true;
      }
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

  /** Gracefully disconnect producer, all consumers, and admin. */
  public async disconnect(): Promise<void> {
    const tasks: Promise<void>[] = [this.producer.disconnect()];
    if (this.txProducer) {
      tasks.push(this.txProducer.disconnect());
      this.txProducer = undefined;
    }
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

  // ── Private helpers ──────────────────────────────────────────────

  private async preparePayload(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
  ) {
    registerSchema(topicOrDesc, this.schemaRegistry);
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

  private async ensureTopic(topic: string): Promise<void> {
    if (!this.autoCreateTopicsEnabled || this.ensuredTopics.has(topic)) return;
    if (!this.isAdminConnected) {
      await this.admin.connect();
      this.isAdminConnected = true;
    }
    await this.admin.createTopics({
      topics: [{ topic, numPartitions: this.numPartitions }],
    });
    this.ensuredTopics.add(topic);
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

  // ── Deps object getters ──────────────────────────────────────────

  private get producerOpsDeps(): BuildSendPayloadDeps {
    return {
      schemaRegistry: this.schemaRegistry,
      strictSchemasEnabled: this.strictSchemasEnabled,
      instrumentation: this.instrumentation,
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
    };
  }

  private get retryTopicDeps() {
    return {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
      ensureTopic: (t: string) => this.ensureTopic(t),
      getOrCreateConsumer: (gid: string, fb: boolean, ac: boolean) =>
        getOrCreateConsumer(gid, fb, ac, this.consumerOpsDeps),
      runningConsumers: this.runningConsumers,
    };
  }
}
