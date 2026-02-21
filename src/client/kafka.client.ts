import { KafkaJS } from "@confluentinc/kafka-javascript";
type Kafka = KafkaJS.Kafka;
type Producer = KafkaJS.Producer;
type Consumer = KafkaJS.Consumer;
type Admin = KafkaJS.Admin;
const { Kafka: KafkaClass, logLevel: KafkaLogLevel } = KafkaJS;
import { TopicDescriptor, SchemaLike } from "./topic";
import {
  buildEnvelopeHeaders,
  decodeHeaders,
  extractEnvelope,
  runWithEnvelopeContext,
} from "./envelope";
import type { EventEnvelope } from "./envelope";
import {
  toError,
  parseJsonMessage,
  validateWithSchema,
  executeWithRetry,
} from "./consumer-pipeline";
import { subscribeWithRetry } from "./subscribe-retry";
import type {
  ClientId,
  GroupId,
  SendOptions,
  MessageHeaders,
  BatchMessageItem,
  ConsumerOptions,
  TransactionContext,
  TopicMapConstraint,
  IKafkaClient,
  KafkaClientOptions,
  KafkaInstrumentation,
  KafkaLogger,
  BatchMeta,
} from "./types";

// Re-export all types so existing `import { ... } from './kafka.client'` keeps working
export * from "./types";

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
  private readonly runningConsumers = new Map<string, "eachMessage" | "eachBatch">();
  private readonly instrumentation: KafkaInstrumentation[];

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
      warn: (msg, ...args) => console.warn(`[KafkaClient:${clientId}] ${msg}`, ...args),
      error: (msg, ...args) => console.error(`[KafkaClient:${clientId}] ${msg}`, ...args),
    };
    this.autoCreateTopicsEnabled = options?.autoCreateTopics ?? false;
    this.strictSchemasEnabled = options?.strictSchemas ?? true;
    this.numPartitions = options?.numPartitions ?? 1;
    this.instrumentation = options?.instrumentation ?? [];

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
    const payload = this.buildSendPayload(topicOrDesc, [
      {
        value: message,
        key: options.key,
        headers: options.headers,
        correlationId: options.correlationId,
        schemaVersion: options.schemaVersion,
        eventId: options.eventId,
      },
    ]);
    await this.ensureTopic(payload.topic);
    await this.producer.send(payload);
    for (const inst of this.instrumentation) {
      inst.afterSend?.(payload.topic);
    }
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
    const payload = this.buildSendPayload(topicOrDesc, messages);
    await this.ensureTopic(payload.topic);
    await this.producer.send(payload);
    for (const inst of this.instrumentation) {
      inst.afterSend?.(payload.topic);
    }
  }

  /** Execute multiple sends atomically. Commits on success, aborts on error. */
  public async transaction(
    fn: (ctx: TransactionContext<T>) => Promise<void>,
  ): Promise<void> {
    if (!this.txProducer) {
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
          const payload = this.buildSendPayload(topicOrDesc, [
            {
              value: message,
              key: options.key,
              headers: options.headers,
              correlationId: options.correlationId,
              schemaVersion: options.schemaVersion,
              eventId: options.eventId,
            },
          ]);
          await this.ensureTopic(payload.topic);
          await tx.send(payload);
        },
        sendBatch: async (topicOrDesc: any, messages: BatchMessageItem<any>[]) => {
          const payload = this.buildSendPayload(topicOrDesc, messages);
          await this.ensureTopic(payload.topic);
          await tx.send(payload);
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
  ): Promise<void>;
  public async startConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleMessage: (envelope: EventEnvelope<D["__type"]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;
  public async startConsumer(
    topics: any[],
    handleMessage: (envelope: EventEnvelope<any>) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<void> {
    const { consumer, schemaMap, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachMessage", options);

    const deps = { logger: this.logger, producer: this.producer, instrumentation: this.instrumentation };

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) {
          this.logger.warn(`Received empty message from topic ${topic}`);
          return;
        }

        const raw = message.value.toString();
        const parsed = parseJsonMessage(raw, topic, this.logger);
        if (parsed === null) return;

        const validated = await validateWithSchema(
          parsed, raw, topic, schemaMap, interceptors, dlq, deps,
        );
        if (validated === null) return;

        const headers = decodeHeaders(message.headers);
        const envelope = extractEnvelope(
          validated, headers, topic, partition, message.offset,
        );

        await executeWithRetry(
          () =>
            runWithEnvelopeContext(
              { correlationId: envelope.correlationId, traceparent: envelope.traceparent },
              () => handleMessage(envelope),
            ),
          { envelope, rawMessages: [raw], interceptors, dlq, retry },
          deps,
        );
      },
    });

    this.runningConsumers.set(gid, "eachMessage");
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
  ): Promise<void>;
  public async startBatchConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleBatch: (
      envelopes: EventEnvelope<D["__type"]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;
  public async startBatchConsumer(
    topics: any[],
    handleBatch: (
      envelopes: EventEnvelope<any>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<void> {
    const { consumer, schemaMap, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachBatch", options);

    const deps = { logger: this.logger, producer: this.producer, instrumentation: this.instrumentation };

    await consumer.run({
      eachBatch: async ({
        batch,
        heartbeat,
        resolveOffset,
        commitOffsetsIfNecessary,
      }) => {
        const envelopes: EventEnvelope<any>[] = [];
        const rawMessages: string[] = [];

        for (const message of batch.messages) {
          if (!message.value) {
            this.logger.warn(
              `Received empty message from topic ${batch.topic}`,
            );
            continue;
          }

          const raw = message.value.toString();
          const parsed = parseJsonMessage(raw, batch.topic, this.logger);
          if (parsed === null) continue;

          const validated = await validateWithSchema(
            parsed, raw, batch.topic, schemaMap, interceptors, dlq, deps,
          );
          if (validated === null) continue;

          const headers = decodeHeaders(message.headers);
          envelopes.push(
            extractEnvelope(validated, headers, batch.topic, batch.partition, message.offset),
          );
          rawMessages.push(raw);
        }

        if (envelopes.length === 0) return;

        const meta: BatchMeta = {
          partition: batch.partition,
          highWatermark: batch.highWatermark,
          heartbeat,
          resolveOffset,
          commitOffsetsIfNecessary,
        };

        await executeWithRetry(
          () => handleBatch(envelopes, meta),
          {
            envelope: envelopes,
            rawMessages: batch.messages
              .filter((m) => m.value)
              .map((m) => m.value!.toString()),
            interceptors,
            dlq,
            retry,
            isBatch: true,
          },
          deps,
        );
      },
    });

    this.runningConsumers.set(gid, "eachBatch");
  }

  // ── Consumer lifecycle ───────────────────────────────────────────

  public async stopConsumer(): Promise<void> {
    const tasks = [];
    for (const consumer of this.consumers.values()) {
      tasks.push(consumer.disconnect());
    }
    await Promise.allSettled(tasks);
    this.consumers.clear();
    this.runningConsumers.clear();
    this.logger.log("All consumers disconnected");
  }

  /** Check broker connectivity and return status, clientId, and available topics. */
  public async checkStatus(): Promise<{ status: 'up'; clientId: string; topics: string[] }> {
    if (!this.isAdminConnected) {
      await this.admin.connect();
      this.isAdminConnected = true;
    }
    const topics = await this.admin.listTopics();
    return { status: 'up', clientId: this.clientId, topics };
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
    this.logger.log("All connections closed");
  }

  // ── Private helpers ──────────────────────────────────────────────

  private getOrCreateConsumer(
    groupId: string,
    fromBeginning: boolean,
    autoCommit: boolean,
  ): Consumer {
    if (!this.consumers.has(groupId)) {
      this.consumers.set(
        groupId,
        this.kafka.consumer({
          kafkaJS: { groupId, fromBeginning, autoCommit },
        }),
      );
    }
    return this.consumers.get(groupId)!;
  }

  private resolveTopicName(topicOrDescriptor: unknown): string {
    if (typeof topicOrDescriptor === "string") return topicOrDescriptor;
    if (
      topicOrDescriptor &&
      typeof topicOrDescriptor === "object" &&
      "__topic" in topicOrDescriptor
    ) {
      return (topicOrDescriptor as TopicDescriptor).__topic;
    }
    return String(topicOrDescriptor);
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

  /** Register schema from descriptor into global registry (side-effect). */
  private registerSchema(topicOrDesc: any): void {
    if (topicOrDesc?.__schema) {
      const topic = this.resolveTopicName(topicOrDesc);
      this.schemaRegistry.set(topic, topicOrDesc.__schema);
    }
  }

  /** Validate message against schema. Pure — no side-effects on registry. */
  private validateMessage(topicOrDesc: any, message: any): any {
    if (topicOrDesc?.__schema) {
      return topicOrDesc.__schema.parse(message);
    }
    if (this.strictSchemasEnabled && typeof topicOrDesc === "string") {
      const schema = this.schemaRegistry.get(topicOrDesc);
      if (schema) return schema.parse(message);
    }
    return message;
  }

  /**
   * Build a kafkajs-ready send payload.
   * Handles: topic resolution, schema registration, validation, JSON serialization,
   * envelope header generation, and instrumentation hooks.
   */
  private buildSendPayload(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
  ): { topic: string; messages: Array<{ value: string; key: string | null; headers: MessageHeaders }> } {
    this.registerSchema(topicOrDesc);
    const topic = this.resolveTopicName(topicOrDesc);
    return {
      topic,
      messages: messages.map((m) => {
        const envelopeHeaders = buildEnvelopeHeaders({
          correlationId: m.correlationId,
          schemaVersion: m.schemaVersion,
          eventId: m.eventId,
          headers: m.headers,
        });

        // Let instrumentation hooks mutate headers (e.g. OTel injects traceparent)
        for (const inst of this.instrumentation) {
          inst.beforeSend?.(topic, envelopeHeaders);
        }

        return {
          value: JSON.stringify(this.validateMessage(topicOrDesc, m.value)),
          key: m.key ?? null,
          headers: envelopeHeaders,
        };
      }),
    };
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

    const consumer = this.getOrCreateConsumer(gid, fromBeginning, options.autoCommit ?? true);
    const schemaMap = this.buildSchemaMap(topics, optionSchemas);

    const topicNames = (topics as any[]).map((t: any) =>
      this.resolveTopicName(t),
    );

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
    await subscribeWithRetry(consumer, topicNames, this.logger, options.subscribeRetry);

    this.logger.log(
      `${mode === "eachBatch" ? "Batch consumer" : "Consumer"} subscribed to topics: ${topicNames.join(", ")}`,
    );

    return { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry };
  }

  private buildSchemaMap(
    topics: any[],
    optionSchemas?: Map<string, SchemaLike>,
  ): Map<string, SchemaLike> {
    const schemaMap = new Map<string, SchemaLike>();
    for (const t of topics) {
      if (t?.__schema) {
        const name = this.resolveTopicName(t);
        schemaMap.set(name, t.__schema);
        this.schemaRegistry.set(name, t.__schema);
      }
    }
    if (optionSchemas) {
      for (const [k, v] of optionSchemas) {
        schemaMap.set(k, v);
        this.schemaRegistry.set(k, v);
      }
    }
    return schemaMap;
  }
}
