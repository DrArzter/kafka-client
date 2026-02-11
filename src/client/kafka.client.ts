import { Consumer, Kafka, Partitioners, Producer, Admin } from "kafkajs";
import { Logger } from "@nestjs/common";
import { TopicDescriptor, SchemaLike } from "./topic";
import { KafkaRetryExhaustedError, KafkaValidationError } from "./errors";
import type {
  ClientId,
  GroupId,
  SendOptions,
  MessageHeaders,
  ConsumerOptions,
  ConsumerInterceptor,
  RetryOptions,
  TransactionContext,
  TopicMapConstraint,
  IKafkaClient,
  KafkaClientOptions,
  BatchMeta,
  SubscribeRetryOptions,
} from "./types";

// Re-export all types so existing `import { ... } from './kafka.client'` keeps working
export * from "./types";

// ── Helpers ──────────────────────────────────────────────────────────

const ACKS_ALL = -1 as const;

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

// ─────────────────────────────────────────────────────────────────────

/**
 * Type-safe Kafka client for NestJS.
 * Wraps kafkajs with JSON serialization, retries, DLQ, transactions, and interceptors.
 *
 * @typeParam T - Topic-to-message type mapping for compile-time safety.
 */
export class KafkaClient<
  T extends TopicMapConstraint<T>,
> implements IKafkaClient<T> {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumers = new Map<string, Consumer>();
  private readonly admin: Admin;
  private readonly logger: Logger;
  private readonly autoCreateTopicsEnabled: boolean;
  private readonly strictSchemasEnabled: boolean;
  private readonly ensuredTopics = new Set<string>();
  private readonly defaultGroupId: string;
  private readonly schemaRegistry = new Map<string, SchemaLike>();
  private readonly runningConsumers = new Map<string, "eachMessage" | "eachBatch">();

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
    this.logger = new Logger(`KafkaClient:${clientId}`);
    this.autoCreateTopicsEnabled = options?.autoCreateTopics ?? false;
    this.strictSchemasEnabled = options?.strictSchemas ?? true;

    this.kafka = new Kafka({
      clientId: this.clientId,
      brokers,
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner,
      idempotent: true,
      transactionalId: `${clientId}-tx`,
      maxInFlightRequests: 1,
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
      { value: message, key: options.key, headers: options.headers },
    ]);
    await this.ensureTopic(payload.topic);
    await this.producer.send(payload);
  }

  /** Send multiple typed messages in one call. Accepts a topic key or a TopicDescriptor. */
  public async sendBatch<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    descriptor: D,
    messages: Array<{
      value: D["__type"];
      key?: string;
      headers?: MessageHeaders;
    }>,
  ): Promise<void>;
  public async sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<{ value: T[K]; key?: string; headers?: MessageHeaders }>,
  ): Promise<void>;
  public async sendBatch(
    topicOrDesc: any,
    messages: Array<{ value: any; key?: string; headers?: MessageHeaders }>,
  ): Promise<void> {
    const payload = this.buildSendPayload(topicOrDesc, messages);
    await this.ensureTopic(payload.topic);
    await this.producer.send(payload);
  }

  /** Execute multiple sends atomically. Commits on success, aborts on error. */
  public async transaction(
    fn: (ctx: TransactionContext<T>) => Promise<void>,
  ): Promise<void> {
    const tx = await this.producer.transaction();
    try {
      const ctx: TransactionContext<T> = {
        send: async (
          topicOrDesc: any,
          message: any,
          options: SendOptions = {},
        ) => {
          const payload = this.buildSendPayload(topicOrDesc, [
            { value: message, key: options.key, headers: options.headers },
          ]);
          await this.ensureTopic(payload.topic);
          await tx.send(payload);
        },
        sendBatch: async (topicOrDesc: any, messages: any[]) => {
          const payload = this.buildSendPayload(topicOrDesc, messages);
          await this.ensureTopic(payload.topic);
          await tx.send(payload);
        },
      };
      await fn(ctx);
      await tx.commit();
    } catch (error) {
      await tx.abort();
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
    handleMessage: (message: T[K[number]], topic: K[number]) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;
  public async startConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleMessage: (message: D["__type"], topic: D["__topic"]) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;
  public async startConsumer(
    topics: any[],
    handleMessage: (message: any, topic: any) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<void> {
    const { consumer, schemaMap, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachMessage", options);

    await consumer.run({
      autoCommit: options.autoCommit ?? true,
      eachMessage: async ({ topic, message }) => {
        if (!message.value) {
          this.logger.warn(`Received empty message from topic ${topic}`);
          return;
        }

        const raw = message.value.toString();
        const parsed = this.parseJsonMessage(raw, topic);
        if (parsed === null) return;

        const validated = await this.validateWithSchema(
          parsed, raw, topic, schemaMap, interceptors, dlq,
        );
        if (validated === null) return;

        await this.executeWithRetry(
          () => handleMessage(validated, topic as any),
          { topic, messages: validated, rawMessages: [raw], interceptors, dlq, retry },
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
      messages: T[K[number]][],
      topic: K[number],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;
  public async startBatchConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleBatch: (
      messages: D["__type"][],
      topic: D["__topic"],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;
  public async startBatchConsumer(
    topics: any[],
    handleBatch: (
      messages: any[],
      topic: any,
      meta: BatchMeta,
    ) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<void> {
    const { consumer, schemaMap, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachBatch", options);

    await consumer.run({
      autoCommit: options.autoCommit ?? true,
      eachBatch: async ({
        batch,
        heartbeat,
        resolveOffset,
        commitOffsetsIfNecessary,
      }) => {
        const validMessages: any[] = [];
        const rawMessages: string[] = [];

        for (const message of batch.messages) {
          if (!message.value) {
            this.logger.warn(
              `Received empty message from topic ${batch.topic}`,
            );
            continue;
          }

          const raw = message.value.toString();
          const parsed = this.parseJsonMessage(raw, batch.topic);
          if (parsed === null) continue;

          const validated = await this.validateWithSchema(
            parsed, raw, batch.topic, schemaMap, interceptors, dlq,
          );
          if (validated === null) continue;

          validMessages.push(validated);
          rawMessages.push(raw);
        }

        if (validMessages.length === 0) return;

        const meta: BatchMeta = {
          partition: batch.partition,
          highWatermark: batch.highWatermark,
          heartbeat,
          resolveOffset,
          commitOffsetsIfNecessary,
        };

        await this.executeWithRetry(
          () => handleBatch(validMessages, batch.topic as any, meta),
          {
            topic: batch.topic,
            messages: validMessages,
            rawMessages: batch.messages
              .filter((m) => m.value)
              .map((m) => m.value!.toString()),
            interceptors,
            dlq,
            retry,
            isBatch: true,
          },
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

  /** Check broker connectivity and return available topics. */
  public async checkStatus(): Promise<{ topics: string[] }> {
    if (!this.isAdminConnected) {
      await this.admin.connect();
      this.isAdminConnected = true;
    }
    const topics = await this.admin.listTopics();
    return { topics };
  }

  public getClientId(): ClientId {
    return this.clientId;
  }

  /** Gracefully disconnect producer, all consumers, and admin. */
  public async disconnect(): Promise<void> {
    const tasks: Promise<void>[] = [this.producer.disconnect()];
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

  private getOrCreateConsumer(groupId?: string): Consumer {
    const gid = groupId || this.defaultGroupId;
    if (!this.consumers.has(gid)) {
      this.consumers.set(gid, this.kafka.consumer({ groupId: gid }));
    }
    return this.consumers.get(gid)!;
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
      topics: [{ topic, numPartitions: 1 }],
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
   * Handles: topic resolution, schema registration, validation, JSON serialization.
   */
  private buildSendPayload(
    topicOrDesc: any,
    messages: Array<{ value: any; key?: string; headers?: MessageHeaders }>,
  ): { topic: string; messages: Array<{ value: string; key: string | null; headers?: MessageHeaders }>; acks: -1 } {
    this.registerSchema(topicOrDesc);
    const topic = this.resolveTopicName(topicOrDesc);
    return {
      topic,
      messages: messages.map((m) => ({
        value: JSON.stringify(this.validateMessage(topicOrDesc, m.value)),
        key: m.key ?? null,
        headers: m.headers,
      })),
      acks: ACKS_ALL,
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

    const consumer = this.getOrCreateConsumer(optGroupId);
    const schemaMap = this.buildSchemaMap(topics, optionSchemas);

    const topicNames = (topics as any[]).map((t: any) =>
      this.resolveTopicName(t),
    );

    await consumer.connect();
    await this.subscribeWithRetry(consumer, topicNames, fromBeginning, options.subscribeRetry);

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

  /** Parse raw message as JSON. Returns null on failure (logs error). */
  private parseJsonMessage(raw: string, topic: string): any | null {
    try {
      return JSON.parse(raw);
    } catch (error) {
      this.logger.error(
        `Failed to parse message from topic ${topic}:`,
        toError(error).stack,
      );
      return null;
    }
  }

  /**
   * Validate a parsed message against the schema map.
   * On failure: logs error, sends to DLQ if enabled, calls interceptor.onError.
   * Returns validated message or null.
   */
  private async validateWithSchema(
    message: any,
    raw: string,
    topic: string,
    schemaMap: Map<string, SchemaLike>,
    interceptors: ConsumerInterceptor<T>[],
    dlq: boolean,
  ): Promise<any | null> {
    const schema = schemaMap.get(topic);
    if (!schema) return message;

    try {
      return schema.parse(message);
    } catch (error) {
      const err = toError(error);
      const validationError = new KafkaValidationError(topic, message, {
        cause: err,
      });
      this.logger.error(
        `Schema validation failed for topic ${topic}:`,
        err.message,
      );
      if (dlq) await this.sendToDlq(topic, raw);
      for (const interceptor of interceptors) {
        await interceptor.onError?.(message, topic, validationError);
      }
      return null;
    }
  }

  /**
   * Execute a handler with retry, interceptors, and DLQ support.
   * Used by both single-message and batch consumers.
   */
  private async executeWithRetry(
    fn: () => Promise<void>,
    ctx: {
      topic: string;
      messages: any;
      rawMessages: string[];
      interceptors: ConsumerInterceptor<T>[];
      dlq: boolean;
      retry?: RetryOptions;
      isBatch?: boolean;
    },
  ): Promise<void> {
    const { topic, messages, rawMessages, interceptors, dlq, retry, isBatch } = ctx;
    const maxAttempts = retry ? retry.maxRetries + 1 : 1;
    const backoffMs = retry?.backoffMs ?? 1000;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        if (isBatch) {
          for (const interceptor of interceptors) {
            for (const msg of messages) {
              await interceptor.before?.(msg, topic);
            }
          }
        } else {
          for (const interceptor of interceptors) {
            await interceptor.before?.(messages, topic);
          }
        }

        await fn();

        if (isBatch) {
          for (const interceptor of interceptors) {
            for (const msg of messages) {
              await interceptor.after?.(msg, topic);
            }
          }
        } else {
          for (const interceptor of interceptors) {
            await interceptor.after?.(messages, topic);
          }
        }
        return;
      } catch (error) {
        const err = toError(error);
        const isLastAttempt = attempt === maxAttempts;

        if (isLastAttempt && maxAttempts > 1) {
          const exhaustedError = new KafkaRetryExhaustedError(
            topic,
            messages,
            maxAttempts,
            { cause: err },
          );
          for (const interceptor of interceptors) {
            await interceptor.onError?.(messages, topic, exhaustedError);
          }
        } else {
          for (const interceptor of interceptors) {
            await interceptor.onError?.(messages, topic, err);
          }
        }

        this.logger.error(
          `Error processing ${isBatch ? "batch" : "message"} from topic ${topic} (attempt ${attempt}/${maxAttempts}):`,
          err.stack,
        );

        if (isLastAttempt) {
          if (dlq) {
            for (const raw of rawMessages) {
              await this.sendToDlq(topic, raw);
            }
          }
        } else {
          await this.sleep(backoffMs * attempt);
        }
      }
    }
  }

  private async sendToDlq(topic: string, rawMessage: string): Promise<void> {
    const dlqTopic = `${topic}.dlq`;
    try {
      await this.producer.send({
        topic: dlqTopic,
        messages: [{ value: rawMessage }],
        acks: ACKS_ALL,
      });
      this.logger.warn(`Message sent to DLQ: ${dlqTopic}`);
    } catch (error) {
      this.logger.error(
        `Failed to send message to DLQ ${dlqTopic}:`,
        toError(error).stack,
      );
    }
  }

  private async subscribeWithRetry(
    consumer: Consumer,
    topics: string[],
    fromBeginning: boolean,
    retryOpts?: SubscribeRetryOptions,
  ): Promise<void> {
    const maxAttempts = retryOpts?.retries ?? 5;
    const backoffMs = retryOpts?.backoffMs ?? 5000;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await consumer.subscribe({ topics, fromBeginning });
        return;
      } catch (error) {
        if (attempt === maxAttempts) throw error;
        const msg = toError(error).message;
        this.logger.warn(
          `Failed to subscribe to [${topics.join(", ")}] (attempt ${attempt}/${maxAttempts}): ${msg}. Retrying in ${backoffMs}ms...`,
        );
        await this.sleep(backoffMs);
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
