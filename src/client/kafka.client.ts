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
  TransactionContext,
  TopicMapConstraint,
  IKafkaClient,
  KafkaClientOptions,
  BatchMeta,
  SubscribeRetryOptions,
} from "./types";

// Re-export all types so existing `import { ... } from './kafka.client'` keeps working
export * from "./types";

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

  private validateMessage(topicOrDesc: any, message: any): any {
    if (topicOrDesc?.__schema) {
      const topic = this.resolveTopicName(topicOrDesc);
      this.schemaRegistry.set(topic, topicOrDesc.__schema);
      return topicOrDesc.__schema.parse(message);
    }
    if (this.strictSchemasEnabled && typeof topicOrDesc === "string") {
      const schema = this.schemaRegistry.get(topicOrDesc);
      if (schema) return schema.parse(message);
    }
    return message;
  }

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
    const validated = this.validateMessage(topicOrDesc, message);
    const topic = this.resolveTopicName(topicOrDesc);
    await this.ensureTopic(topic);
    await this.producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(validated),
          key: options.key ?? null,
          headers: options.headers,
        },
      ],
      acks: -1,
    });
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
    const topic = this.resolveTopicName(topicOrDesc);
    await this.ensureTopic(topic);
    await this.producer.send({
      topic,
      messages: messages.map((m) => ({
        value: JSON.stringify(this.validateMessage(topicOrDesc, m.value)),
        key: m.key ?? null,
        headers: m.headers,
      })),
      acks: -1,
    });
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
          const validated = this.validateMessage(topicOrDesc, message);
          const topic = this.resolveTopicName(topicOrDesc);
          await this.ensureTopic(topic);
          await tx.send({
            topic,
            messages: [
              {
                value: JSON.stringify(validated),
                key: options.key ?? null,
                headers: options.headers,
              },
            ],
            acks: -1,
          });
        },
        sendBatch: async (topicOrDesc: any, messages: any[]) => {
          const topic = this.resolveTopicName(topicOrDesc);
          await this.ensureTopic(topic);
          await tx.send({
            topic,
            messages: messages.map((m: any) => ({
              value: JSON.stringify(this.validateMessage(topicOrDesc, m.value)),
              key: m.key ?? null,
              headers: m.headers,
            })),
            acks: -1,
          });
        },
      };
      await fn(ctx);
      await tx.commit();
    } catch (error) {
      await tx.abort();
      throw error;
    }
  }

  /** Connect the idempotent producer. Called automatically by `KafkaModule.register()`. */
  public async connectProducer(): Promise<void> {
    await this.producer.connect();
    this.logger.log("Producer connected");
  }

  public async disconnectProducer(): Promise<void> {
    await this.producer.disconnect();
    this.logger.log("Producer disconnected");
  }

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
    const {
      groupId: optGroupId,
      fromBeginning = false,
      autoCommit = true,
      retry,
      dlq = false,
      interceptors = [],
      schemas: optionSchemas,
    } = options;

    const gid = optGroupId || this.defaultGroupId;
    const existingMode = this.runningConsumers.get(gid);
    if (existingMode === "eachBatch") {
      throw new Error(
        `Cannot use eachMessage on consumer group "${gid}" — it is already running with eachBatch. ` +
          `Use a different groupId for this consumer.`,
      );
    }

    const consumer = this.getOrCreateConsumer(optGroupId);

    // Build schema map from descriptors and/or options.schemas
    const schemaMap = this.buildSchemaMap(topics, optionSchemas);

    const topicNames = (topics as any[]).map((t: any) =>
      this.resolveTopicName(t),
    );

    await consumer.connect();
    await this.subscribeWithRetry(consumer, topicNames, fromBeginning, options.subscribeRetry);

    this.logger.log(`Consumer subscribed to topics: ${topicNames.join(", ")}`);

    await consumer.run({
      autoCommit,
      eachMessage: async ({ topic, message }) => {
        if (!message.value) {
          this.logger.warn(`Received empty message from topic ${topic}`);
          return;
        }

        const raw = message.value.toString();
        let parsedMessage: any;

        try {
          parsedMessage = JSON.parse(raw);
        } catch (error) {
          this.logger.error(
            `Failed to parse message from topic ${topic}:`,
            error instanceof Error ? error.stack : String(error),
          );
          return;
        }

        const schema = schemaMap.get(topic);
        if (schema) {
          try {
            parsedMessage = schema.parse(parsedMessage);
          } catch (error) {
            const err =
              error instanceof Error ? error : new Error(String(error));
            const validationError = new KafkaValidationError(
              topic,
              parsedMessage,
              { cause: err },
            );
            this.logger.error(
              `Schema validation failed for topic ${topic}:`,
              err.message,
            );
            if (dlq) await this.sendToDlq(topic, raw);
            for (const interceptor of interceptors) {
              await interceptor.onError?.(
                parsedMessage,
                topic,
                validationError,
              );
            }
            return;
          }
        }

        await this.processMessage(parsedMessage, raw, topic, handleMessage, {
          retry,
          dlq,
          interceptors,
        });
      },
    });

    this.runningConsumers.set(gid, "eachMessage");
  }

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
    const {
      groupId: optGroupId,
      fromBeginning = false,
      autoCommit = true,
      retry,
      dlq = false,
      interceptors = [],
      schemas: optionSchemas,
    } = options;

    const gid = optGroupId || this.defaultGroupId;
    const existingMode = this.runningConsumers.get(gid);
    if (existingMode === "eachMessage") {
      throw new Error(
        `Cannot use eachBatch on consumer group "${gid}" — it is already running with eachMessage. ` +
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
      `Batch consumer subscribed to topics: ${topicNames.join(", ")}`,
    );

    await consumer.run({
      autoCommit,
      eachBatch: async ({
        batch,
        heartbeat,
        resolveOffset,
        commitOffsetsIfNecessary,
      }) => {
        const validMessages: any[] = [];

        for (const message of batch.messages) {
          if (!message.value) {
            this.logger.warn(
              `Received empty message from topic ${batch.topic}`,
            );
            continue;
          }

          const raw = message.value.toString();
          let parsedMessage: any;

          try {
            parsedMessage = JSON.parse(raw);
          } catch (error) {
            this.logger.error(
              `Failed to parse message from topic ${batch.topic}:`,
              error instanceof Error ? error.stack : String(error),
            );
            continue;
          }

          const schema = schemaMap.get(batch.topic);
          if (schema) {
            try {
              parsedMessage = schema.parse(parsedMessage);
            } catch (error) {
              const err =
                error instanceof Error ? error : new Error(String(error));
              const validationError = new KafkaValidationError(
                batch.topic,
                parsedMessage,
                { cause: err },
              );
              this.logger.error(
                `Schema validation failed for topic ${batch.topic}:`,
                err.message,
              );
              if (dlq) await this.sendToDlq(batch.topic, raw);
              for (const interceptor of interceptors) {
                await interceptor.onError?.(
                  parsedMessage,
                  batch.topic,
                  validationError,
                );
              }
              continue;
            }
          }

          validMessages.push(parsedMessage);
        }

        if (validMessages.length === 0) return;

        const meta: BatchMeta = {
          partition: batch.partition,
          highWatermark: batch.highWatermark,
          heartbeat,
          resolveOffset,
          commitOffsetsIfNecessary,
        };

        const maxAttempts = retry ? retry.maxRetries + 1 : 1;
        const backoffMs = retry?.backoffMs ?? 1000;

        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          try {
            for (const interceptor of interceptors) {
              for (const msg of validMessages) {
                await interceptor.before?.(msg, batch.topic);
              }
            }

            await handleBatch(validMessages, batch.topic as any, meta);

            for (const interceptor of interceptors) {
              for (const msg of validMessages) {
                await interceptor.after?.(msg, batch.topic);
              }
            }
            return;
          } catch (error) {
            const err =
              error instanceof Error ? error : new Error(String(error));
            const isLastAttempt = attempt === maxAttempts;

            if (isLastAttempt && maxAttempts > 1) {
              const exhaustedError = new KafkaRetryExhaustedError(
                batch.topic,
                validMessages,
                maxAttempts,
                { cause: err },
              );
              for (const interceptor of interceptors) {
                await interceptor.onError?.(
                  validMessages as any,
                  batch.topic,
                  exhaustedError,
                );
              }
            } else {
              for (const interceptor of interceptors) {
                await interceptor.onError?.(
                  validMessages as any,
                  batch.topic,
                  err,
                );
              }
            }

            this.logger.error(
              `Error processing batch from topic ${batch.topic} (attempt ${attempt}/${maxAttempts}):`,
              err.stack,
            );

            if (isLastAttempt) {
              if (dlq) {
                for (const msg of batch.messages) {
                  if (msg.value) {
                    await this.sendToDlq(batch.topic, msg.value.toString());
                  }
                }
              }
            } else {
              await this.sleep(backoffMs * attempt);
            }
          }
        }
      },
    });

    this.runningConsumers.set(gid, "eachBatch");
  }

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

  // --- Private helpers ---

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

  private async processMessage<K extends Array<keyof T>>(
    parsedMessage: T[K[number]],
    raw: string,
    topic: string,
    handleMessage: (message: T[K[number]], topic: K[number]) => Promise<void>,
    opts: Pick<ConsumerOptions<T>, "retry" | "dlq" | "interceptors">,
  ): Promise<void> {
    const { retry, dlq = false, interceptors = [] } = opts;
    const maxAttempts = retry ? retry.maxRetries + 1 : 1;
    const backoffMs = retry?.backoffMs ?? 1000;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        for (const interceptor of interceptors) {
          await interceptor.before?.(parsedMessage, topic);
        }

        await handleMessage(parsedMessage, topic as K[number]);

        for (const interceptor of interceptors) {
          await interceptor.after?.(parsedMessage, topic);
        }
        return;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        const isLastAttempt = attempt === maxAttempts;

        if (isLastAttempt && maxAttempts > 1) {
          const exhaustedError = new KafkaRetryExhaustedError(
            topic,
            parsedMessage,
            maxAttempts,
            { cause: err },
          );
          for (const interceptor of interceptors) {
            await interceptor.onError?.(parsedMessage, topic, exhaustedError);
          }
        } else {
          for (const interceptor of interceptors) {
            await interceptor.onError?.(parsedMessage, topic, err);
          }
        }

        this.logger.error(
          `Error processing message from topic ${topic} (attempt ${attempt}/${maxAttempts}):`,
          err.stack,
        );

        if (isLastAttempt) {
          if (dlq) await this.sendToDlq(topic, raw);
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
        acks: -1,
      });
      this.logger.warn(`Message sent to DLQ: ${dlqTopic}`);
    } catch (error) {
      this.logger.error(
        `Failed to send message to DLQ ${dlqTopic}:`,
        error instanceof Error ? error.stack : String(error),
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
        const msg = error instanceof Error ? error.message : String(error);
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
