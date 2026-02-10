import { Consumer, Kafka, Partitioners, Producer, Admin } from "kafkajs";
import { Logger } from "@nestjs/common";
import { TopicDescriptor } from "./topic";
import { KafkaRetryExhaustedError } from "./errors";
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
} from "./types";

// Re-export all types so existing `import { ... } from './kafka.client'` keeps working
export * from "./types";

/**
 * Type-safe Kafka client for NestJS.
 * Wraps kafkajs with JSON serialization, retries, DLQ, transactions, and interceptors.
 *
 * @typeParam T - Topic-to-message type mapping for compile-time safety.
 */
export class KafkaClient<T extends TopicMapConstraint<T>>
  implements IKafkaClient<T>
{
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly admin: Admin;
  private readonly logger: Logger;
  private readonly autoCreateTopicsEnabled: boolean;
  private readonly ensuredTopics = new Set<string>();

  private isAdminConnected = false;
  public readonly clientId: ClientId;

  constructor(
    clientId: ClientId,
    groupId: GroupId,
    brokers: string[],
    options?: KafkaClientOptions,
  ) {
    this.clientId = clientId;
    this.logger = new Logger(`KafkaClient:${clientId}`);
    this.autoCreateTopicsEnabled = options?.autoCreateTopics ?? false;

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
    this.consumer = this.kafka.consumer({ groupId });
    this.admin = this.kafka.admin();
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
    const topic = this.resolveTopicName(topicOrDesc);
    await this.ensureTopic(topic);
    await this.producer.send({
      topic,
      messages: [
        {
          value: JSON.stringify(message),
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
        value: JSON.stringify(m.value),
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
          const topic = this.resolveTopicName(topicOrDesc);
          await this.ensureTopic(topic);
          await tx.send({
            topic,
            messages: [
              {
                value: JSON.stringify(message),
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
              value: JSON.stringify(m.value),
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
    topics: K | TopicDescriptor[],
    handleMessage: (message: T[K[number]], topic: K[number]) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<void> {
    const {
      fromBeginning = false,
      autoCommit = true,
      retry,
      dlq = false,
      interceptors = [],
    } = options;

    const topicNames = (topics as any[]).map((t: any) =>
      this.resolveTopicName(t),
    );

    await this.consumer.connect();

    for (const t of topicNames) {
      await this.ensureTopic(t);
    }

    await this.consumer.subscribe({ topics: topicNames, fromBeginning });

    this.logger.log(`Consumer subscribed to topics: ${topicNames.join(", ")}`);

    await this.consumer.run({
      autoCommit,
      eachMessage: async ({ topic, message }) => {
        if (!message.value) {
          this.logger.warn(`Received empty message from topic ${topic}`);
          return;
        }

        const raw = message.value.toString();
        let parsedMessage: T[K[number]];

        try {
          parsedMessage = JSON.parse(raw) as T[K[number]];
        } catch (error) {
          this.logger.error(
            `Failed to parse message from topic ${topic}:`,
            error instanceof Error ? error.stack : String(error),
          );
          return;
        }

        await this.processMessage(parsedMessage, raw, topic, handleMessage, {
          retry,
          dlq,
          interceptors,
        });
      },
    });
  }

  public async stopConsumer(): Promise<void> {

    await this.consumer.disconnect();
    this.logger.log("Consumer disconnected");
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

  /** Gracefully disconnect producer, consumer, and admin. */
  public async disconnect(): Promise<void> {

    const tasks = [this.producer.disconnect(), this.consumer.disconnect()];
    if (this.isAdminConnected) {
      tasks.push(this.admin.disconnect());
      this.isAdminConnected = false;
    }
    await Promise.allSettled(tasks);
    this.logger.log("All connections closed");
  }

  // --- Private helpers ---

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
        const err =
          error instanceof Error ? error : new Error(String(error));
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

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
