import { Consumer, Kafka, Partitioners, Producer, Admin } from "kafkajs";
import { Logger } from "@nestjs/common";

/**
 * Mapping of topic names to their message types.
 * Define this interface to get type-safe publish/subscribe across your app.
 *
 * @example
 * ```ts
 * // with explicit extends (IDE hints for values)
 * interface MyTopics extends TTopicMessageMap {
 *   "orders.created": { orderId: string; amount: number };
 *   "users.updated": { userId: string; name: string };
 * }
 *
 * // or plain interface / type — works the same
 * interface MyTopics {
 *   "orders.created": { orderId: string; amount: number };
 * }
 * ```
 */
export type TTopicMessageMap = {
  [topic: string]: Record<string, any>;
};

/**
 * Generic constraint for topic-message maps.
 * Works with both `type` aliases and `interface` declarations.
 */
export type TopicMapConstraint<T> = { [K in keyof T]: Record<string, any> };

export type ClientId = string;
export type GroupId = string;

export type MessageHeaders = Record<string, string>;

/** Options for sending a single message. */
export interface SendOptions {
  /** Partition key for message routing. */
  key?: string;
  /** Custom headers attached to the message. */
  headers?: MessageHeaders;
}

/** Options for configuring a Kafka consumer. */
export interface ConsumerOptions<
  T extends TopicMapConstraint<T> = TTopicMessageMap,
> {
  /** Start reading from earliest offset. Default: `false`. */
  fromBeginning?: boolean;
  /** Automatically commit offsets. Default: `true`. */
  autoCommit?: boolean;
  /** Retry policy for failed message processing. */
  retry?: RetryOptions;
  /** Send failed messages to a Dead Letter Queue (`<topic>.dlq`). */
  dlq?: boolean;
  /** Interceptors called before/after each message. */
  interceptors?: ConsumerInterceptor<T>[];
}

/** Configuration for consumer retry behavior. */
export interface RetryOptions {
  /** Maximum number of retry attempts before giving up. */
  maxRetries: number;
  /** Base delay between retries in ms (multiplied by attempt number). Default: `1000`. */
  backoffMs?: number;
}

/**
 * Interceptor hooks for consumer message processing.
 * All methods are optional — implement only what you need.
 */
export interface ConsumerInterceptor<
  T extends TopicMapConstraint<T> = TTopicMessageMap,
> {
  /** Called before the message handler. */
  before?(message: T[keyof T], topic: string): Promise<void> | void;
  /** Called after the message handler succeeds. */
  after?(message: T[keyof T], topic: string): Promise<void> | void;
  /** Called when the message handler throws. */
  onError?(
    message: T[keyof T],
    topic: string,
    error: Error,
  ): Promise<void> | void;
}

/** Context passed to the `transaction()` callback with type-safe send methods. */
export interface TransactionContext<T extends TopicMapConstraint<T>> {
  send<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;
  sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<{ value: T[K]; key?: string; headers?: MessageHeaders }>,
  ): Promise<void>;
}

/** Interface describing all public methods of the Kafka client. */
export interface IKafkaClient<T extends TopicMapConstraint<T>> {
  checkStatus(): Promise<{ topics: string[] }>;

  startConsumer<K extends Array<keyof T>>(
    topics: K,
    handleMessage: (message: T[K[number]], topic: K[number]) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<void>;

  stopConsumer(): Promise<void>;

  sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;

  sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<{ value: T[K]; key?: string; headers?: MessageHeaders }>,
  ): Promise<void>;

  transaction(fn: (ctx: TransactionContext<T>) => Promise<void>): Promise<void>;

  getClientId: () => ClientId;

  disconnect(): Promise<void>;
}

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
  private readonly consumer: Consumer;
  private readonly admin: Admin;
  private readonly logger: Logger;
  private isConsumerRunning = false;
  private isAdminConnected = false;
  public readonly clientId: ClientId;

  constructor(clientId: ClientId, groupId: GroupId, brokers: string[]) {
    this.clientId = clientId;
    this.logger = new Logger(`KafkaClient:${clientId}`);

    this.kafka = new Kafka({
      clientId: this.clientId,
      brokers,
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner,
      idempotent: true,
      maxInFlightRequests: 1,
    });
    this.consumer = this.kafka.consumer({ groupId });
    this.admin = this.kafka.admin();
  }

  /** Send a single typed message to a topic. */
  public async sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
    options: SendOptions = {},
  ): Promise<void> {
    await this.producer.send({
      topic: topic as string,
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

  /** Send multiple typed messages to a topic in one call. */
  public async sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<{ value: T[K]; key?: string; headers?: MessageHeaders }>,
  ): Promise<void> {
    await this.producer.send({
      topic: topic as string,
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
        send: async (topic, message, options = {}) => {
          await tx.send({
            topic: topic as string,
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
        sendBatch: async (topic, messages) => {
          await tx.send({
            topic: topic as string,
            messages: messages.map((m) => ({
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
    topics: K,
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

    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: topics as string[],
      fromBeginning,
    });
    this.isConsumerRunning = true;
    this.logger.log(
      `Consumer subscribed to topics: ${(topics as string[]).join(", ")}`,
    );

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
            for (const interceptor of interceptors) {
              await interceptor.onError?.(parsedMessage, topic, err);
            }

            const isLastAttempt = attempt === maxAttempts;
            this.logger.error(
              `Error processing message from topic ${topic} (attempt ${attempt}/${maxAttempts}):`,
              err.stack,
            );

            if (isLastAttempt) {
              if (dlq) {
                await this.sendToDlq(topic, raw);
              }
            } else {
              await this.sleep(backoffMs * attempt);
            }
          }
        }
      },
    });
  }

  public async stopConsumer(): Promise<void> {
    this.isConsumerRunning = false;
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
    this.isConsumerRunning = false;
    const tasks = [
      this.producer.disconnect(),
      this.consumer.disconnect(),
    ];
    if (this.isAdminConnected) {
      tasks.push(this.admin.disconnect());
      this.isAdminConnected = false;
    }
    await Promise.allSettled(tasks);
    this.logger.log("All connections closed");
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
