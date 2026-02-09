import {
  Consumer,
  Kafka,
  Partitioners,
  Producer,
  Admin,
} from "kafkajs";
import { Logger } from "@nestjs/common";

export type TTopicMessageMap = {
  [topic: string]: Record<string, any>;
};

export type ClientId = string;
export type GroupId = string;

export interface ConsumerOptions {
  fromBeginning?: boolean;
  autoCommit?: boolean;
}

export interface IKafkaClient<T extends TTopicMessageMap> {
  checkStatus(): Promise<{ topics: string[] }>;

  startConsumer<K extends Array<keyof T>>(
    topics: K,
    handleMessage: (message: T[K[number]], topic: K[number]) => Promise<void>,
    options?: ConsumerOptions,
  ): Promise<void>;

  stopConsumer(): Promise<void>;

  sendMessage<K extends keyof T>(topic: K, message: T[K]): Promise<void>;

  getClientId: () => ClientId;
}

export class KafkaClient<T extends TTopicMessageMap>
  implements IKafkaClient<T>
{
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly admin: Admin;
  private readonly logger: Logger;
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

  public async sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
  ): Promise<void> {
    await this.producer.send({
      topic: topic as string,
      messages: [{ value: JSON.stringify(message) }],
      acks: -1,
    });
  }

  public async connectProducer(): Promise<void> {
    await this.producer.connect();
    this.logger.log("Producer connected");
  }

  public async disconnectProducer(): Promise<void> {
    await this.producer.disconnect();
    this.logger.log("Producer disconnected");
  }

  public async startConsumer<K extends Array<keyof T>>(
    topics: K,
    handleMessage: (message: T[K[number]], topic: K[number]) => Promise<void>,
    options: ConsumerOptions = {},
  ): Promise<void> {
    const { fromBeginning = false, autoCommit = true } = options;

    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: topics as string[],
      fromBeginning,
    });
    this.logger.log(`Consumer subscribed to topics: ${(topics as string[]).join(", ")}`);

    await this.consumer.run({
      autoCommit,
      eachMessage: async ({ topic, message }) => {
        if (!message.value) {
          this.logger.warn(`Received empty message from topic ${topic}`);
          return;
        }
        try {
          const parsedMessage = JSON.parse(
            message.value.toString(),
          ) as T[K[number]];
          await handleMessage(parsedMessage, topic as K[number]);
        } catch (error) {
          this.logger.error(
            `Error processing message from topic ${topic}:`,
            error instanceof Error ? error.stack : String(error),
          );
        }
      },
    });
  }

  public async stopConsumer(): Promise<void> {
    await this.consumer.disconnect();
    this.logger.log("Consumer disconnected");
  }

  public async checkStatus(): Promise<{ topics: string[] }> {
    await this.admin.connect();
    const topics = await this.admin.listTopics();
    await this.admin.disconnect();
    return { topics };
  }

  public getClientId(): ClientId {
    return this.clientId;
  }

  public async disconnect(): Promise<void> {
    await Promise.allSettled([
      this.producer.disconnect(),
      this.consumer.disconnect(),
    ]);
    this.logger.log("All connections closed");
  }
}
