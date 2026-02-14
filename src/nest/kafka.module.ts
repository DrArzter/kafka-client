import { Module, DynamicModule, Provider, Logger } from "@nestjs/common";
import { DiscoveryModule } from "@nestjs/core";
import {
  KafkaClient,
  ClientId,
  GroupId,
  TopicMapConstraint,
} from "../client/kafka.client";
import { getKafkaClientToken } from "./kafka.constants";
import { KafkaExplorer } from "./kafka.explorer";

/** Synchronous configuration for `KafkaModule.register()`. */
export interface KafkaModuleOptions {
  /** Optional name for multi-client setups. Must match `@InjectKafkaClient(name)`. */
  name?: string;
  /** Unique Kafka client identifier. */
  clientId: ClientId;
  /** Consumer group identifier. */
  groupId: GroupId;
  /** List of Kafka broker addresses. */
  brokers: string[];
  /** If true, makes KAFKA_CLIENT available globally without importing KafkaModule in every feature module. */
  isGlobal?: boolean;
  /** Auto-create topics via admin on first use (send/consume). Useful for development. */
  autoCreateTopics?: boolean;
}

/** Async configuration for `KafkaModule.registerAsync()` with dependency injection. */
export interface KafkaModuleAsyncOptions {
  name?: string;
  /** If true, makes KAFKA_CLIENT available globally without importing KafkaModule in every feature module. */
  isGlobal?: boolean;
  /** Auto-create topics via admin on first use (send/consume). Useful for development. */
  autoCreateTopics?: boolean;
  imports?: any[];
  useFactory: (
    ...args: any[]
  ) => KafkaModuleOptions | Promise<KafkaModuleOptions>;
  inject?: any[];
}

/**
 * NestJS dynamic module for registering type-safe Kafka clients.
 * Use `register()` for static config or `registerAsync()` for DI-based config.
 */
@Module({})
export class KafkaModule {
  /** Register a Kafka client with static options. */
  static register<T extends TopicMapConstraint<T>>(
    options: KafkaModuleOptions,
  ): DynamicModule {
    const token = getKafkaClientToken(options.name);

    const kafkaClientProvider: Provider = {
      provide: token,
      useFactory: async (): Promise<KafkaClient<T>> => {
        const client = new KafkaClient<T>(
          options.clientId,
          options.groupId,
          options.brokers,
          {
            autoCreateTopics: options.autoCreateTopics,
            logger: new Logger(`KafkaClient:${options.clientId}`),
          },
        );
        await client.connectProducer();
        return client;
      },
    };

    const destroyProvider: Provider = {
      provide: `${token}_DESTROY`,
      useFactory: (client: KafkaClient<T>) => ({
        onModuleDestroy: () => client.disconnect(),
      }),
      inject: [token],
    };

    return {
      global: options.isGlobal ?? false,
      module: KafkaModule,
      imports: [DiscoveryModule],
      providers: [kafkaClientProvider, destroyProvider, KafkaExplorer],
      exports: [kafkaClientProvider],
    };
  }

  /** Register a Kafka client with async/factory-based options. */
  static registerAsync<T extends TopicMapConstraint<T>>(
    asyncOptions: KafkaModuleAsyncOptions,
  ): DynamicModule {
    const token = getKafkaClientToken(asyncOptions.name);

    const kafkaClientProvider: Provider = {
      provide: token,
      useFactory: async (...args: any[]): Promise<KafkaClient<T>> => {
        const options = await asyncOptions.useFactory(...args);
        const client = new KafkaClient<T>(
          options.clientId,
          options.groupId,
          options.brokers,
          {
            autoCreateTopics: options.autoCreateTopics,
            logger: new Logger(`KafkaClient:${options.clientId}`),
          },
        );
        await client.connectProducer();
        return client;
      },
      inject: asyncOptions.inject || [],
    };

    const destroyProvider: Provider = {
      provide: `${token}_DESTROY`,
      useFactory: (client: KafkaClient<T>) => ({
        onModuleDestroy: () => client.disconnect(),
      }),
      inject: [token],
    };

    return {
      global: asyncOptions.isGlobal ?? false,
      module: KafkaModule,
      imports: [...(asyncOptions.imports || []), DiscoveryModule],
      providers: [kafkaClientProvider, destroyProvider, KafkaExplorer],
      exports: [kafkaClientProvider],
    };
  }
}
