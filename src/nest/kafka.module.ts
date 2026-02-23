import { Module, DynamicModule, Provider, Logger } from "@nestjs/common";
import { DiscoveryModule } from "@nestjs/core";
import {
  KafkaClient,
  ClientId,
  GroupId,
  TopicMapConstraint,
  KafkaInstrumentation,
  KafkaClientOptions,
} from "../client/kafka.client";
import { getKafkaClientToken } from "./kafka.constants";
import { KafkaExplorer } from "./kafka.explorer";

/** Shared configuration fields for both `register()` and `registerAsync()`. */
interface KafkaModuleBaseOptions {
  /** Optional name for multi-client setups. Must match `@InjectKafkaClient(name)`. */
  name?: string;
  /** If true, makes KAFKA_CLIENT available globally without importing KafkaModule in every feature module. */
  isGlobal?: boolean;
}

/** Synchronous configuration for `KafkaModule.register()`. */
export interface KafkaModuleOptions extends KafkaModuleBaseOptions {
  /** Unique Kafka client identifier. */
  clientId: ClientId;
  /** Consumer group identifier. */
  groupId: GroupId;
  /** List of Kafka broker addresses. */
  brokers: string[];
  /** Auto-create topics via admin on first use (send/consume). Useful for development. */
  autoCreateTopics?: boolean;
  /** When `true`, string topic keys are validated against any schema previously registered via a TopicDescriptor. Default: `true`. */
  strictSchemas?: boolean;
  /** Number of partitions for auto-created topics. Default: `1`. */
  numPartitions?: number;
  /** Client-wide instrumentation hooks (e.g. OTel). Applied to both send and consume paths. */
  instrumentation?: KafkaInstrumentation[];
  /** Called when a message is dropped without being sent to a DLQ. @see `KafkaClientOptions.onMessageLost` */
  onMessageLost?: KafkaClientOptions["onMessageLost"];
  /** Called whenever a consumer group rebalance occurs. @see `KafkaClientOptions.onRebalance` */
  onRebalance?: KafkaClientOptions["onRebalance"];
}

/** Async configuration for `KafkaModule.registerAsync()` with dependency injection. */
export interface KafkaModuleAsyncOptions extends KafkaModuleBaseOptions {
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
      useFactory: () => KafkaModule.buildClient<T>(options),
    };

    return {
      global: options.isGlobal ?? false,
      module: KafkaModule,
      imports: [DiscoveryModule],
      providers: [kafkaClientProvider, KafkaExplorer],
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
      useFactory: async (...args: any[]): Promise<KafkaClient<T>> =>
        KafkaModule.buildClient<T>(await asyncOptions.useFactory(...args)),
      inject: asyncOptions.inject || [],
    };

    return {
      global: asyncOptions.isGlobal ?? false,
      module: KafkaModule,
      imports: [...(asyncOptions.imports || []), DiscoveryModule],
      providers: [kafkaClientProvider, KafkaExplorer],
      exports: [kafkaClientProvider],
    };
  }

  private static async buildClient<T extends TopicMapConstraint<T>>(
    options: KafkaModuleOptions,
  ): Promise<KafkaClient<T>> {
    const client = new KafkaClient<T>(
      options.clientId,
      options.groupId,
      options.brokers,
      {
        autoCreateTopics: options.autoCreateTopics,
        strictSchemas: options.strictSchemas,
        numPartitions: options.numPartitions,
        instrumentation: options.instrumentation,
        onMessageLost: options.onMessageLost,
        onRebalance: options.onRebalance,
        logger: new Logger(`KafkaClient:${options.clientId}`),
      },
    );
    await client.connectProducer();
    return client;
  }

}
