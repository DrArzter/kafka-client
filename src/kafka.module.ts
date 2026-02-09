import {
  Module,
  DynamicModule,
  Provider,
  OnModuleDestroy,
  Inject,
  Optional,
} from "@nestjs/common";
import {
  KafkaClient,
  ClientId,
  GroupId,
  TTopicMessageMap,
} from "./kafka.client";
import { KAFKA_CLIENT } from "./kafka.constants";

export interface KafkaModuleOptions {
  clientId: ClientId;
  groupId: GroupId;
  brokers: string[];
}

export interface KafkaModuleAsyncOptions {
  imports?: any[];
  useFactory: (
    ...args: any[]
  ) => KafkaModuleOptions | Promise<KafkaModuleOptions>;
  inject?: any[];
}

@Module({})
export class KafkaModule implements OnModuleDestroy {
  constructor(
    @Optional() @Inject(KAFKA_CLIENT) private readonly client?: KafkaClient<any>,
  ) {}

  async onModuleDestroy() {
    if (this.client) {
      await this.client.disconnect();
    }
  }

  static register<T extends TTopicMessageMap>(
    options: KafkaModuleOptions,
  ): DynamicModule {
    const kafkaClientProvider: Provider = {
      provide: KAFKA_CLIENT,
      useFactory: async (): Promise<KafkaClient<T>> => {
        const client = new KafkaClient<T>(
          options.clientId,
          options.groupId,
          options.brokers,
        );
        await client.connectProducer();
        return client;
      },
    };

    return {
      module: KafkaModule,
      providers: [kafkaClientProvider],
      exports: [kafkaClientProvider],
    };
  }

  static registerAsync<T extends TTopicMessageMap>(
    asyncOptions: KafkaModuleAsyncOptions,
  ): DynamicModule {
    const kafkaClientProvider: Provider = {
      provide: KAFKA_CLIENT,
      useFactory: async (...args: any[]): Promise<KafkaClient<T>> => {
        const options = await asyncOptions.useFactory(...args);
        const client = new KafkaClient<T>(
          options.clientId,
          options.groupId,
          options.brokers,
        );
        await client.connectProducer();
        return client;
      },
      inject: asyncOptions.inject || [],
    };

    return {
      module: KafkaModule,
      imports: asyncOptions.imports || [],
      providers: [kafkaClientProvider],
      exports: [kafkaClientProvider],
    };
  }
}
