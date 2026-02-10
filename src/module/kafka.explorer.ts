import { Injectable, OnModuleInit, Logger } from "@nestjs/common";
import { DiscoveryService, ModuleRef } from "@nestjs/core";
import { KafkaClient } from "../client/kafka.client";
import {
  KAFKA_SUBSCRIBER_METADATA,
  KafkaSubscriberMetadata,
} from "../decorators/kafka.decorator";
import { getKafkaClientToken } from "./kafka.constants";

interface SubscriberEntry extends KafkaSubscriberMetadata {
  methodName: string | symbol;
}

/** Discovers `@SubscribeTo()` decorators and wires them to their Kafka clients on startup. */
@Injectable()
export class KafkaExplorer implements OnModuleInit {
  private readonly logger = new Logger(KafkaExplorer.name);

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly moduleRef: ModuleRef,
  ) {}

  async onModuleInit() {
    const providers = this.discoveryService.getProviders();

    for (const wrapper of providers) {
      const { instance } = wrapper;
      if (!instance || typeof instance !== "object") continue;

      const metadata: SubscriberEntry[] | undefined = Reflect.getMetadata(
        KAFKA_SUBSCRIBER_METADATA,
        instance.constructor,
      );

      if (!metadata || metadata.length === 0) continue;

      for (const entry of metadata) {
        const token = getKafkaClientToken(entry.clientName);
        let client: KafkaClient<any>;

        try {
          client = this.moduleRef.get(token, { strict: false });
        } catch {
          this.logger.error(
            `KafkaClient "${entry.clientName || "default"}" not found for @SubscribeTo on ${instance.constructor.name}.${String(entry.methodName)}`,
          );
          continue;
        }

        const handler = (instance as any)[entry.methodName].bind(instance);

        await client.startConsumer(
          entry.topics as any,
          async (message: any, topic: any) => {
            await handler(message, topic);
          },
          entry.options,
        );

        this.logger.log(
          `Registered @SubscribeTo(${entry.topics.join(", ")}) on ${instance.constructor.name}.${String(entry.methodName)}`,
        );
      }
    }
  }
}
