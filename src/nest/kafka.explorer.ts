import { Inject, Injectable, OnModuleInit, Logger } from "@nestjs/common";
import { DiscoveryService, ModuleRef } from "@nestjs/core";
import { KafkaClient } from "../client/kafka.client";
import {
  KAFKA_SUBSCRIBER_METADATA,
  KafkaSubscriberMetadata,
} from "./kafka.decorator";
import { getKafkaClientToken } from "./kafka.constants";

interface SubscriberEntry extends KafkaSubscriberMetadata {
  methodName: string | symbol;
}

/** Discovers `@SubscribeTo()` decorators and wires them to their Kafka clients on startup. */
@Injectable()
export class KafkaExplorer implements OnModuleInit {
  private readonly logger = new Logger(KafkaExplorer.name);

  constructor(
    @Inject(DiscoveryService)
    private readonly discoveryService: DiscoveryService,
    @Inject(ModuleRef)
    private readonly moduleRef: ModuleRef,
  ) {}

  /**
   * Scan all NestJS providers for `@SubscribeTo()` metadata and wire each decorated
   * method to its Kafka client via `startConsumer` or `startBatchConsumer`.
   *
   * Called automatically by the NestJS lifecycle — do not invoke manually.
   */
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

        const consumerOptions = { ...entry.options };
        if (entry.schemas) {
          consumerOptions.schemas = entry.schemas;
        }

        if (entry.batch) {
          await client.startBatchConsumer(
            entry.topics as any,
            async (envelopes: any[], meta: any) => {
              await handler(envelopes, meta);
            },
            consumerOptions,
          );
        } else {
          await client.startConsumer(
            entry.topics as any,
            async (envelope: any) => {
              await handler(envelope);
            },
            consumerOptions,
          );
        }

        this.logger.log(
          `Registered @SubscribeTo(${entry.topics.join(", ")})${entry.batch ? " [batch]" : ""} on ${instance.constructor.name}.${String(entry.methodName)}`,
        );
      }
    }
  }
}
