import { Inject } from "@nestjs/common";
import { getKafkaClientToken } from "../module/kafka.constants";
import { ConsumerOptions } from "../client/kafka.client";

export const KAFKA_SUBSCRIBER_METADATA = "KAFKA_SUBSCRIBER_METADATA";

export interface KafkaSubscriberMetadata {
  topics: string[];
  options?: ConsumerOptions;
  clientName?: string;
}

/** Inject a `KafkaClient` instance. Pass a name to target a specific named client. */
export const InjectKafkaClient = (name?: string): ParameterDecorator =>
  Inject(getKafkaClientToken(name));

/**
 * Decorator that auto-subscribes a method to Kafka topics on module init.
 * The decorated method receives `(message, topic)` for each consumed message.
 */
export const SubscribeTo = (
  topics: string | string[],
  options?: ConsumerOptions & { clientName?: string },
): MethodDecorator => {
  const topicsArray = Array.isArray(topics) ? topics : [topics];
  const { clientName, ...consumerOptions } = options || {};

  return (target, propertyKey, _descriptor) => {
    const existing: KafkaSubscriberMetadata[] =
      Reflect.getMetadata(KAFKA_SUBSCRIBER_METADATA, target.constructor) || [];

    Reflect.defineMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      [
        ...existing,
        {
          topics: topicsArray,
          options: Object.keys(consumerOptions).length
            ? consumerOptions
            : undefined,
          clientName,
          methodName: propertyKey,
        },
      ],
      target.constructor,
    );
  };
};
