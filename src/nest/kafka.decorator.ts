import { Inject } from "@nestjs/common";
import { getKafkaClientToken } from "./kafka.constants";
import { ConsumerOptions } from "../client/kafka.client";
import { TopicDescriptor, SchemaLike } from "../client/topic";

export const KAFKA_SUBSCRIBER_METADATA = "KAFKA_SUBSCRIBER_METADATA";

export interface KafkaSubscriberMetadata {
  topics: string[];
  schemas?: Map<string, SchemaLike>;
  options?: ConsumerOptions;
  clientName?: string;
  batch?: boolean;
  methodName?: string | symbol;
}

/** Inject a `KafkaClient` instance. Pass a name to target a specific named client. */
export const InjectKafkaClient = (name?: string): ParameterDecorator =>
  Inject(getKafkaClientToken(name));

/**
 * Decorator that auto-subscribes a method to Kafka topics on module init.
 * The decorated method receives `(message, topic)` for each consumed message.
 */
export const SubscribeTo = (
  topics:
    | string
    | string[]
    | TopicDescriptor
    | TopicDescriptor[]
    | (string | TopicDescriptor)[],
  options?: ConsumerOptions & { clientName?: string; batch?: boolean },
): MethodDecorator => {
  const arr = Array.isArray(topics) ? topics : [topics];
  const topicsArray = arr.map((t) => (typeof t === "string" ? t : t.__topic));

  // Extract schemas from descriptors that have them
  const schemas = new Map<string, SchemaLike>();
  for (const t of arr) {
    if (typeof t !== "string" && t.__schema) {
      schemas.set(t.__topic, t.__schema);
    }
  }

  const { clientName, batch, ...consumerOptions } = options || {};

  return (target, propertyKey, _descriptor) => {
    const existing: KafkaSubscriberMetadata[] =
      Reflect.getMetadata(KAFKA_SUBSCRIBER_METADATA, target.constructor) || [];

    Reflect.defineMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      [
        ...existing,
        {
          topics: topicsArray,
          schemas: schemas.size > 0 ? schemas : undefined,
          options: Object.keys(consumerOptions).length
            ? consumerOptions
            : undefined,
          clientName,
          batch,
          methodName: propertyKey,
        },
      ],
      target.constructor,
    );
  };
};
