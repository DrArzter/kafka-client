import { Inject } from "@nestjs/common";
import { getKafkaClientToken } from "./kafka.constants";
import { ConsumerOptions } from "../client/kafka.client";
import { TopicDescriptor, SchemaLike } from "../client/message/topic";

/** Reflect metadata key used to store `@SubscribeTo` entries on a class constructor. */
export const KAFKA_SUBSCRIBER_METADATA = "KAFKA_SUBSCRIBER_METADATA";

/** Internal shape stored per `@SubscribeTo()` decoration on a class. */
export interface KafkaSubscriberMetadata {
  /** Resolved topic name strings (descriptors are unwrapped to their `__topic` string). */
  topics: string[];
  /** Per-topic schema validators extracted from `TopicDescriptor` objects (if any). */
  schemas?: Map<string, SchemaLike>;
  /** Additional consumer options forwarded to `startConsumer` / `startBatchConsumer`. */
  options?: ConsumerOptions;
  /** Named client identifier — resolves to `KAFKA_CLIENT_<clientName>` in the DI container. */
  clientName?: string;
  /** When `true`, routes to `startBatchConsumer` instead of `startConsumer`. */
  batch?: boolean;
  /** Name of the decorated method on the provider class. */
  methodName?: string | symbol;
}

/** Inject a `KafkaClient` instance. Pass a name to target a specific named client. */
export const InjectKafkaClient = (name?: string): ParameterDecorator =>
  Inject(getKafkaClientToken(name));

/**
 * Method decorator that auto-subscribes the decorated method to one or more Kafka topics
 * when the NestJS module initialises.
 *
 * The decorated method receives a fully-decoded `EventEnvelope` for each message
 * (or an array of envelopes + `BatchMeta` when `batch: true`).
 *
 * @param topics One or more topic names or `TopicDescriptor` objects. Schemas embedded in
 *   descriptors are automatically extracted and forwarded to the consumer.
 * @param options Consumer and routing options:
 *   - All `ConsumerOptions` fields (`groupId`, `retry`, `dlq`, `fromBeginning`, …)
 *   - `clientName` — target a named `KafkaClient` (resolves `KAFKA_CLIENT_<name>` from the DI container)
 *   - `batch` — use `startBatchConsumer` instead of `startConsumer`
 *
 * @example
 * ```ts
 * @SubscribeTo('orders.created', { groupId: 'orders-svc', retry: { maxRetries: 3 } })
 * async handleOrder(envelope: EventEnvelope<Order>) { ... }
 *
 * @SubscribeTo(OrdersTopic, { batch: true })
 * async handleBatch(envelopes: EventEnvelope<Order>[], meta: BatchMeta) { ... }
 * ```
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
