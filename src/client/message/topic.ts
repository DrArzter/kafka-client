import type { MessageHeaders } from "../types";

/**
 * Context passed as the second argument to `SchemaLike.parse()`.
 * Enables schema-registry adapters, version-aware migration, and
 * header-driven parsing without coupling validators to Kafka internals.
 *
 * All fields are optional-friendly — validators that don't need the context
 * can simply ignore the second argument.
 */
export interface SchemaParseContext {
  /** Topic the message was produced to / consumed from. */
  topic: string;
  /** Decoded message headers (envelope headers included). */
  headers: MessageHeaders;
  /** Value of the `x-schema-version` header, defaults to `1`. */
  version: number;
}

/**
 * Any validation library with a `.parse()` method.
 * Works with Zod, Valibot, ArkType, or any custom validator.
 *
 * The optional `ctx` argument carries topic/header/version metadata so
 * validators can perform schema-registry lookups or version-aware migrations.
 * Existing validators that only use the first argument continue to work
 * unchanged — the second argument is silently ignored.
 *
 * @example
 * ```ts
 * import { z } from 'zod';
 * const schema: SchemaLike<{ id: string }> = z.object({ id: z.string() });
 *
 * // Context-aware validator:
 * const schema: SchemaLike<MyType> = {
 *   parse(data, ctx) {
 *     const version = ctx?.version ?? 1;
 *     return version >= 2 ? migrateV1toV2(data) : validateV1(data);
 *   }
 * };
 * ```
 */
export interface SchemaLike<T = any> {
  parse(data: unknown, ctx?: SchemaParseContext): T | Promise<T>;
}

/** Infer the output type from a SchemaLike. */
export type InferSchema<S extends SchemaLike> =
  S extends SchemaLike<infer T> ? T : never;

/**
 * A typed topic descriptor that pairs a topic name with its message type.
 * Created via the `topic()` factory function.
 *
 * @typeParam N - The literal topic name string.
 * @typeParam M - The message payload type for this topic.
 */
export interface TopicDescriptor<
  N extends string = string,
  M extends Record<string, any> = Record<string, any>,
> {
  readonly __topic: N;
  /** @internal Phantom type — never has a real value at runtime. */
  readonly __type: M;
  /** Runtime schema validator. Present only when created via `topic().schema()`. */
  readonly __schema?: SchemaLike<M>;
}

/**
 * Define a typed topic descriptor.
 *
 * @example
 * ```ts
 * // Without schema — explicit type via .type<T>():
 * const OrderCreated = topic('order.created').type<{ orderId: string; amount: number }>();
 *
 * // With schema — type inferred from schema:
 * const OrderCreated = topic('order.created').schema(z.object({
 *   orderId: z.string(),
 *   amount: z.number(),
 * }));
 *
 * // Use with KafkaClient:
 * await kafka.sendMessage(OrderCreated, { orderId: '123', amount: 100 });
 *
 * // Use with @SubscribeTo:
 * @SubscribeTo(OrderCreated)
 * async handleOrder(msg) { ... }
 * ```
 */
export function topic<N extends string>(name: N) {
  return {
    /** Provide an explicit message type without a runtime schema. */
    type: <M extends Record<string, any>>(): TopicDescriptor<N, M> => ({
      __topic: name,
      __type: undefined as unknown as M,
    }),

    schema: <S extends SchemaLike<Record<string, any>>>(
      schema: S,
    ): TopicDescriptor<N, InferSchema<S>> => ({
      __topic: name,
      __type: undefined as unknown as InferSchema<S>,
      __schema: schema as unknown as SchemaLike<InferSchema<S>>,
    }),
  };
}

/**
 * Build a topic-message map type from a union of TopicDescriptors.
 *
 * @example
 * ```ts
 * const OrderCreated = topic('order.created').type<{ orderId: string }>();
 * const OrderCompleted = topic('order.completed').type<{ completedAt: string }>();
 *
 * type MyTopics = TopicsFrom<typeof OrderCreated | typeof OrderCompleted>;
 * // { 'order.created': { orderId: string }; 'order.completed': { completedAt: string } }
 * ```
 */
export type TopicsFrom<D extends TopicDescriptor<any, any>> = {
  [K in D as K["__topic"]]: K["__type"];
};
