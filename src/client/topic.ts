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
}

/**
 * Define a typed topic descriptor.
 *
 * @example
 * ```ts
 * const OrderCreated = topic('order.created')<{ orderId: string; amount: number }>();
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
  return <M extends Record<string, any>>(): TopicDescriptor<N, M> => ({
    __topic: name,
    __type: undefined as unknown as M,
  });
}

/**
 * Build a topic-message map type from a union of TopicDescriptors.
 *
 * @example
 * ```ts
 * const OrderCreated = topic('order.created')<{ orderId: string }>();
 * const OrderCompleted = topic('order.completed')<{ completedAt: string }>();
 *
 * type MyTopics = TopicsFrom<typeof OrderCreated | typeof OrderCompleted>;
 * // { 'order.created': { orderId: string }; 'order.completed': { completedAt: string } }
 * ```
 */
export type TopicsFrom<D extends TopicDescriptor<any, any>> = {
  [K in D as K["__topic"]]: K["__type"];
};
