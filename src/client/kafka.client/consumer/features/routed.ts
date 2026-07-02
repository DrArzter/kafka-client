import type { KafkaClientContext } from "../../context";
import type {
  TopicMapConstraint,
  ConsumerOptions,
  ConsumerHandle,
  RoutingOptions,
} from "../../../types";
import type { EventEnvelope } from "../../../message/envelope";
import { startConsumerImpl } from "../start";

// ── startRoutedConsumer ───────────────────────────────────────────────────────

export async function startRoutedConsumerImpl<
  T extends TopicMapConstraint<T>,
  K extends Array<keyof T>,
>(
  ctx: KafkaClientContext<T>,
  topics: K,
  routing: RoutingOptions<T[K[number]]>,
  options?: ConsumerOptions<T>,
): Promise<ConsumerHandle> {
  const { header, routes, fallback } = routing;
  const handleMessage = async (
    envelope: EventEnvelope<T[K[number]]>,
  ): Promise<void> => {
    const headerValue = envelope.headers[header];
    const routeHandler =
      headerValue === undefined ? undefined : routes[headerValue];
    if (routeHandler) {
      await routeHandler(envelope);
    } else {
      await fallback?.(envelope);
    }
  };
  return startConsumerImpl(ctx, topics, handleMessage, options);
}
