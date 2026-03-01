import type { KafkaJS } from "@confluentinc/kafka-javascript";
import type { KafkaLogger, SubscribeRetryOptions } from "../types";
import { toError, sleep } from "./pipeline";

export async function subscribeWithRetry(
  consumer: KafkaJS.Consumer,
  topics: (string | RegExp)[],
  logger: KafkaLogger,
  retryOpts?: SubscribeRetryOptions,
): Promise<void> {
  const maxAttempts = retryOpts?.retries ?? 5;
  const backoffMs = retryOpts?.backoffMs ?? 5000;
  const displayTopics = topics
    .map((t) => (t instanceof RegExp ? t.toString() : t))
    .join(", ");

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await consumer.subscribe({ topics });
      return;
    } catch (error) {
      if (attempt === maxAttempts) throw error;
      const msg = toError(error).message;
      const delay = Math.floor(Math.random() * backoffMs);
      logger.warn(
        `Failed to subscribe to [${displayTopics}] (attempt ${attempt}/${maxAttempts}): ${msg}. Retrying in ${delay}ms...`,
      );
      await sleep(delay);
    }
  }
}
