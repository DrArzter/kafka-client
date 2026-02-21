import type { KafkaJS } from "@confluentinc/kafka-javascript";
import type { KafkaLogger, SubscribeRetryOptions } from "../types";
import { toError, sleep } from "./pipeline";

export async function subscribeWithRetry(
  consumer: KafkaJS.Consumer,
  topics: string[],
  logger: KafkaLogger,
  retryOpts?: SubscribeRetryOptions,
): Promise<void> {
  const maxAttempts = retryOpts?.retries ?? 5;
  const backoffMs = retryOpts?.backoffMs ?? 5000;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await consumer.subscribe({ topics });
      return;
    } catch (error) {
      if (attempt === maxAttempts) throw error;
      const msg = toError(error).message;
      logger.warn(
        `Failed to subscribe to [${topics.join(", ")}] (attempt ${attempt}/${maxAttempts}): ${msg}. Retrying in ${backoffMs}ms...`,
      );
      await sleep(backoffMs);
    }
  }
}
