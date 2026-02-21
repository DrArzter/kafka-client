import { KafkaJS } from "@confluentinc/kafka-javascript";
type Consumer = KafkaJS.Consumer;
type Producer = KafkaJS.Producer;
import {
  decodeHeaders,
  extractEnvelope,
  runWithEnvelopeContext,
} from "../message/envelope";
import type { EventEnvelope } from "../message/envelope";
import {
  parseJsonMessage,
  validateWithSchema,
  runHandlerWithPipeline,
  notifyInterceptorsOnError,
  sendToDlq,
  sendToRetryTopic,
  sleep,
  RETRY_HEADER_ATTEMPT,
  RETRY_HEADER_AFTER,
  RETRY_HEADER_MAX_RETRIES,
  RETRY_HEADER_ORIGINAL_TOPIC,
} from "../consumer/pipeline";
import { KafkaRetryExhaustedError } from "../errors";
import { subscribeWithRetry } from "../consumer/subscribe-retry";
import type { SchemaLike } from "../message/topic";
import type {
  ConsumerInterceptor,
  KafkaClientOptions,
  KafkaInstrumentation,
  KafkaLogger,
} from "../types";

export type RetryTopicDeps = {
  logger: KafkaLogger;
  producer: Producer;
  instrumentation: KafkaInstrumentation[];
  onMessageLost: KafkaClientOptions["onMessageLost"];
  ensureTopic: (topic: string) => Promise<void>;
  getOrCreateConsumer: (
    groupId: string,
    fromBeginning: boolean,
    autoCommit: boolean,
  ) => Consumer;
  runningConsumers: Map<string, "eachMessage" | "eachBatch">;
};

/**
 * Poll `consumer.assignment()` until the consumer has received at least one
 * partition for the given topics, then return. Logs a warning and returns
 * (rather than throwing) on timeout so that a slow broker does not break
 * the caller — in the worst case a message sent immediately after would be
 * missed, which is the same behaviour as before this guard was added.
 */
export async function waitForPartitionAssignment(
  consumer: Consumer,
  topics: string[],
  logger: KafkaLogger,
  timeoutMs = 10_000,
): Promise<void> {
  const topicSet = new Set(topics);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const assigned: { topic: string; partition: number }[] =
        consumer.assignment();
      if (assigned.some((a) => topicSet.has(a.topic))) return;
    } catch {
      // consumer.assignment() throws if not yet in CONNECTED state — keep polling
    }
    await sleep(200);
  }
  logger.warn(
    `Retry consumer did not receive partition assignments for [${topics.join(", ")}] within ${timeoutMs}ms`,
  );
}

/**
 * Auto-start companion consumers on `<topic>.retry` for each original topic.
 * Called by `startConsumer` when `retryTopics: true`.
 *
 * Flow per message:
 *   1. Sleep until `x-retry-after` (scheduled by the main consumer or previous retry hop)
 *   2. Call the original handler
 *   3. On failure: if retries remain → re-send to `<originalTopic>.retry` with incremented attempt
 *                  if exhausted       → DLQ or onMessageLost
 */
export async function startRetryTopicConsumers(
  originalTopics: string[],
  originalGroupId: string,
  handleMessage: (envelope: EventEnvelope<any>) => Promise<void>,
  retry: { maxRetries: number; backoffMs?: number; maxBackoffMs?: number },
  dlq: boolean,
  interceptors: ConsumerInterceptor<any>[],
  schemaMap: Map<string, SchemaLike>,
  deps: RetryTopicDeps,
): Promise<void> {
  const {
    logger,
    producer,
    instrumentation,
    onMessageLost,
    ensureTopic,
    getOrCreateConsumer,
    runningConsumers,
  } = deps;

  const retryTopicNames = originalTopics.map((t) => `${t}.retry`);
  const retryGroupId = `${originalGroupId}-retry`;
  const backoffMs = retry.backoffMs ?? 1_000;
  const maxBackoffMs = retry.maxBackoffMs ?? 30_000;
  const pipelineDeps = { logger, producer, instrumentation, onMessageLost };

  for (const rt of retryTopicNames) {
    await ensureTopic(rt);
  }

  const consumer = getOrCreateConsumer(retryGroupId, false, true);
  await consumer.connect();
  await subscribeWithRetry(consumer, retryTopicNames, logger);

  await consumer.run({
    eachMessage: async ({ topic: retryTopic, partition, message }) => {
      if (!message.value) return;

      const raw = message.value.toString();
      const parsed = parseJsonMessage(raw, retryTopic, logger);
      if (parsed === null) return;

      const headers = decodeHeaders(message.headers);
      const originalTopic =
        (headers[RETRY_HEADER_ORIGINAL_TOPIC] as string | undefined) ??
        retryTopic.replace(/\.retry$/, "");
      const currentAttempt = parseInt(
        (headers[RETRY_HEADER_ATTEMPT] as string | undefined) ?? "1",
        10,
      );
      const maxRetries = parseInt(
        (headers[RETRY_HEADER_MAX_RETRIES] as string | undefined) ??
          String(retry.maxRetries),
        10,
      );
      const retryAfter = parseInt(
        (headers[RETRY_HEADER_AFTER] as string | undefined) ?? "0",
        10,
      );

      // Pause only this partition for the scheduled delay so that other
      // topic-partitions on the same retry consumer continue processing.
      const remaining = retryAfter - Date.now();
      if (remaining > 0) {
        consumer.pause([{ topic: retryTopic, partitions: [partition] }]);
        await sleep(remaining);
        consumer.resume([{ topic: retryTopic, partitions: [partition] }]);
      }

      // Validate schema against original topic's schema (if any)
      const validated = await validateWithSchema(
        parsed,
        raw,
        originalTopic,
        schemaMap,
        interceptors,
        dlq,
        { ...pipelineDeps, originalHeaders: headers },
      );
      if (validated === null) return;

      // Build envelope with originalTopic so correlationId/traceparent are correct
      const envelope = extractEnvelope(
        validated,
        headers,
        originalTopic,
        partition,
        message.offset,
      );

      const error = await runHandlerWithPipeline(
        () =>
          runWithEnvelopeContext(
            {
              correlationId: envelope.correlationId,
              traceparent: envelope.traceparent,
            },
            () => handleMessage(envelope),
          ),
        [envelope],
        interceptors,
        instrumentation,
      );

      if (error) {
        const nextAttempt = currentAttempt + 1;
        const exhausted = currentAttempt >= maxRetries;
        const reportedError =
          exhausted && maxRetries > 1
            ? new KafkaRetryExhaustedError(
                originalTopic,
                [envelope.payload],
                maxRetries,
                { cause: error },
              )
            : error;

        await notifyInterceptorsOnError(
          [envelope],
          interceptors,
          reportedError,
        );

        logger.error(
          `Retry consumer error for ${originalTopic} (attempt ${currentAttempt}/${maxRetries}):`,
          error.stack,
        );

        if (!exhausted) {
          // currentAttempt is the hop that just failed (1-based).
          // Before hop N+1 we want backoffMs * 2^N, so exponent = currentAttempt.
          const cap = Math.min(backoffMs * 2 ** currentAttempt, maxBackoffMs);
          const delay = Math.floor(Math.random() * cap);
          await sendToRetryTopic(
            originalTopic,
            [raw],
            nextAttempt,
            maxRetries,
            delay,
            headers,
            pipelineDeps,
          );
        } else if (dlq) {
          await sendToDlq(originalTopic, raw, pipelineDeps, {
            error,
            // +1 to account for the main consumer's initial attempt before
            // routing to the retry topic, making this consistent with the
            // in-process retry path where attempt counts all tries.
            attempt: currentAttempt + 1,
            originalHeaders: headers,
          });
        } else {
          await onMessageLost?.({
            topic: originalTopic,
            error,
            attempt: currentAttempt,
            headers,
          });
        }
      }
    },
  });

  runningConsumers.set(retryGroupId, "eachMessage");

  // Block until the retry consumer has received at least one partition assignment.
  // consumer.run() starts the group-join protocol asynchronously; without this wait,
  // the caller may send a message to the original topic before the retry consumer
  // has established its "latest" offset on the (initially empty) retry partition,
  // causing it to skip any retry messages produced in that window.
  await waitForPartitionAssignment(consumer, retryTopicNames, logger);

  logger.log(
    `Retry topic consumers started for: ${originalTopics.join(", ")} (group: ${retryGroupId})`,
  );
}
