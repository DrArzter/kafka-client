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
 * the caller.
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
 * Start a single retry level consumer on `<topic>.retry.<level>`.
 *
 * At-least-once guarantee:
 *   - The partition is paused while waiting for the scheduled delay window.
 *     The offset is NOT committed during this window, so a process crash
 *     causes the message to be redelivered on restart.
 *   - The offset is committed only after the handler succeeds OR after the
 *     message has been safely routed to the next level / DLQ.
 *
 * Flow per message:
 *   1. If `x-retry-after` is in the future: pause partition → sleep → resume.
 *   2. Validate and call the original handler.
 *   3. On success: commit offset.
 *   4. On failure:
 *      - Not exhausted → route to `<topic>.retry.<level+1>`
 *      - Exhausted + dlq → send to `<topic>.dlq`
 *      - Exhausted, no dlq → call `onMessageLost`
 *   5. Commit offset after routing (message safely in next destination).
 */
async function startLevelConsumer(
  level: number,
  levelTopics: string[],
  levelGroupId: string,
  originalTopics: string[],
  handleMessage: (envelope: EventEnvelope<any>) => Promise<void>,
  retry: { maxRetries: number; backoffMs?: number; maxBackoffMs?: number },
  dlq: boolean,
  interceptors: ConsumerInterceptor<any>[],
  schemaMap: Map<string, SchemaLike>,
  deps: RetryTopicDeps,
  assignmentTimeoutMs?: number,
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

  const backoffMs = retry.backoffMs ?? 1_000;
  const maxBackoffMs = retry.maxBackoffMs ?? 30_000;
  const pipelineDeps = { logger, producer, instrumentation, onMessageLost };

  for (const lt of levelTopics) {
    await ensureTopic(lt);
  }

  // autoCommit: false — offsets are committed manually after processing.
  const consumer = getOrCreateConsumer(levelGroupId, false, false);
  await consumer.connect();
  await subscribeWithRetry(consumer, levelTopics, logger);

  await consumer.run({
    eachMessage: async ({ topic: levelTopic, partition, message }) => {
      const nextOffset = {
        topic: levelTopic,
        partition,
        offset: (parseInt(message.offset, 10) + 1).toString(),
      };

      if (!message.value) {
        await consumer.commitOffsets([nextOffset]);
        return;
      }

      const headers = decodeHeaders(message.headers);
      const retryAfter = parseInt(
        (headers[RETRY_HEADER_AFTER] as string | undefined) ?? "0",
        10,
      );
      const remaining = retryAfter - Date.now();

      // Pause this partition for the scheduled delay.
      // The offset is not committed yet — a crash here causes redelivery (at-least-once).
      if (remaining > 0) {
        consumer.pause([{ topic: levelTopic, partitions: [partition] }]);
        await sleep(remaining);
        consumer.resume([{ topic: levelTopic, partitions: [partition] }]);
      }

      const raw = message.value.toString();
      const parsed = parseJsonMessage(raw, levelTopic, logger);
      if (parsed === null) {
        await consumer.commitOffsets([nextOffset]);
        return;
      }

      const currentMaxRetries = parseInt(
        (headers[RETRY_HEADER_MAX_RETRIES] as string | undefined) ??
          String(retry.maxRetries),
        10,
      );
      const originalTopic =
        (headers[RETRY_HEADER_ORIGINAL_TOPIC] as string | undefined) ??
        levelTopic.replace(/\.retry\.\d+$/, "");

      const validated = await validateWithSchema(
        parsed,
        raw,
        originalTopic,
        schemaMap,
        interceptors,
        dlq,
        { ...pipelineDeps, originalHeaders: headers },
      );
      if (validated === null) {
        await consumer.commitOffsets([nextOffset]);
        return;
      }

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

      if (!error) {
        await consumer.commitOffsets([nextOffset]);
        return;
      }

      const exhausted = level >= currentMaxRetries;
      const reportedError =
        exhausted && currentMaxRetries > 1
          ? new KafkaRetryExhaustedError(
              originalTopic,
              [envelope.payload],
              currentMaxRetries,
              { cause: error },
            )
          : error;

      await notifyInterceptorsOnError([envelope], interceptors, reportedError);

      logger.error(
        `Retry consumer error for ${originalTopic} (level ${level}/${currentMaxRetries}):`,
        error.stack,
      );

      if (!exhausted) {
        const nextLevel = level + 1;
        // Exponent = current level: level 1 → delay cap = backoffMs * 2^1, etc.
        const cap = Math.min(backoffMs * 2 ** level, maxBackoffMs);
        const delay = Math.floor(Math.random() * cap);
        await sendToRetryTopic(
          originalTopic,
          [raw],
          nextLevel,
          currentMaxRetries,
          delay,
          headers,
          pipelineDeps,
        );
      } else if (dlq) {
        await sendToDlq(originalTopic, raw, pipelineDeps, {
          error,
          // +1 to account for the main consumer's initial attempt before routing.
          attempt: level + 1,
          originalHeaders: headers,
        });
      } else {
        await onMessageLost?.({
          topic: originalTopic,
          error,
          attempt: level,
          headers,
        });
      }

      // Commit after routing — the message is safely in the next destination.
      // If we crash between routing and this commit, the message is redelivered
      // and routed again (duplicate in the next level), which is acceptable for
      // at-least-once semantics.
      await consumer.commitOffsets([nextOffset]);
    },
  });

  runningConsumers.set(levelGroupId, "eachMessage");

  await waitForPartitionAssignment(consumer, levelTopics, logger, assignmentTimeoutMs);

  logger.log(
    `Retry level ${level}/${retry.maxRetries} consumer started for: ${originalTopics.join(", ")} (group: ${levelGroupId})`,
  );
}

/**
 * Start one consumer per retry level on `<topic>.retry.<level>` topics.
 *
 * With `maxRetries: N`, creates N consumers:
 *   - `${groupId}-retry.1` → `<topic>.retry.1`
 *   - `${groupId}-retry.2` → `<topic>.retry.2`
 *   - …
 *   - `${groupId}-retry.N` → `<topic>.retry.N`
 *
 * Each level consumer uses pause/sleep/resume to honour the scheduled delay
 * without committing the offset early, guaranteeing at-least-once delivery
 * of retry messages even across process restarts.
 *
 * Returns the list of started consumer group IDs so the caller can stop
 * them selectively via `stopConsumer`.
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
  assignmentTimeoutMs?: number,
): Promise<string[]> {
  const levelGroupIds: string[] = [];

  for (let level = 1; level <= retry.maxRetries; level++) {
    const levelTopics = originalTopics.map((t) => `${t}.retry.${level}`);
    const levelGroupId = `${originalGroupId}-retry.${level}`;

    await startLevelConsumer(
      level,
      levelTopics,
      levelGroupId,
      originalTopics,
      handleMessage,
      retry,
      dlq,
      interceptors,
      schemaMap,
      deps,
      assignmentTimeoutMs,
    );

    levelGroupIds.push(levelGroupId);
  }

  return levelGroupIds;
}
