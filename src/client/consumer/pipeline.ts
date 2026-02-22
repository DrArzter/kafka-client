import type { KafkaJS } from "@confluentinc/kafka-javascript";
type Producer = KafkaJS.Producer;
import type { EventEnvelope } from "../message/envelope";
import { extractEnvelope } from "../message/envelope";
import { KafkaRetryExhaustedError, KafkaValidationError } from "../errors";
import type { SchemaLike } from "../message/topic";
import type {
  ConsumerInterceptor,
  KafkaInstrumentation,
  KafkaLogger,
  MessageHeaders,
  MessageLostContext,
  RetryOptions,
  TopicMapConstraint,
} from "../types";

// ── Helpers ──────────────────────────────────────────────────────────

export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── JSON parsing ────────────────────────────────────────────────────

/** Parse raw message as JSON. Returns null on failure (logs error). */
export function parseJsonMessage(
  raw: string,
  topic: string,
  logger: KafkaLogger,
): any | null {
  try {
    return JSON.parse(raw);
  } catch (error) {
    logger.error(
      `Failed to parse message from topic ${topic}:`,
      toError(error).stack,
    );
    return null;
  }
}

// ── Schema validation ───────────────────────────────────────────────

/**
 * Validate a parsed message against the schema map.
 * On failure: logs error, sends to DLQ if enabled, calls interceptor.onError.
 * Returns validated message or null.
 */
export async function validateWithSchema<T extends TopicMapConstraint<T>>(
  message: any,
  raw: string,
  topic: string,
  schemaMap: Map<string, SchemaLike>,
  interceptors: ConsumerInterceptor<T>[],
  dlq: boolean,
  deps: {
    logger: KafkaLogger;
    producer: Producer;
    onMessageLost?: (ctx: MessageLostContext) => void | Promise<void>;
    originalHeaders?: MessageHeaders;
  },
): Promise<any | null> {
  const schema = schemaMap.get(topic);
  if (!schema) return message;

  try {
    return await schema.parse(message);
  } catch (error) {
    const err = toError(error);
    const validationError = new KafkaValidationError(topic, message, {
      cause: err,
    });
    deps.logger.error(
      `Schema validation failed for topic ${topic}:`,
      err.message,
    );
    if (dlq) {
      await sendToDlq(topic, raw, deps, {
        error: validationError,
        attempt: 0,
        originalHeaders: deps.originalHeaders,
      });
    } else {
      await deps.onMessageLost?.({
        topic,
        error: validationError,
        attempt: 0,
        headers: deps.originalHeaders ?? {},
      });
    }
    // Validation errors don't have an envelope yet — call onError with a minimal envelope
    const errorEnvelope = extractEnvelope(
      message,
      deps.originalHeaders ?? {},
      topic,
      -1,
      "",
    );
    for (const interceptor of interceptors) {
      await interceptor.onError?.(errorEnvelope, validationError);
    }
    return null;
  }
}

// ── DLQ ─────────────────────────────────────────────────────────────

export interface DlqMetadata {
  error: Error;
  attempt: number;
  /** Original Kafka message headers — forwarded to DLQ to preserve correlationId, traceparent, etc. */
  originalHeaders?: MessageHeaders;
}

export async function sendToDlq(
  topic: string,
  rawMessage: string,
  deps: { logger: KafkaLogger; producer: Producer },
  meta?: DlqMetadata,
): Promise<void> {
  const dlqTopic = `${topic}.dlq`;
  const headers: MessageHeaders = {
    ...(meta?.originalHeaders ?? {}),
    "x-dlq-original-topic": topic,
    "x-dlq-failed-at": new Date().toISOString(),
    "x-dlq-error-message": meta?.error.message ?? "unknown",
    "x-dlq-error-stack": meta?.error.stack?.slice(0, 2000) ?? "",
    "x-dlq-attempt-count": String(meta?.attempt ?? 0),
  };
  try {
    await deps.producer.send({
      topic: dlqTopic,
      messages: [{ value: rawMessage, headers }],
    });
    deps.logger.warn(`Message sent to DLQ: ${dlqTopic}`);
  } catch (error) {
    deps.logger.error(
      `Failed to send message to DLQ ${dlqTopic}:`,
      toError(error).stack,
    );
  }
}

// ── Retry topic routing ─────────────────────────────────────────────

/** Headers stamped on messages sent to a `<topic>.retry` topic. */
export const RETRY_HEADER_ATTEMPT = "x-retry-attempt";
export const RETRY_HEADER_AFTER = "x-retry-after";
export const RETRY_HEADER_MAX_RETRIES = "x-retry-max-retries";
export const RETRY_HEADER_ORIGINAL_TOPIC = "x-retry-original-topic";

/**
 * Send raw messages to the retry topic `<originalTopic>.retry`.
 * Stamps scheduling headers so the retry consumer knows when and how many times to retry.
 */
export async function sendToRetryTopic(
  originalTopic: string,
  rawMessages: string[],
  attempt: number,
  maxRetries: number,
  delayMs: number,
  originalHeaders: MessageHeaders,
  deps: { logger: KafkaLogger; producer: Producer },
): Promise<void> {
  const retryTopic = `${originalTopic}.retry.${attempt}`;
  // Strip any stale retry headers from a previous hop so they don't leak through.
  const {
    [RETRY_HEADER_ATTEMPT]: _a,
    [RETRY_HEADER_AFTER]: _b,
    [RETRY_HEADER_MAX_RETRIES]: _c,
    [RETRY_HEADER_ORIGINAL_TOPIC]: _d,
    ...userHeaders
  } = originalHeaders;
  const headers: MessageHeaders = {
    ...userHeaders,
    [RETRY_HEADER_ATTEMPT]: String(attempt),
    [RETRY_HEADER_AFTER]: String(Date.now() + delayMs),
    [RETRY_HEADER_MAX_RETRIES]: String(maxRetries),
    [RETRY_HEADER_ORIGINAL_TOPIC]: originalTopic,
  };
  try {
    for (const raw of rawMessages) {
      await deps.producer.send({
        topic: retryTopic,
        messages: [{ value: raw, headers }],
      });
    }
    deps.logger.warn(
      `Message queued in retry topic ${retryTopic} (attempt ${attempt}/${maxRetries})`,
    );
  } catch (error) {
    deps.logger.error(
      `Failed to send message to retry topic ${retryTopic}:`,
      toError(error).stack,
    );
  }
}

// ── Pipeline helpers ─────────────────────────────────────────────────

async function broadcastToInterceptors<T extends TopicMapConstraint<T>>(
  envelopes: EventEnvelope<any>[],
  interceptors: ConsumerInterceptor<T>[],
  cb: (
    interceptor: ConsumerInterceptor<T>,
    env: EventEnvelope<any>,
  ) => Promise<void> | void | undefined,
): Promise<void> {
  for (const env of envelopes) {
    for (const interceptor of interceptors) {
      await cb(interceptor, env);
    }
  }
}

/**
 * Run `fn` through the full instrumentation and interceptor lifecycle:
 *   beforeConsume → interceptor.before → fn → interceptor.after → cleanup
 *
 * On error: fires `onConsumeError` and cleanup, then returns the error.
 * The caller is responsible for calling `notifyInterceptorsOnError` (with
 * a possibly-wrapped error) and deciding what happens next.
 *
 * Returns `null` on success, the caught `Error` on failure.
 */
export async function runHandlerWithPipeline<T extends TopicMapConstraint<T>>(
  fn: () => Promise<void>,
  envelopes: EventEnvelope<any>[],
  interceptors: ConsumerInterceptor<T>[],
  instrumentation: KafkaInstrumentation[],
): Promise<Error | null> {
  const cleanups: (() => void)[] = [];

  try {
    for (const env of envelopes) {
      for (const inst of instrumentation) {
        const cleanup = inst.beforeConsume?.(env);
        if (typeof cleanup === "function") cleanups.push(cleanup);
      }
    }
    for (const env of envelopes) {
      for (const interceptor of interceptors) {
        await interceptor.before?.(env);
      }
    }

    await fn();

    for (const env of envelopes) {
      for (const interceptor of interceptors) {
        await interceptor.after?.(env);
      }
    }
    for (const cleanup of cleanups) cleanup();

    return null;
  } catch (error) {
    const err = toError(error);
    for (const env of envelopes) {
      for (const inst of instrumentation) {
        inst.onConsumeError?.(env, err);
      }
    }
    for (const cleanup of cleanups) cleanup();
    return err;
  }
}

/**
 * Call `interceptor.onError` for every envelope with the given error.
 * Separated from `runHandlerWithPipeline` so callers can wrap the raw error
 * (e.g. in `KafkaRetryExhaustedError`) before notifying interceptors.
 */
export async function notifyInterceptorsOnError<
  T extends TopicMapConstraint<T>,
>(
  envelopes: EventEnvelope<any>[],
  interceptors: ConsumerInterceptor<T>[],
  error: Error,
): Promise<void> {
  await broadcastToInterceptors(envelopes, interceptors, (i, env) =>
    i.onError?.(env, error),
  );
}

// ── Retry pipeline ──────────────────────────────────────────────────

export interface ExecuteWithRetryContext<T extends TopicMapConstraint<T>> {
  envelope: EventEnvelope<any> | EventEnvelope<any>[];
  rawMessages: string[];
  interceptors: ConsumerInterceptor<T>[];
  dlq: boolean;
  retry?: RetryOptions;
  isBatch?: boolean;
  /**
   * When `true`, failed messages are routed to `<topic>.retry` instead of being
   * retried in-process. All backoff and subsequent attempts are handled by the
   * companion retry consumer started by `startRetryTopicConsumers`.
   */
  retryTopics?: boolean;
}

/**
 * Execute a handler with retry, interceptors, instrumentation, and DLQ support.
 * Used by both single-message and batch consumers.
 */
export async function executeWithRetry<T extends TopicMapConstraint<T>>(
  fn: () => Promise<void>,
  ctx: ExecuteWithRetryContext<T>,
  deps: {
    logger: KafkaLogger;
    producer: Producer;
    instrumentation: KafkaInstrumentation[];
    onMessageLost?: (ctx: MessageLostContext) => void | Promise<void>;
  },
): Promise<void> {
  const {
    envelope,
    rawMessages,
    interceptors,
    dlq,
    retry,
    isBatch,
    retryTopics,
  } = ctx;
  // With retryTopics mode the main consumer tries exactly once — retry consumer takes over.
  const maxAttempts = retryTopics ? 1 : retry ? retry.maxRetries + 1 : 1;
  const backoffMs = retry?.backoffMs ?? 1000;
  const maxBackoffMs = retry?.maxBackoffMs ?? 30_000;
  const envelopes = Array.isArray(envelope) ? envelope : [envelope];
  const topic = envelopes[0]?.topic ?? "unknown";

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const error = await runHandlerWithPipeline(
      fn,
      envelopes,
      interceptors,
      deps.instrumentation,
    );
    if (!error) return;

    const isLastAttempt = attempt === maxAttempts;
    const reportedError =
      isLastAttempt && maxAttempts > 1
        ? new KafkaRetryExhaustedError(
            topic,
            envelopes.map((e) => e.payload),
            maxAttempts,
            { cause: error },
          )
        : error;

    await notifyInterceptorsOnError(envelopes, interceptors, reportedError);

    deps.logger.error(
      `Error processing ${isBatch ? "batch" : "message"} from topic ${topic} (attempt ${attempt}/${maxAttempts}):`,
      error.stack,
    );

    if (retryTopics && retry) {
      // Route to retry topic — retry consumer handles backoff and further attempts.
      // Always use attempt 1 here (main consumer never retries in-process).
      const cap = Math.min(backoffMs, maxBackoffMs);
      const delay = Math.floor(Math.random() * cap);
      await sendToRetryTopic(
        topic,
        rawMessages,
        1,
        retry.maxRetries,
        delay,
        envelopes[0]?.headers ?? {},
        deps,
      );
    } else if (isLastAttempt) {
      if (dlq) {
        const dlqMeta: DlqMetadata = {
          error,
          attempt,
          originalHeaders: envelopes[0]?.headers,
        };
        for (const raw of rawMessages) {
          await sendToDlq(topic, raw, deps, dlqMeta);
        }
      } else {
        await deps.onMessageLost?.({
          topic,
          error,
          attempt,
          headers: envelopes[0]?.headers ?? {},
        });
      }
    } else {
      // Exponential backoff with full jitter to avoid thundering herd
      const cap = Math.min(backoffMs * 2 ** (attempt - 1), maxBackoffMs);
      await sleep(Math.random() * cap);
    }
  }
}
