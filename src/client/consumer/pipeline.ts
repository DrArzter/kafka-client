import type { KafkaJS } from "@confluentinc/kafka-javascript";
type Producer = KafkaJS.Producer;
import type { EventEnvelope } from "../message/envelope";
import { extractEnvelope } from "../message/envelope";
import { KafkaRetryExhaustedError, KafkaValidationError } from "../errors";
import type { SchemaLike, SchemaParseContext } from "../message/topic";
import type {
  BeforeConsumeResult,
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
    instrumentation?: KafkaInstrumentation[];
  },
): Promise<any | null> {
  const schema = schemaMap.get(topic);
  if (!schema) return message;

  const ctx: SchemaParseContext = {
    topic,
    headers: deps.originalHeaders ?? {},
    version: Number(deps.originalHeaders?.["x-schema-version"] ?? 1),
  };

  try {
    return await schema.parse(message, ctx);
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
    // Validation errors don't have an envelope yet — call hooks with a minimal envelope
    const errorEnvelope = extractEnvelope(
      message,
      deps.originalHeaders ?? {},
      topic,
      -1,
      "",
    );
    for (const inst of (deps.instrumentation ?? [])) {
      inst.onConsumeError?.(errorEnvelope, validationError);
    }
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

/** Build the DLQ send payload without sending it. Used by sendToDlq and EOS routing. */
export function buildDlqPayload(
  topic: string,
  rawMessage: string,
  meta?: DlqMetadata,
): { topic: string; messages: Array<{ value: string; headers: MessageHeaders }> } {
  const dlqTopic = `${topic}.dlq`;
  const headers: MessageHeaders = {
    ...(meta?.originalHeaders ?? {}),
    "x-dlq-original-topic": topic,
    "x-dlq-failed-at": new Date().toISOString(),
    "x-dlq-error-message": meta?.error.message ?? "unknown",
    "x-dlq-error-stack": meta?.error.stack?.slice(0, 2000) ?? "",
    "x-dlq-attempt-count": String(meta?.attempt ?? 0),
  };
  return { topic: dlqTopic, messages: [{ value: rawMessage, headers }] };
}

export async function sendToDlq(
  topic: string,
  rawMessage: string,
  deps: {
    logger: KafkaLogger;
    producer: Producer;
    onMessageLost?: (ctx: MessageLostContext) => void | Promise<void>;
  },
  meta?: DlqMetadata,
): Promise<void> {
  const payload = buildDlqPayload(topic, rawMessage, meta);
  try {
    await deps.producer.send(payload);
    deps.logger.warn(`Message sent to DLQ: ${payload.topic}`);
  } catch (error) {
    const err = toError(error);
    deps.logger.error(`Failed to send message to DLQ ${payload.topic}:`, err.stack);
    await deps.onMessageLost?.({
      topic,
      error: err,
      attempt: meta?.attempt ?? 0,
      headers: meta?.originalHeaders ?? {},
    });
  }
}

// ── Retry topic routing ─────────────────────────────────────────────

/** Headers stamped on messages sent to a `<topic>.retry` topic. */
export const RETRY_HEADER_ATTEMPT = "x-retry-attempt";
export const RETRY_HEADER_AFTER = "x-retry-after";
export const RETRY_HEADER_MAX_RETRIES = "x-retry-max-retries";
export const RETRY_HEADER_ORIGINAL_TOPIC = "x-retry-original-topic";

/** Build the retry topic send payload without sending it. Used by sendToRetryTopic and EOS routing. */
export function buildRetryTopicPayload(
  originalTopic: string,
  rawMessages: string[],
  attempt: number,
  maxRetries: number,
  delayMs: number,
  /** Per-message headers (array) or a single object applied to all messages. */
  originalHeaders: MessageHeaders | MessageHeaders[],
): { topic: string; messages: Array<{ value: string; headers: MessageHeaders }> } {
  const retryTopic = `${originalTopic}.retry.${attempt}`;
  function buildHeaders(hdr: MessageHeaders): MessageHeaders {
    // Strip any stale retry headers from a previous hop so they don't leak through.
    const {
      [RETRY_HEADER_ATTEMPT]: _a,
      [RETRY_HEADER_AFTER]: _b,
      [RETRY_HEADER_MAX_RETRIES]: _c,
      [RETRY_HEADER_ORIGINAL_TOPIC]: _d,
      ...userHeaders
    } = hdr;
    return {
      ...userHeaders,
      [RETRY_HEADER_ATTEMPT]: String(attempt),
      [RETRY_HEADER_AFTER]: String(Date.now() + delayMs),
      [RETRY_HEADER_MAX_RETRIES]: String(maxRetries),
      [RETRY_HEADER_ORIGINAL_TOPIC]: originalTopic,
    };
  }
  return {
    topic: retryTopic,
    messages: rawMessages.map((value, i) => ({
      value,
      headers: buildHeaders(
        Array.isArray(originalHeaders) ? (originalHeaders[i] ?? {}) : originalHeaders,
      ),
    })),
  };
}

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
  /** Per-message headers (array) or a single object applied to all messages. */
  originalHeaders: MessageHeaders | MessageHeaders[],
  deps: {
    logger: KafkaLogger;
    producer: Producer;
    onMessageLost?: (ctx: MessageLostContext) => void | Promise<void>;
  },
): Promise<void> {
  const payload = buildRetryTopicPayload(
    originalTopic,
    rawMessages,
    attempt,
    maxRetries,
    delayMs,
    originalHeaders,
  );
  try {
    await deps.producer.send(payload);
    deps.logger.warn(
      `Message queued in retry topic ${payload.topic} (attempt ${attempt}/${maxRetries})`,
    );
  } catch (error) {
    const err = toError(error);
    deps.logger.error(
      `Failed to send message to retry topic ${payload.topic}:`,
      err.stack,
    );
    await deps.onMessageLost?.({
      topic: originalTopic,
      error: err,
      attempt,
      headers: Array.isArray(originalHeaders) ? (originalHeaders[0] ?? {}) : originalHeaders,
    });
  }
}

// ── Deduplication routing ────────────────────────────────────────────

export interface DuplicateMetadata {
  /** The `x-lamport-clock` value from the incoming (duplicate) message. */
  incomingClock: number;
  /** The last processed clock value for this topic/partition. */
  lastProcessedClock: number;
  /** Original Kafka message headers — forwarded to preserve correlationId, traceparent, etc. */
  originalHeaders?: MessageHeaders;
}

/** Build the payload for a duplicate message forwarded to a custom topic. */
export function buildDuplicateTopicPayload(
  sourceTopic: string,
  rawMessage: string,
  destinationTopic: string,
  meta?: DuplicateMetadata,
): { topic: string; messages: Array<{ value: string; headers: MessageHeaders }> } {
  const headers: MessageHeaders = {
    ...(meta?.originalHeaders ?? {}),
    "x-duplicate-original-topic": sourceTopic,
    "x-duplicate-detected-at": new Date().toISOString(),
    "x-duplicate-reason": "lamport-clock-duplicate",
    "x-duplicate-incoming-clock": String(meta?.incomingClock ?? 0),
    "x-duplicate-last-processed-clock": String(meta?.lastProcessedClock ?? 0),
  };
  return { topic: destinationTopic, messages: [{ value: rawMessage, headers }] };
}

/**
 * Forward a duplicate message to a dedicated topic (e.g. `<topic>.duplicates`).
 * Stamps reason metadata headers so consumers of that topic know why it landed there.
 */
export async function sendToDuplicatesTopic(
  sourceTopic: string,
  rawMessage: string,
  destinationTopic: string,
  deps: { logger: KafkaLogger; producer: Producer },
  meta?: DuplicateMetadata,
): Promise<void> {
  const payload = buildDuplicateTopicPayload(sourceTopic, rawMessage, destinationTopic, meta);
  try {
    await deps.producer.send(payload);
    deps.logger.warn(`Duplicate message forwarded to ${destinationTopic}`);
  } catch (error) {
    deps.logger.error(
      `Failed to forward duplicate to ${destinationTopic}:`,
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
  const wraps: Array<(fn: () => Promise<void>) => Promise<void>> = [];

  try {
    for (const env of envelopes) {
      for (const inst of instrumentation) {
        const result: BeforeConsumeResult | void = inst.beforeConsume?.(env);
        if (typeof result === "function") {
          cleanups.push(result);
        } else if (result) {
          if (result.cleanup) cleanups.push(result.cleanup);
          if (result.wrap) wraps.push(result.wrap);
        }
      }
    }
    for (const env of envelopes) {
      for (const interceptor of interceptors) {
        await interceptor.before?.(env);
      }
    }

    // Compose wraps: first instrumentation is outermost, last is innermost.
    let runFn: () => Promise<void> = fn;
    for (let i = wraps.length - 1; i >= 0; i--) {
      const wrap = wraps[i];
      const inner = runFn;
      runFn = () => wrap(inner);
    }
    await runFn();

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
      //
      // NOTE: this send is NOT EOS. The main consumer routes to <topic>.retry.1
      // via the regular (non-transactional) producer. If the process crashes after
      // producer.send() but before the offset commit, the message will appear in
      // retry.1 AND be redelivered to the main consumer — handled twice.
      // EOS (read-process-write atomicity) is guaranteed only from retry.N onward
      // (see startRetryTopicConsumers). This is a known limitation tracked in the
      // roadmap under 0.5.6.
      const cap = Math.min(backoffMs, maxBackoffMs);
      const delay = Math.floor(Math.random() * cap);
      await sendToRetryTopic(
        topic,
        rawMessages,
        1,
        retry.maxRetries,
        delay,
        isBatch ? envelopes.map((e) => e.headers) : (envelopes[0]?.headers ?? {}),
        deps,
      );
    } else if (isLastAttempt) {
      if (dlq) {
        // Use per-message headers so each DLQ message preserves its own
        // correlationId / traceparent instead of copying envelopes[0]'s headers
        // onto every message in the batch.
        for (let i = 0; i < rawMessages.length; i++) {
          await sendToDlq(topic, rawMessages[i], deps, {
            error,
            attempt,
            originalHeaders: envelopes[i]?.headers,
          });
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
      await sleep(Math.floor(Math.random() * cap));
    }
  }
}
