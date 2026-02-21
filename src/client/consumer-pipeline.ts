import type { KafkaJS } from "@confluentinc/kafka-javascript";
type Producer = KafkaJS.Producer;
import type { EventEnvelope } from "./envelope";
import { extractEnvelope } from "./envelope";
import { KafkaRetryExhaustedError, KafkaValidationError } from "./errors";
import type { SchemaLike } from "./topic";
import type {
  ConsumerInterceptor,
  KafkaInstrumentation,
  KafkaLogger,
  MessageHeaders,
  MessageLostContext,
  RetryOptions,
  TopicMapConstraint,
} from "./types";


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
      await deps.onMessageLost?.({ topic, error: validationError, attempt: 0, headers: deps.originalHeaders ?? {} });
    }
    // Validation errors don't have an envelope yet — call onError with a minimal envelope
    const errorEnvelope = extractEnvelope(message, deps.originalHeaders ?? {}, topic, -1, "");
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
    'x-dlq-original-topic': topic,
    'x-dlq-failed-at': new Date().toISOString(),
    'x-dlq-error-message': meta?.error.message ?? 'unknown',
    'x-dlq-error-stack': meta?.error.stack?.slice(0, 2000) ?? '',
    'x-dlq-attempt-count': String(meta?.attempt ?? 0),
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

// ── Retry pipeline ──────────────────────────────────────────────────

export interface ExecuteWithRetryContext<T extends TopicMapConstraint<T>> {
  envelope: EventEnvelope<any> | EventEnvelope<any>[];
  rawMessages: string[];
  interceptors: ConsumerInterceptor<T>[];
  dlq: boolean;
  retry?: RetryOptions;
  isBatch?: boolean;
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
  const { envelope, rawMessages, interceptors, dlq, retry, isBatch } = ctx;
  const maxAttempts = retry ? retry.maxRetries + 1 : 1;
  const backoffMs = retry?.backoffMs ?? 1000;
  const maxBackoffMs = retry?.maxBackoffMs ?? 30_000;
  const envelopes = Array.isArray(envelope) ? envelope : [envelope];
  const topic = envelopes[0]?.topic ?? "unknown";

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    // Collect instrumentation cleanup functions
    const cleanups: (() => void)[] = [];

    try {
      // Instrumentation: beforeConsume
      for (const env of envelopes) {
        for (const inst of deps.instrumentation) {
          const cleanup = inst.beforeConsume?.(env);
          if (typeof cleanup === "function") cleanups.push(cleanup);
        }
      }

      // Consumer interceptors: before
      for (const env of envelopes) {
        for (const interceptor of interceptors) {
          await interceptor.before?.(env);
        }
      }

      await fn();

      // Consumer interceptors: after
      for (const env of envelopes) {
        for (const interceptor of interceptors) {
          await interceptor.after?.(env);
        }
      }

      // Instrumentation: cleanup (end spans etc.)
      for (const cleanup of cleanups) cleanup();

      return;
    } catch (error) {
      const err = toError(error);
      const isLastAttempt = attempt === maxAttempts;

      // Instrumentation: onConsumeError
      for (const env of envelopes) {
        for (const inst of deps.instrumentation) {
          inst.onConsumeError?.(env, err);
        }
      }
      // Instrumentation: cleanup even on error
      for (const cleanup of cleanups) cleanup();

      if (isLastAttempt && maxAttempts > 1) {
        const exhaustedError = new KafkaRetryExhaustedError(
          topic,
          envelopes.map((e) => e.payload),
          maxAttempts,
          { cause: err },
        );
        for (const env of envelopes) {
          for (const interceptor of interceptors) {
            await interceptor.onError?.(env, exhaustedError);
          }
        }
      } else {
        for (const env of envelopes) {
          for (const interceptor of interceptors) {
            await interceptor.onError?.(env, err);
          }
        }
      }

      deps.logger.error(
        `Error processing ${isBatch ? "batch" : "message"} from topic ${topic} (attempt ${attempt}/${maxAttempts}):`,
        err.stack,
      );

      if (isLastAttempt) {
        if (dlq) {
          const dlqMeta: DlqMetadata = {
            error: err,
            attempt,
            originalHeaders: envelopes[0]?.headers,
          };
          for (const raw of rawMessages) {
            await sendToDlq(topic, raw, deps, dlqMeta);
          }
        } else {
          await deps.onMessageLost?.({
            topic,
            error: err,
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
}
