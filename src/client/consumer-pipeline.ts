import type { Producer } from "kafkajs";
import type { EventEnvelope } from "./envelope";
import { extractEnvelope } from "./envelope";
import { KafkaRetryExhaustedError, KafkaValidationError } from "./errors";
import type { SchemaLike } from "./topic";
import type {
  ConsumerInterceptor,
  KafkaInstrumentation,
  KafkaLogger,
  RetryOptions,
  TopicMapConstraint,
} from "./types";

const ACKS_ALL = -1 as const;

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
  deps: { logger: KafkaLogger; producer: Producer },
): Promise<any | null> {
  const schema = schemaMap.get(topic);
  if (!schema) return message;

  try {
    return schema.parse(message);
  } catch (error) {
    const err = toError(error);
    const validationError = new KafkaValidationError(topic, message, {
      cause: err,
    });
    deps.logger.error(
      `Schema validation failed for topic ${topic}:`,
      err.message,
    );
    if (dlq) await sendToDlq(topic, raw, deps);
    // Validation errors don't have an envelope yet — call onError with a minimal envelope
    const errorEnvelope = extractEnvelope(message, {}, topic, -1, "");
    for (const interceptor of interceptors) {
      await interceptor.onError?.(errorEnvelope, validationError);
    }
    return null;
  }
}

// ── DLQ ─────────────────────────────────────────────────────────────

export async function sendToDlq(
  topic: string,
  rawMessage: string,
  deps: { logger: KafkaLogger; producer: Producer },
): Promise<void> {
  const dlqTopic = `${topic}.dlq`;
  try {
    await deps.producer.send({
      topic: dlqTopic,
      messages: [{ value: rawMessage }],
      acks: ACKS_ALL,
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
  },
): Promise<void> {
  const { envelope, rawMessages, interceptors, dlq, retry, isBatch } = ctx;
  const maxAttempts = retry ? retry.maxRetries + 1 : 1;
  const backoffMs = retry?.backoffMs ?? 1000;
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
          for (const raw of rawMessages) {
            await sendToDlq(topic, raw, deps);
          }
        }
      } else {
        await sleep(backoffMs * attempt);
      }
    }
  }
}
