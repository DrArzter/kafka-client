import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { MessageHeaders } from "../types";

// ── Header keys ──────────────────────────────────────────────────────

export const HEADER_EVENT_ID = "x-event-id";
export const HEADER_CORRELATION_ID = "x-correlation-id";
export const HEADER_TIMESTAMP = "x-timestamp";
export const HEADER_SCHEMA_VERSION = "x-schema-version";
export const HEADER_TRACEPARENT = "traceparent";

// ── EventEnvelope ────────────────────────────────────────────────────

/**
 * Typed wrapper combining a parsed message payload with Kafka metadata
 * and envelope headers.
 *
 * On **send**, the library auto-generates envelope headers
 * (`x-event-id`, `x-correlation-id`, `x-timestamp`, `x-schema-version`).
 *
 * On **consume**, the library extracts those headers and assembles
 * an `EventEnvelope` that is passed to the handler.
 */
export interface EventEnvelope<T> {
  /** Deserialized + validated message body. */
  payload: T;
  /** Topic the message was produced to / consumed from. */
  topic: string;
  /** Kafka partition (consume-side only, `-1` on send). */
  partition: number;
  /** Kafka offset (consume-side only, empty string on send). */
  offset: string;
  /** ISO-8601 timestamp set by the producer. */
  timestamp: string;
  /** Unique ID for this event (UUID v4). */
  eventId: string;
  /** Correlation ID — auto-propagated via AsyncLocalStorage. */
  correlationId: string;
  /** Schema version of the payload. */
  schemaVersion: number;
  /** W3C Trace Context `traceparent` header (set by OTel instrumentation). */
  traceparent?: string;
  /** All decoded Kafka headers for extensibility. */
  headers: MessageHeaders;
}

// ── AsyncLocalStorage context ────────────────────────────────────────

interface EnvelopeCtx {
  correlationId: string;
  traceparent?: string;
}

const envelopeStorage = new AsyncLocalStorage<EnvelopeCtx>();

/** Read the current envelope context (correlationId / traceparent) from ALS. */
export function getEnvelopeContext(): EnvelopeCtx | undefined {
  return envelopeStorage.getStore();
}

/** Execute `fn` inside an envelope context so nested sends inherit correlationId. */
export function runWithEnvelopeContext<R>(ctx: EnvelopeCtx, fn: () => R): R {
  return envelopeStorage.run(ctx, fn);
}

// ── Header helpers ───────────────────────────────────────────────────

/** Options accepted by `buildEnvelopeHeaders`. */
export interface EnvelopeHeaderOptions {
  correlationId?: string;
  schemaVersion?: number;
  eventId?: string;
  headers?: MessageHeaders;
}

/**
 * Generate envelope headers for the send path.
 *
 * Priority for `correlationId`:
 *   explicit option → ALS context → new UUID.
 */
export function buildEnvelopeHeaders(
  options: EnvelopeHeaderOptions = {},
): MessageHeaders {
  const ctx = getEnvelopeContext();

  const correlationId =
    options.correlationId ?? ctx?.correlationId ?? randomUUID();
  const eventId = options.eventId ?? randomUUID();
  const timestamp = new Date().toISOString();
  const schemaVersion = String(options.schemaVersion ?? 1);

  const envelope: MessageHeaders = {
    [HEADER_EVENT_ID]: eventId,
    [HEADER_CORRELATION_ID]: correlationId,
    [HEADER_TIMESTAMP]: timestamp,
    [HEADER_SCHEMA_VERSION]: schemaVersion,
  };

  // Propagate traceparent from ALS if present (OTel may override via instrumentation)
  if (ctx?.traceparent) {
    envelope[HEADER_TRACEPARENT] = ctx.traceparent;
  }

  // User-provided headers win on conflict
  return { ...envelope, ...options.headers };
}

/**
 * Decode kafkajs headers (`Record<string, Buffer | string | undefined>`)
 * into plain `Record<string, string>`.
 */
export function decodeHeaders(
  raw:
    | Record<string, Buffer | string | (Buffer | string)[] | undefined>
    | undefined,
): MessageHeaders {
  if (!raw) return {};
  const result: MessageHeaders = {};
  for (const [key, value] of Object.entries(raw)) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      // Kafka allows multiple headers with the same key — take the last one (last-wins),
      // consistent with HTTP semantics. Joining would corrupt values that contain commas.
      const items = value.map((v) => (Buffer.isBuffer(v) ? v.toString() : v));
      result[key] = items[items.length - 1] ?? "";
    } else {
      result[key] = Buffer.isBuffer(value) ? value.toString() : value;
    }
  }
  return result;
}

/**
 * Build an `EventEnvelope` from a consumed kafkajs message.
 * Tolerates missing envelope headers — generates defaults so messages
 * from non-envelope producers still work.
 */
export function extractEnvelope<T>(
  payload: T,
  headers: MessageHeaders,
  topic: string,
  partition: number,
  offset: string,
): EventEnvelope<T> {
  return {
    payload,
    topic,
    partition,
    offset,
    eventId: headers[HEADER_EVENT_ID] ?? randomUUID(),
    correlationId: headers[HEADER_CORRELATION_ID] ?? randomUUID(),
    timestamp: headers[HEADER_TIMESTAMP] ?? new Date().toISOString(),
    schemaVersion: Number(headers[HEADER_SCHEMA_VERSION] ?? 1),
    traceparent: headers[HEADER_TRACEPARENT],
    headers,
  };
}
