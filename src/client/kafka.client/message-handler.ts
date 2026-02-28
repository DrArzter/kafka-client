import { KafkaJS } from "@confluentinc/kafka-javascript";
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
  executeWithRetry,
  sendToDlq,
  sendToDuplicatesTopic,
} from "../consumer/pipeline";
import type { DuplicateMetadata } from "../consumer/pipeline";
import { HEADER_LAMPORT_CLOCK } from "../message/envelope";
import type { SchemaLike } from "../message/topic";
import type {
  BatchMeta,
  ConsumerInterceptor,
  DeduplicationOptions,
  DlqReason,
  KafkaClientOptions,
  KafkaInstrumentation,
  KafkaLogger,
  RetryOptions,
} from "../types";

export type MessageHandlerDeps = {
  logger: KafkaLogger;
  producer: Producer;
  instrumentation: KafkaInstrumentation[];
  onMessageLost: KafkaClientOptions["onMessageLost"];
  onRetry?: (
    envelope: EventEnvelope<any>,
    attempt: number,
    maxRetries: number,
  ) => void;
  onDlq?: (envelope: EventEnvelope<any>, reason: DlqReason) => void;
  onDuplicate?: (
    envelope: EventEnvelope<any>,
    strategy: "drop" | "dlq" | "topic",
  ) => void;
  onMessage?: (envelope: EventEnvelope<any>) => void;
};

/** Active deduplication context passed from KafkaClient to the message handler. */
export type DeduplicationContext = {
  options: DeduplicationOptions;
  /** Mutable map: `"topic:partition"` → last processed Lamport clock value. */
  state: Map<string, number>;
};

export type EachMessageOpts = {
  schemaMap: Map<string, SchemaLike>;
  handleMessage: (envelope: EventEnvelope<any>) => Promise<void>;
  interceptors: ConsumerInterceptor<any>[];
  dlq: boolean;
  retry: RetryOptions | undefined;
  retryTopics: boolean | undefined;
  timeoutMs: number | undefined;
  wrapWithTimeout: <R>(
    fn: () => Promise<R>,
    ms: number,
    topic: string,
  ) => Promise<R>;
  deduplication?: DeduplicationContext;
};

/**
 * Check Lamport clock header against per-partition state.
 * Returns `true` if the message is a duplicate and should be skipped.
 * Updates the state map on a fresh message.
 */
async function applyDeduplication(
  envelope: EventEnvelope<any>,
  raw: string,
  dedup: DeduplicationContext,
  dlq: boolean,
  deps: MessageHandlerDeps,
): Promise<boolean> {
  const clockRaw = envelope.headers[HEADER_LAMPORT_CLOCK];
  if (clockRaw === undefined) return false; // no clock → pass through

  const incomingClock = Number(clockRaw);
  if (Number.isNaN(incomingClock)) return false; // malformed header → pass through

  const stateKey = `${envelope.topic}:${envelope.partition}`;
  const lastProcessedClock = dedup.state.get(stateKey) ?? -1;

  if (incomingClock <= lastProcessedClock) {
    const meta: DuplicateMetadata = {
      incomingClock,
      lastProcessedClock,
      originalHeaders: envelope.headers,
    };
    const strategy = dedup.options.strategy ?? "drop";
    deps.logger.warn(
      `Duplicate message on ${envelope.topic}[${envelope.partition}]: ` +
        `clock=${incomingClock} <= last=${lastProcessedClock} — strategy=${strategy}`,
    );

    deps.onDuplicate?.(envelope, strategy);

    if (strategy === "dlq" && dlq) {
      const augmentedHeaders = {
        ...envelope.headers,
        "x-dlq-reason": "lamport-clock-duplicate",
        "x-dlq-duplicate-incoming-clock": String(incomingClock),
        "x-dlq-duplicate-last-processed-clock": String(lastProcessedClock),
      };
      await sendToDlq(envelope.topic, raw, deps, {
        error: new Error("Lamport Clock duplicate detected"),
        attempt: 0,
        originalHeaders: augmentedHeaders,
      });
    } else if (strategy === "topic") {
      const destination =
        dedup.options.duplicatesTopic ?? `${envelope.topic}.duplicates`;
      await sendToDuplicatesTopic(envelope.topic, raw, destination, deps, meta);
    }
    // strategy === 'drop': already logged, nothing more to do

    return true; // signal: skip this message
  }

  dedup.state.set(stateKey, incomingClock);
  return false;
}

/** Parse, validate and extract an envelope from a single raw Kafka message. Returns null to skip. */
async function parseSingleMessage(
  message: {
    value: Buffer | null;
    headers?: Record<string, any>;
    offset: string;
  },
  topic: string,
  partition: number,
  schemaMap: Map<string, SchemaLike>,
  interceptors: ConsumerInterceptor<any>[],
  dlq: boolean,
  deps: MessageHandlerDeps,
): Promise<EventEnvelope<any> | null> {
  if (!message.value) {
    deps.logger.warn(`Received empty message from topic ${topic}`);
    return null;
  }

  const raw = message.value.toString();
  const parsed = parseJsonMessage(raw, topic, deps.logger);
  if (parsed === null) return null;

  const headers = decodeHeaders(message.headers);
  const validated = await validateWithSchema(
    parsed,
    raw,
    topic,
    schemaMap,
    interceptors,
    dlq,
    { ...deps, originalHeaders: headers },
  );
  if (validated === null) return null;

  return extractEnvelope(validated, headers, topic, partition, message.offset);
}

export async function handleEachMessage(
  payload: {
    topic: string;
    partition: number;
    message: {
      value: Buffer | null;
      headers?: Record<string, any>;
      offset: string;
    };
  },
  opts: EachMessageOpts,
  deps: MessageHandlerDeps,
): Promise<void> {
  const { topic, partition, message } = payload;
  const {
    schemaMap,
    handleMessage,
    interceptors,
    dlq,
    retry,
    retryTopics,
    timeoutMs,
    wrapWithTimeout,
  } = opts;

  const envelope = await parseSingleMessage(
    message,
    topic,
    partition,
    schemaMap,
    interceptors,
    dlq,
    deps,
  );
  if (envelope === null) return;

  if (opts.deduplication) {
    const isDuplicate = await applyDeduplication(
      envelope,
      message.value!.toString(),
      opts.deduplication,
      dlq,
      deps,
    );
    if (isDuplicate) return;
  }

  await executeWithRetry(
    () => {
      const fn = () =>
        runWithEnvelopeContext(
          {
            correlationId: envelope.correlationId,
            traceparent: envelope.traceparent,
          },
          () => handleMessage(envelope),
        );
      return timeoutMs ? wrapWithTimeout(fn, timeoutMs, topic) : fn();
    },
    {
      envelope,
      rawMessages: [message.value!.toString()],
      interceptors,
      dlq,
      retry,
      retryTopics,
    },
    deps,
  );
}

export type EachBatchOpts = {
  schemaMap: Map<string, SchemaLike>;
  handleBatch: (
    envelopes: EventEnvelope<any>[],
    meta: BatchMeta,
  ) => Promise<void>;
  interceptors: ConsumerInterceptor<any>[];
  dlq: boolean;
  retry: RetryOptions | undefined;
  retryTopics: boolean | undefined;
  timeoutMs: number | undefined;
  wrapWithTimeout: <R>(
    fn: () => Promise<R>,
    ms: number,
    topic: string,
  ) => Promise<R>;
  deduplication?: DeduplicationContext;
};

export async function handleEachBatch(
  payload: {
    batch: {
      topic: string;
      partition: number;
      highWatermark: string;
      messages: Array<{
        value: Buffer | null;
        headers?: Record<string, any>;
        offset: string;
      }>;
    };
    heartbeat(): Promise<void>;
    resolveOffset(offset: string): void;
    commitOffsetsIfNecessary(): Promise<void>;
  },
  opts: EachBatchOpts,
  deps: MessageHandlerDeps,
): Promise<void> {
  const { batch, heartbeat, resolveOffset, commitOffsetsIfNecessary } = payload;
  const {
    schemaMap,
    handleBatch,
    interceptors,
    dlq,
    retry,
    retryTopics,
    timeoutMs,
    wrapWithTimeout,
  } = opts;

  const envelopes: EventEnvelope<any>[] = [];
  const rawMessages: string[] = [];

  for (const message of batch.messages) {
    const envelope = await parseSingleMessage(
      message,
      batch.topic,
      batch.partition,
      schemaMap,
      interceptors,
      dlq,
      deps,
    );
    if (envelope === null) continue;

    if (opts.deduplication) {
      const raw = message.value!.toString();
      const isDuplicate = await applyDeduplication(
        envelope,
        raw,
        opts.deduplication,
        dlq,
        deps,
      );
      if (isDuplicate) continue;
    }

    envelopes.push(envelope);
    rawMessages.push(message.value!.toString());
  }

  if (envelopes.length === 0) return;

  const meta: BatchMeta = {
    partition: batch.partition,
    highWatermark: batch.highWatermark,
    heartbeat,
    resolveOffset,
    commitOffsetsIfNecessary,
  };

  await executeWithRetry(
    () => {
      const fn = () => handleBatch(envelopes, meta);
      return timeoutMs ? wrapWithTimeout(fn, timeoutMs, batch.topic) : fn();
    },
    {
      envelope: envelopes,
      rawMessages,
      interceptors,
      dlq,
      retry,
      isBatch: true,
      retryTopics,
    },
    deps,
  );
}
