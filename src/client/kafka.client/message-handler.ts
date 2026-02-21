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
} from "../consumer/pipeline";
import type { SchemaLike } from "../message/topic";
import type {
  BatchMeta,
  ConsumerInterceptor,
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
};

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
  timeoutMs: number | undefined;
  wrapWithTimeout: <R>(
    fn: () => Promise<R>,
    ms: number,
    topic: string,
  ) => Promise<R>;
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
      rawMessages: batch.messages
        .filter((m) => m.value)
        .map((m) => m.value!.toString()),
      interceptors,
      dlq,
      retry,
      isBatch: true,
    },
    deps,
  );
}
