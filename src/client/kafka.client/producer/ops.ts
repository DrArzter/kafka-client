import {
  buildEnvelopeHeaders,
  HEADER_LAMPORT_CLOCK,
} from "../../message/envelope";
import { KafkaValidationError } from "../../errors";
import type { SchemaLike, SchemaParseContext } from "../../message/topic";
import type { MessageSerde } from "../../message/serde";
import type {
  BatchMessageItem,
  CompressionType,
  KafkaInstrumentation,
  KafkaLogger,
  MessageHeaders,
} from "../../types";

/**
 * Resolve the serde to use for a topic: the per-topic override
 * (`TopicDescriptor.__serde`) if present, otherwise the client-wide default.
 */
export function resolveSerde(
  topicOrDesc: any,
  clientSerde: MessageSerde,
): MessageSerde {
  return topicOrDesc?.__serde ?? clientSerde;
}

/**
 * Extract the plain topic name string from either a `TopicDescriptor` object or a raw string.
 * Falls back to `String(topicOrDescriptor)` for any other value.
 */
export function resolveTopicName(topicOrDescriptor: unknown): string {
  if (typeof topicOrDescriptor === "string") return topicOrDescriptor;
  if (
    topicOrDescriptor &&
    typeof topicOrDescriptor === "object" &&
    "__topic" in topicOrDescriptor
  ) {
    return (topicOrDescriptor as { __topic: string }).__topic;
  }
  return String(topicOrDescriptor);
}

/**
 * Register the schema attached to a `TopicDescriptor` into the shared schema registry.
 * If a different schema is already registered for the same topic, logs a warning and
 * overwrites it — consistent schemas across all call sites avoid silent validation drift.
 *
 * @param topicOrDesc A `TopicDescriptor` (with `__schema`) or a plain string. Plain strings are ignored.
 * @param schemaRegistry Mutable map of topic name → validator shared across the client.
 * @param logger Optional logger for conflict warnings.
 */
export function registerSchema(
  topicOrDesc: any,
  schemaRegistry: Map<string, SchemaLike>,
  logger?: KafkaLogger,
): void {
  if (topicOrDesc?.__schema) {
    const topic = resolveTopicName(topicOrDesc);
    const existing = schemaRegistry.get(topic);
    if (existing && existing !== topicOrDesc.__schema) {
      logger?.warn(
        `Schema conflict for topic "${topic}": a different schema is already registered. ` +
          `Using the new schema — ensure consistent schemas to avoid silent validation mismatches.`,
      );
    }
    schemaRegistry.set(topic, topicOrDesc.__schema);
  }
}

/**
 * Validate `message` against the schema attached to `topicOrDesc` (or looked up from the
 * registry when `strictSchemasEnabled` is on). Returns the parsed (potentially transformed)
 * value on success, or throws a `KafkaValidationError` on failure.
 *
 * Validation priority:
 * 1. Inline schema on the `TopicDescriptor` — always applied.
 * 2. Registry schema — applied only when `strictSchemasEnabled` is `true`.
 * 3. No schema found — returns `message` unchanged.
 *
 * @param topicOrDesc Topic descriptor carrying an inline `__schema`, or a plain topic string.
 * @param message The raw message payload to validate.
 * @param deps Schema registry and strict-mode flag.
 * @param ctx Optional parse context forwarded to the schema (topic name, headers, version).
 */
export async function validateMessage(
  topicOrDesc: any,
  message: any,
  deps: {
    schemaRegistry: Map<string, SchemaLike>;
    strictSchemasEnabled: boolean;
  },
  ctx?: SchemaParseContext,
): Promise<any> {
  const topicName = resolveTopicName(topicOrDesc);
  if (topicOrDesc?.__schema) {
    try {
      return await topicOrDesc.__schema.parse(message, ctx);
    } catch (error) {
      throw new KafkaValidationError(topicName, message, {
        cause: error instanceof Error ? error : new Error(String(error)),
      });
    }
  }
  if (deps.strictSchemasEnabled && typeof topicOrDesc === "string") {
    const schema = deps.schemaRegistry.get(topicOrDesc);
    if (schema) {
      try {
        return await schema.parse(message, ctx);
      } catch (error) {
        throw new KafkaValidationError(topicName, message, {
          cause: error instanceof Error ? error : new Error(String(error)),
        });
      }
    }
  }
  return message;
}

export type BuildSendPayloadDeps = {
  schemaRegistry: Map<string, SchemaLike>;
  strictSchemasEnabled: boolean;
  instrumentation: KafkaInstrumentation[];
  logger: KafkaLogger;
  /** Client-wide serde; a per-topic `TopicDescriptor.__serde` overrides it. */
  serde: MessageSerde;
  /** Called once per message to get the next Lamport clock value. Omit to disable clock stamping. */
  nextLamportClock?: () => number;
};

/**
 * Build the Kafka producer payload from a topic descriptor (or name) and an array of messages.
 *
 * For each message the function:
 * 1. Builds envelope headers (`x-event-id`, `x-correlation-id`, `x-timestamp`, …).
 * 2. Stamps the Lamport clock header when `deps.nextLamportClock` is provided.
 * 3. Calls `beforeSend` instrumentation hooks so tracing can inject `traceparent` etc.
 * 4. Validates the payload against the attached or registry schema.
 * 5. Serialises the validated value via the resolved serde (per-topic override
 *    or the client-wide default; `JsonSerde` by default).
 *
 * @param topicOrDesc Topic descriptor or plain topic name string.
 * @param messages Array of outgoing messages with optional key, headers, and metadata.
 * @param deps Schema registry, strict-mode flag, instrumentation hooks, serde, and Lamport clock factory.
 * @param compression Optional compression codec to include in the returned payload object.
 * @returns Kafka producer `send()` payload — `{ topic, messages, compression? }`.
 */
export async function buildSendPayload(
  topicOrDesc: any,
  messages: Array<BatchMessageItem<any>>,
  deps: BuildSendPayloadDeps,
  compression?: CompressionType,
): Promise<{
  topic: string;
  compression?: CompressionType;
  messages: Array<{
    value: string | Buffer;
    key: string | null;
    headers: MessageHeaders;
  }>;
}> {
  const topic = resolveTopicName(topicOrDesc);
  const serde = resolveSerde(topicOrDesc, deps.serde);
  const builtMessages = await Promise.all(
    messages.map(async (m) => {
      const envelopeHeaders = buildEnvelopeHeaders({
        correlationId: m.correlationId,
        schemaVersion: m.schemaVersion,
        eventId: m.eventId,
        headers: m.headers,
      });

      // Stamp Lamport clock for consumer-side deduplication
      if (deps.nextLamportClock) {
        envelopeHeaders[HEADER_LAMPORT_CLOCK] = String(deps.nextLamportClock());
      }

      // beforeSend: let instrumentation mutate headers (e.g. OTel injects traceparent)
      for (const inst of deps.instrumentation) {
        inst.beforeSend?.(topic, envelopeHeaders);
      }

      const sendCtx: SchemaParseContext = {
        topic,
        headers: envelopeHeaders,
        version: m.schemaVersion ?? 1,
      };

      // Validate the OBJECT first, then serialize the validated value via the serde.
      const validated = await validateMessage(topicOrDesc, m.value, deps, sendCtx);
      return {
        value: await serde.serialize(validated, {
          topic,
          headers: envelopeHeaders,
          isKey: false,
        }),
        // Explicit key wins; otherwise fall back to the descriptor's .key()
        // extractor (runs on the original, pre-validation payload).
        key: m.key ?? topicOrDesc?.__key?.(m.value) ?? null,
        headers: envelopeHeaders,
      };
    }),
  );
  return { topic, messages: builtMessages, ...(compression && { compression }) };
}
