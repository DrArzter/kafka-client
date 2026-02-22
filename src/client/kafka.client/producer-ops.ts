import { buildEnvelopeHeaders } from "../message/envelope";
import { KafkaValidationError } from "../errors";
import type { SchemaLike } from "../message/topic";
import type {
  BatchMessageItem,
  KafkaInstrumentation,
  MessageHeaders,
} from "../types";

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

export function registerSchema(
  topicOrDesc: any,
  schemaRegistry: Map<string, SchemaLike>,
): void {
  if (topicOrDesc?.__schema) {
    const topic = resolveTopicName(topicOrDesc);
    schemaRegistry.set(topic, topicOrDesc.__schema);
  }
}

export async function validateMessage(
  topicOrDesc: any,
  message: any,
  deps: {
    schemaRegistry: Map<string, SchemaLike>;
    strictSchemasEnabled: boolean;
  },
): Promise<any> {
  const topicName = resolveTopicName(topicOrDesc);
  if (topicOrDesc?.__schema) {
    try {
      return await topicOrDesc.__schema.parse(message);
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
        return await schema.parse(message);
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
};

export async function buildSendPayload(
  topicOrDesc: any,
  messages: Array<BatchMessageItem<any>>,
  deps: BuildSendPayloadDeps,
): Promise<{
  topic: string;
  messages: Array<{
    value: string;
    key: string | null;
    headers: MessageHeaders;
  }>;
}> {
  const topic = resolveTopicName(topicOrDesc);
  const builtMessages = await Promise.all(
    messages.map(async (m) => {
      const envelopeHeaders = buildEnvelopeHeaders({
        correlationId: m.correlationId,
        schemaVersion: m.schemaVersion,
        eventId: m.eventId,
        headers: m.headers,
      });

      // beforeSend: let instrumentation mutate headers (e.g. OTel injects traceparent)
      for (const inst of deps.instrumentation) {
        inst.beforeSend?.(topic, envelopeHeaders);
      }

      return {
        value: JSON.stringify(
          await validateMessage(topicOrDesc, m.value, deps),
        ),
        key: m.key ?? null,
        headers: envelopeHeaders,
      };
    }),
  );
  return { topic, messages: builtMessages };
}
