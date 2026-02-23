import {
  trace,
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
} from "@opentelemetry/api";
import type { BeforeConsumeResult, KafkaInstrumentation } from "./client/types";
import type { EventEnvelope } from "./client/message/envelope";

/**
 * Create a `KafkaInstrumentation` that automatically propagates
 * W3C Trace Context via Kafka headers.
 *
 * Requires `@opentelemetry/api` as a peer dependency.
 *
 * **Send path:** injects `traceparent` into message headers from the
 * active OpenTelemetry context.
 *
 * **Consume path:** extracts `traceparent` from message headers,
 * starts a `CONSUMER` span as a child of the extracted context,
 * and ends it when the handler completes.
 *
 * @example
 * ```ts
 * import { otelInstrumentation } from '@drarzter/kafka-client/otel';
 *
 * const kafka = new KafkaClient('my-app', 'my-group', brokers, {
 *   instrumentation: [otelInstrumentation()],
 * });
 * ```
 */
export function otelInstrumentation(): KafkaInstrumentation {
  const tracer = trace.getTracer("@drarzter/kafka-client");
  const activeSpans = new Map<string, ReturnType<typeof tracer.startSpan>>();

  return {
    beforeSend(_topic: string, headers: Record<string, string>) {
      propagation.inject(context.active(), headers);
    },

    afterSend(_topic: string) {
      // Span management for producers is left to the caller's OTel setup.
      // We only inject context — creating producer spans here would be
      // inaccurate since buildSendPayload runs synchronously per-message.
    },

    beforeConsume(envelope: EventEnvelope<any>): BeforeConsumeResult {
      const parentCtx = propagation.extract(context.active(), envelope.headers);
      const span = tracer.startSpan(
        `kafka.consume ${envelope.topic}`,
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            "messaging.system": "kafka",
            "messaging.destination.name": envelope.topic,
            "messaging.message.id": envelope.eventId,
            "messaging.kafka.partition": envelope.partition,
            "messaging.kafka.offset": envelope.offset,
          },
        },
        parentCtx,
      );
      const spanCtx = trace.setSpan(parentCtx, span);
      activeSpans.set(envelope.eventId, span);
      return {
        cleanup() {
          span.end();
          activeSpans.delete(envelope.eventId);
        },
        wrap(fn) {
          return context.with(spanCtx, fn);
        },
      };
    },

    onConsumeError(envelope: EventEnvelope<any>, error: Error) {
      const span = activeSpans.get(envelope.eventId);
      if (span) {
        span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
        span.recordException(error);
      }
    },
  };
}
