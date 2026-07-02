import {
  trace,
  context,
  propagation,
  metrics,
  SpanKind,
  SpanStatusCode,
} from "@opentelemetry/api";
import type { Meter, ObservableResult } from "@opentelemetry/api";
import type { BeforeConsumeResult, KafkaInstrumentation } from "./client/types";
import type { IKafkaAdmin } from "./client/types/admin.interface";
import type { TopicMapConstraint } from "./client/types/common";
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
  // Keyed by envelope object identity (not eventId) so two in-flight messages
  // that share an eventId cannot overwrite each other's span. WeakMap also
  // guarantees no leak if a cleanup path is ever missed.
  const activeSpans = new WeakMap<
    EventEnvelope<any>,
    ReturnType<typeof tracer.startSpan>
  >();

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
      activeSpans.set(envelope, span);
      return {
        cleanup() {
          span.end();
          activeSpans.delete(envelope);
        },
        wrap(fn) {
          return context.with(spanCtx, fn);
        },
      };
    },

    onConsumeError(envelope: EventEnvelope<any>, error: Error) {
      const span = activeSpans.get(envelope);
      if (span) {
        span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
        span.recordException(error);
      }
    },
  };
}

/**
 * Create a `KafkaInstrumentation` that records OpenTelemetry **metrics** for
 * both the send and consume paths.
 *
 * Requires `@opentelemetry/api` as a peer dependency. Instruments are created
 * once per instrumentation instance (not per message), so a single call to this
 * factory registers all counters/histograms exactly once.
 *
 * Recorded instruments (all under meter `@drarzter/kafka-client`):
 *
 * | Instrument | Type | Attributes | Recorded in |
 * |---|---|---|---|
 * | `kafka.client.messages.sent` | Counter | `topic` | `afterSend` |
 * | `kafka.client.messages.processed` | Counter | `topic` | `onMessage` |
 * | `kafka.client.messages.retried` | Counter | `topic` | `onRetry` |
 * | `kafka.client.messages.dlq` | Counter | `topic`, `reason` | `onDlq` |
 * | `kafka.client.messages.duplicate` | Counter | `topic`, `strategy` | `onDuplicate` |
 * | `kafka.client.consume.errors` | Counter | `topic` | `onConsumeError` |
 * | `kafka.client.consume.duration` | Histogram (ms) | `topic` | `beforeConsume` → `cleanup()` |
 *
 * Composes with `otelInstrumentation()` (traces): list both in
 * `instrumentation`. They share nothing and can be added in any order.
 *
 * @param options.meter - Override the meter used to create instruments.
 *   Defaults to `metrics.getMeter("@drarzter/kafka-client")`.
 *
 * @example
 * ```ts
 * import {
 *   otelInstrumentation,
 *   otelMetricsInstrumentation,
 * } from '@drarzter/kafka-client/otel';
 *
 * const kafka = new KafkaClient('my-app', 'my-group', brokers, {
 *   instrumentation: [otelInstrumentation(), otelMetricsInstrumentation()],
 * });
 * ```
 */
export function otelMetricsInstrumentation(options?: {
  meter?: Meter;
}): KafkaInstrumentation {
  const meter = options?.meter ?? metrics.getMeter("@drarzter/kafka-client");

  const sentCounter = meter.createCounter("kafka.client.messages.sent", {
    description: "Number of messages successfully sent to Kafka.",
  });
  const processedCounter = meter.createCounter(
    "kafka.client.messages.processed",
    {
      description: "Number of messages successfully processed by a consumer.",
    },
  );
  const retriedCounter = meter.createCounter("kafka.client.messages.retried", {
    description: "Number of messages queued for retry.",
  });
  const dlqCounter = meter.createCounter("kafka.client.messages.dlq", {
    description: "Number of messages routed to a DLQ topic.",
  });
  const duplicateCounter = meter.createCounter(
    "kafka.client.messages.duplicate",
    {
      description: "Number of Lamport-clock duplicate messages detected.",
    },
  );
  const consumeErrorsCounter = meter.createCounter(
    "kafka.client.consume.errors",
    {
      description: "Number of consumer handler errors.",
    },
  );
  const consumeDuration = meter.createHistogram(
    "kafka.client.consume.duration",
    {
      description: "Consumer handler duration.",
      unit: "ms",
    },
  );

  return {
    afterSend(topic: string) {
      sentCounter.add(1, { topic });
    },

    beforeConsume(envelope: EventEnvelope<any>): BeforeConsumeResult {
      const topic = envelope.topic;
      const start = Date.now();
      return {
        cleanup() {
          consumeDuration.record(Date.now() - start, { topic });
        },
      };
    },

    onConsumeError(envelope: EventEnvelope<any>, _error: Error) {
      consumeErrorsCounter.add(1, { topic: envelope.topic });
    },

    onRetry(envelope: EventEnvelope<any>, _attempt: number, _max: number) {
      retriedCounter.add(1, { topic: envelope.topic });
    },

    onDlq(envelope: EventEnvelope<any>, reason: string) {
      dlqCounter.add(1, { topic: envelope.topic, reason });
    },

    onDuplicate(
      envelope: EventEnvelope<any>,
      strategy: "drop" | "dlq" | "topic",
    ) {
      duplicateCounter.add(1, { topic: envelope.topic, strategy });
    },

    onMessage(envelope: EventEnvelope<any>) {
      processedCounter.add(1, { topic: envelope.topic });
    },
  };
}

/**
 * Register an OpenTelemetry **ObservableGauge** `kafka.client.consumer.lag`
 * that reports per-partition consumer lag by polling `kafka.getConsumerLag()`
 * on each metric collection cycle.
 *
 * Requires `@opentelemetry/api` as a peer dependency. Accepts any object
 * implementing the `IKafkaAdmin` sub-interface (i.e. any `KafkaClient`).
 *
 * The async callback swallows errors silently — a broker query failure during
 * a collection cycle simply reports no lag samples for that cycle rather than
 * throwing inside the OTel metric reader.
 *
 * Gauge attributes: `topic`, `partition`, and `groupId` (empty string when the
 * client's default group is used).
 *
 * @param kafka - The client (or any `IKafkaAdmin`) to poll for lag.
 * @param options.meter - Override the meter. Defaults to
 *   `metrics.getMeter("@drarzter/kafka-client")`.
 * @param options.groupId - Consumer group to query. Defaults to the client's
 *   constructor group.
 * @returns An unregister function that removes the observable callback. Call it
 *   on shutdown to stop observing.
 *
 * @example
 * ```ts
 * import { otelLagGauge } from '@drarzter/kafka-client/otel';
 *
 * const unregister = otelLagGauge(kafka, { groupId: 'billing-service' });
 * // ...later, on shutdown:
 * unregister();
 * ```
 */
export function otelLagGauge<T extends TopicMapConstraint<T>>(
  kafka: IKafkaAdmin<T>,
  options?: { meter?: Meter; groupId?: string },
): () => void {
  const meter = options?.meter ?? metrics.getMeter("@drarzter/kafka-client");
  const groupId = options?.groupId;

  const gauge = meter.createObservableGauge("kafka.client.consumer.lag", {
    description: "Consumer group lag per topic partition.",
  });

  const callback = async (result: ObservableResult) => {
    try {
      const lag = await kafka.getConsumerLag(groupId);
      for (const entry of lag) {
        result.observe(entry.lag, {
          topic: entry.topic,
          partition: entry.partition,
          groupId: groupId ?? "",
        });
      }
    } catch {
      // Swallow — a failed lag query should never break metric collection.
    }
  };

  gauge.addCallback(callback);

  return () => {
    gauge.removeCallback(callback);
  };
}
