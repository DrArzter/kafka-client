import type { EventEnvelope } from "../../message/envelope";
import type { DlqReason, KafkaInstrumentation, KafkaMetrics } from "../../types";

export type MetricsManagerDeps = {
  instrumentation: KafkaInstrumentation[];
  onCircuitFailure: (envelope: EventEnvelope<any>, gid: string) => void;
  onCircuitSuccess: (envelope: EventEnvelope<any>, gid: string) => void;
};

/**
 * Maintains per-topic event counters and dispatches instrumentation hooks.
 * Created once per `KafkaClient` instance and shared across all consumers and producers.
 */
export class MetricsManager {
  private readonly topicMetrics = new Map<string, KafkaMetrics>();

  constructor(private readonly deps: MetricsManagerDeps) {}

  private metricsFor(topic: string): KafkaMetrics {
    let m = this.topicMetrics.get(topic);
    if (!m) {
      m = { processedCount: 0, retryCount: 0, dlqCount: 0, dedupCount: 0 };
      this.topicMetrics.set(topic, m);
    }
    return m;
  }

  /** Fire `afterSend` instrumentation hooks for each message in a batch. */
  notifyAfterSend(topic: string, count: number): void {
    for (let i = 0; i < count; i++)
      for (const inst of this.deps.instrumentation) inst.afterSend?.(topic);
  }

  /**
   * Increment the retry counter for the envelope's topic and fire all `onRetry` instrumentation hooks.
   * @param envelope The message envelope being retried.
   * @param attempt Current retry attempt number (1-based).
   * @param maxRetries Maximum number of retries configured for this consumer.
   */
  notifyRetry(envelope: EventEnvelope<any>, attempt: number, maxRetries: number): void {
    this.metricsFor(envelope.topic).retryCount++;
    for (const inst of this.deps.instrumentation) inst.onRetry?.(envelope, attempt, maxRetries);
  }

  /**
   * Increment the DLQ counter for the envelope's topic, fire all `onDlq` instrumentation hooks,
   * and notify the circuit breaker of a failure (when `gid` is provided).
   * @param envelope The message envelope being sent to the DLQ.
   * @param reason The reason the message is being dead-lettered.
   * @param gid Consumer group ID — used to drive circuit breaker state.
   */
  notifyDlq(envelope: EventEnvelope<any>, reason: DlqReason, gid?: string): void {
    this.metricsFor(envelope.topic).dlqCount++;
    for (const inst of this.deps.instrumentation) inst.onDlq?.(envelope, reason);
    if (gid) this.deps.onCircuitFailure(envelope, gid);
  }

  /**
   * Increment the deduplication counter for the envelope's topic and fire all `onDuplicate` hooks.
   * @param envelope The duplicate message envelope.
   * @param strategy The deduplication strategy applied (`"drop"`, `"dlq"`, or `"topic"`).
   */
  notifyDuplicate(envelope: EventEnvelope<any>, strategy: "drop" | "dlq" | "topic"): void {
    this.metricsFor(envelope.topic).dedupCount++;
    for (const inst of this.deps.instrumentation) inst.onDuplicate?.(envelope, strategy);
  }

  /**
   * Increment the processed counter for the envelope's topic, fire all `onMessage` hooks,
   * and notify the circuit breaker of a success (when `gid` is provided).
   * @param envelope The successfully processed message envelope.
   * @param gid Consumer group ID — used to drive circuit breaker state.
   */
  notifyMessage(envelope: EventEnvelope<any>, gid?: string): void {
    this.metricsFor(envelope.topic).processedCount++;
    for (const inst of this.deps.instrumentation) inst.onMessage?.(envelope);
    if (gid) this.deps.onCircuitSuccess(envelope, gid);
  }

  /**
   * Return a snapshot of event counters.
   * @param topic When provided, returns counters for that topic only; otherwise aggregates all topics.
   * @returns Read-only `KafkaMetrics` snapshot. Returns zero-valued counters if the topic has no events.
   */
  getMetrics(topic?: string): Readonly<KafkaMetrics> {
    if (topic !== undefined) {
      const m = this.topicMetrics.get(topic);
      return m ? { ...m } : { processedCount: 0, retryCount: 0, dlqCount: 0, dedupCount: 0 };
    }
    const agg: KafkaMetrics = { processedCount: 0, retryCount: 0, dlqCount: 0, dedupCount: 0 };
    for (const m of this.topicMetrics.values()) {
      agg.processedCount += m.processedCount;
      agg.retryCount += m.retryCount;
      agg.dlqCount += m.dlqCount;
      agg.dedupCount += m.dedupCount;
    }
    return agg;
  }

  /**
   * Reset event counters to zero.
   * @param topic When provided, clears counters for that topic only; otherwise clears all topics.
   */
  resetMetrics(topic?: string): void {
    if (topic !== undefined) {
      this.topicMetrics.delete(topic);
      return;
    }
    this.topicMetrics.clear();
  }
}
