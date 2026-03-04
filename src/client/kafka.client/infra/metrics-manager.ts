import type { EventEnvelope } from "../../message/envelope";
import type { DlqReason, KafkaInstrumentation, KafkaMetrics } from "../../types";

export type MetricsManagerDeps = {
  instrumentation: KafkaInstrumentation[];
  onCircuitFailure: (envelope: EventEnvelope<any>, gid: string) => void;
  onCircuitSuccess: (envelope: EventEnvelope<any>, gid: string) => void;
};

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

  notifyRetry(envelope: EventEnvelope<any>, attempt: number, maxRetries: number): void {
    this.metricsFor(envelope.topic).retryCount++;
    for (const inst of this.deps.instrumentation) inst.onRetry?.(envelope, attempt, maxRetries);
  }

  notifyDlq(envelope: EventEnvelope<any>, reason: DlqReason, gid?: string): void {
    this.metricsFor(envelope.topic).dlqCount++;
    for (const inst of this.deps.instrumentation) inst.onDlq?.(envelope, reason);
    if (gid) this.deps.onCircuitFailure(envelope, gid);
  }

  notifyDuplicate(envelope: EventEnvelope<any>, strategy: "drop" | "dlq" | "topic"): void {
    this.metricsFor(envelope.topic).dedupCount++;
    for (const inst of this.deps.instrumentation) inst.onDuplicate?.(envelope, strategy);
  }

  notifyMessage(envelope: EventEnvelope<any>, gid?: string): void {
    this.metricsFor(envelope.topic).processedCount++;
    for (const inst of this.deps.instrumentation) inst.onMessage?.(envelope);
    if (gid) this.deps.onCircuitSuccess(envelope, gid);
  }

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

  resetMetrics(topic?: string): void {
    if (topic !== undefined) {
      this.topicMetrics.delete(topic);
      return;
    }
    this.topicMetrics.clear();
  }
}
