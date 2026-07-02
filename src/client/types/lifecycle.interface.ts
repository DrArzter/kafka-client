import type { ClientId, KafkaMetrics } from "./common";

/** Lifecycle and observability methods of `IKafkaClient`. */
export interface IKafkaLifecycle {
  /**
   * @example
   * ```ts
   * const id = kafka.getClientId(); // e.g. 'my-service'
   * ```
   */
  getClientId(): ClientId;

  /**
   * Return a snapshot of internal event counters (retry / DLQ / dedup).
   * - `getMetrics()` — aggregate across all topics.
   * - `getMetrics(topic)` — counters for a specific topic only; returns all-zero
   *   if no events have been observed for that topic yet.
   *
   * Counters accumulate since client creation or the last `resetMetrics()` call.
   *
   * @example
   * ```ts
   * const { processedCount, dlqCount, retryCount } = kafka.getMetrics();
   * console.log(`Processed: ${processedCount}, DLQ: ${dlqCount}`);
   * ```
   */
  getMetrics(topic?: string): Readonly<KafkaMetrics>;

  /**
   * Reset internal event counters to zero.
   * - `resetMetrics()` — reset all topics.
   * - `resetMetrics(topic)` — reset a single topic only.
   *
   * @example
   * ```ts
   * kafka.resetMetrics();
   * kafka.resetMetrics('orders.created');
   * ```
   */
  resetMetrics(topic?: string): void;

  /**
   * Connect the producer, recover the Lamport clock (when `clockRecovery` is
   * configured), and start the lag-throttle poller (when `lagThrottle` is set).
   * Must be called before any send. NestJS apps call this automatically inside
   * `KafkaModule.register()` / `registerAsync()`.
   *
   * @example
   * ```ts
   * await kafka.connectProducer();
   * ```
   */
  connectProducer(): Promise<void>;

  /**
   * Drain in-flight handlers, then disconnect all producers, consumers, and admin.
   * @param drainTimeoutMs Max ms to wait for in-flight handlers (default 30 000).
   *
   * @example
   * ```ts
   * await kafka.disconnect();
   * ```
   */
  disconnect(drainTimeoutMs?: number): Promise<void>;

  /**
   * Register SIGTERM / SIGINT signal handlers that drain in-flight messages before
   * disconnecting. Call once after constructing the client in non-NestJS apps.
   * NestJS apps get drain automatically via `onModuleDestroy` → `disconnect()`.
   *
   * @example
   * ```ts
   * kafka.enableGracefulShutdown(['SIGTERM', 'SIGINT'], 30_000);
   * ```
   */
  enableGracefulShutdown(
    signals?: NodeJS.Signals[],
    drainTimeoutMs?: number,
  ): void;
}
