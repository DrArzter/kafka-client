import type { EventEnvelope } from "../../message/envelope";
import type { CircuitBreakerOptions, KafkaInstrumentation, KafkaLogger } from "../../types";

type CircuitState = {
  status: "closed" | "open" | "half-open";
  window: boolean[];
  successes: number;
  timer?: ReturnType<typeof setTimeout>;
};

type CircuitBreakerDeps = {
  pauseConsumer: (gid: string, assignments: Array<{ topic: string; partitions: number[] }>) => void;
  resumeConsumer: (gid: string, assignments: Array<{ topic: string; partitions: number[] }>) => void;
  logger: KafkaLogger;
  instrumentation: KafkaInstrumentation[];
};

export class CircuitBreakerManager {
  private readonly states = new Map<string, CircuitState>();
  private readonly configs = new Map<string, CircuitBreakerOptions>();

  constructor(private readonly deps: CircuitBreakerDeps) {}

  setConfig(gid: string, options: CircuitBreakerOptions): void {
    this.configs.set(gid, options);
  }

  /**
   * Returns a snapshot of the circuit breaker state for a given topic-partition.
   * Returns `undefined` when no state exists for the key.
   */
  getState(
    topic: string,
    partition: number,
    gid: string,
  ): { status: "closed" | "open" | "half-open"; failures: number; windowSize: number } | undefined {
    const state = this.states.get(`${gid}:${topic}:${partition}`);
    if (!state) return undefined;
    return {
      status: state.status,
      failures: state.window.filter((v) => !v).length,
      windowSize: state.window.length,
    };
  }

  /**
   * Record a failure for the given envelope and group.
   * Drives the CLOSED → OPEN and HALF-OPEN → OPEN transitions.
   */
  onFailure(envelope: EventEnvelope<any>, gid: string): void {
    const cfg = this.configs.get(gid);
    if (!cfg) return;

    const threshold = cfg.threshold ?? 5;
    const recoveryMs = cfg.recoveryMs ?? 30_000;

    const stateKey = `${gid}:${envelope.topic}:${envelope.partition}`;
    let state = this.states.get(stateKey);
    if (!state) {
      state = { status: "closed", window: [], successes: 0 };
      this.states.set(stateKey, state);
    }
    if (state.status === "open") return;

    const openCircuit = () => {
      state!.status = "open";
      state!.window = [];
      state!.successes = 0;
      clearTimeout(state!.timer);
      for (const inst of this.deps.instrumentation)
        inst.onCircuitOpen?.(envelope.topic, envelope.partition);
      this.deps.pauseConsumer(gid, [{ topic: envelope.topic, partitions: [envelope.partition] }]);
      state!.timer = setTimeout(() => {
        state!.status = "half-open";
        state!.successes = 0;
        this.deps.logger.log(
          `[CircuitBreaker] HALF-OPEN — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition}`,
        );
        for (const inst of this.deps.instrumentation)
          inst.onCircuitHalfOpen?.(envelope.topic, envelope.partition);
        this.deps.resumeConsumer(gid, [{ topic: envelope.topic, partitions: [envelope.partition] }]);
      }, recoveryMs);
    };

    if (state.status === "half-open") {
      clearTimeout(state.timer);
      this.deps.logger.warn(
        `[CircuitBreaker] OPEN (half-open failure) — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition}`,
      );
      openCircuit();
      return;
    }

    // CLOSED: update sliding window
    const windowSize = cfg.windowSize ?? Math.max(threshold * 2, 10);
    state.window = [...state.window, false];
    if (state.window.length > windowSize) {
      state.window = state.window.slice(state.window.length - windowSize);
    }
    const failures = state.window.filter((v) => !v).length;

    if (failures >= threshold) {
      this.deps.logger.warn(
        `[CircuitBreaker] OPEN — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition} ` +
          `(${failures}/${state.window.length} failures, threshold=${threshold})`,
      );
      openCircuit();
    }
  }

  /**
   * Record a success for the given envelope and group.
   * Drives the HALF-OPEN → CLOSED transition and updates the success window.
   */
  onSuccess(envelope: EventEnvelope<any>, gid: string): void {
    const cfg = this.configs.get(gid);
    if (!cfg) return;

    const stateKey = `${gid}:${envelope.topic}:${envelope.partition}`;
    const state = this.states.get(stateKey);
    if (!state) return;

    const halfOpenSuccesses = cfg.halfOpenSuccesses ?? 1;

    if (state.status === "half-open") {
      state.successes++;
      if (state.successes >= halfOpenSuccesses) {
        clearTimeout(state.timer);
        state.timer = undefined;
        state.status = "closed";
        state.window = [];
        state.successes = 0;
        this.deps.logger.log(
          `[CircuitBreaker] CLOSED — group="${gid}" topic="${envelope.topic}" partition=${envelope.partition}`,
        );
        for (const inst of this.deps.instrumentation)
          inst.onCircuitClose?.(envelope.topic, envelope.partition);
      }
    } else if (state.status === "closed") {
      const threshold = cfg.threshold ?? 5;
      const windowSize = cfg.windowSize ?? Math.max(threshold * 2, 10);
      state.window = [...state.window, true];
      if (state.window.length > windowSize) {
        state.window = state.window.slice(state.window.length - windowSize);
      }
    }
  }

  /**
   * Remove all circuit state and config for the given group.
   * Called when a consumer is stopped via `stopConsumer(groupId)`.
   */
  removeGroup(gid: string): void {
    for (const key of [...this.states.keys()]) {
      if (key.startsWith(`${gid}:`)) {
        clearTimeout(this.states.get(key)!.timer);
        this.states.delete(key);
      }
    }
    this.configs.delete(gid);
  }

  /** Clear all circuit state and config. Called on `disconnect()`. */
  clear(): void {
    for (const state of this.states.values()) clearTimeout(state.timer);
    this.states.clear();
    this.configs.clear();
  }
}
