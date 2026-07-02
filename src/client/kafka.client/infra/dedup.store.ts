import type { DedupStore } from "../../types";

/**
 * In-memory {@link DedupStore} backed by a nested map:
 * `groupId` → `"topic:partition"` → last processed Lamport clock.
 *
 * This is the default store used when `DeduplicationOptions.store` is not set.
 * State lives only in the current process and is cleared on `stopConsumer` /
 * `disconnect` and lost on restart or rebalance. Provide a persistent
 * `DedupStore` (e.g. Redis-backed) to survive restarts and rebalances.
 *
 * The backing map is injected so the client can share a single map across all
 * consumer groups and clear it during shutdown.
 *
 * @example
 * ```ts
 * const states = new Map<string, Map<string, number>>();
 * const store = new InMemoryDedupStore(states);
 * store.setLastClock('billing', 'orders:0', 42);
 * store.getLastClock('billing', 'orders:0'); // 42
 * ```
 */
export class InMemoryDedupStore implements DedupStore {
  constructor(
    private readonly states: Map<string, Map<string, number>>,
  ) {}

  getLastClock(groupId: string, topicPartition: string): number | undefined {
    return this.states.get(groupId)?.get(topicPartition);
  }

  setLastClock(groupId: string, topicPartition: string, clock: number): void {
    let group = this.states.get(groupId);
    if (!group) {
      group = new Map();
      this.states.set(groupId, group);
    }
    group.set(topicPartition, clock);
  }
}
