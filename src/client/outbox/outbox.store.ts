import type { OutboxMessage, OutboxStore } from "./outbox.types";

/**
 * In-memory reference implementation of {@link OutboxStore}.
 *
 * Intended for tests and as executable documentation of the expected
 * {@link OutboxStore} semantics — **not** for production use: it provides no
 * durability, so a process restart loses all pending rows and the "same DB
 * transaction as the business write" guarantee (the whole point of the outbox
 * pattern) does not hold. Back a real relay with a persistent store (Postgres,
 * MySQL, …) implementing the same interface.
 *
 * Semantics mirrored:
 * - {@link fetchUnpublished} returns unpublished rows **oldest first** (insertion
 *   order), capped at `limit`.
 * - {@link markPublished} is idempotent — unknown ids are ignored.
 *
 * @example
 * ```ts
 * const store = new InMemoryOutboxStore();
 * store.add({ id: '1', topic: 'orders.created', payload: { orderId: 'o1' } });
 *
 * const handle = startOutboxRelay(kafka, store);
 * // ... later
 * await handle.stop();
 * ```
 */
export class InMemoryOutboxStore implements OutboxStore {
  /** Insertion-ordered rows. `published` flips to true after `markPublished`. */
  private readonly rows: Array<{ message: OutboxMessage; published: boolean }> =
    [];

  /**
   * Append a message to the outbox. In a real store this INSERT would run inside
   * the same DB transaction as the corresponding business write.
   */
  add(message: OutboxMessage): void {
    this.rows.push({ message, published: false });
  }

  async fetchUnpublished(limit: number): Promise<OutboxMessage[]> {
    const out: OutboxMessage[] = [];
    for (const row of this.rows) {
      if (row.published) continue;
      out.push(row.message);
      if (out.length >= limit) break;
    }
    return out;
  }

  async markPublished(ids: string[]): Promise<void> {
    const idSet = new Set(ids);
    for (const row of this.rows) {
      if (idSet.has(row.message.id)) row.published = true;
    }
  }

  /** Test helper: count of rows not yet marked published. */
  get pendingCount(): number {
    return this.rows.filter((r) => !r.published).length;
  }

  /** Test helper: count of rows marked published. */
  get publishedCount(): number {
    return this.rows.filter((r) => r.published).length;
  }
}
