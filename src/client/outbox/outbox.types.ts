import type { MessageHeaders, TopicMapConstraint } from "../types";
import type { TransactionContext } from "../types/consumer.types";

/**
 * A single row from the application's transactional-outbox table.
 *
 * Application code inserts one of these into the outbox table inside the **same
 * database transaction** as its business writes. The relay ({@link startOutboxRelay})
 * later reads unpublished rows and publishes them to Kafka.
 *
 * @example
 * ```ts
 * // Inside a DB transaction, alongside your business INSERT/UPDATE:
 * await tx.query(
 *   `INSERT INTO outbox (id, topic, payload, key, correlation_id, event_id)
 *    VALUES ($1, $2, $3, $4, $5, $6)`,
 *   [randomUUID(), 'orders.created', JSON.stringify(order), order.id, corrId, eventId],
 * );
 * ```
 */
export interface OutboxMessage {
  /**
   * Unique row identifier. Used purely for idempotent marking — the relay passes
   * these ids to {@link OutboxStore.markPublished} after Kafka acks the batch.
   * This is **not** the Kafka `eventId` (use {@link OutboxMessage.eventId} for that).
   */
  id: string;
  /** Destination Kafka topic. */
  topic: string;
  /** JSON-serialisable message body. Published as the Kafka message value. */
  payload: unknown;
  /** Optional Kafka partition key (guarantees per-key ordering). */
  key?: string;
  /** Optional custom Kafka headers merged with the auto-generated envelope headers. */
  headers?: MessageHeaders;
  /** Optional correlation id propagated to the Kafka `x-correlation-id` header. */
  correlationId?: string;
  /**
   * Optional stable Kafka `x-event-id`. Persist this on the outbox row so that a
   * re-published duplicate (see {@link startOutboxRelay} at-least-once semantics)
   * carries the **same** event id, letting consumers deduplicate.
   */
  eventId?: string;
}

/**
 * DB-agnostic contract the application implements to back the outbox relay.
 *
 * The library never touches your database directly — you own the schema and the
 * queries. The relay only needs to (1) read unpublished rows oldest-first and
 * (2) durably mark rows published once Kafka has acked them.
 *
 * A ready-made in-memory implementation ({@link InMemoryOutboxStore}) is provided
 * for tests and as executable documentation.
 *
 * @example
 * ```ts
 * // Pseudo-Postgres store:
 * const store: OutboxStore = {
 *   async fetchUnpublished(limit) {
 *     const { rows } = await pool.query(
 *       `SELECT id, topic, payload, key, correlation_id AS "correlationId",
 *               event_id AS "eventId", headers
 *        FROM outbox
 *        WHERE published_at IS NULL
 *        ORDER BY created_at ASC
 *        LIMIT $1`,
 *       [limit],
 *     );
 *     return rows;
 *   },
 *   async markPublished(ids) {
 *     await pool.query(
 *       `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`,
 *       [ids],
 *     );
 *   },
 * };
 * ```
 */
export interface OutboxStore {
  /**
   * Fetch up to `limit` unpublished messages, **oldest first**.
   *
   * Must be safe to call concurrently with inserts from application code. The
   * relay calls this once per poll tick; returning an empty array signals there
   * is nothing to publish and the relay simply waits for the next tick.
   */
  fetchUnpublished(limit: number): Promise<OutboxMessage[]>;
  /**
   * Durably mark these ids as published. Called **only after** Kafka has acked
   * the entire batch (i.e. the publishing transaction committed).
   *
   * This write should be durable and idempotent — a crash between the Kafka
   * commit and this call causes the batch to be re-published on the next poll
   * (at-least-once, see {@link startOutboxRelay}).
   */
  markPublished(ids: string[]): Promise<void>;
}

/**
 * Tuning and observability options for {@link startOutboxRelay}.
 *
 * @example
 * ```ts
 * startOutboxRelay(kafka, store, {
 *   pollIntervalMs: 500,
 *   batchSize: 200,
 *   onPublished: (n) => metrics.increment('outbox.published', n),
 *   onError: (err, batch) =>
 *     logger.error(`outbox batch of ${batch.length} failed`, err),
 * });
 * ```
 */
export interface OutboxRelayOptions {
  /** Interval between poll ticks in ms. Default: `1000`. */
  pollIntervalMs?: number;
  /** Maximum rows fetched (and published in one transaction) per tick. Default: `100`. */
  batchSize?: number;
  /**
   * Called when a poll iteration fails — either the publishing transaction
   * threw or the store threw. The failed `batch` is **not** marked published and
   * is retried on the next tick. The relay keeps running regardless.
   *
   * Default: log the error via the client logger.
   */
  onError?: (error: Error, batch: OutboxMessage[]) => void;
  /** Called after a batch is successfully published and marked, with the batch size. */
  onPublished?: (count: number) => void;
}

/**
 * The narrowest producer surface the outbox relay needs: the ability to run an
 * atomic Kafka transaction. Both `KafkaClient<T>` and the public
 * `IKafkaProducer<T>` interface satisfy this.
 */
export interface OutboxProducer<T extends TopicMapConstraint<T>> {
  transaction(fn: (ctx: TransactionContext<T>) => Promise<void>): Promise<void>;
}

/** Handle returned by {@link startOutboxRelay}. */
export interface OutboxRelayHandle {
  /**
   * Stop the relay: halt the timer and wait for any in-flight iteration to
   * finish before resolving. Idempotent.
   */
  stop(): Promise<void>;
}
