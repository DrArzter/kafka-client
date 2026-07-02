import type { TopicMapConstraint } from "../types";
import type {
  OutboxMessage,
  OutboxProducer,
  OutboxRelayHandle,
  OutboxRelayOptions,
  OutboxStore,
} from "./outbox.types";

/** Coerce an unknown thrown value into an `Error`. */
function toError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}

/**
 * Start a transactional-outbox relay.
 *
 * The outbox pattern decouples "write my business state" from "publish an event"
 * so the two can never diverge: application code writes an event row into an
 * outbox table **in the same DB transaction** as its business writes; this relay
 * polls that table and publishes the rows to Kafka, marking them published only
 * after Kafka has acked them.
 *
 * ## Poll loop
 * Every `pollIntervalMs` the relay calls {@link OutboxStore.fetchUnpublished}
 * with `batchSize`. An empty result means there is nothing to do — it waits for
 * the next tick. A non-empty batch is published **entirely inside one Kafka
 * transaction** (`kafka.transaction`, one `tx.send` per message), and only after
 * that transaction commits does it call {@link OutboxStore.markPublished} with
 * the row ids.
 *
 * ## Delivery guarantee: at-least-once
 * If the process crashes **after** the Kafka transaction commits but **before**
 * `markPublished` runs, those rows are still unpublished in the store and will be
 * re-published on the next poll. This produces **duplicates**, but each duplicate
 * carries the **same** `x-event-id` (when {@link OutboxMessage.eventId} is set on
 * the row), so consumers can deduplicate — either via this library's Lamport-clock
 * deduplication or an application-level idempotency check keyed on the event id.
 * Persist a stable `eventId` on each outbox row to make this work.
 *
 * ## Failure handling
 * The loop never dies and never leaks an unhandled rejection:
 * - If the publishing transaction throws, `onError` fires, the batch is **not**
 *   marked published, and it is retried on the next tick.
 * - If the store throws (either `fetchUnpublished` or `markPublished`), same
 *   behaviour: `onError`, no marking, retried next tick.
 *
 * Note the one asymmetry: if `markPublished` throws *after* a successful Kafka
 * commit, the batch is re-published next tick — an intentional consequence of
 * the at-least-once guarantee above.
 *
 * ## Concurrency
 * Iterations never overlap. If an iteration runs longer than `pollIntervalMs`,
 * intervening ticks are skipped (a guard flag) rather than stacked.
 *
 * ## Shutdown
 * `handle.stop()` clears the timer and awaits any in-flight iteration before
 * resolving, so no publish is left half-done. It is idempotent.
 *
 * @param kafka  Any producer exposing `transaction()` — a `KafkaClient<T>` or
 *               `IKafkaProducer<T>`. Its producer must already be connected.
 * @param store  Your DB-backed {@link OutboxStore} implementation.
 * @param options Tuning and observability — see {@link OutboxRelayOptions}.
 *
 * @example
 * ```ts
 * // Pseudo-Postgres outbox store.
 * const store: OutboxStore = {
 *   async fetchUnpublished(limit) {
 *     const { rows } = await pool.query(
 *       `SELECT id, topic, payload, key, correlation_id AS "correlationId",
 *               event_id AS "eventId", headers
 *          FROM outbox
 *         WHERE published_at IS NULL
 *         ORDER BY created_at ASC
 *         LIMIT $1`,
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
 *
 * await kafka.connectProducer();
 * const relay = startOutboxRelay(kafka, store, {
 *   pollIntervalMs: 500,
 *   batchSize: 200,
 *   onPublished: (n) => metrics.increment('outbox.published', n),
 * });
 *
 * // On shutdown:
 * await relay.stop();
 * await kafka.disconnect();
 * ```
 */
export function startOutboxRelay<T extends TopicMapConstraint<T>>(
  kafka: OutboxProducer<T>,
  store: OutboxStore,
  options: OutboxRelayOptions = {},
): OutboxRelayHandle {
  const pollIntervalMs = options.pollIntervalMs ?? 1_000;
  const batchSize = options.batchSize ?? 100;
  const onError =
    options.onError ??
    ((error: Error, batch: OutboxMessage[]) => {
      // Default: log to stderr and keep the relay running.

      console.error(
        `[outbox] batch of ${batch.length} message(s) failed — will retry:`,
        error,
      );
    });
  const onPublished = options.onPublished;

  let stopped = false;
  /** True while an iteration is running — guards against overlapping ticks. */
  let running = false;
  /** The in-flight iteration promise, awaited by `stop()`. */
  let inFlight: Promise<void> = Promise.resolve();

  /**
   * One poll iteration. Fully self-contained: it swallows every error into
   * `onError` so it can never reject and never leak an unhandled rejection.
   */
  const iterate = async (): Promise<void> => {
    let batch: OutboxMessage[] = [];
    try {
      batch = await store.fetchUnpublished(batchSize);
      if (batch.length === 0) return;

      // Publish the whole batch atomically. Either every message lands or none.
      await kafka.transaction(async (tx) => {
        for (const msg of batch) {
          await tx.send(msg.topic as keyof T, msg.payload as T[keyof T], {
            key: msg.key,
            headers: msg.headers,
            correlationId: msg.correlationId,
            eventId: msg.eventId,
          });
        }
      });

      // Transaction committed — mark published. A crash before this line
      // re-publishes the batch next tick (at-least-once, same eventId).
      await store.markPublished(batch.map((m) => m.id));

      onPublished?.(batch.length);
    } catch (err) {
      onError(toError(err), batch);
    }
  };

  const tick = (): void => {
    if (stopped || running) return; // skip stacked ticks
    running = true;
    inFlight = iterate().finally(() => {
      running = false;
    });
  };

  const timer = setInterval(tick, pollIntervalMs);
  timer.unref?.();

  return {
    stop: async (): Promise<void> => {
      stopped = true;
      clearInterval(timer);
      // Await whatever iteration is currently in flight (no-op if idle).
      await inFlight;
    },
  };
}
