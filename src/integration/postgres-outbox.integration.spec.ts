import "reflect-metadata";
import {
  GenericContainer,
  Wait,
  type StartedTestContainer,
} from "testcontainers";
import { Pool } from "pg";
import { randomUUID } from "crypto";
import type {
  OutboxMessage,
  OutboxStore,
} from "../client/outbox";
import { startOutboxRelay } from "../client/outbox";
import { KafkaClient, getBrokers } from "./helpers";

/**
 * Integration test proving a *real* Postgres-backed {@link OutboxStore} drives
 * {@link startOutboxRelay} correctly against the shared real Kafka broker,
 * including the documented at-least-once (duplicate, same eventId) semantics
 * when `markPublished` fails after a successful Kafka commit.
 *
 * Uses:
 *   - a real `postgres:16-alpine` container (started here) holding the outbox
 *     table, and
 *   - the shared real Kafka broker (from global-setup) as the message bus.
 *
 * The `PostgresOutboxStore` below is a REFERENCE implementation — small and
 * heavily commented so users can copy it into their own codebase.
 */

// ── Reference implementation — copy this into your app ────────────────────────

/**
 * Persistent {@link OutboxStore} backed by Postgres.
 *
 * Assumes an `outbox` table:
 * ```sql
 * CREATE TABLE outbox (
 *   id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 *   topic          text        NOT NULL,
 *   payload        jsonb       NOT NULL,
 *   key            text,
 *   headers        jsonb,
 *   correlation_id text,
 *   event_id       text,
 *   published_at   timestamptz,
 *   created_at     timestamptz NOT NULL DEFAULT now()
 * );
 * ```
 *
 * Application code inserts a row here inside the SAME database transaction as
 * its business writes; this store lets the relay read and mark those rows.
 */
class PostgresOutboxStore implements OutboxStore {
  constructor(private readonly pool: Pool) {}

  /**
   * Fetch up to `limit` unpublished rows, oldest first.
   *
   * Concurrency note: a single-relay deployment can use a plain SELECT. If you
   * run MULTIPLE relay instances against the same table, wrap this in a
   * transaction and add `FOR UPDATE SKIP LOCKED` so each instance claims a
   * disjoint set of rows without blocking the others:
   *
   * ```sql
   * BEGIN;
   *   SELECT ... WHERE published_at IS NULL
   *   ORDER BY created_at ASC LIMIT $1
   *   FOR UPDATE SKIP LOCKED;
   *   -- publish + markPublished happen while the rows stay locked, then COMMIT
   * COMMIT;
   * ```
   *
   * The single-relay form is shown here for clarity.
   */
  async fetchUnpublished(limit: number): Promise<OutboxMessage[]> {
    const { rows } = await this.pool.query(
      `SELECT id::text                AS id,
              topic                    AS topic,
              payload                  AS payload,
              key                      AS key,
              headers                  AS headers,
              correlation_id           AS "correlationId",
              event_id                 AS "eventId"
         FROM outbox
        WHERE published_at IS NULL
        ORDER BY created_at ASC
        LIMIT $1`,
      [limit],
    );
    // `payload`/`headers` come back already parsed (jsonb → JS object).
    return rows.map((r) => ({
      id: r.id,
      topic: r.topic,
      payload: r.payload,
      key: r.key ?? undefined,
      headers: r.headers ?? undefined,
      correlationId: r.correlationId ?? undefined,
      eventId: r.eventId ?? undefined,
    }));
  }

  /**
   * Durably mark rows published. Idempotent — re-marking an already-published
   * row is a harmless no-op. Called only AFTER Kafka acks the batch.
   */
  async markPublished(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.pool.query(
      `UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])`,
      [ids],
    );
  }
}

// ── Test-only helpers ─────────────────────────────────────────────────────────

/**
 * Wraps a store so `markPublished` can be made to throw exactly once, letting us
 * exercise the relay's at-least-once path: Kafka committed, then `markPublished`
 * failed → the batch is re-published (duplicate, same eventId) on the next tick.
 */
class FlakyMarkStore implements OutboxStore {
  failNextMark = false;

  constructor(private readonly inner: OutboxStore) {}

  fetchUnpublished(limit: number): Promise<OutboxMessage[]> {
    return this.inner.fetchUnpublished(limit);
  }

  async markPublished(ids: string[]): Promise<void> {
    if (this.failNextMark) {
      this.failNextMark = false;
      throw new Error("simulated markPublished failure (post Kafka commit)");
    }
    await this.inner.markPublished(ids);
  }
}

type OutboxTopics = Record<string, { orderId: string; amount: number }>;

function createOutboxClient(name: string): KafkaClient<OutboxTopics> {
  return new KafkaClient<OutboxTopics>(
    `integration-${name}`,
    `group-${name}-${Date.now()}`,
    getBrokers(),
    {
      autoCreateTopics: true,
      // Unique transactional id per client — the relay publishes via kafka.transaction().
      transactionalId: `outbox-tx-${name}-${Date.now()}`,
    },
  ) as unknown as KafkaClient<OutboxTopics>;
}

function uniqueTopic(): string {
  return `test.pg-outbox.${Date.now()}.${Math.floor(Math.random() * 1e6)}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
  cond: () => boolean | Promise<boolean>,
  timeoutMs: number,
): Promise<void> {
  const start = Date.now();
  while (!(await cond())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await sleep(100);
  }
}

/** Insert one business event into the outbox table (simulates an app write). */
async function insertOutboxRow(
  pool: Pool,
  row: {
    id: string;
    topic: string;
    payload: unknown;
    key?: string;
    correlationId?: string;
    eventId?: string;
  },
): Promise<void> {
  await pool.query(
    `INSERT INTO outbox (id, topic, payload, key, correlation_id, event_id)
     VALUES ($1, $2, $3::jsonb, $4, $5, $6)`,
    [
      row.id,
      row.topic,
      JSON.stringify(row.payload),
      row.key ?? null,
      row.correlationId ?? null,
      row.eventId ?? null,
    ],
  );
}

// ── Suite ─────────────────────────────────────────────────────────────────────

describe("Integration — Postgres-backed OutboxStore + startOutboxRelay against real Postgres + Kafka", () => {
  let pgContainer: StartedTestContainer;
  let pool: Pool;

  beforeAll(async () => {
    pgContainer = await new GenericContainer("postgres:16-alpine")
      .withExposedPorts(5432)
      .withEnvironment({
        POSTGRES_PASSWORD: "outbox",
        POSTGRES_USER: "outbox",
        POSTGRES_DB: "outbox",
      })
      .withWaitStrategy(
        Wait.forLogMessage(
          /database system is ready to accept connections/,
          2,
        ),
      )
      .start();

    pool = new Pool({
      host: pgContainer.getHost(),
      port: pgContainer.getMappedPort(5432),
      user: "outbox",
      password: "outbox",
      database: "outbox",
      max: 5,
    });

    // Create the outbox table exactly as documented for the reference store.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS outbox (
        id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        topic          text        NOT NULL,
        payload        jsonb       NOT NULL,
        key            text,
        headers        jsonb,
        correlation_id text,
        event_id       text,
        published_at   timestamptz,
        created_at     timestamptz NOT NULL DEFAULT now()
      )
    `);
  }, 120_000);

  afterAll(async () => {
    try {
      await pool?.end();
    } catch {
      // ignore
    }
    try {
      await pgContainer?.stop();
    } catch {
      // ignore
    }
  }, 60_000);

  it("relays inserted rows to Kafka (with matching eventId), marks them published, and picks up rows inserted while running", async () => {
    const topic = uniqueTopic();
    const store = new PostgresOutboxStore(pool);

    const producer = createOutboxClient("pg-outbox-relay");
    const consumerClient = createOutboxClient("pg-outbox-cons");
    await producer.connectProducer();
    await consumerClient.connectProducer();

    // ── Consumer on the target topic collects arrivals (topic + eventId) ──────
    const received: Array<{
      orderId: string;
      amount: number;
      eventId: string;
    }> = [];
    const handle = await consumerClient.startConsumer(
      [topic],
      async (envelope) => {
        received.push({
          orderId: envelope.payload.orderId,
          amount: envelope.payload.amount,
          eventId: envelope.eventId,
        });
      },
      { groupId: `pg-outbox-cons-${Date.now()}`, fromBeginning: true },
    );
    await handle.ready();

    // ── Simulate 3 business writes into the outbox table ──────────────────────
    const initial = [
      { id: randomUUID(), eventId: randomUUID(), orderId: "o1", amount: 100 },
      { id: randomUUID(), eventId: randomUUID(), orderId: "o2", amount: 200 },
      { id: randomUUID(), eventId: randomUUID(), orderId: "o3", amount: 300 },
    ];
    for (const r of initial) {
      await insertOutboxRow(pool, {
        id: r.id,
        topic,
        payload: { orderId: r.orderId, amount: r.amount },
        eventId: r.eventId,
      });
    }

    // ── Start the relay against the REAL broker ───────────────────────────────
    const relay = startOutboxRelay(producer, store, { pollIntervalMs: 300 });

    // All 3 must arrive with correct payloads AND the eventId stored on the row.
    await waitFor(() => received.length >= 3, 60_000);
    for (const r of initial) {
      const match = received.find((m) => m.orderId === r.orderId);
      expect(match).toBeDefined();
      expect(match!.amount).toBe(r.amount);
      // The relay preserved the stored eventId — the linchpin of dedup on replay.
      expect(match!.eventId).toBe(r.eventId);
    }

    // Rows were marked published in Postgres.
    await waitFor(async () => {
      const { rows } = await pool.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM outbox WHERE topic = $1 AND published_at IS NULL`,
        [topic],
      );
      return Number(rows[0].n) === 0;
    }, 30_000);
    const { rows: publishedRows } = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM outbox WHERE topic = $1 AND published_at IS NOT NULL`,
      [topic],
    );
    expect(Number(publishedRows[0].n)).toBe(3);

    // ── Insert 2 MORE rows while the relay is running → they must arrive too ───
    const extra = [
      { id: randomUUID(), eventId: randomUUID(), orderId: "o4", amount: 400 },
      { id: randomUUID(), eventId: randomUUID(), orderId: "o5", amount: 500 },
    ];
    for (const r of extra) {
      await insertOutboxRow(pool, {
        id: r.id,
        topic,
        payload: { orderId: r.orderId, amount: r.amount },
        eventId: r.eventId,
      });
    }

    await waitFor(() => received.length >= 5, 60_000);
    for (const r of extra) {
      const match = received.find((m) => m.orderId === r.orderId);
      expect(match).toBeDefined();
      expect(match!.amount).toBe(r.amount);
      expect(match!.eventId).toBe(r.eventId);
    }

    await relay.stop();
    await handle.stop();
    await producer.disconnect();
    await consumerClient.disconnect();
  }, 150_000);

  it("re-publishes a batch (duplicate with SAME eventId) when markPublished fails once — at-least-once", async () => {
    const topic = uniqueTopic();
    const store = new FlakyMarkStore(new PostgresOutboxStore(pool));

    const producer = createOutboxClient("pg-outbox-flaky");
    const consumerClient = createOutboxClient("pg-outbox-flaky-cons");
    await producer.connectProducer();
    await consumerClient.connectProducer();

    // Record every arrival keyed by eventId to detect duplicates.
    const arrivalsByEventId = new Map<string, number>();
    const handle = await consumerClient.startConsumer(
      [topic],
      async (envelope) => {
        arrivalsByEventId.set(
          envelope.eventId,
          (arrivalsByEventId.get(envelope.eventId) ?? 0) + 1,
        );
      },
      { groupId: `pg-outbox-flaky-cons-${Date.now()}`, fromBeginning: true },
    );
    await handle.ready();

    // Single business event so the whole batch is one row → deterministic dup.
    const eventId = randomUUID();
    const rowId = randomUUID();
    await insertOutboxRow(pool, {
      id: rowId,
      topic,
      payload: { orderId: "flaky-1", amount: 999 },
      eventId,
    });

    // Arm the store to fail markPublished exactly once. The relay will:
    //   tick 1: Kafka commit OK → markPublished throws → onError, row still
    //           unpublished → published once, NOT marked.
    //   tick 2: same row re-fetched → Kafka commit OK → markPublished OK →
    //           published a SECOND time (duplicate, same eventId), then marked.
    store.failNextMark = true;

    const relay = startOutboxRelay(producer, store, { pollIntervalMs: 300 });

    // Wait until the SAME eventId has been observed at least twice — proof the
    // batch was re-published after the markPublished failure.
    await waitFor(() => (arrivalsByEventId.get(eventId) ?? 0) >= 2, 60_000);
    expect(arrivalsByEventId.get(eventId)).toBeGreaterThanOrEqual(2);

    // The row is eventually marked published (the second tick's mark succeeded).
    await waitFor(async () => {
      const { rows } = await pool.query<{ published: boolean }>(
        `SELECT (published_at IS NOT NULL) AS published FROM outbox WHERE id = $1`,
        [rowId],
      );
      return rows.length > 0 && rows[0].published === true;
    }, 30_000);
    const { rows: finalRows } = await pool.query<{ published: boolean }>(
      `SELECT (published_at IS NOT NULL) AS published FROM outbox WHERE id = $1`,
      [rowId],
    );
    expect(finalRows[0].published).toBe(true);

    await relay.stop();
    await handle.stop();
    await producer.disconnect();
    await consumerClient.disconnect();
  }, 150_000);
});
