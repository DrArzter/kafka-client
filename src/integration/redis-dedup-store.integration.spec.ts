import "reflect-metadata";
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers";
import { createClient as createRedisClient, type RedisClientType } from "redis";
import type { DedupStore } from "../client/kafka.client";
import type { KafkaLogger } from "../client/kafka.client";
import { KafkaClient, getBrokers } from "./helpers";

/**
 * Integration test proving a *real* Redis-backed {@link DedupStore} makes
 * Lamport-clock deduplication survive a process restart — the exact scenario
 * the built-in in-memory store cannot survive (its state resets whenever the
 * consumer stops or the process restarts).
 *
 * Uses:
 *   - a real `redis:7-alpine` container (started here) as the store backend, and
 *   - the shared real Kafka broker (from global-setup) as the message bus.
 *
 * The `RedisDedupStore` below is written as a REFERENCE implementation: it is
 * deliberately small and heavily commented so users can copy it verbatim into
 * their own codebase.
 */

// ── Reference implementation — copy this into your app ────────────────────────

/**
 * Persistent {@link DedupStore} backed by Redis.
 *
 * State is keyed by consumer `groupId` and `"topic:partition"` and stored as a
 * plain string-encoded integer under the key `dedup:{groupId}:{topicPartition}`.
 * Because Redis persists independently of the consumer process, the last-processed
 * Lamport clock survives consumer restarts and rebalances — unlike the built-in
 * in-memory store.
 *
 * The library treats the store as **fail-open**: if either method throws, the
 * error is logged and the message is processed as if it were not a duplicate.
 * That biases towards at-least-once delivery during a store outage.
 */
class RedisDedupStore implements DedupStore {
  constructor(private readonly redis: RedisClientType) {}

  /** Namespaced key for a group + topic:partition. */
  private key(groupId: string, topicPartition: string): string {
    return `dedup:${groupId}:${topicPartition}`;
  }

  /**
   * Return the last processed Lamport clock, or `undefined` when Redis has no
   * record yet. `GET` returns `null` for a missing key.
   */
  async getLastClock(
    groupId: string,
    topicPartition: string,
  ): Promise<number | undefined> {
    const raw = await this.redis.get(this.key(groupId, topicPartition));
    return raw === null ? undefined : Number(raw);
  }

  /**
   * Persist the last processed Lamport clock. A plain `SET` — no TTL, because the
   * dedup watermark must never expire while the topic still exists (an expired
   * key would silently reset dedup and let stale replays through).
   */
  async setLastClock(
    groupId: string,
    topicPartition: string,
    clock: number,
  ): Promise<void> {
    await this.redis.set(this.key(groupId, topicPartition), String(clock));
  }
}

// ── Test-only helpers ─────────────────────────────────────────────────────────

/** Captures every log line so we can assert fail-open logged an error. */
function makeCapturingLogger(): KafkaLogger & {
  errors: string[];
  warns: string[];
} {
  const errors: string[] = [];
  const warns: string[] = [];
  return {
    errors,
    warns,
    log() {},
    debug() {},
    warn(message: string) {
      warns.push(message);
    },
    error(message: string) {
      errors.push(message);
    },
  };
}

type DedupTopics = Record<string, { value: string }>;

function createDedupClient(
  name: string,
  options?: import("../client/kafka.client").KafkaClientOptions,
): KafkaClient<DedupTopics> {
  return new KafkaClient<DedupTopics>(
    `integration-${name}`,
    `group-${name}-${Date.now()}`,
    getBrokers(),
    { autoCreateTopics: true, ...options },
  ) as unknown as KafkaClient<DedupTopics>;
}

function uniqueTopic(): string {
  return `test.redis-dedup.${Date.now()}.${Math.floor(Math.random() * 1e6)}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(cond: () => boolean, timeoutMs: number): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await sleep(100);
  }
}

// ── Suite ─────────────────────────────────────────────────────────────────────

describe("Integration — Redis-backed DedupStore against real Redis + Kafka", () => {
  let redisContainer: StartedTestContainer;
  let redis: RedisClientType;
  let redisUrl: string;

  beforeAll(async () => {
    redisContainer = await new GenericContainer("redis:7-alpine")
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage("Ready to accept connections"))
      .start();

    const host = redisContainer.getHost();
    const port = redisContainer.getMappedPort(6379);
    redisUrl = `redis://${host}:${port}`;

    redis = createRedisClient({ url: redisUrl }) as RedisClientType;
    await redis.connect();
  }, 120_000);

  afterAll(async () => {
    try {
      if (redis?.isOpen) await redis.quit();
    } catch {
      // ignore
    }
    try {
      await redisContainer?.stop();
    } catch {
      // ignore
    }
  }, 60_000);

  it("persists the advancing clock across a consumer restart and drops a stale replay", async () => {
    const topic = uniqueTopic();
    const sharedGroupId = `redis-dedup-group-${Date.now()}`;
    const store = new RedisDedupStore(redis);

    const consumerClient = createDedupClient("redis-dedup-cons");
    const producerA = createDedupClient("redis-dedup-A");
    // Producer B is a *fresh* client → its Lamport clock restarts at 0, exactly
    // like a redeployed producer would.
    const producerB = createDedupClient("redis-dedup-B");

    await consumerClient.connectProducer();
    await producerA.connectProducer();
    await producerB.connectProducer();

    const processed: string[] = [];

    // ── Phase 1: first consumer session, process A's two messages ─────────────
    const handle1 = await consumerClient.startConsumer(
      [topic],
      async (envelope) => {
        processed.push(envelope.payload.value);
      },
      {
        groupId: sharedGroupId,
        fromBeginning: true,
        deduplication: { strategy: "drop", store },
      },
    );
    await handle1.ready();

    await producerA.sendMessage(topic, { value: "A-1" }); // Lamport clock 1
    await producerA.sendMessage(topic, { value: "A-2" }); // Lamport clock 2

    await waitFor(() => processed.length >= 2, 30_000);
    expect(processed).toEqual(["A-1", "A-2"]);

    // Redis holds the advancing clock under the expected key name.
    const stateKey = `${topic}:0`;
    const redisKey = `dedup:${sharedGroupId}:${stateKey}`;
    expect(await redis.get(redisKey)).toBe("2");

    // The key exists and is TTL-less (-1 = "no expiry set"). A watermark that
    // could expire would silently reset dedup.
    const keys = await redis.keys(`dedup:${sharedGroupId}:*`);
    expect(keys).toContain(redisKey);
    expect(await redis.ttl(redisKey)).toBe(-1);

    // ── Phase 2: RESTART — stop the consumer, then start a BRAND-NEW client ────
    // A new KafkaClient has fresh in-memory state (Lamport clock reset to 0),
    // simulating a redeploy. It reuses the SAME groupId + SAME Redis store.
    await handle1.stop();
    await consumerClient.disconnect();

    const restartedClient = createDedupClient("redis-dedup-cons-restart");
    await restartedClient.connectProducer();

    const handle2 = await restartedClient.startConsumer(
      [topic],
      async (envelope) => {
        processed.push(envelope.payload.value);
      },
      {
        groupId: sharedGroupId,
        fromBeginning: true,
        deduplication: { strategy: "drop", store },
      },
    );
    await handle2.ready();

    // ── Phase 3: Producer B replays with a stale (lower) clock ────────────────
    // Fresh client → first send stamps clock 1, which is <= persisted 2. The
    // in-memory store would have reset to 0 and processed this; the Redis store
    // still yields 2, so it is detected as a duplicate and dropped.
    await producerB.sendMessage(topic, { value: "B-replay" }); // Lamport clock 1

    await sleep(8_000);

    // The stale replay must NOT have been processed.
    expect(processed).not.toContain("B-replay");
    expect(processed).toEqual(["A-1", "A-2"]);

    // Redis clock never advanced past 2 (the duplicate did not persist).
    expect(await redis.get(redisKey)).toBe("2");

    await handle2.stop();
    await restartedClient.disconnect();
    await producerA.disconnect();
    await producerB.disconnect();
  }, 120_000);

  it("fails open when Redis is unreachable — message is still processed and an error is logged", async () => {
    const topic = uniqueTopic();
    const groupId = `redis-dedup-failopen-${Date.now()}`;

    // A dedicated Redis connection we can forcibly close mid-test to simulate an
    // outage without disturbing the shared `redis` client used by other tests.
    const brokenRedis = createRedisClient({
      url: redisUrl,
      // Never auto-reconnect — once we disconnect it, every command must reject
      // so the library's fail-open path is exercised.
      socket: { reconnectStrategy: false },
    }) as RedisClientType;
    // Swallow the client-level error the disconnect raises — we assert on the
    // library's own fail-open log line, not on this raw connection error.
    brokenRedis.on("error", () => {});
    await brokenRedis.connect();
    const store = new RedisDedupStore(brokenRedis);

    const logger = makeCapturingLogger();
    const consumerClient = createDedupClient("redis-dedup-failopen", {
      logger,
    });
    const producer = createDedupClient("redis-dedup-failopen-prod");

    await consumerClient.connectProducer();
    await producer.connectProducer();

    const processed: string[] = [];
    const handle = await consumerClient.startConsumer(
      [topic],
      async (envelope) => {
        processed.push(envelope.payload.value);
      },
      {
        groupId,
        fromBeginning: true,
        deduplication: { strategy: "drop", store },
      },
    );
    await handle.ready();

    // Take the store down BEFORE any message arrives → getLastClock throws.
    await brokenRedis.disconnect();

    await producer.sendMessage(topic, { value: "survives-outage" });

    // Fail-open: the message is processed despite the store being down.
    await waitFor(() => processed.includes("survives-outage"), 30_000);
    expect(processed).toContain("survives-outage");

    // And the library logged the fail-open path.
    expect(
      logger.errors.some(
        (e) => e.includes("Dedup store") && e.includes("fail-open"),
      ),
    ).toBe(true);

    await handle.stop();
    await consumerClient.disconnect();
    await producer.disconnect();
  }, 90_000);
});
