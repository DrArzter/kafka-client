import "reflect-metadata";
import type { DedupStore } from "../client/kafka.client";
import { KafkaClient, getBrokers } from "./helpers";

/**
 * Integration test for a *persistent* pluggable DedupStore.
 *
 * The default deduplication path keeps clock state in memory keyed by groupId
 * (`ctx.dedupStates`) — that state is wiped on `stopConsumer` / restart. A
 * custom `DedupStore` owns its own storage, so it survives a consumer restart.
 *
 * We prove the store persists across a stop+start of the *same* groupId by:
 *   1. Producer A sends two messages (Lamport clocks 1, 2) → both processed,
 *      the store records ascending clocks.
 *   2. Consumer is stopped and restarted on the SAME groupId with the SAME
 *      store instance.
 *   3. Producer B — a *fresh* KafkaClient whose clock restarts at 0 — replays
 *      a message carrying clock 1 (<= persisted 2). Because the store still
 *      yields 2 after the restart, the replay is detected as a duplicate and
 *      dropped. The default in-memory path would have reset and processed it.
 */

type DedupTopics = Record<string, { value: string }>;

/** Map-backed DedupStore that records every call for assertions. */
interface RecordingDedupStore extends DedupStore {
  readonly getCalls: Array<{ groupId: string; tp: string }>;
  readonly setCalls: Array<{ groupId: string; tp: string; clock: number }>;
  readonly backing: Map<string, number>;
}

function makeRecordingStore(): RecordingDedupStore {
  const backing = new Map<string, number>();
  const getCalls: Array<{ groupId: string; tp: string }> = [];
  const setCalls: Array<{ groupId: string; tp: string; clock: number }> = [];
  const key = (groupId: string, tp: string) => `${groupId}::${tp}`;
  return {
    backing,
    getCalls,
    setCalls,
    getLastClock(groupId, tp) {
      getCalls.push({ groupId, tp });
      return backing.get(key(groupId, tp));
    },
    setLastClock(groupId, tp, clock) {
      setCalls.push({ groupId, tp, clock });
      backing.set(key(groupId, tp), clock);
    },
  };
}

function createDedupClient(name: string): KafkaClient<DedupTopics> {
  return new KafkaClient<DedupTopics>(
    `integration-${name}`,
    `group-${name}-${Date.now()}`,
    getBrokers(),
    { autoCreateTopics: true },
  );
}

function uniqueTopic(): string {
  return `test.dedup-store.${Date.now()}.${Math.floor(Math.random() * 1e6)}`;
}

describe("Integration — Persistent DedupStore across consumer restart", () => {
  it("drops a replayed message after restart because the store persisted the last clock", async () => {
    const topic = uniqueTopic();
    const sharedGroupId = `dedup-store-group-${Date.now()}`;

    const store = makeRecordingStore();

    const consumerClient = createDedupClient("dedup-store-cons");
    const producerA = createDedupClient("dedup-store-A");
    // Producer B is a fresh client → its Lamport clock restarts at 0.
    const producerB = createDedupClient("dedup-store-B");

    await consumerClient.connectProducer();
    await producerA.connectProducer();
    await producerB.connectProducer();

    const processed: string[] = [];

    // ── Phase 1: first consumer session, process A's two messages ──────────
    const handle1 = await consumerClient.startConsumer(
      [topic],
      async (envelope) => {
        processed.push(envelope.payload.value);
      },
      {
        groupId: sharedGroupId,
        fromBeginning: true,
        deduplication: { strategy: "drop", store },
      } as any,
    );
    await handle1.ready();

    await producerA.sendMessage(topic, { value: "A-1" }); // clock 1
    await producerA.sendMessage(topic, { value: "A-2" }); // clock 2

    // Wait for both to be processed.
    await waitFor(() => processed.length >= 2, 20_000);
    expect(processed).toEqual(["A-1", "A-2"]);

    // Store consulted with the right group + topic:partition, and recorded
    // ascending clocks (1 then 2).
    const stateKey = `${topic}:0`;
    expect(store.getCalls).toContainEqual({
      groupId: sharedGroupId,
      tp: stateKey,
    });
    const setClocks = store.setCalls
      .filter((c) => c.groupId === sharedGroupId && c.tp === stateKey)
      .map((c) => c.clock);
    expect(setClocks).toEqual([1, 2]);
    expect(store.backing.get(`${sharedGroupId}::${stateKey}`)).toBe(2);

    // ── Phase 2: RESTART the consumer on the SAME groupId + SAME store ──────
    await handle1.stop();

    // The store still yields the persisted value after the restart — no reset.
    expect(store.getLastClock(sharedGroupId, stateKey)).toBe(2);

    const setCallsBeforeRestart = store.setCalls.length;

    const handle2 = await consumerClient.startConsumer(
      [topic],
      async (envelope) => {
        processed.push(envelope.payload.value);
      },
      {
        groupId: sharedGroupId,
        // fromBeginning so the restarted consumer re-reads from the log if
        // offsets were not committed; dedup must still drop the replay.
        fromBeginning: true,
        deduplication: { strategy: "drop", store },
      } as any,
    );
    await handle2.ready();

    // ── Phase 3: Producer B replays with a stale (lower) clock ─────────────
    // Fresh client → first send stamps clock 1, which is <= persisted 2.
    await producerB.sendMessage(topic, { value: "B-replay" }); // clock 1

    // Give the duplicate ample time to arrive and be dropped.
    await sleep(6_000);

    // The replay must NOT have been processed — dedup persisted across restart.
    expect(processed).not.toContain("B-replay");

    // The store was consulted again in the second session (proof the restarted
    // consumer used the same persistent store rather than a reset in-memory one).
    const getCallsAfterRestart = store.getCalls.filter(
      (c) => c.groupId === sharedGroupId && c.tp === stateKey,
    ).length;
    expect(getCallsAfterRestart).toBeGreaterThan(1);

    // No new setLastClock for the dropped duplicate (clock never advanced past 2).
    const dupSetCalls = store.setCalls.slice(setCallsBeforeRestart);
    expect(dupSetCalls.every((c) => c.clock <= 2)).toBe(true);
    expect(store.backing.get(`${sharedGroupId}::${stateKey}`)).toBe(2);

    await handle2.stop();
    await consumerClient.disconnect();
    await producerA.disconnect();
    await producerB.disconnect();
  }, 60_000);
});

// ── Small local helpers ──────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
  cond: () => boolean,
  timeoutMs: number,
): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor timed out");
    }
    await sleep(100);
  }
}
