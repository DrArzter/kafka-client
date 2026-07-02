import "reflect-metadata";
import { StartedKafkaContainer } from "@testcontainers/kafka";
import type { KafkaClient } from "../client/kafka.client";
import {
  startBroker,
  brokersOf,
  createTopics,
  makeClient,
  waitUntil,
  sleep,
} from "./helpers";

type Topics = { "chaos.rebalance-load": { seq: number; eventId: string } };

/**
 * FIELD TRIAL — cooperative-sticky rebalance while a producer hammers a
 * 4-partition topic.
 *
 * Timeline:
 *   1. Consumer A starts (owns all 4 partitions).
 *   2. Producer sends keyed messages continuously.
 *   3. After ~30 sent, consumer B joins the same group → rebalance (A gives up
 *      ~2 partitions to B).
 *   4. After ~30 more, consumer A stops → its partitions revert to B.
 *
 * Assertion: every ACKED eventId is processed AT LEAST ONCE across A+B
 * combined. Duplicates are allowed (and expected around the rebalance windows,
 * since auto-commit + rebalance can reprocess in-flight offsets) and are only
 * logged, not failed.
 */
describe("Chaos — rebalance under continuous load", () => {
  jest.setTimeout(240_000);

  const TOPIC = "chaos.rebalance-load";
  let container: StartedKafkaContainer;
  let producer: KafkaClient<Topics>;
  let consumerA: KafkaClient<Topics>;
  let consumerB: KafkaClient<Topics>;

  beforeAll(async () => {
    container = await startBroker();
    await createTopics(brokersOf(container), [
      { topic: TOPIC, numPartitions: 4 },
    ]);
  }, 180_000);

  afterAll(async () => {
    await producer?.disconnect().catch(() => {});
    await consumerA?.disconnect().catch(() => {});
    await consumerB?.disconnect().catch(() => {});
    await container?.stop().catch(() => {});
  }, 60_000);

  it("processes every acked message at least once across a mid-load rebalance", async () => {
    const TOTAL = 120;

    producer = makeClient<Topics>(container, "rb-producer");
    await producer.connectProducer();

    // A shared group. Both A and B must use the SAME groupId to trigger a
    // cooperative rebalance rather than two independent consumer groups.
    const GROUP = `chaos-rebalance-group-${Date.now()}`;

    // Count every delivery per consumer (duplicates included) + a union set.
    const byA: string[] = [];
    const byB: string[] = [];
    const processed = new Set<string>();

    consumerA = makeClient<Topics>(container, "rb-a");
    consumerB = makeClient<Topics>(container, "rb-b");

    const handleA = await consumerA.startConsumer(
      [TOPIC],
      async (env) => {
        byA.push(env.payload.eventId);
        processed.add(env.payload.eventId);
      },
      { groupId: GROUP, fromBeginning: true, autoCommit: true },
    );
    await handleA.ready(); // A owns all 4 partitions

    const ackedIds: string[] = [];
    let stopProducing = false;

    // Background producer: keyed across 4 partitions, steady drip so the
    // rebalances happen mid-stream rather than after all sends complete.
    const produceLoop = (async () => {
      for (let seq = 0; seq < TOTAL && !stopProducing; seq++) {
        const eventId = `evt-${seq}`;
        try {
          await producer.sendMessage(
            TOPIC,
            { seq, eventId },
            { key: `k-${seq % 8}`, eventId },
          );
          ackedIds.push(eventId);
        } catch {
          // transient — skip (not counted as acked)
        }
        await sleep(60); // ~60ms/msg → ~7s for 120 msgs
      }
    })();

    // After ~30 messages have been acked, bring up consumer B.
    await waitUntil(() => ackedIds.length >= 30, 60_000);
    const handleB = await consumerB.startConsumer(
      [TOPIC],
      async (env) => {
        byB.push(env.payload.eventId);
        processed.add(env.payload.eventId);
      },
      { groupId: GROUP, fromBeginning: true, autoCommit: true },
    );
    await handleB.ready(); // B has taken over at least one partition

    // After ~60 acked, stop consumer A — its partitions revert to B.
    await waitUntil(() => ackedIds.length >= 60, 60_000);
    await consumerA.stopConsumer(GROUP);

    // Let the producer finish.
    await produceLoop;
    stopProducing = true;

    // Every acked id must be processed at least once across A+B combined.
    await waitUntil(
      () => ackedIds.every((id) => processed.has(id)),
      120_000,
    );

    const missing = ackedIds.filter((id) => !processed.has(id));
    expect(missing).toEqual([]);
    expect(ackedIds.length).toBeGreaterThanOrEqual(TOTAL - 10);

    // Duplicates: total deliveries minus unique. Allowed — just report.
    const totalDeliveries = byA.length + byB.length;
    const duplicates = totalDeliveries - processed.size;

    // eslint-disable-next-line no-console
    console.log(
      `[rebalance-load] acked=${ackedIds.length} unique-processed=${processed.size} ` +
        `A=${byA.length} B=${byB.length} duplicates=${duplicates} — no loss`,
    );

    await handleB.stop();
  });
});
