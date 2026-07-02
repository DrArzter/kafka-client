import "reflect-metadata";
import { StartedKafkaContainer } from "@testcontainers/kafka";
import type { KafkaClient } from "../client/kafka.client";
import {
  startBroker,
  brokersOf,
  createTopics,
  makeClient,
  waitUntil,
} from "./helpers";

type Topics = {
  "chaos.poison": { seq: number; poison: boolean };
  "chaos.poison.dlq": { seq: number; poison: boolean };
};

/**
 * FIELD TRIAL — a permanently-failing "poison" message must not stall the
 * partition (head-of-line blocking) and must land in the DLQ with x-dlq-*
 * headers, while every healthy message is still processed.
 *
 * Layout: 10 normal, 1 poison, 10 normal — all on one partition (single-key)
 * so they are delivered in order and the poison genuinely sits between the two
 * healthy batches. If the client head-of-line-stalled, the second batch would
 * never arrive.
 */
describe("Chaos — poison message recovery (retry → DLQ, no stall)", () => {
  jest.setTimeout(240_000);

  const TOPIC = "chaos.poison";
  const DLQ = "chaos.poison.dlq";
  let container: StartedKafkaContainer;
  let producer: KafkaClient<Topics>;
  let consumer: KafkaClient<Topics>;
  let dlqConsumer: KafkaClient<Topics>;

  beforeAll(async () => {
    container = await startBroker();
    await createTopics(brokersOf(container), [
      { topic: TOPIC, numPartitions: 1 },
      { topic: DLQ, numPartitions: 1 },
    ]);
  }, 180_000);

  afterAll(async () => {
    await producer?.disconnect().catch(() => {});
    await consumer?.disconnect().catch(() => {});
    await dlqConsumer?.disconnect().catch(() => {});
    await container?.stop().catch(() => {});
  }, 60_000);

  it("routes the poison to DLQ, keeps consuming healthy messages, and replayDlq(dryRun) reports 1", async () => {
    producer = makeClient<Topics>(container, "poison-producer");
    consumer = makeClient<Topics>(container, "poison-consumer");
    dlqConsumer = makeClient<Topics>(container, "poison-dlq");

    await producer.connectProducer();
    // The DLQ path publishes via the CONSUMER client's own producer, so it must
    // be connected too — otherwise the DLQ send fails with
    // "Cannot send without awaiting connect()".
    await consumer.connectProducer();

    const processedNormal = new Set<number>();
    let poisonAttempts = 0;

    const handle = await consumer.startConsumer(
      [TOPIC],
      async (env) => {
        if (env.payload.poison) {
          poisonAttempts++;
          throw new Error("poison message — permanent failure");
        }
        processedNormal.add(env.payload.seq);
      },
      {
        fromBeginning: true,
        retry: { maxRetries: 2, backoffMs: 200 },
        dlq: true,
        autoCommit: true,
      },
    );
    await handle.ready();

    // DLQ observer.
    const dlqCaptured: Array<{
      payload: Topics["chaos.poison.dlq"];
      headers: Record<string, string>;
    }> = [];
    const dlqHandle = await dlqConsumer.startConsumer(
      [DLQ],
      async (env) => {
        dlqCaptured.push({
          payload: env.payload,
          headers: env.headers as Record<string, string>,
        });
      },
      { fromBeginning: true, autoCommit: true },
    );
    await dlqHandle.ready();

    // Single key → single partition → strict ordering. 10 normal, 1 poison, 10 normal.
    const KEY = "poison-lane";
    for (let seq = 0; seq < 10; seq++) {
      await producer.sendMessage(TOPIC, { seq, poison: false }, { key: KEY });
    }
    await producer.sendMessage(TOPIC, { seq: 999, poison: true }, { key: KEY });
    for (let seq = 10; seq < 20; seq++) {
      await producer.sendMessage(TOPIC, { seq, poison: false }, { key: KEY });
    }

    // All 20 healthy messages must be processed — this only happens if the
    // poison did NOT block the partition head-of-line.
    await waitUntil(() => processedNormal.size >= 20, 90_000);
    expect(processedNormal.size).toBe(20);

    // Poison was attempted maxRetries + 1 times, then DLQ'd.
    expect(poisonAttempts).toBe(3);

    // Poison landed in the DLQ with standard x-dlq-* headers.
    await waitUntil(() => dlqCaptured.length >= 1, 30_000);
    expect(dlqCaptured.length).toBe(1);
    expect(dlqCaptured[0].payload).toEqual({ seq: 999, poison: true });
    expect(dlqCaptured[0].headers["x-dlq-original-topic"]).toBe(TOPIC);
    expect(dlqCaptured[0].headers["x-dlq-error-message"]).toContain("poison");
    expect(dlqCaptured[0].headers["x-dlq-failed-at"]).toBeDefined();
    expect(dlqCaptured[0].headers["x-dlq-attempt-count"]).toBe("3");

    // replayDlq dry-run should report exactly the 1 poison message.
    // NOTE: replayDlq takes the ORIGINAL topic name and appends ".dlq" itself,
    // so we pass TOPIC (not the DLQ topic).
    const replay = await producer.replayDlq(TOPIC, { dryRun: true });
    expect(replay.replayed).toBe(1);

    // eslint-disable-next-line no-console
    console.log(
      `[poison] healthy-processed=${processedNormal.size}/20 poison-attempts=${poisonAttempts} ` +
        `dlq=${dlqCaptured.length} replayDlq.replayed=${replay.replayed} — no head-of-line stall`,
    );

    await handle.stop();
    await dlqHandle.stop();
  });
});
