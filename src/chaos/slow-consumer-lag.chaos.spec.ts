import "reflect-metadata";
import { StartedKafkaContainer } from "@testcontainers/kafka";
import type { KafkaClient } from "../client/kafka.client";
import {
  startBroker,
  brokersOf,
  createTopics,
  makeClient,
  waitUntil,
  capturingLogger,
  sleep,
} from "./helpers";

type Topics = { "chaos.lag": { seq: number } };

/**
 * FIELD TRIAL — lag throttling.
 *
 * A deliberately slow consumer (~150 ms/msg, auto-commit so its offsets are
 * visible to getConsumerLag) falls behind while a fast producer floods the
 * topic. A SEPARATE producer client is configured with:
 *     lagThrottle: { maxLag: 20, groupId: <slow group>, pollIntervalMs: 1000, maxWaitMs: 3000 }
 * We assert its logger receives the "producer sends will be delayed" warning
 * at least once (throttle engaged) and later the "sends resumed" log once the
 * consumer drains below the threshold (throttle released).
 */
describe("Chaos — slow consumer lag throttling", () => {
  jest.setTimeout(240_000);

  const TOPIC = "chaos.lag";
  let container: StartedKafkaContainer;
  let floodProducer: KafkaClient<Topics>;
  let throttledProducer: KafkaClient<Topics>;
  let slowConsumer: KafkaClient<Topics>;

  beforeAll(async () => {
    container = await startBroker();
    await createTopics(brokersOf(container), [
      { topic: TOPIC, numPartitions: 1 },
    ]);
  }, 180_000);

  afterAll(async () => {
    await floodProducer?.disconnect().catch(() => {});
    await throttledProducer?.disconnect().catch(() => {});
    await slowConsumer?.disconnect().catch(() => {});
    await container?.stop().catch(() => {});
  }, 60_000);

  it("engages the throttle when lag exceeds maxLag and releases when the consumer catches up", async () => {
    const SLOW_GROUP = `chaos-lag-slow-group-${Date.now()}`;

    // Slow consumer — reads from the earliest offset and sleeps 150ms per msg.
    let handlerActive = true;
    let consumed = 0;
    slowConsumer = makeClient<Topics>(container, "lag-slow");
    const slowHandle = await slowConsumer.startConsumer(
      [TOPIC],
      async () => {
        if (handlerActive) await sleep(150);
        consumed++;
      },
      { groupId: SLOW_GROUP, fromBeginning: true, autoCommit: true },
    );
    await slowHandle.ready();

    // Flood producer — no throttle, just dumps 100 messages fast to build lag.
    floodProducer = makeClient<Topics>(container, "lag-flood");
    await floodProducer.connectProducer();
    for (let seq = 0; seq < 100; seq++) {
      await floodProducer.sendMessage(TOPIC, { seq });
    }

    // Throttled producer — watches the SLOW_GROUP's lag. Capturing logger so we
    // can assert the throttle engaged/released messages.
    const logger = capturingLogger();
    throttledProducer = makeClient<Topics>(container, "lag-throttled", {
      logger,
      lagThrottle: {
        groupId: SLOW_GROUP,
        maxLag: 20,
        pollIntervalMs: 1_000,
        maxWaitMs: 3_000,
      },
    });
    await throttledProducer.connectProducer(); // starts the lag poller

    // Give the poller a couple of cycles to observe the (large) lag and engage.
    await waitUntil(
      () => logger.hasWarn("producer sends will be delayed"),
      60_000,
    );
    expect(logger.hasWarn("producer sends will be delayed")).toBe(true);

    // While throttled, a send blocks in waitIfThrottled up to maxWaitMs. Fire a
    // few sends — they proceed (best-effort throttle), which is fine; the point
    // is the throttle state was observed.
    const throttledSends = Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        throttledProducer.sendMessage(TOPIC, { seq: 1000 + i }),
      ),
    );

    // Now let the consumer drain fast so lag falls below maxLag.
    handlerActive = false;

    // Throttle should RELEASE — the poller logs "producer sends resumed".
    await waitUntil(
      () => logger.logs.some((l) => l.includes("producer sends resumed")),
      120_000,
    );
    expect(
      logger.logs.some((l) => l.includes("producer sends resumed")),
    ).toBe(true);

    await throttledSends;

    // eslint-disable-next-line no-console
    console.log(
      `[lag] consumed=${consumed} ` +
        `throttle-warns=${logger.warns.filter((w) => w.includes("delayed")).length} ` +
        `resumed-logs=${logger.logs.filter((l) => l.includes("resumed")).length} — engaged then released`,
    );

    await slowHandle.stop();
  });
});
