import "reflect-metadata";
import { StartedKafkaContainer } from "@testcontainers/kafka";
import type { KafkaClient } from "../client/kafka.client";
import {
  startBroker,
  brokersOf,
  createTopics,
  pauseContainer,
  unpauseContainer,
  makeClient,
  waitUntil,
  sleep,
} from "./helpers";

type Topics = { "chaos.broker-restart": { seq: number; eventId: string } };

/**
 * FIELD TRIAL — broker unavailability mid-stream.
 *
 * We pause the broker PROCESS (docker pause) after the consumer has received
 * some messages, keep producing during the outage (sends must be wrapped in a
 * retry loop — they fail while the broker is frozen), then unpause and assert
 * that librdkafka reconnects on its own and the consumer eventually receives
 * every acked message AT LEAST ONCE (duplicates allowed, loss forbidden).
 *
 * We use `docker pause` rather than `container.restart()` because restart
 * re-runs KafkaContainer's advertised-listener setup and can remap the port,
 * desyncing the client. Pause freezes the process without touching ports.
 */
describe("Chaos — broker outage mid-stream", () => {
  jest.setTimeout(240_000);

  const TOPIC = "chaos.broker-restart";
  let container: StartedKafkaContainer;
  let producer: KafkaClient<Topics>;
  let consumer: KafkaClient<Topics>;

  beforeAll(async () => {
    container = await startBroker();
    await createTopics(brokersOf(container), [
      { topic: TOPIC, numPartitions: 1 },
    ]);
  }, 180_000);

  afterAll(async () => {
    // Best-effort: make sure the container isn't left paused, then tear down.
    try {
      await unpauseContainer(container);
    } catch {
      // already running
    }
    await producer?.disconnect().catch(() => {});
    await consumer?.disconnect().catch(() => {});
    await container?.stop().catch(() => {});
  }, 60_000);

  it("rides through a broker pause: reconnects, loses no acked message", async () => {
    const TOTAL = 20;

    producer = makeClient<Topics>(container, "restart-producer");
    consumer = makeClient<Topics>(container, "restart-consumer");

    await producer.connectProducer();

    const received = new Set<string>();
    const handle = await consumer.startConsumer(
      [TOPIC],
      async (env) => {
        received.add(env.payload.eventId);
      },
      {
        fromBeginning: true,
        retry: { maxRetries: 5, backoffMs: 200, maxBackoffMs: 2_000 },
        autoCommit: true,
      },
    );
    await handle.ready();

    // Every send that is ACKED goes here — these MUST all be delivered.
    const ackedIds: string[] = [];

    /** Send one message, retrying through transient broker-down errors. */
    const sendWithRetry = async (seq: number): Promise<void> => {
      const eventId = `evt-${seq}`;
      for (let attempt = 0; attempt < 40; attempt++) {
        try {
          await producer.sendMessage(TOPIC, { seq, eventId }, { eventId });
          ackedIds.push(eventId); // only record on successful ack
          return;
        } catch {
          // Broker likely paused — back off and retry.
          await sleep(500);
        }
      }
      // Never acked within the retry window — do NOT add to ackedIds, so we
      // don't assert delivery of a message that was never accepted.
    };

    // Produce first half so the consumer definitely has traffic to chew on.
    for (let seq = 0; seq < 10; seq++) {
      await sendWithRetry(seq);
    }

    // Wait until the consumer has received ~half, then freeze the broker.
    await waitUntil(() => received.size >= 8, 60_000);

    const pauseStart = Date.now();
    await pauseContainer(container);

    // Keep producing DURING the outage. These sends will fail and retry.
    // Kick them off concurrently so they're genuinely in-flight while paused.
    const midOutageSends = Promise.all(
      Array.from({ length: 5 }, (_, i) => sendWithRetry(10 + i)),
    );

    // Simulate ~12s of broker unavailability.
    await sleep(12_000);
    await unpauseContainer(container);
    const outageMs = Date.now() - pauseStart;

    // The in-flight sends should now complete against the recovered broker.
    await midOutageSends;

    // Produce the remainder after recovery.
    for (let seq = 15; seq < TOTAL; seq++) {
      await sendWithRetry(seq);
    }

    // Assert: every ACKED eventId is eventually received at least once.
    await waitUntil(
      () => ackedIds.every((id) => received.has(id)),
      120_000,
    );

    const missing = ackedIds.filter((id) => !received.has(id));
    expect(missing).toEqual([]);
    // We acked a meaningful number of messages (sanity — not a no-op run).
    expect(ackedIds.length).toBeGreaterThanOrEqual(TOTAL - 5);

    // eslint-disable-next-line no-console
    console.log(
      `[broker-restart] acked=${ackedIds.length} received=${received.size} ` +
        `outage≈${outageMs}ms — no acked message lost`,
    );

    await handle.stop();
  });
});
