import "reflect-metadata";
import { TestTopics, createClient, waitForMessages } from "./helpers";

/** Wait for partition assignment to complete before sending messages. */
const waitForAssignment = () => new Promise((r) => setTimeout(r, 4_000));

/**
 * Integration tests for Lamport Clock deduplication.
 *
 * Strategy: two separate producer clients (A and B) share the same topic.
 * Client A sends N messages — its counter runs 1…N.
 * Client B is a fresh instance — its counter resets to 0 and runs 1…M.
 * Once the consumer has processed A's messages (last clock = N),
 * all of B's messages have clocks ≤ N and are therefore duplicates.
 */
describe("Integration — Deduplication (Lamport Clock)", () => {
  // ── Happy path: all messages from a single producer arrive ────────────

  it("should pass all messages when clocks are monotonically increasing", async () => {
    const client = createClient("dedup-happy");
    await client.connectProducer();

    const { messages, promise } = waitForMessages<TestTopics["test.dedup"]>(3);

    await client.startConsumer(
      ["test.dedup"],
      async (envelope) => {
        messages.push(envelope.payload);
      },
      {
        fromBeginning: false,
        autoCreateTopics: true,
        deduplication: {},
      } as any,
    );
    await waitForAssignment();

    await client.sendMessage("test.dedup", { value: "msg-1" });
    await client.sendMessage("test.dedup", { value: "msg-2" });
    await client.sendMessage("test.dedup", { value: "msg-3" });

    const result = await promise;
    expect(result).toHaveLength(3);
    expect(result.map((m) => m.value)).toEqual(["msg-1", "msg-2", "msg-3"]);

    await client.disconnect();
  });

  // ── Producer stamps x-lamport-clock in message headers ───────────────

  it("should stamp x-lamport-clock headers visible on the consumer side", async () => {
    const client = createClient("dedup-clock-visible");
    await client.connectProducer();

    const clocks: string[] = [];
    const { messages, promise } = waitForMessages<TestTopics["test.dedup"]>(2);

    await client.startConsumer(
      ["test.dedup"],
      async (envelope) => {
        const clock = envelope.headers["x-lamport-clock"];
        if (clock !== undefined) clocks.push(clock);
        messages.push(envelope.payload);
      },
      { fromBeginning: false } as any,
    );
    await waitForAssignment();

    await client.sendMessage("test.dedup", { value: "a" });
    await client.sendMessage("test.dedup", { value: "b" });

    await promise;

    // Both clocks should be present and strictly increasing
    expect(clocks).toHaveLength(2);
    const [c1, c2] = clocks.map(Number);
    expect(c1).toBeGreaterThan(0);
    expect(c2).toBeGreaterThan(c1);

    await client.disconnect();
  });

  // ── Duplicate detection: 'drop' strategy ─────────────────────────────

  it("should drop messages from a restarted producer (stale clocks) — strategy: drop", async () => {
    const consumerClient = createClient("dedup-drop-consumer");
    const producerA = createClient("dedup-drop-A");
    const producerB = createClient("dedup-drop-B"); // fresh clock, will start at 1

    await consumerClient.connectProducer();
    await producerA.connectProducer();
    await producerB.connectProducer();

    const received: string[] = [];
    const { messages: freshMessages, promise: freshPromise } =
      waitForMessages<TestTopics["test.dedup"]>(3);

    await consumerClient.startConsumer(
      ["test.dedup"],
      async (envelope) => {
        received.push(envelope.payload.value);
        freshMessages.push(envelope.payload);
      },
      {
        fromBeginning: false,
        deduplication: { strategy: "drop" },
      },
    );
    await waitForAssignment();

    // Producer A: sends 3 messages → clocks 1, 2, 3 (all fresh)
    await producerA.sendMessage("test.dedup", { value: "A-1" });
    await producerA.sendMessage("test.dedup", { value: "A-2" });
    await producerA.sendMessage("test.dedup", { value: "A-3" });

    await freshPromise;
    expect(received).toContain("A-1");
    expect(received).toContain("A-2");
    expect(received).toContain("A-3");

    const countAfterA = received.length;

    // Producer B: fresh instance → clocks 1, 2, 3 — all ≤ last processed (3)
    // These are duplicates and should be silently dropped.
    await producerB.sendMessage("test.dedup", { value: "B-1" });
    await producerB.sendMessage("test.dedup", { value: "B-2" });
    await producerB.sendMessage("test.dedup", { value: "B-3" });

    // Wait briefly to give duplicates time to arrive (they should be dropped)
    await new Promise((r) => setTimeout(r, 4_000));

    // No new messages should have been added by B
    expect(received.length).toBe(countAfterA);
    expect(received).not.toContain("B-1");
    expect(received).not.toContain("B-2");
    expect(received).not.toContain("B-3");

    await consumerClient.disconnect();
    await producerA.disconnect();
    await producerB.disconnect();
  }, 30_000);

  // ── Duplicate detection: 'topic' strategy ────────────────────────────

  it("should forward duplicates to <topic>.duplicates — strategy: topic", async () => {
    const consumerClient = createClient("dedup-topic-consumer");
    const producerA = createClient("dedup-topic-A");
    const producerB = createClient("dedup-topic-B");
    const dupWatcher = createClient("dedup-topic-watcher");

    await consumerClient.connectProducer();
    await producerA.connectProducer();
    await producerB.connectProducer();
    await dupWatcher.connectProducer();

    const received: string[] = [];
    const duplicates: string[] = [];

    const { messages: freshMessages, promise: freshPromise } =
      waitForMessages<TestTopics["test.dedup"]>(3);
    const { messages: dupMessages, promise: dupPromise } =
      waitForMessages<TestTopics["test.dedup.duplicates"]>(3, 20_000);

    await consumerClient.startConsumer(
      ["test.dedup"],
      async (envelope) => {
        received.push(envelope.payload.value);
        freshMessages.push(envelope.payload);
      },
      {
        fromBeginning: false,
        deduplication: { strategy: "topic" },
      },
    );

    await dupWatcher.startConsumer(
      ["test.dedup.duplicates"],
      async (envelope) => {
        duplicates.push(envelope.payload.value);
        dupMessages.push(envelope.payload);
      },
      { fromBeginning: false },
    );
    await waitForAssignment();

    // Producer A: clocks 1, 2, 3 → all pass through
    await producerA.sendMessage("test.dedup", { value: "A-1" });
    await producerA.sendMessage("test.dedup", { value: "A-2" });
    await producerA.sendMessage("test.dedup", { value: "A-3" });

    await freshPromise;
    expect(received).toHaveLength(3);

    // Producer B: fresh clock → clocks 1, 2, 3 (all ≤ last=3 → duplicates)
    await producerB.sendMessage("test.dedup", { value: "B-1" });
    await producerB.sendMessage("test.dedup", { value: "B-2" });
    await producerB.sendMessage("test.dedup", { value: "B-3" });

    const dupResult = await dupPromise;

    // 3 messages forwarded to .duplicates
    expect(dupResult).toHaveLength(3);

    // Main handler never called for B's messages
    expect(received).not.toContain("B-1");
    expect(received).not.toContain("B-2");
    expect(received).not.toContain("B-3");

    await consumerClient.disconnect();
    await producerA.disconnect();
    await producerB.disconnect();
    await dupWatcher.disconnect();
  }, 60_000);

  // ── Duplicate detection: 'dlq' strategy ──────────────────────────────

  it("should forward duplicates to <topic>.dlq with reason metadata — strategy: dlq", async () => {
    const consumerClient = createClient("dedup-dlq-consumer");
    const producerA = createClient("dedup-dlq-A");
    const producerB = createClient("dedup-dlq-B");
    const dlqWatcher = createClient("dedup-dlq-watcher");

    await consumerClient.connectProducer();
    await producerA.connectProducer();
    await producerB.connectProducer();
    await dlqWatcher.connectProducer();

    const received: string[] = [];
    const dlqPayloads: Array<{ value: string; headers: Record<string, string> }> = [];

    const { messages: freshMessages, promise: freshPromise } =
      waitForMessages<TestTopics["test.dedup"]>(2);
    const { messages: dlqMessages, promise: dlqPromise } =
      waitForMessages<TestTopics["test.dedup.dlq"]>(2, 20_000);

    await consumerClient.startConsumer(
      ["test.dedup"],
      async (envelope) => {
        received.push(envelope.payload.value);
        freshMessages.push(envelope.payload);
      },
      {
        fromBeginning: false,
        dlq: true,
        deduplication: { strategy: "dlq" },
      },
    );

    await dlqWatcher.startConsumer(
      ["test.dedup.dlq"],
      async (envelope) => {
        dlqPayloads.push({
          value: envelope.payload.value,
          headers: envelope.headers,
        });
        dlqMessages.push(envelope.payload);
      },
      { fromBeginning: false },
    );
    await waitForAssignment();

    // Producer A: clocks 1, 2 → pass through
    await producerA.sendMessage("test.dedup", { value: "A-1" });
    await producerA.sendMessage("test.dedup", { value: "A-2" });

    await freshPromise;
    expect(received).toHaveLength(2);

    // Producer B: fresh clock → clocks 1, 2 → all duplicates
    await producerB.sendMessage("test.dedup", { value: "B-1" });
    await producerB.sendMessage("test.dedup", { value: "B-2" });

    const dlqResult = await dlqPromise;
    expect(dlqResult).toHaveLength(2);

    // DLQ messages should carry reason metadata
    for (const { headers } of dlqPayloads) {
      expect(headers["x-dlq-reason"]).toBe("lamport-clock-duplicate");
      expect(headers["x-dlq-original-topic"]).toBe("test.dedup");
      expect(headers["x-dlq-duplicate-incoming-clock"]).toBeDefined();
      expect(headers["x-dlq-duplicate-last-processed-clock"]).toBeDefined();
    }

    await consumerClient.disconnect();
    await producerA.disconnect();
    await producerB.disconnect();
    await dlqWatcher.disconnect();
  }, 60_000);
});
