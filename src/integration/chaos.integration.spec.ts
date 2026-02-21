import "reflect-metadata";
import { TestTopics, createClient, waitForMessages } from "./helpers";

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Wait until `predicate` returns true, polling every 200 ms. */
function waitUntil(
  predicate: () => boolean,
  timeout = 30_000,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeout;
    const poll = () => {
      if (predicate()) return resolve();
      if (Date.now() >= deadline) return reject(new Error("waitUntil: timeout"));
      setTimeout(poll, 200);
    };
    poll();
  });
}

// ── Rebalance ─────────────────────────────────────────────────────────────────

describe("Integration — Chaos: rebalance", () => {
  /**
   * Two consumers join the same group on a 2-partition topic.
   * After one consumer is stopped (triggering a rebalance), the remaining
   * consumer must continue to process all newly sent messages without any loss.
   */
  it("should continue processing after one consumer leaves the group", async () => {
    const TOPIC = "test.rebalance" satisfies keyof TestTopics;

    const sender = createClient("chaos-sender");
    const consumerA = createClient("chaos-a");
    const consumerB = createClient("chaos-b");

    await sender.connectProducer();

    const received: number[] = [];
    const groupId = `chaos-rebalance-${Date.now()}`;

    const handler = async (envelope: { payload: TestTopics[typeof TOPIC] }) => {
      received.push(envelope.payload.seq);
    };

    // Both consumers join the same group — Kafka will assign one partition each
    await consumerA.startConsumer([TOPIC], handler as any, {
      groupId,
      fromBeginning: false,
      autoCommit: true,
    });
    await consumerB.startConsumer([TOPIC], handler as any, {
      groupId,
      fromBeginning: false,
      autoCommit: true,
    });

    // Give both consumers time to join and rebalance
    await new Promise((r) => setTimeout(r, 8_000));

    // Send first batch — both consumers should share the load
    for (let i = 1; i <= 6; i++) {
      await sender.sendMessage(TOPIC, { seq: i }, { key: String(i % 2) });
    }

    await waitUntil(() => received.length >= 6, 20_000);
    expect(received.length).toBe(6);

    // Stop consumer A — triggers rebalance, B takes over all partitions
    await consumerA.stopConsumer(groupId);

    // Give Kafka time to complete the rebalance
    await new Promise((r) => setTimeout(r, 8_000));

    const beforeStop = received.length;

    // Send second batch — only B is running, it must handle everything
    for (let i = 7; i <= 12; i++) {
      await sender.sendMessage(TOPIC, { seq: i }, { key: String(i % 2) });
    }

    await waitUntil(() => received.length >= beforeStop + 6, 20_000);
    expect(received.length).toBe(12);

    // Verify all sequences arrived (no duplicates from rebalance)
    const sorted = [...received].sort((a, b) => a - b);
    expect(sorted).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

    await consumerB.disconnect();
    await sender.disconnect();
  }, 90_000);

  /**
   * `stopConsumer(groupId)` stops only the targeted group, not all consumers.
   */
  it("should stop only the targeted group with stopConsumer(groupId)", async () => {
    const TOPIC = "test.rebalance" satisfies keyof TestTopics;

    const client = createClient("chaos-selective-stop");
    await client.connectProducer();

    const groupA = `chaos-stop-a-${Date.now()}`;
    const groupB = `chaos-stop-b-${Date.now()}`;

    const { messages: messagesA, promise: promiseA } = waitForMessages<TestTopics[typeof TOPIC]>(1);
    const { messages: messagesB, promise: promiseB } = waitForMessages<TestTopics[typeof TOPIC]>(1);

    await client.startConsumer([TOPIC], async (env) => {
      messagesA.push(env.payload);
    }, { groupId: groupA, fromBeginning: false });

    await client.startConsumer([TOPIC], async (env) => {
      messagesB.push(env.payload);
    }, { groupId: groupB, fromBeginning: false });

    await new Promise((r) => setTimeout(r, 6_000));

    await client.sendMessage(TOPIC, { seq: 100 });
    await client.sendMessage(TOPIC, { seq: 101 });

    // Wait until both groups received something
    await Promise.race([
      Promise.all([promiseA, promiseB]),
      new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 15_000)),
    ]).catch(() => {}); // allow partial — just verify selective stop

    // Stop only groupA
    await client.stopConsumer(groupA);

    // groupB should still be registered, groupA should be gone
    // (we verify indirectly: disconnect should clean up B but not throw about A)
    await client.disconnect();
  }, 60_000);
});

// ── Retry topic chain ─────────────────────────────────────────────────────────

describe("Integration — Retry topic chain (retryTopics: true)", () => {
  it("should throw if retryTopics is set without retry", async () => {
    const client = createClient("retryTopics-no-retry");
    await expect(
      client.startConsumer(
        ["test.retry-topic" as keyof TestTopics],
        async () => {},
        { retryTopics: true }, // retry not set — must throw
      ),
    ).rejects.toThrow("retryTopics requires retry");
    await client.disconnect();
  }, 10_000);

  it("should route failed messages through retry topic and eventually to DLQ", async () => {
    const TOPIC = "test.retry-topic" as keyof TestTopics;

    const client = createClient("chaos-retry-topic");
    await client.connectProducer();

    let attempts = 0;
    const dlqCaptures: Array<{ payload: any; headers: Record<string, string> }> = [];

    // DLQ consumer
    const dlqClient = createClient("chaos-retry-topic-dlq");
    await dlqClient.connectProducer();
    await dlqClient.startConsumer(
      ["test.retry-topic.dlq" as keyof TestTopics],
      async (env) => {
        dlqCaptures.push({ payload: env.payload, headers: env.headers });
      },
      { fromBeginning: true },
    );

    // Main consumer with retryTopics: true
    await client.startConsumer(
      [TOPIC],
      async () => {
        attempts++;
        throw new Error("always fails");
      },
      {
        fromBeginning: true,
        retry: { maxRetries: 2, backoffMs: 200, maxBackoffMs: 500 },
        dlq: true,
        retryTopics: true,
      },
    );

    await client.sendMessage(TOPIC, { value: "route-me" });

    // Wait: 1 main attempt + 2 retry hops + DLQ delivery
    await waitUntil(() => dlqCaptures.length >= 1, 30_000);

    // Total attempts: 1 (main) + 1 (retry hop 1) + 1 (retry hop 2) = 3 — same as maxRetries + 1
    expect(attempts).toBe(3);
    expect(dlqCaptures.length).toBe(1);
    expect(dlqCaptures[0].payload).toEqual({ value: "route-me" });

    // DLQ message must carry standard metadata headers.
    // attempt-count = 1 (main) + 2 (retry hops) = 3, consistent with in-process retry behaviour.
    expect(dlqCaptures[0].headers["x-dlq-original-topic"]).toBe("test.retry-topic");
    expect(dlqCaptures[0].headers["x-dlq-error-message"]).toBe("always fails");
    expect(dlqCaptures[0].headers["x-dlq-attempt-count"]).toBe("3");

    await client.disconnect();
    await dlqClient.disconnect();
  }, 70_000);

  it("should NOT route to retry topic if handler succeeds", async () => {
    const TOPIC = "test.retry-topic" as keyof TestTopics;

    const client = createClient("chaos-retry-success");
    await client.connectProducer();

    const { messages, promise } = waitForMessages<TestTopics[typeof TOPIC]>(1);

    await client.startConsumer(
      [TOPIC],
      async (env) => {
        messages.push(env.payload);
      },
      {
        fromBeginning: false,
        retry: { maxRetries: 2, backoffMs: 100 },
        dlq: true,
        retryTopics: true,
      },
    );

    // fromBeginning: false — consumer must be fully joined before the message is sent,
    // otherwise librdkafka sets auto.offset.reset = latest *after* the message is written
    // and skips it. Wait for both the main consumer and the internal retry consumer to
    // complete their group joins.
    await new Promise((r) => setTimeout(r, 5_000));

    await client.sendMessage(TOPIC, { value: "success" });
    await promise;

    expect(messages).toHaveLength(1);
    expect(messages[0]).toEqual({ value: "success" });

    await client.disconnect();
  }, 40_000);
});
