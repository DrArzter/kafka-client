import {
  TestTopicMap,
  createClient,
  mockRun,
  mockSend,
  mockListTopics,
  KafkaClient,
} from "./helpers";

// ── Helpers ──────────────────────────────────────────────────────────

/** Deliver a single message via eachMessage with an optional Lamport clock header. */
function setupMessageWithClock(clock: number | undefined, partition = 0) {
  const headers: Record<string, string[]> = {};
  if (clock !== undefined) {
    headers["x-lamport-clock"] = [String(clock)];
  }
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    await eachMessage({
      topic: "test.topic",
      partition,
      message: {
        value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
        headers,
        offset: "0",
      },
    });
  });
}

/** Deliver multiple messages in sequence, each with an optional clock and partition. */
function setupMessagesWithClocks(
  clocks: Array<{ clock: number | undefined; partition?: number }>,
) {
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    for (const { clock, partition = 0 } of clocks) {
      const headers: Record<string, string[]> = {};
      if (clock !== undefined) {
        headers["x-lamport-clock"] = [String(clock)];
      }
      await eachMessage({
        topic: "test.topic",
        partition,
        message: {
          value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          headers,
          offset: "0",
        },
      });
    }
  });
}

// ── Tests ─────────────────────────────────────────────────────────────

describe("KafkaClient — Deduplication (Lamport Clock)", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  // ── Producer: clock stamping ────────────────────────────────────────

  describe("Producer — Lamport clock stamping", () => {
    it("stamps x-lamport-clock=1 on the first sent message", async () => {
      await client.sendMessage("test.topic", { id: "1", value: 1 });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: [
            expect.objectContaining({
              headers: expect.objectContaining({ "x-lamport-clock": "1" }),
            }),
          ],
        }),
      );
    });

    it("increments the clock for every subsequent send", async () => {
      await client.sendMessage("test.topic", { id: "1", value: 1 });
      await client.sendMessage("test.topic", { id: "2", value: 2 });

      const [call1, call2] = mockSend.mock.calls;
      expect(call1[0].messages[0].headers["x-lamport-clock"]).toBe("1");
      expect(call2[0].messages[0].headers["x-lamport-clock"]).toBe("2");
    });

    it("stamps independent clock values for each message in a batch", async () => {
      await client.sendBatch("test.topic", [
        { value: { id: "1", value: 1 } },
        { value: { id: "2", value: 2 } },
      ]);

      const { messages } = mockSend.mock.calls[0][0];
      expect(messages[0].headers["x-lamport-clock"]).toBe("1");
      expect(messages[1].headers["x-lamport-clock"]).toBe("2");
    });
  });

  // ── Consumer: no deduplication ──────────────────────────────────────

  describe("Consumer — deduplication disabled", () => {
    it("passes all messages when deduplication is not configured", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 3 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler);

      expect(handler).toHaveBeenCalledTimes(2);
    });
  });

  // ── Consumer: strategy 'drop' (default) ────────────────────────────

  describe("Consumer — strategy: 'drop' (default)", () => {
    it("passes a message that has no x-lamport-clock header (backwards compat)", async () => {
      setupMessageWithClock(undefined);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("passes a message with a malformed clock header", async () => {
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: { "x-lamport-clock": ["not-a-number"] },
            offset: "0",
          },
        });
      });
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("passes fresh messages in ascending order", async () => {
      setupMessagesWithClocks([{ clock: 1 }, { clock: 2 }, { clock: 3 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });

      expect(handler).toHaveBeenCalledTimes(3);
    });

    it("drops a duplicate (same clock as last processed)", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("drops a duplicate (clock less than last processed)", async () => {
      setupMessagesWithClocks([{ clock: 10 }, { clock: 3 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("does not route dropped duplicate to DLQ", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        dlq: true,
        deduplication: { strategy: "drop" },
      });

      expect(mockSend).not.toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
    });
  });

  // ── Consumer: strategy 'dlq' ────────────────────────────────────────

  describe("Consumer — strategy: 'dlq'", () => {
    it("routes duplicate to DLQ with reason metadata headers", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 3 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        dlq: true,
        deduplication: { strategy: "dlq" },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: "test.topic.dlq",
          messages: [
            expect.objectContaining({
              headers: expect.objectContaining({
                "x-dlq-reason": "lamport-clock-duplicate",
                "x-dlq-duplicate-incoming-clock": "3",
                "x-dlq-duplicate-last-processed-clock": "5",
                "x-dlq-error-message": "Lamport Clock duplicate detected",
              }),
            }),
          ],
        }),
      );
    });

    it("includes original message headers in DLQ payload", async () => {
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        // First: fresh message
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: { "x-lamport-clock": ["5"], "x-correlation-id": ["corr-abc"] },
            offset: "0",
          },
        });
        // Second: duplicate
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: { "x-lamport-clock": ["5"], "x-correlation-id": ["corr-abc"] },
            offset: "1",
          },
        });
      });

      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        dlq: true,
        deduplication: { strategy: "dlq" },
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: "test.topic.dlq",
          messages: [
            expect.objectContaining({
              headers: expect.objectContaining({
                "x-correlation-id": "corr-abc",
              }),
            }),
          ],
        }),
      );
    });

    it("silently drops duplicate (no DLQ send) when dlq option is false", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        dlq: false,
        deduplication: { strategy: "dlq" },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(mockSend).not.toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
    });
  });

  // ── Consumer: strategy 'topic' ──────────────────────────────────────

  describe("Consumer — strategy: 'topic'", () => {
    it("routes duplicate to <topic>.duplicates by default", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 3 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { strategy: "topic" },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: "test.topic.duplicates",
          messages: [
            expect.objectContaining({
              headers: expect.objectContaining({
                "x-duplicate-reason": "lamport-clock-duplicate",
                "x-duplicate-incoming-clock": "3",
                "x-duplicate-last-processed-clock": "5",
                "x-duplicate-original-topic": "test.topic",
              }),
            }),
          ],
        }),
      );
    });

    it("routes duplicate to custom duplicatesTopic when specified", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);
      mockListTopics.mockResolvedValueOnce(["test.topic", "my-custom.duplicates"]);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: {
          strategy: "topic",
          duplicatesTopic: "my-custom.duplicates",
        },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "my-custom.duplicates" }),
      );
    });

    it("stamps x-duplicate-detected-at in forwarded message headers", async () => {
      setupMessagesWithClocks([{ clock: 5 }, { clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { strategy: "topic" },
      });

      const dlqCall = mockSend.mock.calls.find(
        ([p]: any) => p.topic === "test.topic.duplicates",
      );
      expect(dlqCall).toBeDefined();
      expect(dlqCall[0].messages[0].headers["x-duplicate-detected-at"]).toMatch(
        /^\d{4}-\d{2}-\d{2}T/,
      );
    });
  });

  // ── Consumer: per-partition isolation ──────────────────────────────

  describe("Consumer — per-partition clock isolation", () => {
    it("tracks clock state independently per partition", async () => {
      // p0: clock 5 (fresh), p1: clock 3 (fresh for p1)
      // p0: clock 3 (duplicate — below p0's last=5), p1: clock 5 (fresh — above p1's last=3)
      setupMessagesWithClocks([
        { clock: 5, partition: 0 },
        { clock: 3, partition: 1 },
        { clock: 3, partition: 0 }, // duplicate on p0
        { clock: 5, partition: 1 }, // fresh on p1
      ]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });

      // p0: 5✓, 3✗ → 1 call; p1: 3✓, 5✓ → 2 calls; total = 3
      expect(handler).toHaveBeenCalledTimes(3);
    });
  });

  // ── Consumer: state lifecycle ───────────────────────────────────────

  describe("Consumer — dedup state lifecycle", () => {
    it("clears dedup state when the consumer is stopped and restarted", async () => {
      setupMessagesWithClocks([{ clock: 10 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      const handle = await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });
      await handle.stop();

      // clock 5 would be a duplicate if state persisted (10 > 5), but it was cleared
      setupMessagesWithClocks([{ clock: 5 }]);
      await client.startConsumer(["test.topic"], handler, {
        deduplication: {},
      });

      expect(handler).toHaveBeenCalledTimes(2);
    });
  });

  // ── Batch consumer ──────────────────────────────────────────────────

  describe("Batch consumer — deduplication", () => {
    it("filters duplicate messages out of a batch", async () => {
      mockRun.mockImplementation(async ({ eachBatch }: any) => {
        await eachBatch({
          batch: {
            topic: "test.topic",
            partition: 0,
            highWatermark: "10",
            messages: [
              {
                value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
                headers: { "x-lamport-clock": ["5"] },
                offset: "0",
              },
              {
                value: Buffer.from(JSON.stringify({ id: "2", value: 2 })),
                headers: { "x-lamport-clock": ["3"] }, // duplicate (3 < 5)
                offset: "1",
              },
              {
                value: Buffer.from(JSON.stringify({ id: "3", value: 3 })),
                headers: { "x-lamport-clock": ["7"] }, // fresh
                offset: "2",
              },
            ],
          },
          heartbeat: jest.fn(),
          resolveOffset: jest.fn(),
          commitOffsetsIfNecessary: jest.fn(),
        });
      });

      const batchHandler = jest.fn().mockResolvedValue(undefined);

      await client.startBatchConsumer(["test.topic"], batchHandler, {
        deduplication: {},
      });

      // Only clocks 5 and 7 pass through — clock 3 is a duplicate
      expect(batchHandler).toHaveBeenCalledTimes(1);
      const [envelopes] = batchHandler.mock.calls[0];
      expect(envelopes).toHaveLength(2);
      expect(envelopes[0].payload).toEqual({ id: "1", value: 1 });
      expect(envelopes[1].payload).toEqual({ id: "3", value: 3 });
    });

    it("skips the entire batch when all messages are duplicates", async () => {
      mockRun.mockImplementation(async ({ eachBatch }: any) => {
        await eachBatch({
          batch: {
            topic: "test.topic",
            partition: 0,
            highWatermark: "5",
            messages: [
              {
                value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
                headers: { "x-lamport-clock": ["10"] },
                offset: "0",
              },
              {
                value: Buffer.from(JSON.stringify({ id: "2", value: 2 })),
                headers: { "x-lamport-clock": ["3"] }, // duplicate (3 < 10)
                offset: "1",
              },
            ],
          },
          heartbeat: jest.fn(),
          resolveOffset: jest.fn(),
          commitOffsetsIfNecessary: jest.fn(),
        });
        // Second batch: all duplicates
        await eachBatch({
          batch: {
            topic: "test.topic",
            partition: 0,
            highWatermark: "5",
            messages: [
              {
                value: Buffer.from(JSON.stringify({ id: "3", value: 3 })),
                headers: { "x-lamport-clock": ["8"] }, // duplicate (8 < 10)
                offset: "2",
              },
            ],
          },
          heartbeat: jest.fn(),
          resolveOffset: jest.fn(),
          commitOffsetsIfNecessary: jest.fn(),
        });
      });

      const batchHandler = jest.fn().mockResolvedValue(undefined);

      await client.startBatchConsumer(["test.topic"], batchHandler, {
        deduplication: {},
      });

      // First batch: 1 passes (clock 10), 1 dropped (clock 3) → batchHandler called once with 1 envelope
      // Second batch: all dropped → batchHandler NOT called
      expect(batchHandler).toHaveBeenCalledTimes(1);
      expect(batchHandler.mock.calls[0][0]).toHaveLength(1);
    });
  });
});
