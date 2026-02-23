import {
  TestTopicMap,
  createClient,
  envelopeWith,
  mockConsumer,
  mockSubscribe,
  mockRun,
  mockSend,
  mockListTopics,
  KafkaClient,
  topic,
  SchemaLike,
} from "./helpers";

describe("KafkaClient — Batch Consumer", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
    mockRun.mockReset().mockResolvedValue(undefined);
  });

  it("should subscribe and run with eachBatch", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startBatchConsumer(["test.topic"], handler);

    expect(mockConsumer.connect).toHaveBeenCalled();
    expect(mockSubscribe).toHaveBeenCalledWith({
      topics: ["test.topic"],
    });
    expect(mockRun).toHaveBeenCalledWith(
      expect.objectContaining({
        eachBatch: expect.any(Function),
      }),
    );
  });

  it("should pass envelopes array and meta to handler", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const mockHeartbeat = jest.fn().mockResolvedValue(undefined);
    const mockResolveOffset = jest.fn();
    const mockCommitOffsets = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachBatch }: any) => {
      await eachBatch({
        batch: {
          topic: "test.topic",
          partition: 0,
          highWatermark: "100",
          messages: [
            {
              value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
              offset: "0",
            },
            {
              value: Buffer.from(JSON.stringify({ id: "2", value: 20 })),
              offset: "1",
            },
          ],
        },
        heartbeat: mockHeartbeat,
        resolveOffset: mockResolveOffset,
        commitOffsetsIfNecessary: mockCommitOffsets,
      });
    });

    await client.startBatchConsumer(["test.topic"], handler);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(
      [
        envelopeWith({ id: "1", value: 10 }),
        envelopeWith({ id: "2", value: 20 }),
      ],
      expect.objectContaining({
        partition: 0,
        highWatermark: "100",
        heartbeat: mockHeartbeat,
        resolveOffset: mockResolveOffset,
        commitOffsetsIfNecessary: mockCommitOffsets,
      }),
    );
  });

  it("should skip empty messages in batch", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    mockRun.mockImplementation(async ({ eachBatch }: any) => {
      await eachBatch({
        batch: {
          topic: "test.topic",
          partition: 0,
          highWatermark: "100",
          messages: [
            { value: null, offset: "0" },
            {
              value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
              offset: "1",
            },
          ],
        },
        heartbeat: jest.fn(),
        resolveOffset: jest.fn(),
        commitOffsetsIfNecessary: jest.fn(),
      });
    });

    await client.startBatchConsumer(["test.topic"], handler);

    expect(handler).toHaveBeenCalledWith(
      [envelopeWith({ id: "1", value: 10 })],
      expect.any(Object),
    );
  });

  it("should not call handler when all messages are empty", async () => {
    const handler = jest.fn();
    mockRun.mockImplementation(async ({ eachBatch }: any) => {
      await eachBatch({
        batch: {
          topic: "test.topic",
          partition: 0,
          highWatermark: "100",
          messages: [{ value: null, offset: "0" }],
        },
        heartbeat: jest.fn(),
        resolveOffset: jest.fn(),
        commitOffsetsIfNecessary: jest.fn(),
      });
    });

    await client.startBatchConsumer(["test.topic"], handler);
    expect(handler).not.toHaveBeenCalled();
  });

  it("should validate batch messages with schema and skip invalid ones", async () => {
    const strictSchema: SchemaLike<{ id: string; value: number }> = {
      parse(data: unknown) {
        const d = data as any;
        if (typeof d?.id !== "string") throw new Error("Validation failed");
        return d;
      },
    };
    const Desc = topic("test.topic").schema(strictSchema);
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachBatch }: any) => {
      await eachBatch({
        batch: {
          topic: "test.topic",
          partition: 0,
          highWatermark: "100",
          messages: [
            {
              value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
              offset: "0",
            },
            {
              value: Buffer.from(JSON.stringify({ id: 123, value: 20 })),
              offset: "1",
            },
          ],
        },
        heartbeat: jest.fn(),
        resolveOffset: jest.fn(),
        commitOffsetsIfNecessary: jest.fn(),
      });
    });

    await client.startBatchConsumer([Desc] as any, handler);

    // Only the valid message should be passed
    expect(handler).toHaveBeenCalledWith(
      [envelopeWith({ id: "1", value: 10 })],
      expect.any(Object),
    );
  });

  it("should retry batch on handler failure", async () => {
    const handler = jest
      .fn()
      .mockRejectedValueOnce(new Error("batch fail"))
      .mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachBatch }: any) => {
      await eachBatch({
        batch: {
          topic: "test.topic",
          partition: 0,
          highWatermark: "100",
          messages: [
            {
              value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
              offset: "0",
            },
          ],
        },
        heartbeat: jest.fn(),
        resolveOffset: jest.fn(),
        commitOffsetsIfNecessary: jest.fn(),
      });
    });

    await client.startBatchConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
    });

    expect(handler).toHaveBeenCalledTimes(2);
  });

  it("should log debug (not warn) when autoCommit is not explicitly false", async () => {
    const debugSpy = jest.spyOn(console, "debug").mockImplementation(() => {});
    await client.startBatchConsumer(["test.topic"], jest.fn());

    expect(debugSpy).toHaveBeenCalledWith(
      expect.stringContaining("autoCommit is enabled"),
    );
    debugSpy.mockRestore();
  });

  it("should NOT log autoCommit debug message when autoCommit: false is set", async () => {
    const debugSpy = jest.spyOn(console, "debug").mockImplementation(() => {});
    await client.startBatchConsumer(["test.topic"], jest.fn(), {
      autoCommit: false,
    });

    const autoCommitDebug = debugSpy.mock.calls.filter((args) =>
      String(args[0]).includes("autoCommit is enabled"),
    );
    expect(autoCommitDebug).toHaveLength(0);
    debugSpy.mockRestore();
  });

  it("should send to DLQ after batch retries exhausted", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("always fails"));

    mockRun.mockImplementation(async ({ eachBatch }: any) => {
      await eachBatch({
        batch: {
          topic: "test.topic",
          partition: 0,
          highWatermark: "100",
          messages: [
            {
              value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
              offset: "0",
            },
          ],
        },
        heartbeat: jest.fn(),
        resolveOffset: jest.fn(),
        commitOffsetsIfNecessary: jest.fn(),
      });
    });

    await client.startBatchConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      dlq: true,
    });

    expect(handler).toHaveBeenCalledTimes(2);
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic.dlq" }),
    );
  });

  it("should not double-send schema-invalid messages to DLQ when batch handler also fails", async () => {
    // Bug: handleEachBatch rebuilt rawMessages from batch.messages.filter(m => m.value)
    // instead of using the rawMessages built in sync with envelopes.
    // Result: schema-invalid messages (already sent to DLQ by validateWithSchema) appeared
    // in rawMessages again and were sent to DLQ a second time on handler failure.
    const strictSchema: SchemaLike<{ id: string; value: number }> = {
      parse(data: unknown) {
        const d = data as any;
        if (typeof d?.id !== "string") throw new Error("Validation failed");
        return d;
      },
    };
    const Desc = topic("test.topic").schema(strictSchema);
    const handler = jest.fn().mockRejectedValue(new Error("handler fail"));

    mockRun.mockImplementation(async ({ eachBatch }: any) => {
      await eachBatch({
        batch: {
          topic: "test.topic",
          partition: 0,
          highWatermark: "100",
          messages: [
            // msg 0: schema invalid → sent to DLQ by validateWithSchema, skipped by parseSingleMessage
            {
              value: Buffer.from(JSON.stringify({ id: 123, value: 10 })),
              offset: "0",
            },
            // msg 1: schema valid, handler fails → sent to DLQ by executeWithRetry
            {
              value: Buffer.from(JSON.stringify({ id: "2", value: 20 })),
              offset: "1",
            },
          ],
        },
        heartbeat: jest.fn(),
        resolveOffset: jest.fn(),
        commitOffsetsIfNecessary: jest.fn(),
      });
    });

    await client.startBatchConsumer([Desc] as any, handler, { dlq: true });

    // Exactly 2 DLQ sends: one for schema fail (msg 0) + one for handler fail (msg 1).
    // With the bug it would be 3: schema fail + both msgs from the rebuilt rawMessages.
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "test.topic.dlq",
        messages: expect.arrayContaining([
          expect.objectContaining({
            value: JSON.stringify({ id: "2", value: 20 }),
          }),
        ]),
      }),
    );
  });

  it("should preserve per-message headers when routing batch to retryTopics", async () => {
    // Bug: executeWithRetry passed only envelopes[0].headers to sendToRetryTopic for all
    // messages in the batch. Messages 2..N ended up with the first message's correlation
    // ID, traceparent, and custom headers in the retry topic.
    mockListTopics.mockResolvedValue(["test.topic", "test.topic.retry.1"]);
    mockRun
      .mockImplementationOnce(async ({ eachBatch }: any) => {
        await eachBatch({
          batch: {
            topic: "test.topic",
            partition: 0,
            highWatermark: "100",
            messages: [
              {
                value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
                headers: { "x-correlation-id": "corr-A" },
                offset: "0",
              },
              {
                value: Buffer.from(JSON.stringify({ id: "2", value: 20 })),
                headers: { "x-correlation-id": "corr-B" },
                offset: "1",
              },
            ],
          },
          heartbeat: jest.fn(),
          resolveOffset: jest.fn(),
          commitOffsetsIfNecessary: jest.fn(),
        });
      })
      .mockImplementationOnce(async () => {}); // retry.1 consumer — no messages

    const handler = jest.fn().mockRejectedValue(new Error("handler fail"));

    await client.startBatchConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      retryTopics: true,
      retryTopicAssignmentTimeoutMs: 0,
    });

    // Both messages routed in one send — each with their own x-correlation-id
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "test.topic.retry.1",
        messages: [
          expect.objectContaining({ headers: expect.objectContaining({ "x-correlation-id": "corr-A" }) }),
          expect.objectContaining({ headers: expect.objectContaining({ "x-correlation-id": "corr-B" }) }),
        ],
      }),
    );
  });
});
