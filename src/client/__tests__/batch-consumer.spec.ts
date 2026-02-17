import {
  TestTopicMap,
  createClient,
  envelopeWith,
  mockConsumer,
  mockSubscribe,
  mockRun,
  mockSend,
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
});
