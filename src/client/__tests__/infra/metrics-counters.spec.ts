import {
  TestTopicMap,
  createClient,
  mockRun,
  mockSend,
  mockTxSend,
  mockTxCommit,
  mockSendOffsets,
  mockAssignment,
  KafkaClient,
} from "../helpers";

// ── Helpers ────────────────────────────────────────────────────────────

function deliverMessage(clock?: number) {
  const headers: Record<string, string[]> = {};
  if (clock !== undefined) headers["x-lamport-clock"] = [String(clock)];
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    await eachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
        headers,
        offset: "0",
      },
    });
  });
}

// ── Tests ──────────────────────────────────────────────────────────────

describe("KafkaClient — getMetrics / resetMetrics", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("starts with all counters at zero", () => {
    expect(client.getMetrics()).toEqual({
      processedCount: 0,
      retryCount: 0,
      dlqCount: 0,
      dedupCount: 0,
    });
  });

  it("getMetrics returns a snapshot (not a live reference)", () => {
    const snap1 = client.getMetrics();
    const snap2 = client.getMetrics();
    expect(snap1).not.toBe(snap2);
    expect(snap1).toEqual(snap2);
  });

  it("resetMetrics resets all counters to zero", async () => {
    const handler = jest
      .fn()
      .mockRejectedValueOnce(new Error("fail"))
      .mockResolvedValue(undefined);
    deliverMessage();
    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
    });

    expect(client.getMetrics().retryCount).toBeGreaterThan(0);

    client.resetMetrics();

    expect(client.getMetrics()).toEqual({
      processedCount: 0,
      retryCount: 0,
      dlqCount: 0,
      dedupCount: 0,
    });
  });
});

describe("KafkaClient — metrics.processedCount", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("increments processedCount on successful message handling", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    deliverMessage();

    await client.startConsumer(["test.topic"], handler);

    expect(client.getMetrics().processedCount).toBe(1);
  });

  it("does not increment processedCount when handler fails", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("fail"));
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      dlq: true,
    });

    expect(client.getMetrics().processedCount).toBe(0);
  });

  it("increments processedCount for each message in a multi-message run", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (let i = 0; i < 3; i++) {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: String(i), value: i })),
            headers: {},
            offset: String(i),
          },
        });
      }
    });

    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startConsumer(["test.topic"], handler);

    expect(client.getMetrics().processedCount).toBe(3);
  });
});

describe("KafkaClient — metrics.retryCount", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("increments retryCount on each in-process retry attempt", async () => {
    const handler = jest
      .fn()
      .mockRejectedValueOnce(new Error("fail"))
      .mockRejectedValueOnce(new Error("fail"))
      .mockResolvedValue(undefined);
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 2, backoffMs: 1 },
    });

    expect(client.getMetrics().retryCount).toBe(2);
  });

  it("increments retryCount when routing to retry topic (retryTopics: true)", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("fail"));

    mockRun
      .mockImplementationOnce(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: {},
            offset: "0",
          },
        });
      })
      .mockResolvedValue(undefined);

    mockAssignment.mockReturnValue([
      { topic: "test.topic.retry.1", partition: 0 },
      { topic: "test.topic.retry.2", partition: 0 },
      { topic: "test.topic.retry.3", partition: 0 },
    ]);

    const retryClient = new KafkaClient<TestTopicMap>(
      "test-client",
      "test-group",
      ["localhost:9092"],
      { autoCreateTopics: true },
    );

    await retryClient.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 3, backoffMs: 1 },
      retryTopics: true,
    });

    expect(retryClient.getMetrics().retryCount).toBe(1);
    expect(mockTxSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic.retry.1" }),
    );
    expect(mockSendOffsets).toHaveBeenCalled();
    expect(mockTxCommit).toHaveBeenCalled();

    await retryClient.disconnect();
  });
});

describe("KafkaClient — metrics.dlqCount", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("increments dlqCount when message is sent to DLQ after exhausted retries", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("fail"));
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      dlq: true,
    });

    expect(client.getMetrics().dlqCount).toBe(1);
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic.dlq" }),
    );
  });

  it("does not increment dlqCount when message is handled successfully", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      dlq: true,
    });

    expect(client.getMetrics().dlqCount).toBe(0);
  });
});

describe("KafkaClient — metrics.dedupCount", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("increments dedupCount when a Lamport clock duplicate is detected", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (const clock of [5, 3]) {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: { "x-lamport-clock": [String(clock)] },
            offset: "0",
          },
        });
      }
    });

    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startConsumer(["test.topic"], handler, {
      deduplication: { strategy: "drop" },
    });

    expect(client.getMetrics().dedupCount).toBe(1);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it("does not increment dedupCount for non-duplicate messages", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (const clock of [1, 2, 3]) {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: { "x-lamport-clock": [String(clock)] },
            offset: "0",
          },
        });
      }
    });

    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startConsumer(["test.topic"], handler, {
      deduplication: { strategy: "drop" },
    });

    expect(client.getMetrics().dedupCount).toBe(0);
    expect(handler).toHaveBeenCalledTimes(3);
  });
});
