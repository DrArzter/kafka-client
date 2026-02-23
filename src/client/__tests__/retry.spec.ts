import {
  TestTopicMap,
  createClient,
  envelopeWith,
  setupMessage,
  mockRun,
  mockSend,
  mockAdmin,
  mockListTopics,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
  mockSendOffsets,
  mockCommitOffsets,
  KafkaClient,
  KafkaRetryExhaustedError,
} from "./helpers";

describe("KafkaClient — Retry", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("retry on handler failure", () => {
    it("should retry on handler failure", async () => {
      const handler = jest
        .fn()
        .mockRejectedValueOnce(new Error("fail"))
        .mockResolvedValue(undefined);

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        retry: { maxRetries: 2, backoffMs: 1 },
      });

      expect(handler).toHaveBeenCalledTimes(2);
    });

    it("should send to DLQ after all retries exhausted", async () => {
      const handler = jest.fn().mockRejectedValue(new Error("always fails"));

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        retry: { maxRetries: 1, backoffMs: 1 },
        dlq: true,
      });

      expect(handler).toHaveBeenCalledTimes(2);
      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic.dlq",
        messages: [
          expect.objectContaining({
            value: JSON.stringify({ id: "1", value: 1 }),
            headers: expect.objectContaining({
              "x-dlq-original-topic": "test.topic",
              "x-dlq-error-message": "always fails",
              "x-dlq-attempt-count": "2",
            }),
          }),
        ],
      });
    });
  });

  describe("retry edge cases", () => {
    it("should not retry when no retry config (single attempt)", async () => {
      setupMessage();
      const handler = jest.fn().mockRejectedValue(new Error("fail"));

      await client.startConsumer(["test.topic"], handler);

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("should not send to DLQ when dlq is disabled", async () => {
      setupMessage();
      const handler = jest.fn().mockRejectedValue(new Error("fail"));

      await client.startConsumer(["test.topic"], handler, {
        retry: { maxRetries: 1, backoffMs: 1 },
        dlq: false,
      });

      expect(handler).toHaveBeenCalledTimes(2);
      // No DLQ send — only the original message sends should exist
      expect(mockSend).not.toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
    });

    it("should send to DLQ even without retry config (single attempt + dlq)", async () => {
      setupMessage();
      const handler = jest.fn().mockRejectedValue(new Error("fail"));

      await client.startConsumer(["test.topic"], handler, { dlq: true });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
    });

    it("should not throw KafkaRetryExhaustedError when maxAttempts is 1", async () => {
      setupMessage();
      const onError = jest.fn();
      const handler = jest.fn().mockRejectedValue(new Error("fail"));

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [{ onError }],
      });

      expect(onError).toHaveBeenCalledTimes(1);
      // onError(envelope, error) — error is second arg
      expect(onError.mock.calls[0][1]).not.toBeInstanceOf(
        KafkaRetryExhaustedError,
      );
      expect(onError.mock.calls[0][1]).toBeInstanceOf(Error);
    });

    it("should handle handler throwing non-Error object", async () => {
      setupMessage();
      const onError = jest.fn();
      const handler = jest.fn().mockRejectedValue("string error");

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [{ onError }],
      });

      expect(onError).toHaveBeenCalledWith(
        envelopeWith({ id: "1", value: 1 }),
        expect.objectContaining({ message: "string error" }),
      );
    });

    it("should silently handle DLQ send failure", async () => {
      setupMessage();
      mockSend.mockRejectedValueOnce(new Error("DLQ broker down"));
      const handler = jest.fn().mockRejectedValue(new Error("fail"));

      // Should not throw even though DLQ send fails
      await client.startConsumer(["test.topic"], handler, { dlq: true });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("should retry and succeed on second attempt", async () => {
      setupMessage();
      const before = jest.fn();
      const after = jest.fn();
      const handler = jest
        .fn()
        .mockRejectedValueOnce(new Error("transient"))
        .mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        retry: { maxRetries: 1, backoffMs: 1 },
        interceptors: [{ before, after }],
      });

      expect(handler).toHaveBeenCalledTimes(2);
      // before called for both attempts, after only for the successful one
      expect(before).toHaveBeenCalledTimes(2);
      expect(after).toHaveBeenCalledTimes(1);
    });
  });

  describe("KafkaRetryExhaustedError", () => {
    it("should pass KafkaRetryExhaustedError to onError after all retries", async () => {
      const onError = jest.fn();
      const handler = jest.fn().mockRejectedValue(new Error("always fails"));

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        retry: { maxRetries: 2, backoffMs: 1 },
        interceptors: [{ onError }],
      });

      // Last call should be KafkaRetryExhaustedError — onError(envelope, error)
      const lastCall = onError.mock.calls[onError.mock.calls.length - 1];
      expect(lastCall[1]).toBeInstanceOf(KafkaRetryExhaustedError);
      expect((lastCall[1] as KafkaRetryExhaustedError).attempts).toBe(3);
      expect((lastCall[1] as KafkaRetryExhaustedError).topic).toBe(
        "test.topic",
      );
    });
  });

  describe("retry topic existence validation", () => {
    it("should throw at startup when retryTopics:true and retry topics are missing (autoCreateTopics:false)", async () => {
      // Default mockListTopics returns ["topic1","topic2"] — no retry topics
      const client = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { autoCreateTopics: false },
      );

      await expect(
        client.startConsumer(["test.topic"], jest.fn(), {
          retryTopics: true,
          retry: { maxRetries: 2, backoffMs: 100 },
        }),
      ).rejects.toThrow(/retry topics do not exist/);

      expect(mockAdmin.connect).toHaveBeenCalled();
    });

    it("should NOT validate when autoCreateTopics:true (topics are created automatically)", async () => {
      const client = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { autoCreateTopics: true },
      );

      // Should not throw even though listTopics doesn't return retry topics.
      // retryTopicAssignmentTimeoutMs:0 so waitForPartitionAssignment returns
      // immediately in tests (no real broker to assign partitions).
      await expect(
        client.startConsumer(["test.topic"], jest.fn(), {
          retryTopics: true,
          retry: { maxRetries: 1, backoffMs: 100 },
          retryTopicAssignmentTimeoutMs: 0,
        }),
      ).resolves.not.toThrow();
    });

    it("should pass validation when all retry topics exist", async () => {
      mockListTopics.mockResolvedValueOnce([
        "test.topic",
        "test.topic.retry.1",
        "test.topic.retry.2",
      ]);

      const client = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { autoCreateTopics: false },
      );

      await expect(
        client.startConsumer(["test.topic"], jest.fn(), {
          retryTopics: true,
          retry: { maxRetries: 2, backoffMs: 100 },
          retryTopicAssignmentTimeoutMs: 0,
        }),
      ).resolves.not.toThrow();
    });
  });
});

// ── Retry headers helper ─────────────────────────────────────────────────────

/**
 * Build a mock Kafka message that looks like it arrived on a retry topic.
 * `retryAfter` defaults to 0 (in the past) so no sleep is triggered in tests.
 */
function retryLevelMessage(
  payload: object,
  {
    level = 1,
    maxRetries = 2,
    originalTopic = "test.topic",
    offset = "5",
  }: {
    level?: number;
    maxRetries?: number;
    originalTopic?: string;
    offset?: string;
  } = {},
) {
  return {
    topic: `${originalTopic}.retry.${level}`,
    partition: 0,
    message: {
      value: Buffer.from(JSON.stringify(payload)),
      offset,
      headers: {
        "x-retry-attempt": [String(level)],
        "x-retry-after": [String(Date.now() - 1_000)], // in the past — no sleep
        "x-retry-max-retries": [String(maxRetries)],
        "x-retry-original-topic": [originalTopic],
      },
    },
  };
}

describe("KafkaClient — EOS retry routing (Exactly-Once)", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Reset mockRun to a safe no-op default. jest.clearAllMocks() only clears
    // call history — it does NOT reset mockImplementation. Tests in the outer
    // "KafkaClient — Retry" describe call setupMessage() which sets a persistent
    // mockImplementation that would otherwise leak into the level-2 consumer's
    // consumer.run() call here (since maxRetries: 2 produces 3 run() calls).
    mockRun.mockReset();
    mockRun.mockResolvedValue(undefined);
    // Make listTopics return retry topics so validation passes.
    mockListTopics.mockResolvedValue([
      "test.topic",
      "test.topic.retry.1",
      "test.topic.retry.2",
    ]);
  });

  it("routes to next retry level via EOS tx on handler failure (non-exhausted)", async () => {
    const client = new KafkaClient<TestTopicMap>(
      "test-client",
      "test-group",
      ["localhost:9092"],
      { autoCreateTopics: false },
    );

    // Call 1: main consumer — no messages fired.
    // Call 2: level-1 retry consumer — fires a failing message.
    mockRun
      .mockImplementationOnce(async () => {})
      .mockImplementationOnce(async ({ eachMessage }: any) => {
        await eachMessage(
          retryLevelMessage({ id: "1", value: 1 }, { level: 1, maxRetries: 2 }),
        );
      });

    const handler = jest.fn().mockRejectedValue(new Error("handler fails"));

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 2, backoffMs: 1 },
      retryTopics: true,
      retryTopicAssignmentTimeoutMs: 0,
    });

    // EOS: tx.send should route to retry.2
    expect(mockTxSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic.retry.2" }),
    );
    // sendOffsets called with the consumer object and the next offset in the correct format
    expect(mockSendOffsets).toHaveBeenCalledWith(
      expect.objectContaining({
        topics: [expect.objectContaining({ topic: "test.topic.retry.1" })],
      }),
    );
    expect(mockTxCommit).toHaveBeenCalled();
    // Offset is committed atomically via tx — no separate commitOffsets call
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });

  it("routes to DLQ via EOS tx on retry exhaustion (level >= maxRetries)", async () => {
    const client = new KafkaClient<TestTopicMap>(
      "test-client",
      "test-group",
      ["localhost:9092"],
      { autoCreateTopics: false },
    );

    mockListTopics.mockResolvedValue(["test.topic", "test.topic.retry.1", "test.topic.dlq"]);

    // Call 1: main consumer — no messages fired.
    // Call 2: level-1 retry consumer — fires a failing message at maxRetries=1 (exhausted).
    mockRun
      .mockImplementationOnce(async () => {})
      .mockImplementationOnce(async ({ eachMessage }: any) => {
        await eachMessage(
          retryLevelMessage({ id: "1", value: 1 }, { level: 1, maxRetries: 1 }),
        );
      });

    const handler = jest.fn().mockRejectedValue(new Error("always fails"));

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      retryTopics: true,
      dlq: true,
      retryTopicAssignmentTimeoutMs: 0,
    });

    expect(mockTxSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic.dlq" }),
    );
    expect(mockSendOffsets).toHaveBeenCalledWith(
      expect.objectContaining({
        topics: [expect.objectContaining({ topic: "test.topic.retry.1" })],
      }),
    );
    expect(mockTxCommit).toHaveBeenCalled();
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });

  it("does not commit offset when EOS tx fails — message is redelivered", async () => {
    const client = new KafkaClient<TestTopicMap>(
      "test-client",
      "test-group",
      ["localhost:9092"],
      { autoCreateTopics: false },
    );

    // Simulate tx.commit() failing (broker unavailable)
    mockTxCommit.mockRejectedValueOnce(new Error("broker down"));

    mockRun
      .mockImplementationOnce(async () => {})
      .mockImplementationOnce(async ({ eachMessage }: any) => {
        await eachMessage(
          retryLevelMessage({ id: "1", value: 1 }, { level: 1, maxRetries: 2 }),
        );
      });

    const handler = jest.fn().mockRejectedValue(new Error("handler fails"));

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 2, backoffMs: 1 },
      retryTopics: true,
      retryTopicAssignmentTimeoutMs: 0,
    });

    // Transaction was aborted on commit failure
    expect(mockTxAbort).toHaveBeenCalled();
    // Offset NOT committed — message will be redelivered on next poll
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });
});
