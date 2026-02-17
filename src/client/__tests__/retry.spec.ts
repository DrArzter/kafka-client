import {
  TestTopicMap,
  createClient,
  envelopeWith,
  setupMessage,
  mockRun,
  mockSend,
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
        messages: [{ value: JSON.stringify({ id: "1", value: 1 }) }],
        acks: -1,
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
});
