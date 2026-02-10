jest.mock("kafkajs");

import { KafkaClient, TTopicMessageMap } from "./kafka.client";
import { topic } from "./topic";
import { KafkaRetryExhaustedError } from "./errors";
import {
  mockSend,
  mockConnect,
  mockConsumer,
  mockSubscribe,
  mockRun,
  mockListTopics,
  mockCreateTopics,
  mockAdmin,
  mockProducer,
  mockTransaction,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
} from "../__mocks__/kafkajs";

interface TestTopicMap extends TTopicMessageMap {
  "test.topic": { id: string; value: number };
  "test.other": { name: string };
}

describe("KafkaClient", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = new KafkaClient<TestTopicMap>("test-client", "test-group", [
      "localhost:9092",
    ]);
  });

  describe("connectProducer", () => {
    it("should connect the producer", async () => {
      await client.connectProducer();
      expect(mockConnect).toHaveBeenCalled();
    });
  });

  describe("disconnectProducer", () => {
    it("should disconnect the producer", async () => {
      await client.disconnectProducer();
      expect(mockProducer.disconnect).toHaveBeenCalled();
    });
  });

  describe("sendMessage", () => {
    it("should send a JSON-serialized message", async () => {
      const message = { id: "123", value: 42 };
      await client.sendMessage("test.topic", message);

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          { value: JSON.stringify(message), key: null, headers: undefined },
        ],
        acks: -1,
      });
    });

    it("should send a message with a partition key", async () => {
      const message = { id: "123", value: 42 };
      await client.sendMessage("test.topic", message, { key: "123" });

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          { value: JSON.stringify(message), key: "123", headers: undefined },
        ],
        acks: -1,
      });
    });

    it("should send a message with headers", async () => {
      const message = { id: "123", value: 42 };
      await client.sendMessage("test.topic", message, {
        headers: { "x-correlation-id": "abc-123", "x-source": "test" },
      });

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify(message),
            key: null,
            headers: { "x-correlation-id": "abc-123", "x-source": "test" },
          },
        ],
        acks: -1,
      });
    });
  });

  describe("sendBatch", () => {
    it("should send multiple messages in one call", async () => {
      await client.sendBatch("test.topic", [
        { value: { id: "1", value: 10 }, key: "1" },
        { value: { id: "2", value: 20 } },
      ]);

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: "1",
            headers: undefined,
          },
          {
            value: JSON.stringify({ id: "2", value: 20 }),
            key: null,
            headers: undefined,
          },
        ],
        acks: -1,
      });
    });

    it("should send batch with headers", async () => {
      await client.sendBatch("test.topic", [
        {
          value: { id: "1", value: 10 },
          key: "1",
          headers: { "x-trace": "t1" },
        },
      ]);

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: "1",
            headers: { "x-trace": "t1" },
          },
        ],
        acks: -1,
      });
    });
  });

  describe("startConsumer", () => {
    it("should subscribe and run consumer", async () => {
      const handler = jest.fn();
      await client.startConsumer(["test.topic"], handler);

      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ["test.topic"],
        fromBeginning: false,
      });
      expect(mockRun).toHaveBeenCalledWith(
        expect.objectContaining({ autoCommit: true }),
      );
    });

    it("should pass consumer options", async () => {
      const handler = jest.fn();
      await client.startConsumer(["test.topic"], handler, {
        fromBeginning: true,
        autoCommit: false,
      });

      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ["test.topic"],
        fromBeginning: true,
      });
      expect(mockRun).toHaveBeenCalledWith(
        expect.objectContaining({ autoCommit: false }),
      );
    });

    it("should parse and pass message to handler", async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 100 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler);

      expect(handler).toHaveBeenCalledWith(
        { id: "1", value: 100 },
        "test.topic",
      );
    });

    it("should skip empty messages", async () => {
      const handler = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: { value: null },
        });
      });

      await client.startConsumer(["test.topic"], handler);
      expect(handler).not.toHaveBeenCalled();
    });

    it("should handle invalid JSON gracefully", async () => {
      const handler = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: { value: Buffer.from("not json") },
        });
      });

      await client.startConsumer(["test.topic"], handler);
      expect(handler).not.toHaveBeenCalled();
    });

    it("should retry on handler failure", async () => {
      const handler = jest
        .fn()
        .mockRejectedValueOnce(new Error("fail"))
        .mockResolvedValue(undefined);

      mockRun.mockImplementation(async ({ eachMessage }) => {
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

      mockRun.mockImplementation(async ({ eachMessage }) => {
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

  describe("stopConsumer", () => {
    it("should disconnect the consumer", async () => {
      await client.stopConsumer();
      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });
  });

  describe("checkStatus", () => {
    it("should return topics list", async () => {
      const result = await client.checkStatus();
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(result).toEqual({ topics: ["topic1", "topic2"] });
    });

    it("should not reconnect admin on subsequent calls", async () => {
      await client.checkStatus();
      mockAdmin.connect.mockClear();
      await client.checkStatus();
      expect(mockAdmin.connect).not.toHaveBeenCalled();
    });
  });

  describe("disconnect", () => {
    it("should disconnect producer and consumer", async () => {
      await client.disconnect();
      expect(mockProducer.disconnect).toHaveBeenCalled();
      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });

    it("should disconnect admin if it was connected", async () => {
      await client.checkStatus();
      await client.disconnect();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });
  });

  describe("transaction", () => {
    it("should commit on success", async () => {
      await client.transaction(async (tx) => {
        await tx.send("test.topic", { id: "1", value: 10 }, { key: "1" });
        await tx.send("test.other", { name: "hello" });
      });

      expect(mockTransaction).toHaveBeenCalled();
      expect(mockTxSend).toHaveBeenCalledTimes(2);
      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: "1",
            headers: undefined,
          },
        ],
        acks: -1,
      });
      expect(mockTxCommit).toHaveBeenCalled();
      expect(mockTxAbort).not.toHaveBeenCalled();
    });

    it("should abort on error", async () => {
      await expect(
        client.transaction(async (tx) => {
          await tx.send("test.topic", { id: "1", value: 10 });
          throw new Error("something went wrong");
        }),
      ).rejects.toThrow("something went wrong");

      expect(mockTxAbort).toHaveBeenCalled();
      expect(mockTxCommit).not.toHaveBeenCalled();
    });

    it("should support sendBatch in transaction", async () => {
      await client.transaction(async (tx) => {
        await tx.sendBatch("test.topic", [
          { value: { id: "1", value: 10 }, key: "1" },
          { value: { id: "2", value: 20 }, headers: { "x-trace": "t1" } },
        ]);
      });

      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: "1",
            headers: undefined,
          },
          {
            value: JSON.stringify({ id: "2", value: 20 }),
            key: null,
            headers: { "x-trace": "t1" },
          },
        ],
        acks: -1,
      });
      expect(mockTxCommit).toHaveBeenCalled();
    });
  });

  describe("interceptors", () => {
    it("should call before and after on success", async () => {
      const before = jest.fn();
      const after = jest.fn();
      const handler = jest.fn().mockResolvedValue(undefined);

      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [{ before, after }],
      });

      expect(before).toHaveBeenCalledWith({ id: "1", value: 1 }, "test.topic");
      expect(handler).toHaveBeenCalled();
      expect(after).toHaveBeenCalledWith({ id: "1", value: 1 }, "test.topic");
    });

    it("should call onError when handler fails", async () => {
      const onError = jest.fn();
      const handler = jest.fn().mockRejectedValue(new Error("boom"));

      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [{ onError }],
      });

      expect(onError).toHaveBeenCalledWith(
        { id: "1", value: 1 },
        "test.topic",
        expect.any(Error),
      );
    });

    it("should call multiple interceptors in order", async () => {
      const calls: string[] = [];
      const handler = jest.fn().mockResolvedValue(undefined);

      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          {
            before: () => {
              calls.push("a-before");
            },
            after: () => {
              calls.push("a-after");
            },
          },
          {
            before: () => {
              calls.push("b-before");
            },
            after: () => {
              calls.push("b-after");
            },
          },
        ],
      });

      expect(calls).toEqual(["a-before", "b-before", "a-after", "b-after"]);
    });
  });

  describe("getClientId", () => {
    it("should return clientId", () => {
      expect(client.getClientId()).toBe("test-client");
    });
  });

  describe("TopicDescriptor support", () => {
    const TestTopic = topic("test.topic")<{ id: string; value: number }>();

    it("should send message via TopicDescriptor", async () => {
      await client.sendMessage(TestTopic, { id: "1", value: 42 });

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 42 }),
            key: null,
            headers: undefined,
          },
        ],
        acks: -1,
      });
    });

    it("should sendBatch via TopicDescriptor", async () => {
      await client.sendBatch(TestTopic, [
        { value: { id: "1", value: 10 }, key: "1" },
      ]);

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: "1",
            headers: undefined,
          },
        ],
        acks: -1,
      });
    });

    it("should work in transaction via TopicDescriptor", async () => {
      await client.transaction(async (tx) => {
        await tx.send(TestTopic, { id: "1", value: 10 });
      });

      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: null,
            headers: undefined,
          },
        ],
        acks: -1,
      });
      expect(mockTxCommit).toHaveBeenCalled();
    });

    it("should startConsumer with TopicDescriptor array", async () => {
      const handler = jest.fn();
      await client.startConsumer([TestTopic] as any, handler);

      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ["test.topic"],
        fromBeginning: false,
      });
    });
  });

  describe("autoCreateTopics", () => {
    let autoClient: KafkaClient<TestTopicMap>;

    beforeEach(() => {
      autoClient = new KafkaClient<TestTopicMap>(
        "auto-client",
        "auto-group",
        ["localhost:9092"],
        { autoCreateTopics: true },
      );
    });

    it("should create topic on sendMessage", async () => {
      await autoClient.sendMessage("test.topic", { id: "1", value: 1 });

      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic", numPartitions: 1 }],
      });
    });

    it("should not create topic twice", async () => {
      await autoClient.sendMessage("test.topic", { id: "1", value: 1 });
      mockCreateTopics.mockClear();
      await autoClient.sendMessage("test.topic", { id: "2", value: 2 });

      expect(mockCreateTopics).not.toHaveBeenCalled();
    });

    it("should not create topics when disabled", async () => {
      await client.sendMessage("test.topic", { id: "1", value: 1 });

      expect(mockCreateTopics).not.toHaveBeenCalled();
    });
  });

  describe("interceptor error handling", () => {
    function setupMessage() {
      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });
    }

    it("should propagate error when before interceptor throws", async () => {
      setupMessage();
      const handler = jest.fn().mockResolvedValue(undefined);
      const onError = jest.fn();

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          { before: () => { throw new Error("before failed"); }, onError },
        ],
      });

      expect(handler).not.toHaveBeenCalled();
      expect(onError).toHaveBeenCalledWith(
        { id: "1", value: 1 },
        "test.topic",
        expect.objectContaining({ message: "before failed" }),
      );
    });

    it("should propagate error when after interceptor throws", async () => {
      setupMessage();
      const handler = jest.fn().mockResolvedValue(undefined);
      const onError = jest.fn();

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          { after: () => { throw new Error("after failed"); }, onError },
        ],
      });

      expect(handler).toHaveBeenCalled();
      expect(onError).toHaveBeenCalledWith(
        { id: "1", value: 1 },
        "test.topic",
        expect.objectContaining({ message: "after failed" }),
      );
    });

    it("should not throw if onError interceptor throws", async () => {
      setupMessage();
      const handler = jest.fn().mockRejectedValue(new Error("fail"));

      // onError itself throws — should not bubble up (logged but swallowed by consumer loop)
      await expect(
        client.startConsumer(["test.topic"], handler, {
          interceptors: [
            { onError: () => { throw new Error("onError failed"); } },
          ],
        }),
      ).rejects.toThrow("onError failed");
    });

    it("should not call second before if first before throws", async () => {
      setupMessage();
      const secondBefore = jest.fn();
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          { before: () => { throw new Error("stop"); } },
          { before: secondBefore },
        ],
      });

      expect(secondBefore).not.toHaveBeenCalled();
      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe("retry edge cases", () => {
    function setupMessage() {
      mockRun.mockImplementation(async ({ eachMessage }) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });
    }

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
      expect(onError.mock.calls[0][2]).not.toBeInstanceOf(
        KafkaRetryExhaustedError,
      );
      expect(onError.mock.calls[0][2]).toBeInstanceOf(Error);
    });

    it("should handle handler throwing non-Error object", async () => {
      setupMessage();
      const onError = jest.fn();
      const handler = jest.fn().mockRejectedValue("string error");

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [{ onError }],
      });

      expect(onError).toHaveBeenCalledWith(
        { id: "1", value: 1 },
        "test.topic",
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

  describe("autoCreateTopics extended", () => {
    let autoClient: KafkaClient<TestTopicMap>;

    beforeEach(() => {
      autoClient = new KafkaClient<TestTopicMap>(
        "auto-client",
        "auto-group",
        ["localhost:9092"],
        { autoCreateTopics: true },
      );
    });

    it("should create topic on sendBatch", async () => {
      await autoClient.sendBatch("test.topic", [
        { value: { id: "1", value: 1 } },
      ]);

      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic", numPartitions: 1 }],
      });
    });

    it("should create topic in transaction", async () => {
      await autoClient.transaction(async (tx) => {
        await tx.send("test.topic", { id: "1", value: 1 });
      });

      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic", numPartitions: 1 }],
      });
    });

    it("should create topics on startConsumer", async () => {
      await autoClient.startConsumer(["test.topic"], jest.fn());

      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic", numPartitions: 1 }],
      });
    });

    it("should reuse admin connection across ensureTopic calls", async () => {
      await autoClient.sendMessage("test.topic", { id: "1", value: 1 });
      await autoClient.sendMessage("test.other", { name: "hi" });

      // admin.connect should only be called once
      expect(mockAdmin.connect).toHaveBeenCalledTimes(1);
    });
  });

  describe("transaction edge cases", () => {
    it("should handle empty transaction (no sends)", async () => {
      await client.transaction(async () => {
        // no-op
      });

      expect(mockTransaction).toHaveBeenCalled();
      expect(mockTxSend).not.toHaveBeenCalled();
      expect(mockTxCommit).toHaveBeenCalled();
    });

    it("should sendBatch via TopicDescriptor in transaction", async () => {
      const TestTopic = topic("test.topic")<{ id: string; value: number }>();

      await client.transaction(async (tx) => {
        await tx.sendBatch(TestTopic, [
          { value: { id: "1", value: 10 } },
          { value: { id: "2", value: 20 } },
        ]);
      });

      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          { value: JSON.stringify({ id: "1", value: 10 }), key: null, headers: undefined },
          { value: JSON.stringify({ id: "2", value: 20 }), key: null, headers: undefined },
        ],
        acks: -1,
      });
      expect(mockTxCommit).toHaveBeenCalled();
    });

    it("should abort if sendBatch throws in transaction", async () => {
      mockTxSend.mockRejectedValueOnce(new Error("batch failed"));

      await expect(
        client.transaction(async (tx) => {
          await tx.sendBatch("test.topic", [
            { value: { id: "1", value: 1 } },
          ]);
        }),
      ).rejects.toThrow("batch failed");

      expect(mockTxAbort).toHaveBeenCalled();
      expect(mockTxCommit).not.toHaveBeenCalled();
    });
  });

  describe("disconnect edge cases", () => {
    it("should not disconnect admin if it was never connected", async () => {
      await client.disconnect();
      expect(mockAdmin.disconnect).not.toHaveBeenCalled();
    });

    it("should handle partial disconnect failure gracefully", async () => {
      mockProducer.disconnect.mockRejectedValueOnce(
        new Error("producer failed"),
      );

      // Promise.allSettled means no throw
      await client.disconnect();

      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });
  });

  describe("KafkaRetryExhaustedError", () => {
    it("should pass KafkaRetryExhaustedError to onError after all retries", async () => {
      const onError = jest.fn();
      const handler = jest.fn().mockRejectedValue(new Error("always fails"));

      mockRun.mockImplementation(async ({ eachMessage }) => {
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

      // Last call should be KafkaRetryExhaustedError
      const lastCall = onError.mock.calls[onError.mock.calls.length - 1];
      expect(lastCall[2]).toBeInstanceOf(KafkaRetryExhaustedError);
      expect((lastCall[2] as KafkaRetryExhaustedError).attempts).toBe(3);
      expect((lastCall[2] as KafkaRetryExhaustedError).topic).toBe(
        "test.topic",
      );
    });
  });
});
