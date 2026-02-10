jest.mock("kafkajs");

import { KafkaClient, TTopicMessageMap } from "./kafka.client";
import {
  mockSend,
  mockConnect,
  mockConsumer,
  mockSubscribe,
  mockRun,
  mockListTopics,
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
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });
  });

  describe("disconnect", () => {
    it("should disconnect producer and consumer", async () => {
      await client.disconnect();
      expect(mockProducer.disconnect).toHaveBeenCalled();
      expect(mockConsumer.disconnect).toHaveBeenCalled();
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
});
