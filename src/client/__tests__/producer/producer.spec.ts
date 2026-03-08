import {
  TestTopicMap,
  createClient,
  mockSend,
  mockConnect,
  mockProducer,
  mockTxSend,
  mockRun,
  mockSeek,
  mockSubscribe,
  mockConsumer,
  mockAdmin,
  mockFetchTopicOffsets,
  KafkaClient,
  topic,
} from "../helpers";
import type { KafkaInstrumentation } from "../../types";

describe("KafkaClient — Producer", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
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
    it("should send a JSON-serialized message with envelope headers", async () => {
      const message = { id: "123", value: 42 };
      await client.sendMessage("test.topic", message);

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify(message),
            key: null,
            headers: expect.objectContaining({
              "x-event-id": expect.any(String),
              "x-correlation-id": expect.any(String),
              "x-timestamp": expect.any(String),
              "x-schema-version": "1",
            }),
          },
        ],
      });
    });

    it("should send a message with a partition key", async () => {
      const message = { id: "123", value: 42 };
      await client.sendMessage("test.topic", message, { key: "123" });

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify(message),
            key: "123",
            headers: expect.any(Object),
          },
        ],
      });
    });

    it("should merge user headers with envelope headers", async () => {
      const message = { id: "123", value: 42 };
      await client.sendMessage("test.topic", message, {
        headers: { "x-source": "test" },
      });

      const sent = mockSend.mock.calls[0][0];
      expect(sent.messages[0].headers).toEqual(
        expect.objectContaining({
          "x-event-id": expect.any(String),
          "x-correlation-id": expect.any(String),
          "x-source": "test",
        }),
      );
    });

    it("should allow overriding correlationId via options", async () => {
      const message = { id: "123", value: 42 };
      await client.sendMessage("test.topic", message, {
        correlationId: "custom-corr",
      });

      const sent = mockSend.mock.calls[0][0];
      expect(sent.messages[0].headers["x-correlation-id"]).toBe("custom-corr");
    });
  });

  describe("sendBatch", () => {
    it("should send multiple messages with envelope headers", async () => {
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
            headers: expect.any(Object),
          },
          {
            value: JSON.stringify({ id: "2", value: 20 }),
            key: null,
            headers: expect.any(Object),
          },
        ],
      });
    });

    it("should merge user headers in batch items", async () => {
      await client.sendBatch("test.topic", [
        {
          value: { id: "1", value: 10 },
          key: "1",
          headers: { "x-trace": "t1" },
        },
      ]);

      const sent = mockSend.mock.calls[0][0];
      expect(sent.messages[0].headers).toEqual(
        expect.objectContaining({
          "x-trace": "t1",
          "x-event-id": expect.any(String),
        }),
      );
    });
  });

  describe("getClientId", () => {
    it("should return clientId", () => {
      expect(client.getClientId()).toBe("test-client");
    });
  });

  describe("TopicDescriptor support", () => {
    const TestTopic = topic("test.topic").type<{ id: string; value: number }>();

    it("should send message via TopicDescriptor", async () => {
      await client.sendMessage(TestTopic, { id: "1", value: 42 });

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 42 }),
            key: null,
            headers: expect.any(Object),
          },
        ],
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
            headers: expect.any(Object),
          },
        ],
      });
    });
  });

  describe("onModuleDestroy", () => {
    it("should call disconnect when onModuleDestroy is called", async () => {
      const disconnectSpy = jest
        .spyOn(client, "disconnect")
        .mockResolvedValue(undefined);

      await client.onModuleDestroy();

      expect(disconnectSpy).toHaveBeenCalledTimes(1);
      disconnectSpy.mockRestore();
    });
  });

  describe("instrumentation — afterSend in transaction", () => {
    it("should call afterSend for each tx.send call", async () => {
      const afterSend = jest.fn();
      const inst: KafkaInstrumentation = { afterSend };
      const instrClient = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { instrumentation: [inst] },
      );

      await instrClient.transaction(async (tx) => {
        await tx.send("test.topic", { id: "1", value: 10 });
        await tx.sendBatch("test.other", [{ value: { name: "hello" } }]);
      });

      expect(afterSend).toHaveBeenCalledTimes(2);
      expect(afterSend).toHaveBeenCalledWith("test.topic");
      expect(afterSend).toHaveBeenCalledWith("test.other");
    });

    it("should NOT call afterSend when transaction aborts", async () => {
      const afterSend = jest.fn();
      const inst: KafkaInstrumentation = { afterSend };
      const instrClient = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { instrumentation: [inst] },
      );

      mockTxSend.mockRejectedValueOnce(new Error("send failed"));

      await expect(
        instrClient.transaction(async (tx) => {
          await tx.send("test.topic", { id: "1", value: 10 });
        }),
      ).rejects.toThrow("send failed");

      // afterSend must not fire — the tx.send threw before it could be called
      expect(afterSend).not.toHaveBeenCalled();
    });
  });

  // ── Clock recovery ────────────────────────────────────────────────

  describe("connectProducer — clock recovery", () => {
    /** Helper: make mockRun deliver one eachMessage call with the given clock header. */
    function deliverMessage(clockValue: string, partition = 0) {
      mockRun.mockImplementationOnce(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition,
          message: {
            offset: "499",
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: { "x-lamport-clock": clockValue },
          },
        });
      });
    }

    it("skips recovery when clockRecovery is not configured", async () => {
      await client.connectProducer();
      expect(mockFetchTopicOffsets).not.toHaveBeenCalled();
      expect(mockConsumer.connect).not.toHaveBeenCalled();
    });

    it("skips recovery when all topics are empty (high == low)", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "0" },
      ]);
      const c = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { clockRecovery: { topics: ["test.topic"] } },
      );
      await c.connectProducer();
      expect(mockConsumer.connect).not.toHaveBeenCalled();
    });

    it("restores _lamportClock from the last message header", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "500" },
      ]);
      deliverMessage("499");

      const c = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { clockRecovery: { topics: ["test.topic"] } },
      );
      await c.connectProducer();

      // Next send must stamp clock = 500 (= recovered 499 + 1)
      await c.sendMessage("test.topic", { id: "1", value: 1 });
      const sent = mockSend.mock.calls[0][0];
      expect(sent.messages[0].headers["x-lamport-clock"]).toBe("500");
    });

    it("seeks the recovery consumer to highWatermark − 1", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "300" },
      ]);
      deliverMessage("299");

      const c = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { clockRecovery: { topics: ["test.topic"] } },
      );
      await c.connectProducer();

      expect(mockSeek).toHaveBeenCalledWith({
        topic: "test.topic",
        partition: 0,
        offset: "299",
      });
    });

    it("subscribes the recovery consumer to the configured topics", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "10" },
      ]);
      deliverMessage("9");

      const c = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { clockRecovery: { topics: ["test.topic"] } },
      );
      await c.connectProducer();

      expect(mockSubscribe).toHaveBeenCalledWith(
        expect.objectContaining({ topics: ["test.topic"] }),
      );
    });

    it("disconnects the recovery consumer after reading", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "10" },
      ]);
      deliverMessage("9");

      mockConsumer.disconnect.mockClear();
      const c = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { clockRecovery: { topics: ["test.topic"] } },
      );
      await c.connectProducer();

      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });

    it("takes the max clock across multiple partitions", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "100" },
        { partition: 1, low: "0", high: "200" },
      ]);

      // Deliver two messages in sequence — one per partition
      mockRun.mockImplementationOnce(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            offset: "99",
            value: Buffer.from("{}"),
            headers: { "x-lamport-clock": "99" },
          },
        });
        await eachMessage({
          topic: "test.topic",
          partition: 1,
          message: {
            offset: "199",
            value: Buffer.from("{}"),
            headers: { "x-lamport-clock": "750" },
          },
        });
      });

      const c = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { clockRecovery: { topics: ["test.topic"] } },
      );
      await c.connectProducer();

      await c.sendMessage("test.topic", { id: "1", value: 1 });
      const sent = mockSend.mock.calls[0][0];
      expect(sent.messages[0].headers["x-lamport-clock"]).toBe("751");
    });

    it("keeps clock at 0 when no x-lamport-clock header is present", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "5" },
      ]);
      mockRun.mockImplementationOnce(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            offset: "4",
            value: Buffer.from("{}"),
            headers: {},
          },
        });
      });

      const c = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { clockRecovery: { topics: ["test.topic"] } },
      );
      await c.connectProducer();

      await c.sendMessage("test.topic", { id: "1", value: 1 });
      const sent = mockSend.mock.calls[0][0];
      expect(sent.messages[0].headers["x-lamport-clock"]).toBe("1");
    });
  });
});
