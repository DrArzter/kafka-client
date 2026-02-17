import {
  TestTopicMap,
  createClient,
  mockSend,
  mockConnect,
  mockProducer,
  KafkaClient,
  topic,
} from "./helpers";

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
        expect.objectContaining({ "x-trace": "t1", "x-event-id": expect.any(String) }),
      );
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
});
