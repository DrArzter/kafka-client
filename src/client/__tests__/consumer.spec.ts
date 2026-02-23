import {
  TestTopicMap,
  createClient,
  mockConsumer,
  mockSubscribe,
  mockRun,
  KafkaClient,
  topic,
} from "./helpers";

describe("KafkaClient — Consumer", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("startConsumer", () => {
    it("should subscribe and run consumer", async () => {
      const handler = jest.fn();
      await client.startConsumer(["test.topic"], handler);

      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ["test.topic"],
      });
      expect(mockRun).toHaveBeenCalledWith(
        expect.objectContaining({ eachMessage: expect.any(Function) }),
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
      });
      expect(mockRun).toHaveBeenCalledWith(
        expect.objectContaining({ eachMessage: expect.any(Function) }),
      );
    });

    it("should pass EventEnvelope to handler", async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 100 })),
            offset: "5",
          },
        });
      });

      await client.startConsumer(["test.topic"], handler);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({
          payload: { id: "1", value: 100 },
          topic: "test.topic",
          partition: 0,
          offset: "5",
          eventId: expect.any(String),
          correlationId: expect.any(String),
          timestamp: expect.any(String),
          schemaVersion: 1,
          headers: expect.any(Object),
        }),
      );
    });

    it("should use last value when a header has multiple values (last-wins)", async () => {
      const handler = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            // Kafka allows multiple headers with the same key — simulate via array.
            // Old behavior: joined to "first-corr,last-corr".
            // New behavior: last-wins → "last-corr".
            headers: { "x-correlation-id": ["first-corr", "last-corr"] },
          },
        });
      });

      await client.startConsumer(["test.topic"], handler);

      const envelope = handler.mock.calls[0][0];
      expect(envelope.correlationId).toBe("last-corr");
    });

    it("should skip empty messages", async () => {
      const handler = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
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
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: { value: Buffer.from("not json") },
        });
      });

      await client.startConsumer(["test.topic"], handler);
      expect(handler).not.toHaveBeenCalled();
    });

    it("should startConsumer with TopicDescriptor array", async () => {
      const TestTopic = topic("test.topic").type<{ id: string; value: number }>();
      const handler = jest.fn();
      await client.startConsumer([TestTopic] as any, handler);

      expect(mockSubscribe).toHaveBeenCalledWith({
        topics: ["test.topic"],
      });
    });
  });

  describe("stopConsumer", () => {
    it("should disconnect all consumers", async () => {
      await client.startConsumer(["test.topic"], jest.fn());
      mockConsumer.disconnect.mockClear();

      await client.stopConsumer();
      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });

    it("should handle empty consumer map gracefully", async () => {
      await client.stopConsumer();
      // No consumers to disconnect — should not throw
    });
  });
});
