import {
  TestTopicMap,
  createClient,
  mockAdmin,
  mockConsumer,
  mockProducer,
  mockCreateTopics,
  mockSend,
  KafkaClient,
} from "./helpers";

describe("KafkaClient — Admin & Lifecycle", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("checkStatus", () => {
    it("should return topics list", async () => {
      const result = await client.checkStatus();
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(result).toEqual({
        status: "up",
        clientId: "test-client",
        topics: ["topic1", "topic2"],
      });
    });

    it("should not reconnect admin on subsequent calls", async () => {
      await client.checkStatus();
      mockAdmin.connect.mockClear();
      await client.checkStatus();
      expect(mockAdmin.connect).not.toHaveBeenCalled();
    });
  });

  describe("disconnect", () => {
    it("should disconnect producer and all consumers", async () => {
      await client.startConsumer(["test.topic"], jest.fn());
      mockConsumer.disconnect.mockClear();

      await client.disconnect();
      expect(mockProducer.disconnect).toHaveBeenCalled();
      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });

    it("should disconnect admin if it was connected", async () => {
      await client.checkStatus();
      await client.disconnect();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    it("should not disconnect admin if it was never connected", async () => {
      await client.disconnect();
      expect(mockAdmin.disconnect).not.toHaveBeenCalled();
    });

    it("should handle partial disconnect failure gracefully", async () => {
      await client.startConsumer(["test.topic"], jest.fn());
      mockConsumer.disconnect.mockClear();
      mockProducer.disconnect.mockRejectedValueOnce(
        new Error("producer failed"),
      );

      // Promise.allSettled means no throw
      await client.disconnect();

      expect(mockConsumer.disconnect).toHaveBeenCalled();
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

    it("should create topics on startConsumer (librdkafka errors on unknown topics)", async () => {
      await autoClient.startConsumer(["test.topic"], jest.fn());

      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic", numPartitions: 1 }],
      });
    });

    it("should create DLQ topics on startConsumer when dlq is enabled", async () => {
      await autoClient.startConsumer(["test.topic"], jest.fn(), { dlq: true });

      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic", numPartitions: 1 }],
      });
      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic.dlq", numPartitions: 1 }],
      });
    });

    it("should reuse admin connection across ensureTopic calls", async () => {
      await autoClient.sendMessage("test.topic", { id: "1", value: 1 });
      await autoClient.sendMessage("test.other", { name: "hi" });

      // admin.connect should only be called once
      expect(mockAdmin.connect).toHaveBeenCalledTimes(1);
    });

    it("should use custom numPartitions when creating topics", async () => {
      const customClient = new KafkaClient<TestTopicMap>(
        "custom-client",
        "custom-group",
        ["localhost:9092"],
        { autoCreateTopics: true, numPartitions: 3 },
      );

      await customClient.sendMessage("test.topic", { id: "1", value: 1 });

      expect(mockCreateTopics).toHaveBeenCalledWith({
        topics: [{ topic: "test.topic", numPartitions: 3 }],
      });
    });
  });
});
