import {
  TestTopicMap,
  createClient,
  mockConsumer,
  mockConsumerPause,
  mockConsumerResume,
  mockDisconnect,
  mockSubscribe,
  mockRun,
  mockListTopics,
  mockSetOffsets,
  mockFetchTopicOffsets,
  KafkaClient,
} from "./helpers";

describe("KafkaClient — Consumer Groups", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
    mockRun.mockReset().mockResolvedValue(undefined);
  });

  describe("multiple consumer groups", () => {
    it("should create separate consumers for different groupIds", async () => {
      const handler = jest.fn();

      await client.startConsumer(["test.topic"], handler, {
        groupId: "group-a",
      });
      await client.startConsumer(["test.other"], handler, {
        groupId: "group-b",
      });

      // The mock kafka.consumer() should be called twice (for two different groups)
      const kafkaInstance = (require("@confluentinc/kafka-javascript") as any)
        .KafkaJS.Kafka.mock.results[0].value;
      expect(kafkaInstance.consumer).toHaveBeenCalledTimes(2);
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: { groupId: "group-a", fromBeginning: false, autoCommit: true },
      });
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: { groupId: "group-b", fromBeginning: false, autoCommit: true },
      });
    });

    it("should throw when startConsumer is called twice with the same groupId", async () => {
      await client.startConsumer(["test.topic"], jest.fn(), {
        groupId: "group-a",
      });

      await expect(
        client.startConsumer(["test.other"], jest.fn(), { groupId: "group-a" }),
      ).rejects.toThrow('startConsumer("group-a") called twice');
    });

    it("should use default groupId when none specified", async () => {
      const handler = jest.fn();

      await client.startConsumer(["test.topic"], handler);

      const kafkaInstance = (require("@confluentinc/kafka-javascript") as any)
        .KafkaJS.Kafka.mock.results[0].value;
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: {
          groupId: "test-group",
          fromBeginning: false,
          autoCommit: true,
        },
      });
    });

    it("should disconnect all consumers on disconnect", async () => {
      const handler = jest.fn();

      await client.startConsumer(["test.topic"], handler, {
        groupId: "group-a",
      });
      await client.startConsumer(["test.other"], handler, {
        groupId: "group-b",
      });

      mockConsumer.disconnect.mockClear();
      await client.disconnect();

      // Both consumers disconnected (mock returns same object, so called twice)
      expect(mockConsumer.disconnect).toHaveBeenCalledTimes(2);
    });

    it("should work with startBatchConsumer and groupId", async () => {
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startBatchConsumer(["test.topic"], handler, {
        groupId: "batch-group",
      });

      const kafkaInstance = (require("@confluentinc/kafka-javascript") as any)
        .KafkaJS.Kafka.mock.results[0].value;
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: {
          groupId: "batch-group",
          fromBeginning: false,
          autoCommit: true,
        },
      });
    });
  });

  describe("mixed consumer mode detection", () => {
    it("should throw when eachBatch is used after eachMessage on same groupId", async () => {
      await client.startConsumer(["test.topic"], jest.fn(), {
        groupId: "shared-group",
      });

      await expect(
        client.startBatchConsumer(["test.other"], jest.fn(), {
          groupId: "shared-group",
        }),
      ).rejects.toThrow(
        'Cannot use eachBatch on consumer group "shared-group"',
      );
    });

    it("should throw when startBatchConsumer is called twice with the same groupId", async () => {
      await client.startBatchConsumer(["test.topic"], jest.fn(), {
        groupId: "shared-group",
      });

      await expect(
        client.startBatchConsumer(["test.other"], jest.fn(), {
          groupId: "shared-group",
        }),
      ).rejects.toThrow('startBatchConsumer("shared-group") called twice');
    });

    it("should throw when eachMessage is used after eachBatch on same groupId", async () => {
      await client.startBatchConsumer(["test.topic"], jest.fn(), {
        groupId: "shared-group",
      });

      await expect(
        client.startConsumer(["test.other"], jest.fn(), {
          groupId: "shared-group",
        }),
      ).rejects.toThrow(
        'Cannot use eachMessage on consumer group "shared-group"',
      );
    });

    it("should allow different modes on different groupIds", async () => {
      await client.startConsumer(["test.topic"], jest.fn(), {
        groupId: "group-a",
      });

      await expect(
        client.startBatchConsumer(["test.other"], jest.fn(), {
          groupId: "group-b",
        }),
      ).resolves.not.toThrow();
    });

    it("should reset mode tracking on stopConsumer", async () => {
      await client.startConsumer(["test.topic"], jest.fn(), {
        groupId: "shared-group",
      });
      await client.stopConsumer();

      // After stop, should allow batch on the same group
      await expect(
        client.startBatchConsumer(["test.topic"], jest.fn(), {
          groupId: "shared-group",
        }),
      ).resolves.not.toThrow();
    });
  });

  describe("subscribeWithRetry", () => {
    it("should retry subscribe when it fails", async () => {
      mockSubscribe
        .mockRejectedValueOnce(new Error("UNKNOWN_TOPIC_OR_PARTITION"))
        .mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], jest.fn(), {
        subscribeRetry: { retries: 3, backoffMs: 1 },
      });

      expect(mockSubscribe).toHaveBeenCalledTimes(2);
    });

    it("should throw after all retry attempts exhausted", async () => {
      mockSubscribe.mockRejectedValue(new Error("UNKNOWN_TOPIC_OR_PARTITION"));

      await expect(
        client.startConsumer(["test.topic"], jest.fn(), {
          subscribeRetry: { retries: 2, backoffMs: 1 },
        }),
      ).rejects.toThrow("UNKNOWN_TOPIC_OR_PARTITION");

      expect(mockSubscribe).toHaveBeenCalledTimes(2);
    });

    it("should work with startBatchConsumer too", async () => {
      mockSubscribe
        .mockRejectedValueOnce(new Error("UNKNOWN_TOPIC_OR_PARTITION"))
        .mockResolvedValue(undefined);

      await client.startBatchConsumer(["test.topic"], jest.fn(), {
        subscribeRetry: { retries: 3, backoffMs: 1 },
      });

      expect(mockSubscribe).toHaveBeenCalledTimes(2);
    });
  });

  describe("retryTxProducer lifecycle", () => {
    it("should disconnect retryTxProducers when stopConsumer(groupId) is called", async () => {
      mockListTopics.mockResolvedValueOnce([
        "test.topic",
        "test.topic.retry.1",
        "test.topic.retry.2",
      ]);
      const handle = await client.startConsumer(["test.topic"], jest.fn(), {
        retry: { maxRetries: 2, backoffMs: 1 },
        retryTopics: true,
        retryTopicAssignmentTimeoutMs: 0,
      });

      mockDisconnect.mockClear();
      await handle.stop();

      // One disconnect per retry level tx producer (retry.1 and retry.2) + main-tx EOS producer
      expect(mockDisconnect).toHaveBeenCalledTimes(3);
    });

    it("should disconnect retryTxProducers when stopConsumer() (all) is called", async () => {
      mockListTopics.mockResolvedValueOnce([
        "test.topic",
        "test.topic.retry.1",
      ]);
      await client.startConsumer(["test.topic"], jest.fn(), {
        retry: { maxRetries: 1, backoffMs: 1 },
        retryTopics: true,
        retryTopicAssignmentTimeoutMs: 0,
      });

      mockDisconnect.mockClear();
      await client.stopConsumer(); // no groupId — stop all

      // retry.1 tx producer + main-tx EOS producer
      expect(mockDisconnect).toHaveBeenCalledTimes(2);
    });
  });

  describe("pauseConsumer / resumeConsumer", () => {
    it("pauses the consumer for the specified topic-partitions", async () => {
      await client.startConsumer(["test.topic"], jest.fn());

      client.pauseConsumer(undefined, [
        { topic: "test.topic", partitions: [0, 1] },
      ]);

      expect(mockConsumerPause).toHaveBeenCalledWith([
        { topic: "test.topic", partitions: [0] },
        { topic: "test.topic", partitions: [1] },
      ]);
    });

    it("resumes the consumer for the specified topic-partitions", async () => {
      await client.startConsumer(["test.topic"], jest.fn());

      client.resumeConsumer(undefined, [
        { topic: "test.topic", partitions: [0] },
      ]);

      expect(mockConsumerResume).toHaveBeenCalledWith([
        { topic: "test.topic", partitions: [0] },
      ]);
    });

    it("logs a warning when no consumer exists for the group", () => {
      const logger = {
        log: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      };
      const c = new KafkaClient<TestTopicMap>("t", "g", ["localhost:9092"], {
        logger,
      });

      c.pauseConsumer(undefined, [{ topic: "test.topic", partitions: [0] }]);

      expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining("g"));
    });
  });

  describe("resetOffsets", () => {
    it("seeks to earliest by passing the low watermark offset", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "5", high: "100" },
        { partition: 1, low: "0", high: "50" },
      ]);

      await client.resetOffsets(undefined, "test.topic", "earliest");

      expect(mockSetOffsets).toHaveBeenCalledWith({
        groupId: "test-group",
        topic: "test.topic",
        partitions: [
          { partition: 0, offset: "5" },
          { partition: 1, offset: "0" },
        ],
      });
    });

    it("seeks to latest by passing the high watermark offset", async () => {
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, low: "0", high: "200" },
      ]);

      await client.resetOffsets(undefined, "test.topic", "latest");

      expect(mockSetOffsets).toHaveBeenCalledWith({
        groupId: "test-group",
        topic: "test.topic",
        partitions: [{ partition: 0, offset: "200" }],
      });
    });

    it("throws when the consumer group is still running", async () => {
      await client.startConsumer(["test.topic"], jest.fn());

      await expect(
        client.resetOffsets(undefined, "test.topic", "earliest"),
      ).rejects.toThrow("still running");
    });
  });

  describe("seekToOffset", () => {
    it("seeks multiple partitions on the same topic in one setOffsets call", async () => {
      await client.seekToOffset(undefined, [
        { topic: "test.topic", partition: 0, offset: "10" },
        { topic: "test.topic", partition: 1, offset: "20" },
      ]);

      expect(mockSetOffsets).toHaveBeenCalledTimes(1);
      expect(mockSetOffsets).toHaveBeenCalledWith({
        groupId: "test-group",
        topic: "test.topic",
        partitions: [
          { partition: 0, offset: "10" },
          { partition: 1, offset: "20" },
        ],
      });
    });

    it("groups by topic — separate setOffsets calls per topic", async () => {
      await client.seekToOffset(undefined, [
        { topic: "test.topic", partition: 0, offset: "5" },
        { topic: "test.other", partition: 0, offset: "3" },
      ]);

      expect(mockSetOffsets).toHaveBeenCalledTimes(2);
      expect(mockSetOffsets).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic" }),
      );
      expect(mockSetOffsets).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.other" }),
      );
    });

    it("uses an explicit groupId when provided", async () => {
      await client.seekToOffset("custom-group", [
        { topic: "test.topic", partition: 0, offset: "99" },
      ]);

      expect(mockSetOffsets).toHaveBeenCalledWith({
        groupId: "custom-group",
        topic: "test.topic",
        partitions: [{ partition: 0, offset: "99" }],
      });
    });

    it("throws when the consumer group is still running", async () => {
      await client.startConsumer(["test.topic"], jest.fn());

      await expect(
        client.seekToOffset(undefined, [
          { topic: "test.topic", partition: 0, offset: "0" },
        ]),
      ).rejects.toThrow("still running");
    });
  });
});
