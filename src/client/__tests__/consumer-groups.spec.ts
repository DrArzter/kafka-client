import {
  TestTopicMap,
  createClient,
  mockConsumer,
  mockSubscribe,
  mockRun,
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
      const kafkaInstance = (require("@confluentinc/kafka-javascript") as any).KafkaJS.Kafka.mock.results[0]
        .value;
      expect(kafkaInstance.consumer).toHaveBeenCalledTimes(2);
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: { groupId: "group-a", fromBeginning: false, autoCommit: true },
      });
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: { groupId: "group-b", fromBeginning: false, autoCommit: true },
      });
    });

    it("should reuse consumer for same groupId", async () => {
      const handler = jest.fn();

      await client.startConsumer(["test.topic"], handler, {
        groupId: "group-a",
      });
      await client.startConsumer(["test.other"], handler, {
        groupId: "group-a",
      });

      const kafkaInstance = (require("@confluentinc/kafka-javascript") as any).KafkaJS.Kafka.mock.results[0]
        .value;
      // Should only create one consumer for group-a
      expect(kafkaInstance.consumer).toHaveBeenCalledTimes(1);
    });

    it("should use default groupId when none specified", async () => {
      const handler = jest.fn();

      await client.startConsumer(["test.topic"], handler);

      const kafkaInstance = (require("@confluentinc/kafka-javascript") as any).KafkaJS.Kafka.mock.results[0]
        .value;
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: { groupId: "test-group", fromBeginning: false, autoCommit: true },
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

      const kafkaInstance = (require("@confluentinc/kafka-javascript") as any).KafkaJS.Kafka.mock.results[0]
        .value;
      expect(kafkaInstance.consumer).toHaveBeenCalledWith({
        kafkaJS: { groupId: "batch-group", fromBeginning: false, autoCommit: true },
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
      mockSubscribe.mockRejectedValue(
        new Error("UNKNOWN_TOPIC_OR_PARTITION"),
      );

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
});
