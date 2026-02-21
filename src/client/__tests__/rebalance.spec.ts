jest.mock("@confluentinc/kafka-javascript");

import { TestTopicMap, KafkaClient } from "./helpers";

/** Build a minimal kafka-javascript mock and capture the config passed to consumer(). */
function buildKafkaMockCapturingConsumerConfig() {
  let capturedConfig: any;

  const { KafkaJS } = jest.requireMock("@confluentinc/kafka-javascript") as any;

  KafkaJS.Kafka.mockImplementationOnce(() => ({
    producer: jest.fn().mockReturnValue({
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue(undefined),
      transaction: jest.fn(),
    }),
    consumer: jest.fn().mockImplementation((config: any) => {
      capturedConfig = config;
      return {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockResolvedValue(undefined),
      };
    }),
    admin: jest.fn().mockReturnValue({
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      listTopics: jest.fn().mockResolvedValue([]),
      createTopics: jest.fn().mockResolvedValue(true),
    }),
  }));

  return { getConfig: () => capturedConfig };
}

describe("KafkaClient — onRebalance hook", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("when onRebalance is provided", () => {
    it("injects rebalance_cb into the consumer config", async () => {
      const { getConfig } = buildKafkaMockCapturingConsumerConfig();
      const onRebalance = jest.fn();

      const client = new KafkaClient<TestTopicMap>(
        "rb-client",
        "rb-group",
        ["localhost:9092"],
        { onRebalance },
      );

      await client.startConsumer(["test.topic"], jest.fn());

      expect(typeof getConfig().rebalance_cb).toBe("function");
    });

    it("calls onRebalance with type 'assign' on ERR__ASSIGN_PARTITIONS (-175)", async () => {
      const { getConfig } = buildKafkaMockCapturingConsumerConfig();
      const onRebalance = jest.fn();

      const client = new KafkaClient<TestTopicMap>(
        "rb-assign-client",
        "rb-assign-group",
        ["localhost:9092"],
        { onRebalance },
      );

      await client.startConsumer(["test.topic"], jest.fn());

      getConfig().rebalance_cb({ code: -175 }, [
        { topic: "test.topic", partition: 0 },
        { topic: "test.topic", partition: 1 },
      ]);

      expect(onRebalance).toHaveBeenCalledTimes(1);
      expect(onRebalance).toHaveBeenCalledWith("assign", [
        { topic: "test.topic", partition: 0 },
        { topic: "test.topic", partition: 1 },
      ]);
    });

    it("calls onRebalance with type 'revoke' on ERR__REVOKE_PARTITIONS (-174)", async () => {
      const { getConfig } = buildKafkaMockCapturingConsumerConfig();
      const onRebalance = jest.fn();

      const client = new KafkaClient<TestTopicMap>(
        "rb-revoke-client",
        "rb-revoke-group",
        ["localhost:9092"],
        { onRebalance },
      );

      await client.startConsumer(["test.topic"], jest.fn());

      getConfig().rebalance_cb({ code: -174 }, [
        { topic: "test.topic", partition: 0 },
      ]);

      expect(onRebalance).toHaveBeenCalledWith("revoke", [
        { topic: "test.topic", partition: 0 },
      ]);
    });

    it("maps assignment to { topic, partition } — strips unrelated fields", async () => {
      const { getConfig } = buildKafkaMockCapturingConsumerConfig();
      const onRebalance = jest.fn();

      const client = new KafkaClient<TestTopicMap>(
        "rb-strip-client",
        "rb-strip-group",
        ["localhost:9092"],
        { onRebalance },
      );

      await client.startConsumer(["test.topic"], jest.fn());

      getConfig().rebalance_cb({ code: -175 }, [
        { topic: "test.topic", partition: 2, leaderEpoch: 5, extraField: "x" },
      ]);

      expect(onRebalance).toHaveBeenCalledWith("assign", [
        { topic: "test.topic", partition: 2 },
      ]);
    });

    it("logs a warning (not throws) when onRebalance callback itself throws", async () => {
      const { getConfig } = buildKafkaMockCapturingConsumerConfig();
      const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

      const client = new KafkaClient<TestTopicMap>(
        "rb-err-client",
        "rb-err-group",
        ["localhost:9092"],
        {
          onRebalance: () => {
            throw new Error("rebalance handler boom");
          },
        },
      );

      await client.startConsumer(["test.topic"], jest.fn());

      expect(() => getConfig().rebalance_cb({ code: -175 }, [])).not.toThrow();

      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining("rebalance handler boom"),
      );

      warnSpy.mockRestore();
    });
  });

  describe("when onRebalance is not provided", () => {
    it("does not add rebalance_cb to the consumer config", async () => {
      const { getConfig } = buildKafkaMockCapturingConsumerConfig();

      const client = new KafkaClient<TestTopicMap>(
        "no-rb-client",
        "no-rb-group",
        ["localhost:9092"],
        // intentionally no onRebalance
      );

      await client.startConsumer(["test.topic"], jest.fn());

      expect(getConfig().rebalance_cb).toBeUndefined();
    });
  });
});
