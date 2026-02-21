jest.mock("@confluentinc/kafka-javascript");

import {
  TestTopicMap,
  createClient,
  mockConsumer,
  KafkaClient,
} from "./helpers";

describe("KafkaClient — ConsumerHandle", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("startConsumer return value", () => {
    it("returns groupId equal to the default group", async () => {
      const handle = await client.startConsumer(["test.topic"], jest.fn());

      expect(handle.groupId).toBe("test-group");
    });

    it("returns groupId matching an explicit per-consumer groupId", async () => {
      const handle = await client.startConsumer(["test.topic"], jest.fn(), {
        groupId: "custom-group",
      });

      expect(handle.groupId).toBe("custom-group");
    });

    it("returns a stop() function", async () => {
      const handle = await client.startConsumer(["test.topic"], jest.fn());

      expect(typeof handle.stop).toBe("function");
    });

    it("stop() disconnects the underlying consumer", async () => {
      const handle = await client.startConsumer(["test.topic"], jest.fn());
      mockConsumer.disconnect.mockClear();

      await handle.stop();

      expect(mockConsumer.disconnect).toHaveBeenCalledTimes(1);
    });

    it("stop() is equivalent to calling stopConsumer(groupId)", async () => {
      const handle = await client.startConsumer(["test.topic"], jest.fn(), {
        groupId: "stop-equiv-group",
      });
      mockConsumer.disconnect.mockClear();

      // stop() via handle
      await handle.stop();
      expect(mockConsumer.disconnect).toHaveBeenCalledTimes(1);

      // The consumer is now gone — stopConsumer should warn (not throw)
      const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
      await client.stopConsumer("stop-equiv-group");
      warnSpy.mockRestore();

      // No second disconnect attempt
      expect(mockConsumer.disconnect).toHaveBeenCalledTimes(1);
    });
  });

  describe("startBatchConsumer return value", () => {
    it("returns groupId and stop() function", async () => {
      const handle = await client.startBatchConsumer(
        ["test.topic"],
        jest.fn().mockResolvedValue(undefined),
      );

      expect(handle.groupId).toBe("test-group");
      expect(typeof handle.stop).toBe("function");
    });

    it("stop() disconnects the batch consumer", async () => {
      const handle = await client.startBatchConsumer(
        ["test.topic"],
        jest.fn().mockResolvedValue(undefined),
      );
      mockConsumer.disconnect.mockClear();

      await handle.stop();

      expect(mockConsumer.disconnect).toHaveBeenCalledTimes(1);
    });
  });
});
