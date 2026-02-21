jest.mock("@confluentinc/kafka-javascript");

import { TestTopicMap, createClient, mockRun, KafkaClient } from "./helpers";

/** Deliver a single valid message through the mocked eachMessage runner. */
function deliverMessage(handlerPromise: Promise<void>) {
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    eachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
      },
    });
    // Return without awaiting — let the test control resolution
    return undefined;
  });
  return handlerPromise;
}

describe("KafkaClient — handlerTimeoutMs", () => {
  let client: KafkaClient<TestTopicMap>;
  let warnSpy: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
    client = createClient();
    warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.useRealTimers();
    warnSpy.mockRestore();
  });

  describe("when handler exceeds the timeout", () => {
    it("logs a warning containing the topic name", async () => {
      let resolveHandler!: () => void;
      const slow = new Promise<void>((res) => {
        resolveHandler = res;
      });

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], () => slow, {
        handlerTimeoutMs: 500,
      });

      jest.advanceTimersByTime(501);

      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining("test.topic"),
      );

      resolveHandler();
    });

    it("includes the timeout duration in the warning", async () => {
      let resolveHandler!: () => void;
      const slow = new Promise<void>((res) => {
        resolveHandler = res;
      });

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], () => slow, {
        handlerTimeoutMs: 1200,
      });

      jest.advanceTimersByTime(1201);

      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("1200ms"));

      resolveHandler();
    });

    it("does not reject the handler — timeout is diagnostic only", async () => {
      let resolveHandler!: () => void;
      const slow = new Promise<void>((res) => {
        resolveHandler = res;
      });

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        // Await so that we can observe the promise settling
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      const startPromise = client.startConsumer(["test.topic"], () => slow, {
        handlerTimeoutMs: 100,
      });

      jest.advanceTimersByTime(200);
      resolveHandler();

      // Should not throw
      await expect(startPromise).resolves.toBeDefined();
    });
  });

  describe("when handler resolves before the timeout", () => {
    it("does not log a stuck-handler warning", async () => {
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(
        ["test.topic"],
        jest.fn().mockResolvedValue(undefined),
        { handlerTimeoutMs: 1000 },
      );

      // The handler resolved synchronously — advance well past the threshold
      jest.advanceTimersByTime(2000);

      const stuckWarnings = warnSpy.mock.calls.filter(
        ([msg]) => typeof msg === "string" && msg.includes("stuck handler"),
      );
      expect(stuckWarnings).toHaveLength(0);
    });
  });

  describe("when handlerTimeoutMs is not set", () => {
    it("does not interfere with handler execution", async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          },
        });
      });

      await client.startConsumer(["test.topic"], handler);

      expect(handler).toHaveBeenCalled();
    });
  });
});
