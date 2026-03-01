import {
  TestTopicMap,
  createClient,
  KafkaClient,
  mockRun,
  mockSend,
} from "./helpers";

function makeMsg(ageMs: number, payload = { id: "1", value: 1 }) {
  const ts = new Date(Date.now() - ageMs).toISOString();
  return {
    value: Buffer.from(JSON.stringify(payload)),
    offset: "0",
    headers: { "x-timestamp": ts },
  };
}

describe("KafkaClient — Message TTL", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
    mockRun.mockReset().mockResolvedValue(undefined);
  });

  describe("eachMessage consumer", () => {
    it("delivers a fresh message to the handler", async () => {
      const handler = jest.fn().mockResolvedValue(undefined);

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: makeMsg(100), // 100ms old — well within TTL
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        messageTtlMs: 5_000,
      });

      expect(handler).toHaveBeenCalledTimes(1);
    });

    it("drops an expired message (no dlq) and calls onTtlExpired", async () => {
      const handler = jest.fn();
      const onTtlExpired = jest.fn().mockResolvedValue(undefined);

      client = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { onTtlExpired },
      );

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: makeMsg(10_000), // 10s old — exceeds TTL
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        messageTtlMs: 1_000,
      });

      expect(handler).not.toHaveBeenCalled();
      expect(onTtlExpired).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: "test.topic",
          messageTtlMs: 1_000,
        }),
      );
    });

    it("expired + dlq:false → onTtlExpired called, onMessageLost NOT called", async () => {
      const onTtlExpired = jest.fn().mockResolvedValue(undefined);
      const onMessageLost = jest.fn();

      client = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { onTtlExpired, onMessageLost },
      );

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: makeMsg(10_000),
        });
      });

      await client.startConsumer(["test.topic"], jest.fn(), {
        messageTtlMs: 1_000,
      });

      expect(onTtlExpired).toHaveBeenCalledTimes(1);
      expect(onMessageLost).not.toHaveBeenCalled();
    });

    it("expired + dlq:true → DLQ sent, onTtlExpired NOT called", async () => {
      const onTtlExpired = jest.fn();

      client = new KafkaClient<TestTopicMap>(
        "test-client",
        "test-group",
        ["localhost:9092"],
        { onTtlExpired },
      );

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: makeMsg(10_000),
        });
      });

      await client.startConsumer(["test.topic"], jest.fn(), {
        messageTtlMs: 1_000,
        dlq: true,
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
      expect(onTtlExpired).not.toHaveBeenCalled();
    });

    it("routes an expired message to DLQ when dlq:true", async () => {
      const handler = jest.fn();

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: makeMsg(10_000),
        });
      });

      await client.startConsumer(["test.topic"], handler, {
        messageTtlMs: 1_000,
        dlq: true,
      });

      expect(handler).not.toHaveBeenCalled();
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
    });
  });

  describe("eachBatch consumer", () => {
    it("excludes expired messages from batch, passes only fresh ones", async () => {
      const handler = jest.fn().mockResolvedValue(undefined);

      mockRun.mockImplementation(async ({ eachBatch }: any) => {
        await eachBatch({
          batch: {
            topic: "test.topic",
            partition: 0,
            highWatermark: "2",
            messages: [
              makeMsg(10_000, { id: "old", value: 0 }), // expired
              makeMsg(100, { id: "fresh", value: 1 }), // fresh
            ],
          },
          heartbeat: jest.fn(),
          resolveOffset: jest.fn(),
          commitOffsetsIfNecessary: jest.fn(),
        });
      });

      await client.startBatchConsumer(["test.topic"], handler, {
        messageTtlMs: 1_000,
      });

      expect(handler).toHaveBeenCalledTimes(1);
      const [envelopes] = handler.mock.calls[0];
      expect(envelopes).toHaveLength(1);
      expect(envelopes[0].payload).toMatchObject({ id: "fresh" });
    });

    it("does not call handler when all batch messages are expired", async () => {
      const handler = jest.fn();

      mockRun.mockImplementation(async ({ eachBatch }: any) => {
        await eachBatch({
          batch: {
            topic: "test.topic",
            partition: 0,
            highWatermark: "1",
            messages: [makeMsg(10_000)],
          },
          heartbeat: jest.fn(),
          resolveOffset: jest.fn(),
          commitOffsetsIfNecessary: jest.fn(),
        });
      });

      await client.startBatchConsumer(["test.topic"], handler, {
        messageTtlMs: 1_000,
      });

      expect(handler).not.toHaveBeenCalled();
    });

    it("routes expired batch messages to DLQ individually when dlq:true", async () => {
      const handler = jest.fn().mockResolvedValue(undefined);

      mockRun.mockImplementation(async ({ eachBatch }: any) => {
        await eachBatch({
          batch: {
            topic: "test.topic",
            partition: 0,
            highWatermark: "2",
            messages: [
              makeMsg(10_000, { id: "old", value: 0 }),
              makeMsg(100, { id: "fresh", value: 1 }),
            ],
          },
          heartbeat: jest.fn(),
          resolveOffset: jest.fn(),
          commitOffsetsIfNecessary: jest.fn(),
        });
      });

      await client.startBatchConsumer(["test.topic"], handler, {
        messageTtlMs: 1_000,
        dlq: true,
      });

      // One DLQ send for the expired message
      expect(mockSend).toHaveBeenCalledTimes(1);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
      // Handler still called with the fresh message
      expect(handler).toHaveBeenCalledTimes(1);
    });
  });
});
