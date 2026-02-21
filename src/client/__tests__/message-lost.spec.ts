import {
  TestTopicMap,
  createClient,
  setupMessage,
  mockRun,
  KafkaClient,
  MessageLostContext,
  topic,
  SchemaLike,
} from "./helpers";

describe("KafkaClient — onMessageLost hook", () => {
  let lostMessages: MessageLostContext[];
  let onMessageLost: jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
    lostMessages = [];
    onMessageLost = jest.fn((ctx: MessageLostContext) => {
      lostMessages.push(ctx);
    });
  });

  function createClientWithHook() {
    return new KafkaClient<TestTopicMap>(
      "test-client",
      "test-group",
      ["localhost:9092"],
      { onMessageLost },
    );
  }

  it("should call onMessageLost when handler fails without DLQ", async () => {
    setupMessage();
    const client = createClientWithHook();
    const handler = jest.fn().mockRejectedValue(new Error("processing error"));

    await client.startConsumer(["test.topic"], handler);

    expect(onMessageLost).toHaveBeenCalledTimes(1);
    expect(onMessageLost).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "test.topic",
        attempt: 1,
        error: expect.objectContaining({ message: "processing error" }),
        headers: expect.any(Object),
      }),
    );
  });

  it("should call onMessageLost with the final attempt count after retries exhausted", async () => {
    const client = createClientWithHook();
    const handler = jest.fn().mockRejectedValue(new Error("always fails"));

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: { value: Buffer.from(JSON.stringify({ id: "1", value: 1 })) },
      });
    });

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 2, backoffMs: 1 },
    });

    expect(onMessageLost).toHaveBeenCalledTimes(1);
    expect(lostMessages[0].attempt).toBe(3); // 1 original + 2 retries
    expect(lostMessages[0].topic).toBe("test.topic");
    expect(lostMessages[0].error.message).toBe("always fails");
  });

  it("should NOT call onMessageLost when DLQ is enabled", async () => {
    setupMessage();
    const client = createClientWithHook();
    const handler = jest.fn().mockRejectedValue(new Error("fail"));

    await client.startConsumer(["test.topic"], handler, { dlq: true });

    expect(onMessageLost).not.toHaveBeenCalled();
  });

  it("should call onMessageLost with attempt=0 on validation failure without DLQ", async () => {
    const failingSchema: SchemaLike<TestTopicMap["test.topic"]> = {
      parse() {
        throw new Error("schema rejected");
      },
    };
    const TopicWithSchema = topic("test.topic").schema(failingSchema);

    setupMessage();
    const client = createClientWithHook();
    const handler = jest.fn();

    await client.startConsumer([TopicWithSchema], handler);

    expect(handler).not.toHaveBeenCalled();
    expect(onMessageLost).toHaveBeenCalledTimes(1);
    expect(lostMessages[0].attempt).toBe(0);
    expect(lostMessages[0].topic).toBe("test.topic");
  });

  it("should NOT call onMessageLost on validation failure when DLQ is enabled", async () => {
    const failingSchema: SchemaLike<TestTopicMap["test.topic"]> = {
      parse() {
        throw new Error("schema rejected");
      },
    };
    const TopicWithSchema = topic("test.topic").schema(failingSchema);

    setupMessage();
    const client = createClientWithHook();
    const handler = jest.fn();

    await client.startConsumer([TopicWithSchema], handler, { dlq: true });

    expect(onMessageLost).not.toHaveBeenCalled();
  });

  it("should include original message headers in context", async () => {
    const client = createClientWithHook();
    const handler = jest.fn().mockRejectedValue(new Error("fail"));

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          headers: { "x-correlation-id": "test-corr-123" },
        },
      });
    });

    await client.startConsumer(["test.topic"], handler);

    expect(lostMessages[0].headers["x-correlation-id"]).toBe("test-corr-123");
  });

  it("should not call onMessageLost when handler succeeds", async () => {
    setupMessage();
    const client = createClientWithHook();
    const handler = jest.fn().mockResolvedValue(undefined);

    await client.startConsumer(["test.topic"], handler);

    expect(onMessageLost).not.toHaveBeenCalled();
  });
});
