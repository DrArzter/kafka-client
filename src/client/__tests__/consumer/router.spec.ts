import { TestTopicMap, createClient, mockRun, KafkaClient } from "../helpers";

/** Build a raw message with the given header and JSON payload. */
function makeMessage(
  headerValue: string | undefined,
  payload: Record<string, unknown> = { id: "1", value: 1 },
) {
  return {
    topic: "test.topic",
    partition: 0,
    message: {
      value: Buffer.from(JSON.stringify(payload)),
      headers: headerValue !== undefined ? { "x-event-type": headerValue } : {},
    },
  };
}

/** Wire mockRun to deliver a single message, then call startRoutedConsumer. */
async function runRouter(
  client: KafkaClient<TestTopicMap>,
  msg: ReturnType<typeof makeMessage>,
  routing: Parameters<KafkaClient<TestTopicMap>["startRoutedConsumer"]>[1],
) {
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    await eachMessage(msg);
  });
  return client.startRoutedConsumer(["test.topic"], routing);
}

describe("KafkaClient — startRoutedConsumer", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("dispatches to the matching route handler", async () => {
    const handlerA = jest.fn().mockResolvedValue(undefined);
    const handlerB = jest.fn().mockResolvedValue(undefined);

    await runRouter(client, makeMessage("order.created"), {
      header: "x-event-type",
      routes: { "order.created": handlerA, "order.cancelled": handlerB },
    });

    expect(handlerA).toHaveBeenCalledTimes(1);
    expect(handlerB).not.toHaveBeenCalled();
  });

  it("passes the full EventEnvelope to the matched handler", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    await runRouter(client, makeMessage("order.created"), {
      header: "x-event-type",
      routes: { "order.created": handler },
    });

    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic", payload: expect.any(Object) }),
    );
  });

  it("calls fallback when no route matches", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const fallback = jest.fn().mockResolvedValue(undefined);

    await runRouter(client, makeMessage("unknown.type"), {
      header: "x-event-type",
      routes: { "order.created": handler },
      fallback,
    });

    expect(handler).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });

  it("calls fallback when header is absent", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const fallback = jest.fn().mockResolvedValue(undefined);

    await runRouter(client, makeMessage(undefined), {
      header: "x-event-type",
      routes: { "order.created": handler },
      fallback,
    });

    expect(handler).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });

  it("silently skips message when no route matches and no fallback", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    await expect(
      runRouter(client, makeMessage("unknown.type"), {
        header: "x-event-type",
        routes: { "order.created": handler },
      }),
    ).resolves.not.toThrow();

    expect(handler).not.toHaveBeenCalled();
  });

  it("silently skips when header is absent and no fallback", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    await expect(
      runRouter(client, makeMessage(undefined), {
        header: "x-event-type",
        routes: { "order.created": handler },
      }),
    ).resolves.not.toThrow();

    expect(handler).not.toHaveBeenCalled();
  });

  it("supports multiple routes on the same topic", async () => {
    const handlerA = jest.fn().mockResolvedValue(undefined);
    const handlerB = jest.fn().mockResolvedValue(undefined);
    const handlerC = jest.fn().mockResolvedValue(undefined);

    // Deliver two messages sequentially
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMessage("type.a"));
      await eachMessage(makeMessage("type.b"));
    });

    await client.startRoutedConsumer(["test.topic"], {
      header: "x-event-type",
      routes: {
        "type.a": handlerA,
        "type.b": handlerB,
        "type.c": handlerC,
      },
    });

    expect(handlerA).toHaveBeenCalledTimes(1);
    expect(handlerB).toHaveBeenCalledTimes(1);
    expect(handlerC).not.toHaveBeenCalled();
  });

  it("returns a ConsumerHandle with stop()", async () => {
    mockRun.mockResolvedValue(undefined);

    const handle = await client.startRoutedConsumer(["test.topic"], {
      header: "x-event-type",
      routes: {},
    });

    expect(handle).toHaveProperty("groupId");
    expect(handle).toHaveProperty("stop");
    expect(typeof handle.stop).toBe("function");
  });

  it("forwards ConsumerOptions to the underlying startConsumer", async () => {
    mockRun.mockResolvedValue(undefined);

    // startConsumer with groupId override should register under that groupId
    const handle = await client.startRoutedConsumer(
      ["test.topic"],
      { header: "x-event-type", routes: {} },
      { groupId: "router-group" },
    );

    expect(handle.groupId).toBe("router-group");
  });
});
