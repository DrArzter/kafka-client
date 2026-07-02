import { createMockKafkaClient, MockKafkaClient } from "../client.mock";

interface TestTopics {
  "orders.created": { orderId: string; amount: number };
  "orders.completed": { orderId: string };
}

describe("createMockKafkaClient", () => {
  let mock: MockKafkaClient<TestTopics>;

  beforeEach(() => {
    mock = createMockKafkaClient<TestTopics>();
  });

  it("returns a mock with all IKafkaClient methods", () => {
    expect(mock.checkStatus).toBeDefined();
    expect(mock.getClientId).toBeDefined();
    expect(mock.sendMessage).toBeDefined();
    expect(mock.sendBatch).toBeDefined();
    expect(mock.transaction).toBeDefined();
    expect(mock.startConsumer).toBeDefined();
    expect(mock.startBatchConsumer).toBeDefined();
    expect(mock.stopConsumer).toBeDefined();
    expect(mock.disconnect).toBeDefined();
  });

  it("checkStatus resolves to { status: 'up', clientId: 'mock-client', topics: [] }", async () => {
    await expect(mock.checkStatus()).resolves.toEqual({
      status: "up",
      clientId: "mock-client",
      topics: [],
    });
  });

  it("getClientId returns 'mock-client'", () => {
    expect(mock.getClientId()).toBe("mock-client");
  });

  it("sendMessage resolves without error", async () => {
    await expect(
      mock.sendMessage("orders.created", { orderId: "1", amount: 100 }),
    ).resolves.toBeUndefined();

    expect(mock.sendMessage).toHaveBeenCalledWith("orders.created", {
      orderId: "1",
      amount: 100,
    });
  });

  it("sendBatch resolves without error", async () => {
    await expect(
      mock.sendBatch("orders.created", [
        { value: { orderId: "1", amount: 50 } },
      ]),
    ).resolves.toBeUndefined();
  });

  it("transaction executes the callback with a mock context", async () => {
    const spy = jest.fn();
    await mock.transaction(async (ctx) => {
      spy();
      await ctx.send("orders.created", { orderId: "1", amount: 10 });
    });

    expect(spy).toHaveBeenCalledTimes(1);
    expect(mock.transaction).toHaveBeenCalledTimes(1);
  });

  it("startConsumer resolves with a ConsumerHandle", async () => {
    const handler = jest.fn();
    const handle = await mock.startConsumer(["orders.created"], handler);
    expect(handle.groupId).toBe("mock-group");
    expect(typeof handle.stop).toBe("function");
  });

  it("startBatchConsumer resolves with a ConsumerHandle", async () => {
    const handler = jest.fn();
    const handle = await mock.startBatchConsumer(["orders.created"], handler);
    expect(handle.groupId).toBe("mock-group");
    expect(typeof handle.stop).toBe("function");
  });

  it("stopConsumer resolves", async () => {
    await expect(mock.stopConsumer()).resolves.toBeUndefined();
  });

  it("disconnect resolves", async () => {
    await expect(mock.disconnect()).resolves.toBeUndefined();
  });

  it("allows overriding return values", async () => {
    mock.checkStatus.mockResolvedValueOnce({ topics: ["orders.created"] });
    await expect(mock.checkStatus()).resolves.toEqual({
      topics: ["orders.created"],
    });
  });

  it("allows mocking rejections", async () => {
    mock.sendMessage.mockRejectedValueOnce(new Error("boom"));
    await expect(
      mock.sendMessage("orders.created", { orderId: "1", amount: 1 }),
    ).rejects.toThrow("boom");
  });

  it("tracks call counts independently", async () => {
    await mock.sendMessage("orders.created", { orderId: "1", amount: 1 });
    await mock.sendMessage("orders.created", { orderId: "2", amount: 2 });
    await mock.sendBatch("orders.completed", [{ value: { orderId: "3" } }]);

    expect(mock.sendMessage).toHaveBeenCalledTimes(2);
    expect(mock.sendBatch).toHaveBeenCalledTimes(1);
  });

  it("enableGracefulShutdown is defined and callable without throwing", () => {
    expect(mock.enableGracefulShutdown).toBeDefined();
    expect(() => mock.enableGracefulShutdown()).not.toThrow();
  });

  describe("extended surface", () => {
    it("sendTombstone resolves without error", async () => {
      await expect(
        mock.sendTombstone("orders.completed", "key-1"),
      ).resolves.toBeUndefined();
    });

    it("startWindowConsumer resolves with a ConsumerHandle", async () => {
      const handle = await mock.startWindowConsumer(
        "orders.created",
        jest.fn(),
        { maxMessages: 10, maxMs: 1000 },
      );
      expect(handle.groupId).toBe("mock-group");
      expect(typeof handle.stop).toBe("function");
    });

    it("startRoutedConsumer resolves with a ConsumerHandle", async () => {
      const handle = await mock.startRoutedConsumer(["orders.created"], {
        header: "x-event-type",
        routes: {},
      });
      expect(handle.groupId).toBe("mock-group");
      expect(typeof handle.stop).toBe("function");
    });

    it("startTransactionalConsumer resolves with a ConsumerHandle", async () => {
      const handle = await mock.startTransactionalConsumer(
        ["orders.created"],
        jest.fn(),
      );
      expect(handle.groupId).toBe("mock-group");
      expect(typeof handle.stop).toBe("function");
    });

    it("listConsumerGroups resolves to []", async () => {
      await expect(mock.listConsumerGroups()).resolves.toEqual([]);
    });

    it("describeTopics resolves to []", async () => {
      await expect(mock.describeTopics()).resolves.toEqual([]);
    });

    it("deleteRecords resolves without error", async () => {
      await expect(
        mock.deleteRecords("orders.created", [{ partition: 0, offset: "5" }]),
      ).resolves.toBeUndefined();
    });

    it("readSnapshot resolves to a Map", async () => {
      const snapshot = await mock.readSnapshot("orders.created");
      expect(snapshot).toBeInstanceOf(Map);
      expect(snapshot.size).toBe(0);
    });

    it("checkpointOffsets resolves to a checkpoint result", async () => {
      await expect(
        mock.checkpointOffsets("mock-group", "checkpoints"),
      ).resolves.toEqual({ groupId: "mock-group", checkpoints: [] });
    });

    it("restoreFromCheckpoint resolves without error", async () => {
      await expect(
        mock.restoreFromCheckpoint("mock-group", "checkpoints"),
      ).resolves.toBeUndefined();
    });

    it("connectProducer resolves without error", async () => {
      await expect(mock.connectProducer()).resolves.toBeUndefined();
    });

    it("disconnectProducer resolves without error", async () => {
      await expect(mock.disconnectProducer()).resolves.toBeUndefined();
    });

    it("onModuleDestroy resolves without error", async () => {
      await expect(mock.onModuleDestroy()).resolves.toBeUndefined();
    });

    it("exposes every extended method as a callable mock function", () => {
      for (const name of [
        "sendTombstone",
        "startWindowConsumer",
        "startRoutedConsumer",
        "startTransactionalConsumer",
        "listConsumerGroups",
        "describeTopics",
        "deleteRecords",
        "readSnapshot",
        "checkpointOffsets",
        "restoreFromCheckpoint",
        "connectProducer",
        "disconnectProducer",
        "onModuleDestroy",
      ] as const) {
        expect(mock[name]).toBeDefined();
        expect(typeof mock[name]).toBe("function");
        expect(typeof mock[name].mock).toBe("object");
      }
    });
  });
});
