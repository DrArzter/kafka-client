import { createMockKafkaClient, MockKafkaClient } from "./mock-client";

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

  it("checkStatus resolves to { topics: [] }", async () => {
    await expect(mock.checkStatus()).resolves.toEqual({ topics: [] });
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

  it("startConsumer resolves", async () => {
    const handler = jest.fn();
    await expect(
      mock.startConsumer(["orders.created"], handler),
    ).resolves.toBeUndefined();
  });

  it("startBatchConsumer resolves", async () => {
    const handler = jest.fn();
    await expect(
      mock.startBatchConsumer(["orders.created"], handler),
    ).resolves.toBeUndefined();
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
    await mock.sendBatch("orders.completed", [
      { value: { orderId: "3" } },
    ]);

    expect(mock.sendMessage).toHaveBeenCalledTimes(2);
    expect(mock.sendBatch).toHaveBeenCalledTimes(1);
  });
});
