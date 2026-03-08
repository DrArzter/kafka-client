import {
  TestTopicMap,
  createClient,
  mockRun,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
  mockSendOffsets,
  mockTransaction,
  mockCommitOffsets,
  KafkaClient,
} from "../helpers";

/** Build a single raw message delivered via mockRun. */
function deliverMessage(
  headers: Record<string, string> = {},
  payload: Record<string, unknown> = { id: "1", value: 42 },
  offset = "5",
) {
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    await eachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify(payload)),
        offset,
        headers,
      },
    });
  });
}

describe("KafkaClient — startTransactionalConsumer", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  // ── Happy path ────────────────────────────────────────────────────

  it("starts a Kafka transaction per message", async () => {
    deliverMessage();
    const handler = jest.fn().mockResolvedValue(undefined);

    await client.startTransactionalConsumer(["test.topic"], handler);

    expect(mockTransaction).toHaveBeenCalledTimes(1);
  });

  it("calls the handler with the decoded EventEnvelope", async () => {
    deliverMessage();
    const handler = jest.fn().mockResolvedValue(undefined);

    await client.startTransactionalConsumer(["test.topic"], handler);

    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic", payload: { id: "1", value: 42 } }),
      expect.objectContaining({ send: expect.any(Function), sendBatch: expect.any(Function) }),
    );
  });

  it("commits the transaction with sendOffsets on handler success", async () => {
    deliverMessage({}, { id: "1", value: 1 }, "10");
    const handler = jest.fn().mockResolvedValue(undefined);

    await client.startTransactionalConsumer(["test.topic"], handler);

    expect(mockSendOffsets).toHaveBeenCalledWith(
      expect.objectContaining({
        topics: [expect.objectContaining({
          topic: "test.topic",
          partitions: [expect.objectContaining({ partition: 0, offset: "11" })],
        })],
      }),
    );
    expect(mockTxCommit).toHaveBeenCalledTimes(1);
    expect(mockTxAbort).not.toHaveBeenCalled();
  });

  it("tx.send() stages a message inside the transaction", async () => {
    deliverMessage();

    await client.startTransactionalConsumer(["test.topic"], async (_envelope, tx) => {
      await tx.send("test.other", { name: "routed" });
    });

    expect(mockTxSend).toHaveBeenCalledTimes(1);
    expect(mockTxSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.other" }),
    );
    expect(mockTxCommit).toHaveBeenCalledTimes(1);
  });

  it("tx.sendBatch() stages multiple messages inside the transaction", async () => {
    deliverMessage();

    await client.startTransactionalConsumer(["test.topic"], async (_envelope, tx) => {
      await tx.sendBatch("test.other", [{ value: { name: "a" } }, { value: { name: "b" } }]);
    });

    expect(mockTxSend).toHaveBeenCalledTimes(1);
    expect(mockTxCommit).toHaveBeenCalledTimes(1);
  });

  // ── Failure path ──────────────────────────────────────────────────

  it("aborts the transaction when the handler throws", async () => {
    deliverMessage();
    const handler = jest.fn().mockRejectedValue(new Error("handler error"));

    await expect(
      client.startTransactionalConsumer(["test.topic"], handler),
    ).rejects.toThrow("handler error");

    expect(mockTxAbort).toHaveBeenCalledTimes(1);
    expect(mockTxCommit).not.toHaveBeenCalled();
  });

  it("does not commit the offset when the handler throws", async () => {
    deliverMessage();
    const handler = jest.fn().mockRejectedValue(new Error("fail"));

    await expect(
      client.startTransactionalConsumer(["test.topic"], handler),
    ).rejects.toThrow();

    expect(mockSendOffsets).not.toHaveBeenCalled();
    expect(mockTxCommit).not.toHaveBeenCalled();
  });

  // ── Skip path ─────────────────────────────────────────────────────

  it("commits offset directly (no tx) when message value is null/empty", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: { value: null, offset: "7", headers: {} },
      });
    });
    const handler = jest.fn();

    await client.startTransactionalConsumer(["test.topic"], handler);

    expect(handler).not.toHaveBeenCalled();
    expect(mockTransaction).not.toHaveBeenCalled();
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: "test.topic", partition: 0, offset: "8" },
    ]);
  });

  // ── Validation ────────────────────────────────────────────────────

  it("throws at startup when retryTopics: true is set", async () => {
    mockRun.mockResolvedValue(undefined);

    await expect(
      client.startTransactionalConsumer(
        ["test.topic"],
        jest.fn(),
        { retryTopics: true, retry: { maxRetries: 3, backoffMs: 100 } },
      ),
    ).rejects.toThrow("retryTopics is not supported");
  });

  // ── Handle ────────────────────────────────────────────────────────

  it("returns a ConsumerHandle with groupId and stop()", async () => {
    mockRun.mockResolvedValue(undefined);

    const handle = await client.startTransactionalConsumer(["test.topic"], jest.fn());

    expect(handle).toHaveProperty("groupId");
    expect(typeof handle.stop).toBe("function");
  });
});
