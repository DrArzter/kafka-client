import {
  TestTopicMap,
  createClient,
  mockTransaction,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
  KafkaClient,
  topic,
} from "./helpers";

describe("KafkaClient — Transaction", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("transaction", () => {
    it("should commit on success", async () => {
      await client.transaction(async (tx) => {
        await tx.send("test.topic", { id: "1", value: 10 }, { key: "1" });
        await tx.send("test.other", { name: "hello" });
      });

      expect(mockTransaction).toHaveBeenCalled();
      expect(mockTxSend).toHaveBeenCalledTimes(2);
      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: "1",
            headers: expect.any(Object),
          },
        ],
      });
      expect(mockTxCommit).toHaveBeenCalled();
      expect(mockTxAbort).not.toHaveBeenCalled();
    });

    it("should abort on error", async () => {
      await expect(
        client.transaction(async (tx) => {
          await tx.send("test.topic", { id: "1", value: 10 });
          throw new Error("something went wrong");
        }),
      ).rejects.toThrow("something went wrong");

      expect(mockTxAbort).toHaveBeenCalled();
      expect(mockTxCommit).not.toHaveBeenCalled();
    });

    it("should support sendBatch in transaction", async () => {
      await client.transaction(async (tx) => {
        await tx.sendBatch("test.topic", [
          { value: { id: "1", value: 10 }, key: "1" },
          { value: { id: "2", value: 20 }, headers: { "x-trace": "t1" } },
        ]);
      });

      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: "1",
            headers: expect.any(Object),
          },
          {
            value: JSON.stringify({ id: "2", value: 20 }),
            key: null,
            headers: expect.objectContaining({ "x-trace": "t1" }),
          },
        ],
      });
      expect(mockTxCommit).toHaveBeenCalled();
    });

    it("should work in transaction via TopicDescriptor", async () => {
      const TestTopic = topic("test.topic").type<{ id: string; value: number }>();

      await client.transaction(async (tx) => {
        await tx.send(TestTopic, { id: "1", value: 10 });
      });

      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: null,
            headers: expect.any(Object),
          },
        ],
      });
      expect(mockTxCommit).toHaveBeenCalled();
    });
  });

  describe("transaction edge cases", () => {
    it("should handle empty transaction (no sends)", async () => {
      await client.transaction(async () => {
        // no-op
      });

      expect(mockTransaction).toHaveBeenCalled();
      expect(mockTxSend).not.toHaveBeenCalled();
      expect(mockTxCommit).toHaveBeenCalled();
    });

    it("should sendBatch via TopicDescriptor in transaction", async () => {
      const TestTopic = topic("test.topic").type<{ id: string; value: number }>();

      await client.transaction(async (tx) => {
        await tx.sendBatch(TestTopic, [
          { value: { id: "1", value: 10 } },
          { value: { id: "2", value: 20 } },
        ]);
      });

      expect(mockTxSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          {
            value: JSON.stringify({ id: "1", value: 10 }),
            key: null,
            headers: expect.any(Object),
          },
          {
            value: JSON.stringify({ id: "2", value: 20 }),
            key: null,
            headers: expect.any(Object),
          },
        ],
      });
      expect(mockTxCommit).toHaveBeenCalled();
    });

    it("should abort if sendBatch throws in transaction", async () => {
      mockTxSend.mockRejectedValueOnce(new Error("batch failed"));

      await expect(
        client.transaction(async (tx) => {
          await tx.sendBatch("test.topic", [{ value: { id: "1", value: 1 } }]);
        }),
      ).rejects.toThrow("batch failed");

      expect(mockTxAbort).toHaveBeenCalled();
      expect(mockTxCommit).not.toHaveBeenCalled();
    });

    it("should throw original error when tx.abort() also fails", async () => {
      mockTxSend.mockRejectedValueOnce(new Error("send failed"));
      mockTxAbort.mockRejectedValueOnce(new Error("abort failed"));

      await expect(
        client.transaction(async (tx) => {
          await tx.send("test.topic", { id: "1", value: 1 });
        }),
      ).rejects.toThrow("send failed");

      expect(mockTxAbort).toHaveBeenCalled();
    });
  });
});
