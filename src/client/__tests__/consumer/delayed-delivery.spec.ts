import {
  createClient,
  mockSend,
  mockRun,
  mockSubscribe,
  mockTransaction,
  mockTxSend,
  mockTxCommit,
  mockSendOffsets,
  mockCommitOffsets,
  mockConsumerPause,
  mockConsumerResume,
} from "../helpers";
import {
  HEADER_DELAYED_TARGET,
  HEADER_DELAYED_UNTIL,
} from "../../message/envelope";

describe("KafkaClient — delayed delivery", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockRun.mockReset().mockResolvedValue(undefined);
  });

  describe("producer: SendOptions.deliverAfterMs", () => {
    it("redirects sendMessage to <topic>.delayed with control headers", async () => {
      const client = createClient();
      const before = Date.now();

      await client.sendMessage(
        "test.topic",
        { id: "1", value: 1 },
        { deliverAfterMs: 60_000 },
      );

      expect(mockSend).toHaveBeenCalledTimes(1);
      const payload = mockSend.mock.calls[0][0];
      expect(payload.topic).toBe("test.topic.delayed");
      const headers = payload.messages[0].headers;
      expect(headers[HEADER_DELAYED_TARGET]).toBe("test.topic");
      const until = Number(headers[HEADER_DELAYED_UNTIL]);
      expect(until).toBeGreaterThanOrEqual(before + 60_000);
      expect(until).toBeLessThanOrEqual(Date.now() + 60_000);
    });

    it("redirects sendBatch and stamps every message", async () => {
      const client = createClient();

      await client.sendBatch(
        "test.topic",
        [
          { value: { id: "1", value: 1 } },
          { value: { id: "2", value: 2 } },
        ],
        { deliverAfterMs: 5_000 },
      );

      const payload = mockSend.mock.calls[0][0];
      expect(payload.topic).toBe("test.topic.delayed");
      for (const m of payload.messages) {
        expect(m.headers[HEADER_DELAYED_TARGET]).toBe("test.topic");
        expect(m.headers[HEADER_DELAYED_UNTIL]).toBeDefined();
      }
    });

    it("does not redirect when deliverAfterMs is absent or 0", async () => {
      const client = createClient();
      await client.sendMessage("test.topic", { id: "1", value: 1 });
      await client.sendMessage(
        "test.topic",
        { id: "2", value: 2 },
        { deliverAfterMs: 0 },
      );

      for (const call of mockSend.mock.calls) {
        expect(call[0].topic).toBe("test.topic");
      }
    });
  });

  describe("relay: startDelayedRelay", () => {
    function makeDelayedMessage(untilOffsetMs: number, offset = "0") {
      return {
        topic: "test.topic.delayed",
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          key: Buffer.from("key-1"),
          offset,
          headers: {
            [HEADER_DELAYED_TARGET]: "test.topic",
            [HEADER_DELAYED_UNTIL]: String(Date.now() + untilOffsetMs),
            "x-correlation-id": "corr-1",
          },
        },
      };
    }

    it("subscribes to <topic>.delayed and reports groupId", async () => {
      const client = createClient();
      const handle = await client.startDelayedRelay("test.topic");

      expect(handle.groupId).toBe("test-group-delayed-relay");
      expect(mockSubscribe).toHaveBeenCalledWith(
        expect.objectContaining({ topics: ["test.topic.delayed"] }),
      );
    });

    it("forwards an expired message transactionally, stripping control headers", async () => {
      let capturedEachMessage: ((p: any) => Promise<void>) | undefined;
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        capturedEachMessage = eachMessage;
      });

      const client = createClient();
      await client.startDelayedRelay("test.topic");

      await capturedEachMessage!(makeDelayedMessage(-1000, "7"));

      expect(mockTransaction).toHaveBeenCalled();
      expect(mockTxSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: "test.topic",
          messages: [
            expect.objectContaining({
              key: "key-1",
              headers: expect.not.objectContaining({
                [HEADER_DELAYED_UNTIL]: expect.anything(),
                [HEADER_DELAYED_TARGET]: expect.anything(),
              }),
            }),
          ],
        }),
      );
      // Original headers survive the hop
      const fwd = mockTxSend.mock.calls[0][0].messages[0];
      expect(fwd.headers["x-correlation-id"]).toBe("corr-1");
      // Offset committed inside the transaction (offset + 1)
      expect(mockSendOffsets).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: [
            expect.objectContaining({
              topic: "test.topic.delayed",
              partitions: [expect.objectContaining({ offset: "8" })],
            }),
          ],
        }),
      );
      expect(mockTxCommit).toHaveBeenCalled();
      // No direct commit outside the tx for the happy path
      expect(mockCommitOffsets).not.toHaveBeenCalled();
    });

    it("pauses the partition until the deadline for future messages", async () => {
      jest.useFakeTimers();
      try {
        let capturedEachMessage: ((p: any) => Promise<void>) | undefined;
        mockRun.mockImplementation(async ({ eachMessage }: any) => {
          capturedEachMessage = eachMessage;
        });

        const client = createClient();
        await client.startDelayedRelay("test.topic");

        const processing = capturedEachMessage!(makeDelayedMessage(2_000));
        // Let the pause happen
        await Promise.resolve();
        expect(mockConsumerPause).toHaveBeenCalledWith([
          { topic: "test.topic.delayed", partitions: [0] },
        ]);
        expect(mockTxSend).not.toHaveBeenCalled();

        await jest.advanceTimersByTimeAsync(2_100);
        await processing;

        expect(mockConsumerResume).toHaveBeenCalledWith([
          { topic: "test.topic.delayed", partitions: [0] },
        ]);
        expect(mockTxSend).toHaveBeenCalled();
        expect(mockTxCommit).toHaveBeenCalled();
      } finally {
        jest.useRealTimers();
      }
    });

    it("rejects a second relay on the same groupId", async () => {
      const client = createClient();
      await client.startDelayedRelay("test.topic");
      await expect(client.startDelayedRelay("test.topic")).rejects.toThrow(
        /called twice/,
      );
    });
  });
});
