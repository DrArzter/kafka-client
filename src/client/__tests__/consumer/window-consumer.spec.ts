import {
  TestTopicMap,
  createClient,
  mockRun,
  mockConsumer,
  KafkaClient,
} from "../helpers";

// ── Helpers ─────────────────────────────────────────────────────────────

function makeMsg(id: string, offset: string) {
  return {
    topic: "test.topic" as const,
    partition: 0,
    message: {
      key: Buffer.from(id),
      value: Buffer.from(JSON.stringify({ id, value: Number(offset) })),
      offset,
      headers: {} as Record<string, string[]>,
    },
  };
}

// ── Tests ────────────────────────────────────────────────────────────────

describe("KafkaClient — startWindowConsumer", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  // ── Validation ──────────────────────────────────────────────────

  it("throws when maxMessages ≤ 0", async () => {
    await expect(
      client.startWindowConsumer("test.topic", jest.fn(), {
        maxMessages: 0,
        maxMs: 1000,
      }),
    ).rejects.toThrow(/maxMessages must be > 0/);
  });

  it("throws when maxMs ≤ 0", async () => {
    await expect(
      client.startWindowConsumer("test.topic", jest.fn(), {
        maxMessages: 3,
        maxMs: 0,
      }),
    ).rejects.toThrow(/maxMs must be > 0/);
  });

  it("throws when retryTopics: true is passed", async () => {
    await expect(
      client.startWindowConsumer("test.topic", jest.fn(), {
        maxMessages: 3,
        maxMs: 1000,
        retryTopics: true,
      } as any),
    ).rejects.toThrow(/does not support retryTopics/);
  });

  // ── Size-triggered flush ─────────────────────────────────────────

  it("flushes when maxMessages is reached", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("a", "0"));
      await eachMessage(makeMsg("b", "1"));
      await eachMessage(makeMsg("c", "2")); // triggers flush at maxMessages=3
    });

    await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 3,
      maxMs: 60_000,
    });

    expect(handler).toHaveBeenCalledTimes(1);
    const [envelopes, meta] = handler.mock.calls[0];
    expect(envelopes).toHaveLength(3);
    expect(meta.trigger).toBe("size");
  });

  it("does not flush mid-window when maxMessages is not reached", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("a", "0"));
      await eachMessage(makeMsg("b", "1")); // only 2 of maxMessages=5
    });

    const handle = await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 5,
      maxMs: 60_000,
    });

    expect(handler).not.toHaveBeenCalled();
    await handle.stop(); // clean up timer + buffer
  });

  it("handles multiple size-triggered flushes in one run", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (let i = 0; i < 6; i++) {
        await eachMessage(makeMsg(String(i), String(i)));
      }
    });

    await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 3,
      maxMs: 60_000,
    });

    expect(handler).toHaveBeenCalledTimes(2);
    expect(handler.mock.calls[0][0]).toHaveLength(3);
    expect(handler.mock.calls[1][0]).toHaveLength(3);
  });

  it("passes correct payload in each window", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("x", "0"));
      await eachMessage(makeMsg("y", "1"));
    });

    await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 2,
      maxMs: 60_000,
    });

    const envelopes = handler.mock.calls[0][0];
    expect(envelopes[0].payload).toEqual({ id: "x", value: 0 });
    expect(envelopes[1].payload).toEqual({ id: "y", value: 1 });
  });

  // ── Time-triggered flush ─────────────────────────────────────────

  it("flushes by time when maxMs elapses", async () => {
    jest.useFakeTimers();
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("a", "0")); // 1 message, maxMessages=100 not reached
    });

    await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 100,
      maxMs: 500,
    });

    expect(handler).not.toHaveBeenCalled();

    await jest.advanceTimersByTimeAsync(500);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][1].trigger).toBe("time");
    jest.useRealTimers();
  });

  it("does not double-flush after size flush clears the timer", async () => {
    jest.useFakeTimers();
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("a", "0"));
      await eachMessage(makeMsg("b", "1")); // triggers size flush
    });

    await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 2,
      maxMs: 500,
    });

    // Size flush already happened
    expect(handler).toHaveBeenCalledTimes(1);

    // Advancing timer should NOT trigger another flush (buffer is empty)
    await jest.advanceTimersByTimeAsync(500);
    expect(handler).toHaveBeenCalledTimes(1);

    jest.useRealTimers();
  });

  // ── WindowMeta ───────────────────────────────────────────────────

  it("populates windowStart and windowEnd in meta", async () => {
    const NOW = 1_700_000_000_000;
    jest.spyOn(Date, "now").mockReturnValue(NOW);

    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("a", "0"));
      await eachMessage(makeMsg("b", "1"));
    });

    await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 2,
      maxMs: 60_000,
    });

    const meta = handler.mock.calls[0][1];
    expect(meta.windowStart).toBe(NOW);
    expect(meta.windowEnd).toBe(NOW);
    jest.restoreAllMocks();
  });

  // ── Shutdown flush (stop()) ──────────────────────────────────────

  it("flushes remaining messages on stop()", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("a", "0"));
      await eachMessage(makeMsg("b", "1")); // 2 messages, maxMessages=10 not reached
    });

    const handle = await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 10,
      maxMs: 60_000,
    });

    expect(handler).not.toHaveBeenCalled();

    await handle.stop();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0]).toHaveLength(2);
    expect(mockConsumer.disconnect).toHaveBeenCalled();
  });

  it("does not call handler on stop() when buffer is empty", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);

    // No messages delivered
    mockRun.mockResolvedValue(undefined);

    const handle = await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 3,
      maxMs: 60_000,
    });

    await handle.stop();

    expect(handler).not.toHaveBeenCalled();
    expect(mockConsumer.disconnect).toHaveBeenCalled();
  });

  it("cancels the timer before stop() flushes", async () => {
    jest.useFakeTimers();
    const handler = jest.fn().mockResolvedValue(undefined);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(makeMsg("a", "0"));
    });

    const handle = await client.startWindowConsumer("test.topic", handler, {
      maxMessages: 10,
      maxMs: 500,
    });

    await handle.stop(); // flushes 1 message, cancels timer

    expect(handler).toHaveBeenCalledTimes(1);

    // Timer should NOT fire after stop
    await jest.advanceTimersByTimeAsync(500);
    expect(handler).toHaveBeenCalledTimes(1);

    jest.useRealTimers();
  });
});
