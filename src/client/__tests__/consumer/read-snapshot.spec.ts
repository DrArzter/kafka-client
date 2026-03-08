import {
  TestTopicMap,
  createClient,
  mockRun,
  mockFetchTopicOffsets,
  mockConsumer,
  KafkaClient,
} from "../helpers";

// ── Helpers ─────────────────────────────────────────────────────────────

function makeSnapshotMessage(
  key: string,
  value: object | null,
  extra: {
    offset?: string;
    partition?: number;
    headers?: Record<string, string>;
  } = {},
) {
  return {
    topic: "test.topic",
    partition: extra.partition ?? 0,
    message: {
      key: Buffer.from(key),
      value: value !== null ? Buffer.from(JSON.stringify(value)) : null,
      offset: extra.offset ?? "0",
      headers: extra.headers
        ? Object.fromEntries(
            Object.entries(extra.headers).map(([k, v]) => [k, [v]]),
          )
        : {},
    },
  };
}

/**
 * Set up mockFetchTopicOffsets and mockRun to deliver the given messages
 * synchronously and resolve after the last one.
 */
function setupSnapshotConsumer(
  messages: ReturnType<typeof makeSnapshotMessage>[],
): void {
  // Group by partition to build HWMs
  const byPartition = new Map<
    number,
    ReturnType<typeof makeSnapshotMessage>[]
  >();
  for (const m of messages) {
    const list = byPartition.get(m.partition) ?? [];
    list.push(m);
    byPartition.set(m.partition, list);
  }

  mockFetchTopicOffsets.mockResolvedValueOnce(
    Array.from(byPartition.entries()).map(([partition, msgs]) => ({
      partition,
      low: "0",
      high: String(msgs.length),
    })),
  );

  // Deliver all messages then stop
  let called = false;
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    if (!called) {
      called = true;
      for (const m of messages) await eachMessage(m);
    }
  });
}

// ── Tests ────────────────────────────────────────────────────────────────

describe("KafkaClient — readSnapshot", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("returns an empty Map when the topic is empty", async () => {
    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "0" },
    ]);

    const result = await client.readSnapshot("test.topic");

    expect(result).toBeInstanceOf(Map);
    expect(result.size).toBe(0);
  });

  it("returns an empty Map when fetchTopicOffsets throws", async () => {
    mockFetchTopicOffsets.mockRejectedValueOnce(
      new Error("broker unavailable"),
    );

    const result = await client.readSnapshot("test.topic");

    expect(result.size).toBe(0);
  });

  it("returns the latest value per key", async () => {
    setupSnapshotConsumer([
      makeSnapshotMessage("order-1", { id: "1", value: 10 }, { offset: "0" }),
      makeSnapshotMessage("order-2", { id: "2", value: 20 }, { offset: "1" }),
    ]);

    const result = await client.readSnapshot("test.topic");

    expect(result.size).toBe(2);
    expect(result.get("order-1")?.payload).toEqual({ id: "1", value: 10 });
    expect(result.get("order-2")?.payload).toEqual({ id: "2", value: 20 });
  });

  it("overwrites earlier values when the same key appears multiple times", async () => {
    setupSnapshotConsumer([
      makeSnapshotMessage("order-1", { id: "1", value: 10 }, { offset: "0" }),
      makeSnapshotMessage("order-1", { id: "1", value: 99 }, { offset: "1" }),
    ]);

    const result = await client.readSnapshot("test.topic");

    expect(result.size).toBe(1);
    expect(result.get("order-1")?.payload).toEqual({ id: "1", value: 99 });
  });

  it("removes a key when a tombstone is received", async () => {
    setupSnapshotConsumer([
      makeSnapshotMessage("order-1", { id: "1", value: 10 }, { offset: "0" }),
      makeSnapshotMessage("order-1", null, { offset: "1" }),
    ]);

    const result = await client.readSnapshot("test.topic");

    expect(result.has("order-1")).toBe(false);
  });

  it("calls onTombstone with the key when a tombstone is received", async () => {
    setupSnapshotConsumer([
      makeSnapshotMessage("order-1", { id: "1", value: 10 }, { offset: "0" }),
      makeSnapshotMessage("order-1", null, { offset: "1" }),
    ]);

    const onTombstone = jest.fn();
    await client.readSnapshot("test.topic", { onTombstone });

    expect(onTombstone).toHaveBeenCalledWith("order-1");
  });

  it("skips messages with no key", async () => {
    // Manually set up a message with null key
    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "1" },
    ]);
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: {
          key: null,
          value: Buffer.from(JSON.stringify({ id: "1" })),
          offset: "0",
          headers: {},
        },
      });
    });

    const result = await client.readSnapshot("test.topic");

    expect(result.size).toBe(0);
  });

  it("builds a correct EventEnvelope (topic, partition, offset, payload)", async () => {
    setupSnapshotConsumer([
      makeSnapshotMessage("order-1", { id: "1", value: 42 }, { offset: "0" }),
    ]);

    const result = await client.readSnapshot("test.topic");
    const envelope = result.get("order-1")!;

    expect(envelope.topic).toBe("test.topic");
    expect(envelope.partition).toBe(0);
    expect(envelope.offset).toBe("0");
    expect(envelope.payload).toEqual({ id: "1", value: 42 });
  });

  it("stops at the high-watermark and does not process messages past it", async () => {
    // HWM = 1, so only offset 0 should be processed; any message at offset 1+ should be ignored
    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "1" },
    ]);

    const processed: string[] = [];
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      // Deliver offset 0 (within range) — should be processed
      await eachMessage(
        makeSnapshotMessage("a", { id: "1", value: 1 }, { offset: "0" }),
      );
      // Deliver offset 1 (past HWM − 1 = 0) — should be ignored (partition already done)
      await eachMessage(
        makeSnapshotMessage("b", { id: "2", value: 2 }, { offset: "1" }),
      );
      processed.push("extra");
    });

    const result = await client.readSnapshot("test.topic");

    expect(result.has("a")).toBe(true);
    expect(result.has("b")).toBe(false);
  });

  it("reads all partitions before resolving", async () => {
    // Two partitions, one message each
    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "1" },
      { partition: 1, low: "0", high: "1" },
    ]);

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage(
        makeSnapshotMessage(
          "key-p0",
          { id: "p0" },
          { offset: "0", partition: 0 },
        ),
      );
      await eachMessage(
        makeSnapshotMessage(
          "key-p1",
          { id: "p1" },
          { offset: "0", partition: 1 },
        ),
      );
    });

    const result = await client.readSnapshot("test.topic");

    expect(result.size).toBe(2);
    expect(result.get("key-p0")?.payload).toEqual({ id: "p0" });
    expect(result.get("key-p1")?.payload).toEqual({ id: "p1" });
  });

  it("skips and warns on JSON parse errors", async () => {
    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "2" },
    ]);
    const warnSpy = jest.fn();
    (client as any).logger.warn = warnSpy;

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      // Invalid JSON
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: {
          key: Buffer.from("k"),
          value: Buffer.from("not-json"),
          offset: "0",
          headers: {},
        },
      });
      // Valid JSON
      await eachMessage(
        makeSnapshotMessage("k", { id: "ok" }, { offset: "1" }),
      );
    });

    const result = await client.readSnapshot("test.topic");

    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("skipping"));
    expect(result.get("k")?.payload).toEqual({ id: "ok" });
  });

  it("validates payload with schema and skips invalid messages", async () => {
    setupSnapshotConsumer([
      makeSnapshotMessage("order-1", { id: "1", value: 10 }, { offset: "0" }),
      makeSnapshotMessage("order-2", { bad: "payload" }, { offset: "1" }),
    ]);

    const schema = {
      parse: (v: unknown) => {
        if (!(v as any).id) throw new Error("missing id");
        return v as { id: string; value: number };
      },
    };

    const warnSpy = jest.fn();
    (client as any).logger.warn = warnSpy;

    const result = await client.readSnapshot("test.topic", { schema });

    expect(result.has("order-1")).toBe(true);
    expect(result.has("order-2")).toBe(false);
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("skipping"));
  });

  it("disconnects the temp consumer after reading", async () => {
    setupSnapshotConsumer([
      makeSnapshotMessage("order-1", { id: "1", value: 10 }, { offset: "0" }),
    ]);

    await client.readSnapshot("test.topic");

    expect(mockConsumer.disconnect).toHaveBeenCalled();
  });
});
