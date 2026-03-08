import {
  TestTopicMap,
  createClient,
  mockFetchOffsets,
  mockFetchTopicOffsets,
  mockRun,
  mockSend,
  mockSetOffsets,
  KafkaClient,
} from "../helpers";

// ── Helpers ─────────────────────────────────────────────────────────────

/** Build a fake fetchOffsets response (committed offsets for a consumer group). */
function makeCommitted(
  entries: Array<{ topic: string; partition: number; offset: string }>,
): Array<{
  topic: string;
  partitions: Array<{ partition: number; offset: string }>;
}> {
  const byTopic = new Map<
    string,
    Array<{ partition: number; offset: string }>
  >();
  for (const { topic, partition, offset } of entries) {
    const list = byTopic.get(topic) ?? [];
    list.push({ partition, offset });
    byTopic.set(topic, list);
  }
  return Array.from(byTopic.entries()).map(([topic, partitions]) => ({
    topic,
    partitions,
  }));
}

interface StoredCheckpoint {
  groupId: string;
  offsets: Array<{ topic: string; partition: number; offset: string }>;
  savedAt: number;
}

/** Setup mockFetchTopicOffsets + mockRun to deliver checkpoint messages. */
function setupCheckpointConsumer(checkpoints: StoredCheckpoint[]): void {
  // One partition with N messages
  mockFetchTopicOffsets.mockResolvedValueOnce([
    { partition: 0, low: "0", high: String(checkpoints.length) },
  ]);

  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    for (let i = 0; i < checkpoints.length; i++) {
      const cp = checkpoints[i];
      await eachMessage({
        topic: "my.checkpoints",
        partition: 0,
        message: {
          key: Buffer.from(cp.groupId),
          value: Buffer.from(JSON.stringify(cp)),
          offset: String(i),
          headers: {},
        },
      });
    }
  });
}

// ── Tests ────────────────────────────────────────────────────────────────

describe("KafkaClient — checkpointOffsets", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("sends a checkpoint message keyed by groupId", async () => {
    mockFetchOffsets.mockResolvedValueOnce(
      makeCommitted([
        { topic: "test.topic", partition: 0, offset: "42" },
        { topic: "test.topic", partition: 1, offset: "7" },
      ]),
    );

    await client.checkpointOffsets("test-group", "my.checkpoints");

    expect(mockSend).toHaveBeenCalledTimes(1);
    const call = (mockSend as jest.Mock).mock.calls[0][0];
    expect(call.topic).toBe("my.checkpoints");
    expect(call.messages[0].key).toBe("test-group");
  });

  it("encodes all committed offsets in the payload", async () => {
    mockFetchOffsets.mockResolvedValueOnce(
      makeCommitted([
        { topic: "test.topic", partition: 0, offset: "10" },
        { topic: "test.other", partition: 0, offset: "20" },
      ]),
    );

    await client.checkpointOffsets("test-group", "my.checkpoints");

    const payload = JSON.parse(
      (mockSend as jest.Mock).mock.calls[0][0].messages[0].value,
    ) as StoredCheckpoint;
    expect(payload.groupId).toBe("test-group");
    expect(payload.offsets).toHaveLength(2);
    expect(payload.offsets).toEqual(
      expect.arrayContaining([
        { topic: "test.topic", partition: 0, offset: "10" },
        { topic: "test.other", partition: 0, offset: "20" },
      ]),
    );
    expect(typeof payload.savedAt).toBe("number");
  });

  it("returns correct metadata", async () => {
    mockFetchOffsets.mockResolvedValueOnce(
      makeCommitted([
        { topic: "test.topic", partition: 0, offset: "5" },
        { topic: "test.topic", partition: 1, offset: "3" },
      ]),
    );

    const result = await client.checkpointOffsets(
      "test-group",
      "my.checkpoints",
    );

    expect(result.groupId).toBe("test-group");
    expect(result.topics).toEqual(["test.topic"]);
    expect(result.partitionCount).toBe(2);
    expect(result.savedAt).toBeGreaterThan(0);
  });

  it("defaults to the client default groupId when groupId is undefined", async () => {
    mockFetchOffsets.mockResolvedValueOnce([]);

    await client.checkpointOffsets(undefined, "my.checkpoints");

    expect(mockFetchOffsets).toHaveBeenCalledWith({ groupId: "test-group" });
  });

  it("works with zero committed offsets (empty group)", async () => {
    mockFetchOffsets.mockResolvedValueOnce([]);

    const result = await client.checkpointOffsets(
      "test-group",
      "my.checkpoints",
    );

    expect(result.partitionCount).toBe(0);
    expect(result.topics).toEqual([]);
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("attaches x-checkpoint-timestamp and x-checkpoint-group-id headers", async () => {
    mockFetchOffsets.mockResolvedValueOnce([]);

    await client.checkpointOffsets("test-group", "my.checkpoints");

    const headers = (mockSend as jest.Mock).mock.calls[0][0].messages[0]
      .headers;
    expect(headers["x-checkpoint-group-id"]).toEqual(["test-group"]);
    expect(headers["x-checkpoint-timestamp"]).toBeDefined();
  });
});

describe("KafkaClient — restoreFromCheckpoint", () => {
  let client: KafkaClient<TestTopicMap>;
  const NOW = 1_700_000_000_000;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(Date, "now").mockReturnValue(NOW);
    client = createClient();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("throws when the consumer group is still running", async () => {
    // Register a running consumer
    (client as any).runningConsumers.set("test-group", "eachMessage");

    await expect(
      client.restoreFromCheckpoint("test-group", "my.checkpoints"),
    ).rejects.toThrow(/still running/);
  });

  it("throws when the checkpoint topic is empty", async () => {
    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "0" },
    ]);

    await expect(
      client.restoreFromCheckpoint("test-group", "my.checkpoints"),
    ).rejects.toThrow(/no checkpoints found/);
  });

  it("throws when fetchTopicOffsets fails (topic does not exist)", async () => {
    mockFetchTopicOffsets.mockRejectedValueOnce(new Error("unknown topic"));

    await expect(
      client.restoreFromCheckpoint("test-group", "my.checkpoints"),
    ).rejects.toThrow(/could not fetch offsets/);
  });

  it("restores the latest checkpoint when no timestamp is given", async () => {
    const older: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "5" }],
      savedAt: NOW - 60_000,
    };
    const newer: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "99" }],
      savedAt: NOW - 10_000,
    };

    setupCheckpointConsumer([older, newer]);

    const result = await client.restoreFromCheckpoint(
      "test-group",
      "my.checkpoints",
    );

    expect(result.restoredAt).toBe(newer.savedAt);
    expect(result.offsets).toEqual(newer.offsets);
    expect(mockSetOffsets).toHaveBeenCalledWith(
      expect.objectContaining({ partitions: [{ partition: 0, offset: "99" }] }),
    );
  });

  it("restores the newest checkpoint at or before the given timestamp", async () => {
    const target = NOW - 30_000;
    const old: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "1" }],
      savedAt: NOW - 90_000,
    };
    const mid: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "50" }],
      savedAt: NOW - 40_000, // ≤ target
    };
    const fresh: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "200" }],
      savedAt: NOW - 5_000, // > target — should be excluded
    };

    setupCheckpointConsumer([old, mid, fresh]);

    const result = await client.restoreFromCheckpoint(
      "test-group",
      "my.checkpoints",
      {
        timestamp: target,
      },
    );

    expect(result.restoredAt).toBe(mid.savedAt);
    expect(result.offsets).toEqual(mid.offsets);
  });

  it("falls back to the oldest checkpoint when all are newer than the target", async () => {
    const oldest: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "1" }],
      savedAt: NOW - 5_000,
    };
    const newer: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "100" }],
      savedAt: NOW - 1_000,
    };

    setupCheckpointConsumer([oldest, newer]);

    const warnSpy = jest.fn();
    (client as any).logger.warn = warnSpy;

    const result = await client.restoreFromCheckpoint(
      "test-group",
      "my.checkpoints",
      {
        timestamp: NOW - 60_000, // older than all checkpoints
      },
    );

    expect(result.restoredAt).toBe(oldest.savedAt);
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("oldest available"),
    );
  });

  it("ignores checkpoint messages for other groups", async () => {
    const foreignCp: StoredCheckpoint = {
      groupId: "other-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "999" }],
      savedAt: NOW - 1_000,
    };

    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "1" },
    ]);
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "my.checkpoints",
        partition: 0,
        message: {
          key: Buffer.from("other-group"),
          value: Buffer.from(JSON.stringify(foreignCp)),
          offset: "0",
          headers: {},
        },
      });
    });

    await expect(
      client.restoreFromCheckpoint("test-group", "my.checkpoints"),
    ).rejects.toThrow(/no checkpoints found/);
  });

  it("skips and warns on malformed checkpoint messages", async () => {
    const valid: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "7" }],
      savedAt: NOW - 1_000,
    };

    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "2" },
    ]);
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "my.checkpoints",
        partition: 0,
        message: {
          key: Buffer.from("test-group"),
          value: Buffer.from("not-json"),
          offset: "0",
          headers: {},
        },
      });
      await eachMessage({
        topic: "my.checkpoints",
        partition: 0,
        message: {
          key: Buffer.from("test-group"),
          value: Buffer.from(JSON.stringify(valid)),
          offset: "1",
          headers: {},
        },
      });
    });

    const warnSpy = jest.fn();
    (client as any).logger.warn = warnSpy;

    const result = await client.restoreFromCheckpoint(
      "test-group",
      "my.checkpoints",
    );

    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("malformed"));
    expect(result.offsets).toEqual(valid.offsets);
  });

  it("returns correct checkpointAge", async () => {
    const cp: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "0" }],
      savedAt: NOW - 5_000,
    };

    setupCheckpointConsumer([cp]);

    const result = await client.restoreFromCheckpoint(
      "test-group",
      "my.checkpoints",
    );

    expect(result.checkpointAge).toBe(5_000);
  });

  it("uses the client default groupId when groupId is undefined", async () => {
    const cp: StoredCheckpoint = {
      groupId: "test-group",
      offsets: [{ topic: "test.topic", partition: 0, offset: "0" }],
      savedAt: NOW - 1_000,
    };

    setupCheckpointConsumer([cp]);

    const result = await client.restoreFromCheckpoint(
      undefined,
      "my.checkpoints",
    );

    expect(result.groupId).toBe("test-group");
  });
});
