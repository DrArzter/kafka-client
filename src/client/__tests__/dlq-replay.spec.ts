import {
  TestTopicMap,
  createClient,
  mockRun,
  mockSend,
  mockFetchTopicOffsets,
  KafkaClient,
} from "./helpers";

// ── Helpers ─────────────────────────────────────────────────────────────

function makeDlqMessage(
  value: object,
  extra: {
    offset?: string;
    partition?: number;
    headers?: Record<string, string>;
  } = {},
) {
  const headers: Record<string, string> = {
    "x-dlq-original-topic": "test.topic",
    "x-dlq-failed-at": "2024-01-01T00:00:00.000Z",
    "x-dlq-error-message": "handler failed",
    "x-dlq-error-stack": "Error: handler failed\n  at ...",
    "x-dlq-attempt-count": "3",
    "x-correlation-id": "corr-123",
    ...extra.headers,
  };
  return {
    topic: "test.topic.dlq",
    partition: extra.partition ?? 0,
    message: {
      value: Buffer.from(JSON.stringify(value)),
      // kafka-javascript passes headers as Record<string, string[]>
      headers: Object.fromEntries(
        Object.entries(headers).map(([k, v]) => [k, [v]]),
      ),
      offset: extra.offset ?? "0",
    },
  };
}

/** Setup mockFetchTopicOffsets to return N messages per partition and mockRun
 * to deliver them synchronously before resolving. */
function setupDlqConsumer(
  messages: ReturnType<typeof makeDlqMessage>[],
): void {
  // Group by partition to set up HWMs
  const byPartition = new Map<number, ReturnType<typeof makeDlqMessage>[]>();
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

  // mockRun: first call (DLQ replay consumer), subsequent calls resolve immediately
  let replayCalled = false;
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    if (!replayCalled) {
      replayCalled = true;
      for (const m of messages) {
        await eachMessage(m);
      }
    }
  });
}

// ── Tests ────────────────────────────────────────────────────────────────

describe("KafkaClient — replayDlq", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("returns { replayed: 0, skipped: 0 } when the DLQ topic is empty", async () => {
    mockFetchTopicOffsets.mockResolvedValueOnce([
      { partition: 0, low: "0", high: "0" },
    ]);

    const result = await client.replayDlq("test.topic");

    expect(result).toEqual({ replayed: 0, skipped: 0 });
    expect(mockSend).not.toHaveBeenCalled();
  });

  it("re-publishes each DLQ message to its x-dlq-original-topic", async () => {
    setupDlqConsumer([
      makeDlqMessage({ id: "1", value: 1 }, { offset: "0" }),
      makeDlqMessage({ id: "2", value: 2 }, { offset: "1" }),
    ]);

    const result = await client.replayDlq("test.topic");

    expect(result).toEqual({ replayed: 2, skipped: 0 });
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic" }),
    );
  });

  it("strips x-dlq-* headers but preserves original headers", async () => {
    setupDlqConsumer([makeDlqMessage({ id: "1", value: 1 }, { offset: "0" })]);

    await client.replayDlq("test.topic");

    const sendCall = (mockSend as jest.Mock).mock.calls[0][0];
    const sentHeaders = sendCall.messages[0].headers;

    // DLQ metadata stripped
    expect(sentHeaders["x-dlq-original-topic"]).toBeUndefined();
    expect(sentHeaders["x-dlq-error-message"]).toBeUndefined();
    // Original headers preserved
    expect(sentHeaders["x-correlation-id"]).toBe("corr-123");
  });

  it("uses options.targetTopic instead of x-dlq-original-topic when provided", async () => {
    setupDlqConsumer([makeDlqMessage({ id: "1", value: 1 }, { offset: "0" })]);

    await client.replayDlq("test.topic", { targetTopic: "test.other" });

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.other" }),
    );
  });

  it("dry-run mode logs but does not send", async () => {
    setupDlqConsumer([makeDlqMessage({ id: "1", value: 1 }, { offset: "0" })]);

    const result = await client.replayDlq("test.topic", { dryRun: true });

    expect(result).toEqual({ replayed: 1, skipped: 0 });
    expect(mockSend).not.toHaveBeenCalled();
  });

  it("filter option skips messages that return false", async () => {
    setupDlqConsumer([
      makeDlqMessage({ id: "1", value: 1 }, { offset: "0" }),
      makeDlqMessage(
        { id: "2", value: 2 },
        {
          offset: "1",
          headers: { "x-correlation-id": "skip-me" },
        },
      ),
    ]);

    const result = await client.replayDlq("test.topic", {
      filter: (headers) => headers["x-correlation-id"] !== "skip-me",
    });

    expect(result).toEqual({ replayed: 1, skipped: 1 });
    expect(mockSend).toHaveBeenCalledTimes(1);
  });
});
