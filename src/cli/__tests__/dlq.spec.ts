import type { EventEnvelope } from "../../core";
import type { DlqReplayOptions } from "../../core";
import {
  DlqUsageError,
  DLQ_SUFFIX,
  countFromWatermarks,
  parseArgs,
  runDlqCommand,
  truncate,
  type DlqCliClient,
  type LsCommand,
  type PartitionWatermarks,
  type PeekCommand,
  type ReplayCommand,
  type RunDeps,
} from "../dlq";

// ── Test doubles ────────────────────────────────────────────────────────────

interface FakeClientState {
  topics: string[];
  offsets: Record<string, PartitionWatermarks[]>;
  peek: Array<EventEnvelope<unknown>>;
  replayResult: { replayed: number; skipped: number };
}

function makeEnvelope(
  overrides: Partial<EventEnvelope<unknown>> = {},
): EventEnvelope<unknown> {
  return {
    payload: { hello: "world" },
    topic: "orders.created.dlq",
    partition: 0,
    offset: "0",
    timestamp: "2026-07-02T00:00:00.000Z",
    eventId: "evt-1",
    correlationId: "corr-1",
    schemaVersion: 1,
    headers: {
      "x-dlq-original-topic": "orders.created",
      "x-dlq-error-message": "boom",
    },
    ...overrides,
  };
}

/** Build a fake client + jest spies for each method. */
function makeFakeClient(state: Partial<FakeClientState> = {}) {
  const fullState: FakeClientState = {
    topics: [],
    offsets: {},
    peek: [],
    replayResult: { replayed: 0, skipped: 0 },
    ...state,
  };

  const replayDlq = jest
    .fn<Promise<{ replayed: number; skipped: number }>, [string, DlqReplayOptions]>()
    .mockResolvedValue(fullState.replayResult);
  const peekMessages = jest
    .fn<Promise<Array<EventEnvelope<unknown>>>, [string, number]>()
    .mockImplementation(async (_topic, limit) =>
      fullState.peek.slice(0, limit),
    );
  const fetchTopicOffsets = jest
    .fn<Promise<PartitionWatermarks[]>, [string]>()
    .mockImplementation(async (topic) => fullState.offsets[topic] ?? []);
  const listTopics = jest
    .fn<Promise<string[]>, []>()
    .mockResolvedValue(fullState.topics);
  const close = jest.fn<Promise<void>, []>().mockResolvedValue(undefined);

  const client: DlqCliClient = {
    listTopics,
    fetchTopicOffsets,
    peekMessages,
    replayDlq,
    close,
  };
  return { client, replayDlq, peekMessages, fetchTopicOffsets, listTopics, close };
}

function makeDeps(client: DlqCliClient): { deps: RunDeps; lines: string[] } {
  const lines: string[] = [];
  const deps: RunDeps = {
    createClient: () => client,
    out: (line) => lines.push(line),
  };
  return { deps, lines };
}

// ── parseArgs ────────────────────────────────────────────────────────────────

describe("parseArgs", () => {
  describe("help", () => {
    it.each([[[]], [["-h"]], [["--help"]], [["help"]]])(
      "returns help for %j",
      (argv) => {
        expect(parseArgs(argv)).toEqual({ command: "help" });
      },
    );
  });

  describe("ls", () => {
    it("parses brokers", () => {
      const cmd = parseArgs(["ls", "--brokers", "localhost:9092"]) as LsCommand;
      expect(cmd).toEqual({
        command: "ls",
        brokers: ["localhost:9092"],
        prefix: undefined,
      });
    });

    it("splits comma-separated brokers and trims whitespace", () => {
      const cmd = parseArgs([
        "ls",
        "--brokers",
        "a:9092, b:9092 ,c:9092",
      ]) as LsCommand;
      expect(cmd.brokers).toEqual(["a:9092", "b:9092", "c:9092"]);
    });

    it("parses --prefix", () => {
      const cmd = parseArgs([
        "ls",
        "--brokers",
        "localhost:9092",
        "--prefix",
        "orders",
      ]) as LsCommand;
      expect(cmd.prefix).toBe("orders");
    });

    it("throws when --brokers is missing", () => {
      expect(() => parseArgs(["ls"])).toThrow(DlqUsageError);
      expect(() => parseArgs(["ls"])).toThrow(/--brokers/);
    });

    it("throws when --brokers has no value", () => {
      expect(() => parseArgs(["ls", "--brokers"])).toThrow(/requires a value/);
    });

    it("throws when --brokers is empty after splitting", () => {
      expect(() => parseArgs(["ls", "--brokers", " , "])).toThrow(
        /at least one broker/,
      );
    });
  });

  describe("peek", () => {
    it("parses topic and defaults limit to 10", () => {
      const cmd = parseArgs([
        "peek",
        "--brokers",
        "localhost:9092",
        "--topic",
        "orders.created",
      ]) as PeekCommand;
      expect(cmd).toEqual({
        command: "peek",
        brokers: ["localhost:9092"],
        topic: "orders.created",
        limit: 10,
      });
    });

    it("parses --limit", () => {
      const cmd = parseArgs([
        "peek",
        "--brokers",
        "localhost:9092",
        "--topic",
        "orders.created",
        "--limit",
        "5",
      ]) as PeekCommand;
      expect(cmd.limit).toBe(5);
    });

    it("throws when --topic is missing", () => {
      expect(() =>
        parseArgs(["peek", "--brokers", "localhost:9092"]),
      ).toThrow(/--topic/);
    });

    it.each(["0", "-3", "abc", "1.5"])(
      "throws for invalid --limit %s",
      (bad) => {
        expect(() =>
          parseArgs([
            "peek",
            "--brokers",
            "localhost:9092",
            "--topic",
            "t",
            "--limit",
            bad,
          ]),
        ).toThrow(/positive integer/);
      },
    );
  });

  describe("replay", () => {
    it("defaults dryRun=false and fromBeginning=true", () => {
      const cmd = parseArgs([
        "replay",
        "--brokers",
        "localhost:9092",
        "--topic",
        "orders.created",
      ]) as ReplayCommand;
      expect(cmd).toEqual({
        command: "replay",
        brokers: ["localhost:9092"],
        topic: "orders.created",
        target: undefined,
        dryRun: false,
        fromBeginning: true,
      });
    });

    it("parses --target and --dry-run", () => {
      const cmd = parseArgs([
        "replay",
        "--brokers",
        "localhost:9092",
        "--topic",
        "orders.created",
        "--target",
        "orders.manual",
        "--dry-run",
      ]) as ReplayCommand;
      expect(cmd.target).toBe("orders.manual");
      expect(cmd.dryRun).toBe(true);
    });

    it("--incremental sets fromBeginning=false", () => {
      const cmd = parseArgs([
        "replay",
        "--brokers",
        "localhost:9092",
        "--topic",
        "t",
        "--incremental",
      ]) as ReplayCommand;
      expect(cmd.fromBeginning).toBe(false);
    });

    it("--from-beginning keeps fromBeginning=true", () => {
      const cmd = parseArgs([
        "replay",
        "--brokers",
        "localhost:9092",
        "--topic",
        "t",
        "--from-beginning",
      ]) as ReplayCommand;
      expect(cmd.fromBeginning).toBe(true);
    });

    it("throws when --from-beginning and --incremental are combined", () => {
      expect(() =>
        parseArgs([
          "replay",
          "--brokers",
          "localhost:9092",
          "--topic",
          "t",
          "--from-beginning",
          "--incremental",
        ]),
      ).toThrow(/mutually exclusive/);
    });

    it("throws when --topic is missing", () => {
      expect(() =>
        parseArgs(["replay", "--brokers", "localhost:9092"]),
      ).toThrow(/--topic/);
    });
  });

  describe("errors", () => {
    it("throws for an unknown command", () => {
      expect(() => parseArgs(["frobnicate"])).toThrow(/Unknown command/);
    });

    it("throws for an unknown flag", () => {
      expect(() =>
        parseArgs(["ls", "--brokers", "localhost:9092", "--bogus", "x"]),
      ).toThrow(/Unknown flag/);
    });

    it("throws for a positional argument", () => {
      expect(() =>
        parseArgs(["ls", "localhost:9092"]),
      ).toThrow(/Unexpected argument/);
    });
  });
});

// ── pure helpers ────────────────────────────────────────────────────────────

describe("countFromWatermarks", () => {
  it("sums high−low across partitions", () => {
    expect(
      countFromWatermarks([
        { partition: 0, low: "0", high: "10" },
        { partition: 1, low: "5", high: "8" },
      ]),
    ).toBe(13);
  });

  it("clamps negative widths to zero", () => {
    expect(
      countFromWatermarks([{ partition: 0, low: "10", high: "5" }]),
    ).toBe(0);
  });

  it("returns 0 for no partitions", () => {
    expect(countFromWatermarks([])).toBe(0);
  });
});

describe("truncate", () => {
  it("leaves short strings intact", () => {
    expect(truncate("short", 200)).toBe("short");
  });
  it("truncates and annotates long strings", () => {
    const out = truncate("x".repeat(300), 10);
    expect(out.startsWith("x".repeat(10))).toBe(true);
    expect(out).toContain("(300 chars)");
  });
});

// ── runDlqCommand ────────────────────────────────────────────────────────────

describe("runDlqCommand", () => {
  it("help prints usage without creating a client", async () => {
    const lines: string[] = [];
    const createClient = jest.fn();
    const result = await runDlqCommand(
      { command: "help" },
      { createClient, out: (l) => lines.push(l) },
    );
    expect(result).toEqual({ command: "help" });
    expect(createClient).not.toHaveBeenCalled();
    expect(lines.join("\n")).toContain("kafka-client-dlq");
  });

  describe("ls", () => {
    it("lists DLQ topics with counts and closes the client", async () => {
      const { client, close } = makeFakeClient({
        topics: [
          "orders.created",
          "orders.created.dlq",
          "payments.settled.dlq",
        ],
        offsets: {
          "orders.created.dlq": [{ partition: 0, low: "0", high: "3" }],
          "payments.settled.dlq": [
            { partition: 0, low: "0", high: "1" },
            { partition: 1, low: "0", high: "4" },
          ],
        },
      });
      const { deps, lines } = makeDeps(client);

      const result = await runDlqCommand(
        { command: "ls", brokers: ["b"] },
        deps,
      );

      expect(result).toEqual({
        command: "ls",
        topics: [
          {
            dlqTopic: "orders.created.dlq",
            baseTopic: "orders.created",
            count: 3,
          },
          {
            dlqTopic: "payments.settled.dlq",
            baseTopic: "payments.settled",
            count: 5,
          },
        ],
      });
      // non-DLQ topic excluded
      expect((result as { topics: unknown[] }).topics).not.toContainEqual(
        expect.objectContaining({ dlqTopic: "orders.created" }),
      );
      expect(lines.join("\n")).toContain("8 message(s) total");
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("filters by --prefix on the base topic name", async () => {
      const { client } = makeFakeClient({
        topics: ["orders.created.dlq", "payments.settled.dlq"],
        offsets: {
          "orders.created.dlq": [{ partition: 0, low: "0", high: "1" }],
          "payments.settled.dlq": [{ partition: 0, low: "0", high: "9" }],
        },
      });
      const { deps } = makeDeps(client);

      const result = (await runDlqCommand(
        { command: "ls", brokers: ["b"], prefix: "orders" },
        deps,
      )) as { command: "ls"; topics: unknown[] };

      expect(result.topics).toHaveLength(1);
      expect(result.topics[0]).toMatchObject({ baseTopic: "orders.created" });
    });

    it("reports when no DLQ topics exist", async () => {
      const { client } = makeFakeClient({ topics: ["orders.created"] });
      const { deps, lines } = makeDeps(client);

      const result = (await runDlqCommand(
        { command: "ls", brokers: ["b"] },
        deps,
      )) as { command: "ls"; topics: unknown[] };

      expect(result.topics).toHaveLength(0);
      expect(lines.join("\n")).toContain("No DLQ topics found");
    });
  });

  describe("peek", () => {
    it("requests <topic>.dlq and stops at the limit", async () => {
      const envelopes = [
        makeEnvelope({ offset: "0" }),
        makeEnvelope({ offset: "1" }),
        makeEnvelope({ offset: "2" }),
      ];
      const { client, peekMessages } = makeFakeClient({ peek: envelopes });
      const { deps, lines } = makeDeps(client);

      const result = await runDlqCommand(
        { command: "peek", brokers: ["b"], topic: "orders.created", limit: 2 },
        deps,
      );

      expect(peekMessages).toHaveBeenCalledWith(
        `orders.created${DLQ_SUFFIX}`,
        2,
      );
      expect(result).toEqual({ command: "peek", printed: 2 });
      const output = lines.join("\n");
      expect(output).toContain("offset 0");
      expect(output).toContain("offset 1");
      expect(output).not.toContain("offset 2");
      // x-dlq-* headers printed
      expect(output).toContain("x-dlq-original-topic: orders.created");
    });

    it("reports an empty DLQ", async () => {
      const { client } = makeFakeClient({ peek: [] });
      const { deps, lines } = makeDeps(client);

      const result = await runDlqCommand(
        { command: "peek", brokers: ["b"], topic: "orders.created", limit: 10 },
        deps,
      );

      expect(result).toEqual({ command: "peek", printed: 0 });
      expect(lines.join("\n")).toContain("No messages in orders.created.dlq");
    });
  });

  describe("replay", () => {
    it("maps target, dryRun and fromBeginning into replayDlq options", async () => {
      const { client, replayDlq } = makeFakeClient({
        replayResult: { replayed: 4, skipped: 1 },
      });
      const { deps, lines } = makeDeps(client);

      const result = await runDlqCommand(
        {
          command: "replay",
          brokers: ["b"],
          topic: "orders.created",
          target: "orders.manual",
          dryRun: true,
          fromBeginning: false,
        },
        deps,
      );

      expect(replayDlq).toHaveBeenCalledWith("orders.created", {
        dryRun: true,
        fromBeginning: false,
        targetTopic: "orders.manual",
      });
      expect(result).toEqual({
        command: "replay",
        replayed: 4,
        skipped: 1,
        dryRun: true,
      });
      expect(lines.join("\n")).toContain("Dry-run: 4 message(s) would be");
    });

    it("omits targetTopic when --target is not set", async () => {
      const { client, replayDlq } = makeFakeClient({
        replayResult: { replayed: 2, skipped: 0 },
      });
      const { deps, lines } = makeDeps(client);

      await runDlqCommand(
        {
          command: "replay",
          brokers: ["b"],
          topic: "orders.created",
          target: undefined,
          dryRun: false,
          fromBeginning: true,
        },
        deps,
      );

      const [, options] = replayDlq.mock.calls[0];
      expect(options).toEqual({ dryRun: false, fromBeginning: true });
      expect(options).not.toHaveProperty("targetTopic");
      expect(lines.join("\n")).toContain("Replayed 2 message(s)");
    });
  });

  it("closes the client even when the command throws", async () => {
    const { client, close, listTopics } = makeFakeClient();
    listTopics.mockRejectedValueOnce(new Error("broker down"));
    const { deps } = makeDeps(client);

    await expect(
      runDlqCommand({ command: "ls", brokers: ["b"] }, deps),
    ).rejects.toThrow("broker down");
    expect(close).toHaveBeenCalledTimes(1);
  });
});
