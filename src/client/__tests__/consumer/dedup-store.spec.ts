import {
  TestTopicMap,
  createClient,
  makeTestLogger,
  mockRun,
  KafkaClient,
} from "../helpers";
import type { DedupStore } from "../../kafka.client";
import { InMemoryDedupStore } from "../../kafka.client";

// ── Helpers ──────────────────────────────────────────────────────────

/** Deliver messages in sequence, each with an optional clock and partition. */
function setupMessagesWithClocks(
  clocks: Array<{ clock: number | undefined; partition?: number }>,
) {
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    for (const { clock, partition = 0 } of clocks) {
      const headers: Record<string, string[]> = {};
      if (clock !== undefined) {
        headers["x-lamport-clock"] = [String(clock)];
      }
      await eachMessage({
        topic: "test.topic",
        partition,
        message: {
          value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
          headers,
          offset: "0",
        },
      });
    }
  });
}

/** A spy store that records calls and delegates to an in-memory map. */
function makeSpyStore(): DedupStore & {
  getLastClock: jest.Mock;
  setLastClock: jest.Mock;
} {
  const backing = new Map<string, number>();
  return {
    getLastClock: jest.fn((_groupId: string, tp: string) => backing.get(tp)),
    setLastClock: jest.fn((_groupId: string, tp: string, clock: number) => {
      backing.set(tp, clock);
    }),
  };
}

// ── Tests ─────────────────────────────────────────────────────────────

describe("KafkaClient — Deduplication (pluggable store)", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ── Custom store consultation ───────────────────────────────────────

  describe("custom store", () => {
    it("consults getLastClock and persists via setLastClock with the consumer groupId", async () => {
      client = createClient();
      const store = makeSpyStore();
      setupMessagesWithClocks([{ clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        groupId: "custom-group",
        deduplication: { store },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(store.getLastClock).toHaveBeenCalledWith(
        "custom-group",
        "test.topic:0",
      );
      expect(store.setLastClock).toHaveBeenCalledWith(
        "custom-group",
        "test.topic:0",
        5,
      );
    });

    it("detects a duplicate when the store returns a clock >= the incoming clock", async () => {
      client = createClient();
      const store: DedupStore = {
        getLastClock: jest.fn().mockReturnValue(10),
        setLastClock: jest.fn(),
      };
      setupMessagesWithClocks([{ clock: 10 }, { clock: 3 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { store },
      });

      // Both messages are duplicates against the persisted clock of 10.
      expect(handler).not.toHaveBeenCalled();
      expect(store.setLastClock).not.toHaveBeenCalled();
    });

    it("processes a fresh message when the store returns a lower clock", async () => {
      client = createClient();
      const store: DedupStore = {
        getLastClock: jest.fn().mockReturnValue(2),
        setLastClock: jest.fn(),
      };
      setupMessagesWithClocks([{ clock: 9 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { store },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(store.setLastClock).toHaveBeenCalledWith("test-group", "test.topic:0", 9);
    });

    it("supports an async store (awaits promises)", async () => {
      client = createClient();
      const backing = new Map<string, number>();
      const store: DedupStore = {
        getLastClock: async (_g, tp) => backing.get(tp),
        setLastClock: async (_g, tp, clock) => {
          backing.set(tp, clock);
        },
      };
      setupMessagesWithClocks([{ clock: 5 }, { clock: 5 }, { clock: 6 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { store },
      });

      // clock 5 fresh, 5 duplicate, 6 fresh → 2 calls
      expect(handler).toHaveBeenCalledTimes(2);
    });
  });

  // ── Fail-open on store errors ───────────────────────────────────────

  describe("fail-open on store errors", () => {
    it("processes the message and logs an error when getLastClock rejects", async () => {
      const logger = makeTestLogger();
      client = createClient({ logger });
      const store: DedupStore = {
        getLastClock: jest.fn().mockRejectedValue(new Error("redis down")),
        setLastClock: jest.fn(),
      };
      setupMessagesWithClocks([{ clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { store },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(store.setLastClock).not.toHaveBeenCalled();
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining("getLastClock failed"),
      );
    });

    it("processes the message and logs an error when setLastClock rejects", async () => {
      const logger = makeTestLogger();
      client = createClient({ logger });
      const store: DedupStore = {
        getLastClock: jest.fn().mockReturnValue(undefined),
        setLastClock: jest.fn().mockRejectedValue(new Error("write failed")),
      };
      setupMessagesWithClocks([{ clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { store },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining("setLastClock failed"),
      );
    });

    it("processes the message when getLastClock throws synchronously", async () => {
      const logger = makeTestLogger();
      client = createClient({ logger });
      const store: DedupStore = {
        getLastClock: jest.fn(() => {
          throw new Error("boom");
        }),
        setLastClock: jest.fn(),
      };
      setupMessagesWithClocks([{ clock: 5 }]);
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        deduplication: { store },
      });

      expect(handler).toHaveBeenCalledTimes(1);
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining("getLastClock failed"),
      );
    });
  });

  // ── InMemoryDedupStore unit behaviour ──────────────────────────────

  describe("InMemoryDedupStore", () => {
    it("returns undefined for an unknown key and round-trips a set value", () => {
      const states = new Map<string, Map<string, number>>();
      const store = new InMemoryDedupStore(states);

      expect(store.getLastClock("g", "t:0")).toBeUndefined();
      store.setLastClock("g", "t:0", 42);
      expect(store.getLastClock("g", "t:0")).toBe(42);
    });

    it("writes through to the backing map so external clearing resets state", () => {
      const states = new Map<string, Map<string, number>>();
      const store = new InMemoryDedupStore(states);

      store.setLastClock("g", "t:0", 7);
      expect(states.get("g")?.get("t:0")).toBe(7);

      states.clear();
      expect(store.getLastClock("g", "t:0")).toBeUndefined();
    });

    it("isolates clock state per groupId", () => {
      const states = new Map<string, Map<string, number>>();
      const store = new InMemoryDedupStore(states);

      store.setLastClock("group-a", "t:0", 1);
      store.setLastClock("group-b", "t:0", 99);

      expect(store.getLastClock("group-a", "t:0")).toBe(1);
      expect(store.getLastClock("group-b", "t:0")).toBe(99);
    });
  });
});
