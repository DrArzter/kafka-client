import {
  startOutboxRelay,
  InMemoryOutboxStore,
} from "../../outbox";
import type {
  OutboxMessage,
  OutboxProducer,
  OutboxStore,
} from "../../outbox";

interface Topics {
  "orders.created": { orderId: string };
}

/** Flush all microtasks so awaited promises settle between fake-timer ticks. */
async function flushMicrotasks(): Promise<void> {
  // Several cycles: one iteration chains fetch → transaction → mark → onPublished
  // → .finally, each an extra microtask hop.
  for (let i = 0; i < 12; i++) await Promise.resolve();
}

/** Advance fake timers by one poll interval and let the async iteration settle. */
async function tick(ms: number): Promise<void> {
  jest.advanceTimersByTime(ms);
  await flushMicrotasks();
}

/** A transaction context whose `send`/`sendBatch` are spies. */
function makeTxCtx() {
  return {
    send: jest.fn().mockResolvedValue(undefined),
    sendBatch: jest.fn().mockResolvedValue(undefined),
  };
}

/**
 * Hand-rolled fake producer. `transaction(cb)` runs `cb(txCtx)` — mirroring the
 * real client — so we can observe `tx.send` calls and control commit success by
 * making the callback (or a send) reject.
 */
function makeKafka() {
  const txCtx = makeTxCtx();
  const transaction = jest.fn(
    async (cb: (ctx: unknown) => Promise<void>) => {
      await cb(txCtx);
    },
  );
  const kafka: OutboxProducer<Topics> = {
    transaction: transaction as unknown as OutboxProducer<Topics>["transaction"],
  };
  return { kafka, txCtx, transaction };
}

const msg = (id: string, orderId = id): OutboxMessage => ({
  id,
  topic: "orders.created",
  payload: { orderId },
  key: orderId,
  eventId: `evt-${id}`,
  correlationId: `corr-${id}`,
});

describe("startOutboxRelay", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("publishes a fetched batch in one transaction and marks published AFTER commit", async () => {
    const { kafka, transaction, txCtx } = makeKafka();

    const order: string[] = [];
    const store: OutboxStore = {
      fetchUnpublished: jest.fn(async () => [msg("1"), msg("2")]),
      markPublished: jest.fn(async () => {
        order.push("mark");
      }),
    };
    transaction.mockImplementation(async (cb) => {
      await cb(txCtx);
      order.push("commit");
    });

    const onPublished = jest.fn();
    const relay = startOutboxRelay<Topics>(kafka, store, {
      pollIntervalMs: 1000,
      onPublished,
    });

    await tick(1000);

    // One transaction, both messages sent inside it.
    expect(transaction).toHaveBeenCalledTimes(1);
    expect(txCtx.send).toHaveBeenCalledTimes(2);
    expect(txCtx.send).toHaveBeenNthCalledWith(
      1,
      "orders.created",
      { orderId: "1" },
      {
        key: "1",
        headers: undefined,
        correlationId: "corr-1",
        eventId: "evt-1",
      },
    );

    // markPublished ran with both ids, and strictly AFTER the commit.
    expect(store.markPublished).toHaveBeenCalledWith(["1", "2"]);
    expect(order).toEqual(["commit", "mark"]);
    expect(onPublished).toHaveBeenCalledWith(2);

    await relay.stop();
  });

  it("does nothing on an empty batch (no transaction, no mark)", async () => {
    const { kafka, transaction } = makeKafka();
    const store: OutboxStore = {
      fetchUnpublished: jest.fn(async () => []),
      markPublished: jest.fn(async () => {}),
    };

    const relay = startOutboxRelay<Topics>(kafka, store, {
      pollIntervalMs: 1000,
    });
    await tick(1000);

    expect(transaction).not.toHaveBeenCalled();
    expect(store.markPublished).not.toHaveBeenCalled();

    await relay.stop();
  });

  it("on transaction failure: fires onError, does NOT mark, retries and succeeds next poll", async () => {
    const { kafka, transaction, txCtx } = makeKafka();
    const store: OutboxStore = {
      fetchUnpublished: jest.fn(async () => [msg("1")]),
      markPublished: jest.fn(async () => {}),
    };

    // First transaction throws, second succeeds.
    transaction
      .mockImplementationOnce(async () => {
        throw new Error("tx boom");
      })
      .mockImplementation(async (cb) => {
        await cb(txCtx);
      });

    const onError = jest.fn();
    const relay = startOutboxRelay<Topics>(kafka, store, {
      pollIntervalMs: 1000,
      onError,
    });

    // First poll: transaction fails.
    await tick(1000);
    expect(onError).toHaveBeenCalledTimes(1);
    const [err, batch] = onError.mock.calls[0];
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toBe("tx boom");
    expect(batch).toEqual([msg("1")]);
    expect(store.markPublished).not.toHaveBeenCalled();

    // Second poll: same batch (still unpublished) → succeeds → marked.
    await tick(1000);
    expect(transaction).toHaveBeenCalledTimes(2);
    expect(store.markPublished).toHaveBeenCalledWith(["1"]);

    await relay.stop();
  });

  it("on markPublished failure: fires onError and re-publishes the batch next poll (duplicate send)", async () => {
    const { kafka, txCtx } = makeKafka();
    const store: OutboxStore = {
      // Batch stays unpublished because markPublished keeps failing.
      fetchUnpublished: jest.fn(async () => [msg("1")]),
      markPublished: jest.fn(async () => {
        throw new Error("mark boom");
      }),
    };

    const onError = jest.fn();
    const relay = startOutboxRelay<Topics>(kafka, store, {
      pollIntervalMs: 1000,
      onError,
    });

    await tick(1000);
    expect(onError).toHaveBeenCalledTimes(1);
    expect((onError.mock.calls[0][0] as Error).message).toBe("mark boom");
    // The message WAS sent to Kafka (commit succeeded) — this is the duplicate source.
    expect(txCtx.send).toHaveBeenCalledTimes(1);

    // Next poll: re-published (duplicate) because the row is still unpublished.
    await tick(1000);
    expect(txCtx.send).toHaveBeenCalledTimes(2);
    // Same eventId carried on the re-send → consumer-side dedup can drop it.
    expect(txCtx.send.mock.calls[0][2]).toMatchObject({ eventId: "evt-1" });
    expect(txCtx.send.mock.calls[1][2]).toMatchObject({ eventId: "evt-1" });

    await relay.stop();
  });

  it("does not overlap iterations when a poll runs longer than the interval", async () => {
    const { kafka, txCtx } = makeKafka();

    let releaseFetch!: () => void;
    const gate = new Promise<void>((r) => {
      releaseFetch = r;
    });
    let fetchCalls = 0;

    const store: OutboxStore = {
      fetchUnpublished: jest.fn(async () => {
        fetchCalls++;
        await gate; // first call blocks until released
        return [msg(String(fetchCalls))];
      }),
      markPublished: jest.fn(async () => {}),
    };

    const relay = startOutboxRelay<Topics>(kafka, store, {
      pollIntervalMs: 1000,
    });

    // Tick 1 starts an iteration that blocks inside fetchUnpublished.
    jest.advanceTimersByTime(1000);
    await flushMicrotasks();
    expect(fetchCalls).toBe(1);

    // Several more ticks fire while iteration 1 is still in flight — all skipped.
    jest.advanceTimersByTime(3000);
    await flushMicrotasks();
    expect(fetchCalls).toBe(1); // no overlap: still just the one call

    // Release the blocked iteration; it completes.
    releaseFetch();
    await flushMicrotasks();
    expect(txCtx.send).toHaveBeenCalledTimes(1);

    // A later tick now runs a fresh iteration.
    await tick(1000);
    expect(fetchCalls).toBe(2);

    await relay.stop();
  });

  it("stop() waits for the in-flight iteration and halts the loop", async () => {
    const { kafka } = makeKafka();

    let releaseFetch!: () => void;
    const gate = new Promise<void>((r) => {
      releaseFetch = r;
    });
    let marked = false;

    const store: OutboxStore = {
      fetchUnpublished: jest.fn(async () => {
        await gate;
        return [msg("1")];
      }),
      markPublished: jest.fn(async () => {
        marked = true;
      }),
    };

    const relay = startOutboxRelay<Topics>(kafka, store, {
      pollIntervalMs: 1000,
    });

    // Start an iteration that is blocked inside fetchUnpublished.
    jest.advanceTimersByTime(1000);
    await flushMicrotasks();

    // stop() must not resolve until the in-flight iteration finishes.
    let stopResolved = false;
    const stopPromise = relay.stop().then(() => {
      stopResolved = true;
    });
    await flushMicrotasks();
    expect(stopResolved).toBe(false);
    expect(marked).toBe(false);

    // Release the iteration → it finishes → stop() resolves.
    releaseFetch();
    await flushMicrotasks();
    await stopPromise;
    expect(stopResolved).toBe(true);
    expect(marked).toBe(true);

    // Loop is halted: further ticks do nothing.
    const callsBefore = (store.fetchUnpublished as jest.Mock).mock.calls.length;
    await tick(5000);
    expect((store.fetchUnpublished as jest.Mock).mock.calls.length).toBe(
      callsBefore,
    );
  });

  it("uses default pollIntervalMs (1000) and batchSize (100) when unset", async () => {
    const { kafka } = makeKafka();
    const store: OutboxStore = {
      fetchUnpublished: jest.fn(async () => []),
      markPublished: jest.fn(async () => {}),
    };

    const relay = startOutboxRelay<Topics>(kafka, store);

    // No tick yet before 1000ms elapses.
    await tick(999);
    expect(store.fetchUnpublished).not.toHaveBeenCalled();

    await tick(1);
    expect(store.fetchUnpublished).toHaveBeenCalledWith(100);

    await relay.stop();
  });
});

describe("InMemoryOutboxStore", () => {
  it("fetches unpublished rows oldest-first, capped at limit", async () => {
    const store = new InMemoryOutboxStore();
    store.add(msg("1"));
    store.add(msg("2"));
    store.add(msg("3"));

    const first = await store.fetchUnpublished(2);
    expect(first.map((m) => m.id)).toEqual(["1", "2"]);
    expect(store.pendingCount).toBe(3);
    expect(store.publishedCount).toBe(0);
  });

  it("markPublished removes rows from subsequent fetches and is idempotent", async () => {
    const store = new InMemoryOutboxStore();
    store.add(msg("1"));
    store.add(msg("2"));

    await store.markPublished(["1"]);
    expect(store.publishedCount).toBe(1);
    expect(store.pendingCount).toBe(1);

    const remaining = await store.fetchUnpublished(10);
    expect(remaining.map((m) => m.id)).toEqual(["2"]);

    // Idempotent: unknown / already-published ids are ignored, no throw.
    await store.markPublished(["1", "unknown"]);
    expect(store.publishedCount).toBe(1);
  });

  it("drives the relay end-to-end: adds get published then marked", async () => {
    jest.useFakeTimers();
    try {
      const { kafka, txCtx } = makeKafka();
      const store = new InMemoryOutboxStore();
      store.add(msg("1"));
      store.add(msg("2"));

      const relay = startOutboxRelay<Topics>(kafka, store, {
        pollIntervalMs: 1000,
      });
      await tick(1000);

      expect(txCtx.send).toHaveBeenCalledTimes(2);
      expect(store.publishedCount).toBe(2);
      expect(store.pendingCount).toBe(0);

      // Nothing left to publish on the next poll.
      const sendsBefore = txCtx.send.mock.calls.length;
      await tick(1000);
      expect(txCtx.send.mock.calls.length).toBe(sendsBefore);

      await relay.stop();
    } finally {
      jest.useRealTimers();
    }
  });
});
