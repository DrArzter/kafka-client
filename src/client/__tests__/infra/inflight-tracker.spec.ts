import { InFlightTracker } from "../../kafka.client/infra/inflight.tracker";

describe("InFlightTracker", () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it("does not leak the counter when fn throws synchronously", async () => {
    const warn = jest.fn();
    const tracker = new InFlightTracker(warn);

    const boom = new Error("sync throw");
    expect(() =>
      tracker.track(() => {
        throw boom;
      }),
    ).toThrow(boom);

    // Counter must be back to zero — waitForDrain resolves immediately (no timer)
    // and never warns.
    await expect(tracker.waitForDrain(10)).resolves.toBeUndefined();
    expect(warn).not.toHaveBeenCalled();
  });

  it("resolves and warns on the timeout path when a handler never settles", async () => {
    jest.useFakeTimers();
    const warn = jest.fn();
    const tracker = new InFlightTracker(warn);

    // A promise that never resolves keeps the tracker in-flight forever.
    void tracker.track(() => new Promise<void>(() => {}));

    const drainPromise = tracker.waitForDrain(1000);

    expect(warn).not.toHaveBeenCalled();

    jest.advanceTimersByTime(1000);

    await expect(drainPromise).resolves.toBeUndefined();
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toMatch(/still in flight/);
  });

  it("resolves without warning when the tracked handler settles before the timeout", async () => {
    jest.useFakeTimers();
    const warn = jest.fn();
    const tracker = new InFlightTracker(warn);

    let resolveHandler!: () => void;
    const tracked = tracker.track(
      () => new Promise<void>((r) => (resolveHandler = r)),
    );

    const drainPromise = tracker.waitForDrain(1000);

    // Let the handler complete before the timeout fires.
    resolveHandler();
    await tracked;

    await expect(drainPromise).resolves.toBeUndefined();
    expect(warn).not.toHaveBeenCalled();
  });
});
