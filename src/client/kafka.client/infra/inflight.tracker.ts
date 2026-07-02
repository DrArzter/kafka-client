/** Tracks in-flight async handlers and provides a drain-wait mechanism. */
export class InFlightTracker {
  private inFlightTotal = 0;
  private readonly drainResolvers: Array<() => void> = [];

  constructor(private readonly warn: (msg: string) => void) {}

  /**
   * Wrap an async handler so its lifetime is counted against the in-flight total.
   * Resolvers registered with `waitForDrain` are notified when the count reaches zero.
   * @param fn The async function to track.
   * @returns The same promise returned by `fn`.
   */
  track<R>(fn: () => Promise<R>): Promise<R> {
    this.inFlightTotal++;
    const done = () => {
      this.inFlightTotal--;
      if (this.inFlightTotal === 0) this.drainResolvers.splice(0).forEach((r) => r());
    };
    try {
      return fn().finally(done);
    } catch (err) {
      // fn threw synchronously — without this the counter would leak and
      // waitForDrain would time out for the lifetime of the process.
      done();
      throw err;
    }
  }

  /**
   * Resolve when all tracked handlers have completed, or after `timeoutMs` elapses.
   * Logs a warning (via the injected `warn` callback) if the timeout is hit before draining.
   * Returns immediately if there are no in-flight handlers.
   * @param timeoutMs Maximum time to wait in milliseconds before resolving anyway.
   */
  waitForDrain(timeoutMs: number): Promise<void> {
    if (this.inFlightTotal === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      let handle: ReturnType<typeof setTimeout>;
      const onDrain = () => { clearTimeout(handle); resolve(); };
      this.drainResolvers.push(onDrain);
      handle = setTimeout(() => {
        const idx = this.drainResolvers.indexOf(onDrain);
        if (idx !== -1) this.drainResolvers.splice(idx, 1);
        this.warn(
          `Drain timed out after ${timeoutMs}ms — ${this.inFlightTotal} handler(s) still in flight`,
        );
        resolve();
      }, timeoutMs);
    });
  }
}
