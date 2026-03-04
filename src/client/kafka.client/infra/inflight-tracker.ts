/** Tracks in-flight async handlers and provides a drain-wait mechanism. */
export class InFlightTracker {
  private inFlightTotal = 0;
  private readonly drainResolvers: Array<() => void> = [];

  constructor(private readonly warn: (msg: string) => void) {}

  track<R>(fn: () => Promise<R>): Promise<R> {
    this.inFlightTotal++;
    return fn().finally(() => {
      this.inFlightTotal--;
      if (this.inFlightTotal === 0) this.drainResolvers.splice(0).forEach((r) => r());
    });
  }

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
