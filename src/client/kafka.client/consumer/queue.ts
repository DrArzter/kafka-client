/** Push-to-pull async queue used by consume() to bridge Kafka's push model to AsyncIterableIterator. */
export class AsyncQueue<V> {
  private readonly items: V[] = [];
  private readonly waiting: Array<{
    resolve: (r: IteratorResult<V>) => void;
    reject: (err: Error) => void;
  }> = [];
  private closed = false;
  private error?: Error;
  private paused = false;

  constructor(
    private readonly highWaterMark = Infinity,
    private readonly onFull: () => void = () => {},
    private readonly onDrained: () => void = () => {},
  ) {}

  push(item: V): void {
    if (this.waiting.length > 0) {
      this.waiting.shift()!.resolve({ value: item, done: false });
    } else {
      this.items.push(item);
      if (!this.paused && this.items.length >= this.highWaterMark) {
        this.paused = true;
        this.onFull();
      }
    }
  }

  fail(err: Error): void {
    this.closed = true;
    this.error = err;
    for (const { reject } of this.waiting.splice(0)) reject(err);
  }

  close(): void {
    this.closed = true;
    for (const { resolve } of this.waiting.splice(0))
      resolve({ value: undefined as any, done: true });
  }

  next(): Promise<IteratorResult<V>> {
    if (this.error) return Promise.reject(this.error);
    if (this.items.length > 0) {
      const value = this.items.shift()!;
      if (
        this.paused &&
        this.items.length <= Math.floor(this.highWaterMark / 2)
      ) {
        this.paused = false;
        this.onDrained();
      }
      return Promise.resolve({ value, done: false });
    }
    if (this.closed) return Promise.resolve({ value: undefined as any, done: true });
    return new Promise((resolve, reject) => this.waiting.push({ resolve, reject }));
  }
}
