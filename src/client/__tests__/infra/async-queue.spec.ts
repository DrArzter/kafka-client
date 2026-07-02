import { AsyncQueue } from "../../kafka.client/consumer/queue";

describe("AsyncQueue", () => {
  it("drops items pushed after close() and returns { done: true }", async () => {
    const queue = new AsyncQueue<number>();

    queue.close();
    queue.push(42); // late push — must be dropped, nobody will consume it

    const result = await queue.next();
    expect(result).toEqual({ value: undefined, done: true });

    // The dropped item is not retained — a subsequent next() is still done.
    const again = await queue.next();
    expect(again).toEqual({ value: undefined, done: true });
  });

  it("delivers buffered items before close(), then reports done", async () => {
    const queue = new AsyncQueue<number>();

    queue.push(1);
    queue.push(2);
    queue.close();
    queue.push(3); // dropped

    await expect(queue.next()).resolves.toEqual({ value: 1, done: false });
    await expect(queue.next()).resolves.toEqual({ value: 2, done: false });
    await expect(queue.next()).resolves.toEqual({ value: undefined, done: true });
  });

  it("resolves a pending next() with done when close() is called", async () => {
    const queue = new AsyncQueue<number>();

    const pending = queue.next(); // suspends — queue is empty
    queue.close();

    await expect(pending).resolves.toEqual({ value: undefined, done: true });
  });
});
