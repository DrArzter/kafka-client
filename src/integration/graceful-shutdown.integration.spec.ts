import "reflect-metadata";
import { TestTopics, createClient } from "./helpers";

describe("Integration — Graceful shutdown with drain", () => {
  it("should wait for an in-flight handler to complete before disconnect returns", async () => {
    const TOPIC = "test.graceful" satisfies keyof TestTopics;

    const client = createClient("graceful-drain");
    await client.connectProducer();

    let handlerCompleted = false;
    let handlerStarted = false;

    await client.startConsumer(
      [TOPIC],
      async () => {
        handlerStarted = true;
        // Slow handler — takes 600 ms
        await new Promise((r) => setTimeout(r, 600));
        handlerCompleted = true;
      },
      { fromBeginning: false },
    );

    // Wait for the consumer to join the group
    await new Promise((r) => setTimeout(r, 5_000));

    await client.sendMessage(TOPIC, { seq: 1 });

    // Wait until the handler has started but hasn't finished yet
    await new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + 10_000;
      const poll = setInterval(() => {
        if (handlerStarted) {
          clearInterval(poll);
          resolve();
        }
        if (Date.now() >= deadline) {
          clearInterval(poll);
          reject(new Error("handler never started"));
        }
      }, 50);
    });

    expect(handlerCompleted).toBe(false); // still running

    // disconnect() must drain — wait for the in-flight handler to settle
    await client.disconnect(10_000);

    // By the time disconnect() resolves, the handler must have completed
    expect(handlerCompleted).toBe(true);
  }, 30_000);

  it("should disconnect immediately when no handlers are in flight", async () => {
    const client = createClient("graceful-idle");
    await client.connectProducer();

    const start = Date.now();
    await client.disconnect();
    const elapsed = Date.now() - start;

    // No in-flight work — should be fast (well under 1 s)
    expect(elapsed).toBeLessThan(1_000);
  }, 10_000);

  it("should log a warning and proceed if drain timeout is exceeded", async () => {
    const TOPIC = "test.graceful" satisfies keyof TestTopics;

    const warnings: string[] = [];
    const client = createClient("graceful-timeout");
    // Inject a custom logger to capture warnings
    (client as any).logger = {
      log: () => {},
      warn: (msg: string) => warnings.push(msg),
      error: () => {},
    };

    await client.connectProducer();

    let handlerStarted = false;

    await client.startConsumer(
      [TOPIC],
      async () => {
        handlerStarted = true;
        // Very slow handler — outlasts the drain timeout
        await new Promise((r) => setTimeout(r, 3_000));
      },
      { fromBeginning: false },
    );

    await new Promise((r) => setTimeout(r, 5_000));

    await client.sendMessage(TOPIC, { seq: 2 });

    await new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + 10_000;
      const poll = setInterval(() => {
        if (handlerStarted) {
          clearInterval(poll);
          resolve();
        }
        if (Date.now() >= deadline) {
          clearInterval(poll);
          reject(new Error("handler never started"));
        }
      }, 50);
    });

    // Disconnect with a very short drain timeout (100 ms) — handler takes 3 s
    await client.disconnect(100);

    // Warning about drain timeout must have been logged
    expect(warnings.some((w) => w.includes("Drain timed out"))).toBe(true);
  }, 30_000);
});
