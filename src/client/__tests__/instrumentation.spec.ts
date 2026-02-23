import { runHandlerWithPipeline } from "../consumer/pipeline";
import type { EventEnvelope } from "../message/envelope";
import type { KafkaInstrumentation } from "../types";

function makeEnvelope(id = "test-id"): EventEnvelope<any> {
  return {
    payload: {},
    topic: "test.topic",
    partition: 0,
    offset: "0",
    timestamp: new Date().toISOString(),
    eventId: id,
    correlationId: "corr-1",
    schemaVersion: 1,
    headers: {},
  };
}

describe("runHandlerWithPipeline — BeforeConsumeResult", () => {
  it("legacy: () => void cleanup is called after handler", async () => {
    const cleanup = jest.fn();
    const inst: KafkaInstrumentation = { beforeConsume: () => cleanup };

    await runHandlerWithPipeline(async () => {}, [makeEnvelope()], [], [inst]);

    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it("object form: cleanup is called after handler succeeds", async () => {
    const cleanup = jest.fn();
    const order: string[] = [];
    const inst: KafkaInstrumentation = { beforeConsume: () => ({ cleanup }) };
    cleanup.mockImplementation(() => order.push("cleanup"));

    await runHandlerWithPipeline(
      async () => { order.push("handler"); },
      [makeEnvelope()],
      [],
      [inst],
    );

    expect(order).toEqual(["handler", "cleanup"]);
  });

  it("object form: cleanup is called after handler throws", async () => {
    const cleanup = jest.fn();
    const inst: KafkaInstrumentation = { beforeConsume: () => ({ cleanup }) };

    const err = await runHandlerWithPipeline(
      async () => { throw new Error("boom"); },
      [makeEnvelope()],
      [],
      [inst],
    );

    expect(err?.message).toBe("boom");
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it("object form: wrap runs the handler inside the provided context", async () => {
    const order: string[] = [];
    const inst: KafkaInstrumentation = {
      beforeConsume: () => ({
        wrap: async (fn) => {
          order.push("wrap-enter");
          await fn();
          order.push("wrap-exit");
        },
      }),
    };

    await runHandlerWithPipeline(
      async () => { order.push("handler"); },
      [makeEnvelope()],
      [],
      [inst],
    );

    expect(order).toEqual(["wrap-enter", "handler", "wrap-exit"]);
  });

  it("multiple wraps compose in declaration order (first instrumentation = outermost)", async () => {
    const order: string[] = [];
    const makeWrapInst = (name: string): KafkaInstrumentation => ({
      beforeConsume: () => ({
        wrap: async (fn) => {
          order.push(`${name}-enter`);
          await fn();
          order.push(`${name}-exit`);
        },
      }),
    });

    await runHandlerWithPipeline(
      async () => { order.push("handler"); },
      [makeEnvelope()],
      [],
      [makeWrapInst("A"), makeWrapInst("B")],
    );

    expect(order).toEqual(["A-enter", "B-enter", "handler", "B-exit", "A-exit"]);
  });

  it("object form with both cleanup and wrap: both are invoked", async () => {
    const cleanup = jest.fn();
    const wrapFn = jest.fn().mockImplementation((fn: () => Promise<void>) => fn());
    const inst: KafkaInstrumentation = {
      beforeConsume: () => ({ cleanup, wrap: wrapFn }),
    };

    await runHandlerWithPipeline(async () => {}, [makeEnvelope()], [], [inst]);

    expect(wrapFn).toHaveBeenCalledTimes(1);
    expect(cleanup).toHaveBeenCalledTimes(1);
  });

  it("wrap is not called when handler errors out before reaching it (interceptor throws)", async () => {
    // This test verifies the existing try/catch behaviour — wrap is still collected
    // before interceptors run, so if an interceptor throws, the handler never runs
    // but cleanup still fires.
    const cleanup = jest.fn();
    const wrapFn = jest.fn().mockImplementation((fn: () => Promise<void>) => fn());
    const inst: KafkaInstrumentation = {
      beforeConsume: () => ({ cleanup, wrap: wrapFn }),
    };

    const err = await runHandlerWithPipeline(
      async () => {},
      [makeEnvelope()],
      [{ before: async () => { throw new Error("interceptor fail"); } }],
      [inst],
    );

    expect(err?.message).toBe("interceptor fail");
    // wrap was composed into runFn but never reached because interceptor threw first
    expect(wrapFn).not.toHaveBeenCalled();
    expect(cleanup).toHaveBeenCalledTimes(1);
  });
});
