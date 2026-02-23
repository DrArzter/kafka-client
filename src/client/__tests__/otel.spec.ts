const mockSpan = {
  end: jest.fn(),
  setStatus: jest.fn(),
  recordException: jest.fn(),
};
const mockSpanCtx = Symbol("spanCtx");
const mockParentCtx = Symbol("parentCtx");

jest.mock("@opentelemetry/api", () => ({
  trace: {
    getTracer: jest.fn(() => ({ startSpan: jest.fn(() => mockSpan) })),
    setSpan: jest.fn(() => mockSpanCtx),
  },
  context: {
    active: jest.fn(() => mockParentCtx),
    with: jest.fn((_ctx: unknown, fn: () => Promise<void>) => fn()),
  },
  propagation: {
    extract: jest.fn(() => mockParentCtx),
    inject: jest.fn(),
  },
  SpanKind: { CONSUMER: 0 },
  SpanStatusCode: { ERROR: 2, OK: 1, UNSET: 0 },
}));

import { trace, context, propagation } from "@opentelemetry/api";
import { otelInstrumentation } from "../../otel";
import type { EventEnvelope } from "../message/envelope";

function makeEnvelope(id = "evt-1"): EventEnvelope<any> {
  return {
    payload: {},
    topic: "orders",
    partition: 0,
    offset: "5",
    timestamp: new Date().toISOString(),
    eventId: id,
    correlationId: "corr",
    schemaVersion: 1,
    headers: { traceparent: "00-abc-def-01" },
  };
}

describe("otelInstrumentation", () => {
  const mockTracer = { startSpan: jest.fn(() => mockSpan) };

  beforeEach(() => {
    jest.clearAllMocks();
    (trace.getTracer as jest.Mock).mockReturnValue(mockTracer);
    (propagation.extract as jest.Mock).mockReturnValue(mockParentCtx);
    (trace.setSpan as jest.Mock).mockReturnValue(mockSpanCtx);
    (context.with as jest.Mock).mockImplementation((_ctx, fn) => fn());
  });

  describe("beforeConsume", () => {
    it("extracts parent context from envelope headers", () => {
      const inst = otelInstrumentation();
      const env = makeEnvelope();

      inst.beforeConsume!(env);

      expect(propagation.extract).toHaveBeenCalledWith(mockParentCtx, env.headers);
    });

    it("starts a CONSUMER span as child of extracted context", () => {
      const inst = otelInstrumentation();
      const env = makeEnvelope();

      inst.beforeConsume!(env);

      expect(mockTracer.startSpan).toHaveBeenCalledWith(
        `kafka.consume ${env.topic}`,
        expect.objectContaining({
          kind: 0, // SpanKind.CONSUMER
          attributes: expect.objectContaining({
            "messaging.system": "kafka",
            "messaging.destination.name": env.topic,
            "messaging.message.id": env.eventId,
          }),
        }),
        mockParentCtx,
      );
    });

    it("sets the span as active via trace.setSpan", () => {
      const inst = otelInstrumentation();
      const env = makeEnvelope();

      inst.beforeConsume!(env);

      expect(trace.setSpan).toHaveBeenCalledWith(mockParentCtx, mockSpan);
    });

    it("wrap calls context.with(spanCtx, fn) so span is active during handler", async () => {
      const inst = otelInstrumentation();
      const env = makeEnvelope();

      const result = inst.beforeConsume!(env) as {
        cleanup(): void;
        wrap(fn: () => Promise<void>): Promise<void>;
      };

      const handler = jest.fn().mockResolvedValue(undefined);
      await result.wrap(handler);

      expect(context.with).toHaveBeenCalledWith(mockSpanCtx, handler);
    });

    it("cleanup ends the span", () => {
      const inst = otelInstrumentation();
      const env = makeEnvelope();

      const result = inst.beforeConsume!(env) as { cleanup(): void };
      result.cleanup();

      expect(mockSpan.end).toHaveBeenCalledTimes(1);
    });
  });

  describe("onConsumeError", () => {
    it("records exception and sets ERROR status on the active span", () => {
      const inst = otelInstrumentation();
      const env = makeEnvelope();
      const err = new Error("handler failed");

      inst.beforeConsume!(env);
      inst.onConsumeError!(env, err);

      expect(mockSpan.setStatus).toHaveBeenCalledWith({
        code: 2, // SpanStatusCode.ERROR
        message: err.message,
      });
      expect(mockSpan.recordException).toHaveBeenCalledWith(err);
    });

    it("is a no-op if beforeConsume was never called for this eventId", () => {
      const inst = otelInstrumentation();
      const env = makeEnvelope("unknown-id");

      expect(() => inst.onConsumeError!(env, new Error("x"))).not.toThrow();
      expect(mockSpan.setStatus).not.toHaveBeenCalled();
    });
  });

  describe("beforeSend", () => {
    it("injects active context into headers", () => {
      const inst = otelInstrumentation();
      const headers: Record<string, string> = {};

      inst.beforeSend!("orders", headers);

      expect(propagation.inject).toHaveBeenCalledWith(mockParentCtx, headers);
    });
  });
});
