/**
 * Integration tests for OTel span activation via BeforeConsumeResult.wrap.
 *
 * Two complementary perspectives are tested:
 *
 * 1. **Custom instrumentation** — a hand-rolled BeforeConsumeResult with a
 *    `wrap` function that stores a "current span ID" in an AsyncLocalStorage,
 *    then verifies the handler ran inside that scope.
 *
 * 2. **otelInstrumentation()** — registers a minimal in-process OTel
 *    TracerProvider (no SDK package needed) and verifies that
 *    `trace.getActiveSpan()` returns the consume span inside the handler.
 *
 * All tests use `fromBeginning: true` so messages sent before consumer group
 * assignment (which takes ~25 s) are still delivered. Sentinel payload values
 * isolate each test's assertions from messages left by earlier tests on the
 * same topic.
 */

import "reflect-metadata";
import { AsyncLocalStorage } from "async_hooks";
import {
  trace,
  context,
  type Span,
  type SpanContext,
  type Tracer,
  type TracerProvider,
  type SpanOptions,
  type Context,
  SpanStatusCode,
  ROOT_CONTEXT,
  TraceFlags,
} from "@opentelemetry/api";
import { waitForMessages, getBrokers, KafkaClient } from "./helpers";
import type { TestTopics } from "./helpers";
import type { KafkaInstrumentation, BeforeConsumeResult } from "../client/types";
import { otelInstrumentation } from "../otel";

// ── Minimal in-process OTel implementation ───────────────────────────────────
//
// We implement just enough of the OTel API interfaces to make
// context.with() propagate the active span through async call stacks.
// This avoids any dependency on @opentelemetry/sdk-trace-base.

class SimpleSpan implements Span {
  private _ended = false;
  readonly status = { code: SpanStatusCode.UNSET };
  readonly recordedExceptions: Error[] = [];
  private readonly _ctx: SpanContext;

  constructor(readonly name: string) {
    this._ctx = {
      traceId: Math.random().toString(16).slice(2).padEnd(32, "0"),
      spanId: Math.random().toString(16).slice(2).padEnd(16, "0"),
      traceFlags: TraceFlags.SAMPLED,
      isRemote: false,
    };
  }

  spanContext(): SpanContext { return this._ctx; }
  setAttribute() { return this; }
  setAttributes() { return this; }
  addEvent() { return this; }
  addLink() { return this; }
  addLinks() { return this; }
  setStatus(s: { code: SpanStatusCode }) {
    (this.status as any).code = s.code;
    return this;
  }
  updateName() { return this; }
  end() { this._ended = true; }
  isRecording() { return !this._ended; }
  recordException(e: Error) { this.recordedExceptions.push(e); return this; }
}

class SimpleTracer implements Tracer {
  readonly spans: SimpleSpan[] = [];

  startSpan(name: string, _options?: SpanOptions, _ctx?: Context): Span {
    const span = new SimpleSpan(name);
    this.spans.push(span);
    return span;
  }

  startActiveSpan(name: string, ...args: any[]): any {
    const cb = args.find((a) => typeof a === "function");
    const span = this.startSpan(name);
    const ctx = trace.setSpan(context.active(), span);
    return context.with(ctx, cb, undefined, span);
  }
}

class SimpleTracerProvider implements TracerProvider {
  readonly tracer = new SimpleTracer();
  getTracer() { return this.tracer; }
}

// ── ALS-backed context manager ───────────────────────────────────────────────
//
// The default @opentelemetry/api context manager is a no-op.
// We register an ALS-based one so context.with(spanCtx, fn) actually
// propagates spanCtx across awaits inside fn.

const alsStorage = new AsyncLocalStorage<Context>();

const alsContextManager = {
  active(): Context { return alsStorage.getStore() ?? ROOT_CONTEXT; },
  with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(
    ctx: Context, fn: F, _thisArg?: unknown, ...args: A
  ): ReturnType<F> {
    return alsStorage.run(ctx, fn, ...args);
  },
  bind<T>(_ctx: Context, fn: T): T { return fn; },
  enable() { return this; },
  disable() { return this; },
};

// ── Suite ────────────────────────────────────────────────────────────────────

describe("Integration — Instrumentation / OTel span activation", () => {
  let provider: SimpleTracerProvider;

  beforeAll(() => {
    context.setGlobalContextManager(alsContextManager as any);
    provider = new SimpleTracerProvider();
    trace.setGlobalTracerProvider(provider);
  });

  // Helper: span name prefix for the test.otel topic
  const isConsumeSpan = (s: SimpleSpan) => s.name.startsWith("kafka.consume");

  // ── 1. Custom wrap: handler runs inside the wrap's async context ─────────────

  it("custom BeforeConsumeResult.wrap: handler runs inside the wrap's async context", async () => {
    const handlerSpanIds: string[] = [];
    const storage = new AsyncLocalStorage<string>();

    const inst: KafkaInstrumentation = {
      beforeConsume(envelope): BeforeConsumeResult {
        const id = `span-${envelope.eventId.slice(0, 8)}`;
        return { wrap: (fn) => storage.run(id, fn) };
      },
    };

    const client = new KafkaClient<TestTopics>(
      "integration-otel-wrap",
      `group-otel-wrap-${Date.now()}`,
      getBrokers(),
      { instrumentation: [inst] },
    );
    await client.connectProducer();

    const received = waitForMessages<TestTopics["test.otel"]>(1);
    await client.startConsumer(
      ["test.otel"],
      async (envelope) => {
        // Sentinel: only capture for this test's own message
        if (envelope.payload.value === "wrap-test") {
          handlerSpanIds.push(storage.getStore() ?? "no-context");
          received.messages.push(envelope.payload);
        }
      },
      { fromBeginning: true },
    );

    await client.sendMessage("test.otel", { value: "wrap-test" });
    await received.promise;

    expect(handlerSpanIds).toHaveLength(1);
    expect(handlerSpanIds[0]).toMatch(/^span-/);

    await client.disconnect();
  }, 60_000);

  // ── 2. otelInstrumentation: span is active during handler ───────────────────

  it("otelInstrumentation: trace.getActiveSpan() returns the consume span inside the handler", async () => {
    const capturedSpans: (Span | undefined)[] = [];
    const received = waitForMessages<TestTopics["test.otel"]>(1);

    const client = new KafkaClient<TestTopics>(
      "integration-otel-active",
      `group-otel-active-${Date.now()}`,
      getBrokers(),
      { instrumentation: [otelInstrumentation()] },
    );
    await client.connectProducer();

    await client.startConsumer(
      ["test.otel"],
      async (envelope) => {
        if (envelope.payload.value === "active-span-test") {
          capturedSpans.push(trace.getActiveSpan());
          received.messages.push(envelope.payload);
        }
      },
      { fromBeginning: true },
    );

    await client.sendMessage("test.otel", { value: "active-span-test" });
    await received.promise;

    expect(capturedSpans).toHaveLength(1);
    expect(capturedSpans[0]).toBeDefined();

    // The captured span must be the last consumer span recorded by our tracer
    // ("active-span-test" is the last message in the topic at this point)
    const consumeSpans = provider.tracer.spans.filter(isConsumeSpan);
    expect(consumeSpans.length).toBeGreaterThanOrEqual(1);
    expect(capturedSpans[0]).toBe(consumeSpans.at(-1));

    await client.disconnect();
  }, 60_000);

  // ── 3. span.isRecording() is false after cleanup ────────────────────────────

  it("otelInstrumentation: span.isRecording() is false after the handler resolves", async () => {
    const received = waitForMessages<TestTopics["test.otel"]>(1);

    const client = new KafkaClient<TestTopics>(
      "integration-otel-end",
      `group-otel-end-${Date.now()}`,
      getBrokers(),
      { instrumentation: [otelInstrumentation()] },
    );
    await client.connectProducer();

    await client.startConsumer(
      ["test.otel"],
      async (envelope) => {
        if (envelope.payload.value === "span-end-test") {
          received.messages.push(envelope.payload);
        }
      },
      { fromBeginning: true },
    );

    await client.sendMessage("test.otel", { value: "span-end-test" });
    await received.promise;
    // cleanup() runs synchronously after the handler inside runHandlerWithPipeline;
    // the 100 ms wait ensures the eachMessage callback has fully unwound.
    await new Promise((r) => setTimeout(r, 100));

    const consumeSpans = provider.tracer.spans.filter(isConsumeSpan);
    expect(consumeSpans.length).toBeGreaterThanOrEqual(1);
    // The span for "span-end-test" was the last one created by this consumer
    expect(consumeSpans.at(-1)!.isRecording()).toBe(false);

    await client.disconnect();
  }, 60_000);

  // ── 4. onConsumeError records exception and sets ERROR status ────────────────

  it("otelInstrumentation: onConsumeError records exception and sets ERROR status", async () => {
    const handlerError = new Error("otel-handler-fail");
    const received = waitForMessages<TestTopics["test.otel"]>(1);
    let threw = false;

    const client = new KafkaClient<TestTopics>(
      "integration-otel-err",
      `group-otel-err-${Date.now()}`,
      getBrokers(),
      { instrumentation: [otelInstrumentation()] },
    );
    await client.connectProducer();

    await client.startConsumer(
      ["test.otel"],
      async (envelope) => {
        // Only throw for this test's own sentinel message
        if (envelope.payload.value === "error-test") {
          received.messages.push(envelope.payload);
          threw = true;
          throw handlerError;
        }
      },
      { fromBeginning: true },
    );

    await client.sendMessage("test.otel", { value: "error-test" });
    await received.promise;
    await new Promise((r) => setTimeout(r, 200));

    expect(threw).toBe(true);

    const consumeSpans = provider.tracer.spans.filter(isConsumeSpan);
    // The last span belongs to "error-test" (last message in the topic)
    const errSpan = consumeSpans.at(-1)!;
    expect(errSpan.status.code).toBe(SpanStatusCode.ERROR);
    expect(errSpan.recordedExceptions).toContain(handlerError);

    await client.disconnect();
  }, 60_000);
});
