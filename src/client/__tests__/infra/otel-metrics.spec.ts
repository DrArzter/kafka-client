import { otelMetricsInstrumentation, otelLagGauge } from "../../../otel";
import type { EventEnvelope } from "../../message/envelope";

// ── Fake Meter ────────────────────────────────────────────────────────────────
// Recording fakes for counters, histograms, and observable gauges. We do NOT
// pull in @opentelemetry/sdk-metrics — the instrumentation only needs a Meter
// whose instruments record calls, which we inject via the `meter` option.

interface FakeCounter {
  add: jest.Mock;
}
interface FakeHistogram {
  record: jest.Mock;
}
interface FakeGauge {
  addCallback: jest.Mock;
  removeCallback: jest.Mock;
  callbacks: Array<(result: FakeObservableResult) => void | Promise<void>>;
}
interface FakeObservableResult {
  observe: jest.Mock;
}

function makeFakeMeter() {
  const counters = new Map<string, FakeCounter>();
  const histograms = new Map<string, FakeHistogram>();
  const gauges = new Map<string, FakeGauge>();

  const meter = {
    createCounter: jest.fn((name: string) => {
      const c: FakeCounter = { add: jest.fn() };
      counters.set(name, c);
      return c;
    }),
    createHistogram: jest.fn((name: string) => {
      const h: FakeHistogram = { record: jest.fn() };
      histograms.set(name, h);
      return h;
    }),
    createObservableGauge: jest.fn((name: string) => {
      const g: FakeGauge = {
        callbacks: [],
        addCallback: jest.fn((cb) => g.callbacks.push(cb)),
        removeCallback: jest.fn((cb) => {
          g.callbacks = g.callbacks.filter((c) => c !== cb);
        }),
      };
      gauges.set(name, g);
      return g;
    }),
  };

  return { meter, counters, histograms, gauges };
}

function makeEnvelope(topic = "orders"): EventEnvelope<any> {
  return {
    payload: {},
    topic,
    partition: 0,
    offset: "5",
    timestamp: new Date().toISOString(),
    eventId: "evt-1",
    correlationId: "corr",
    schemaVersion: 1,
    headers: {},
  };
}

describe("otelMetricsInstrumentation", () => {
  it("creates each instrument exactly once per instance", () => {
    const { meter } = makeFakeMeter();

    otelMetricsInstrumentation({ meter: meter as any });

    expect(meter.createCounter).toHaveBeenCalledWith(
      "kafka.client.messages.sent",
      expect.anything(),
    );
    expect(meter.createCounter).toHaveBeenCalledWith(
      "kafka.client.messages.processed",
      expect.anything(),
    );
    expect(meter.createCounter).toHaveBeenCalledWith(
      "kafka.client.messages.retried",
      expect.anything(),
    );
    expect(meter.createCounter).toHaveBeenCalledWith(
      "kafka.client.messages.dlq",
      expect.anything(),
    );
    expect(meter.createCounter).toHaveBeenCalledWith(
      "kafka.client.messages.duplicate",
      expect.anything(),
    );
    expect(meter.createCounter).toHaveBeenCalledWith(
      "kafka.client.consume.errors",
      expect.anything(),
    );
    expect(meter.createHistogram).toHaveBeenCalledWith(
      "kafka.client.consume.duration",
      expect.anything(),
    );
    // 6 counters + 1 histogram, created once each.
    expect(meter.createCounter).toHaveBeenCalledTimes(6);
    expect(meter.createHistogram).toHaveBeenCalledTimes(1);
  });

  it("afterSend increments the sent counter with topic attribute", () => {
    const { meter, counters } = makeFakeMeter();
    const inst = otelMetricsInstrumentation({ meter: meter as any });

    inst.afterSend!("orders");

    expect(counters.get("kafka.client.messages.sent")!.add).toHaveBeenCalledWith(
      1,
      { topic: "orders" },
    );
  });

  it("onMessage increments the processed counter with topic attribute", () => {
    const { meter, counters } = makeFakeMeter();
    const inst = otelMetricsInstrumentation({ meter: meter as any });

    inst.onMessage!(makeEnvelope("payments"));

    expect(
      counters.get("kafka.client.messages.processed")!.add,
    ).toHaveBeenCalledWith(1, { topic: "payments" });
  });

  it("onRetry increments the retried counter with topic attribute", () => {
    const { meter, counters } = makeFakeMeter();
    const inst = otelMetricsInstrumentation({ meter: meter as any });

    inst.onRetry!(makeEnvelope("orders"), 1, 3);

    expect(
      counters.get("kafka.client.messages.retried")!.add,
    ).toHaveBeenCalledWith(1, { topic: "orders" });
  });

  it("onDlq increments the dlq counter with topic and reason attributes", () => {
    const { meter, counters } = makeFakeMeter();
    const inst = otelMetricsInstrumentation({ meter: meter as any });

    inst.onDlq!(makeEnvelope("orders"), "handler-error");

    expect(counters.get("kafka.client.messages.dlq")!.add).toHaveBeenCalledWith(
      1,
      { topic: "orders", reason: "handler-error" },
    );
  });

  it("onDuplicate increments the duplicate counter with topic and strategy attributes", () => {
    const { meter, counters } = makeFakeMeter();
    const inst = otelMetricsInstrumentation({ meter: meter as any });

    inst.onDuplicate!(makeEnvelope("orders"), "dlq");

    expect(
      counters.get("kafka.client.messages.duplicate")!.add,
    ).toHaveBeenCalledWith(1, { topic: "orders", strategy: "dlq" });
  });

  it("onConsumeError increments the consume errors counter with topic attribute", () => {
    const { meter, counters } = makeFakeMeter();
    const inst = otelMetricsInstrumentation({ meter: meter as any });

    inst.onConsumeError!(makeEnvelope("orders"), new Error("boom"));

    expect(
      counters.get("kafka.client.consume.errors")!.add,
    ).toHaveBeenCalledWith(1, { topic: "orders" });
  });

  it("records the consume duration histogram on cleanup with a non-negative duration", () => {
    const { meter, histograms } = makeFakeMeter();
    const inst = otelMetricsInstrumentation({ meter: meter as any });

    const nowSpy = jest.spyOn(Date, "now");
    nowSpy.mockReturnValueOnce(1000); // beforeConsume start
    nowSpy.mockReturnValueOnce(1042); // cleanup end

    const result = inst.beforeConsume!(makeEnvelope("orders")) as {
      cleanup(): void;
    };
    result.cleanup();

    expect(
      histograms.get("kafka.client.consume.duration")!.record,
    ).toHaveBeenCalledWith(42, { topic: "orders" });

    nowSpy.mockRestore();
  });
});

describe("otelLagGauge", () => {
  it("registers an observable gauge and its callback reports stubbed lag entries", async () => {
    const { meter, gauges } = makeFakeMeter();
    const getConsumerLag = jest.fn().mockResolvedValue([
      { topic: "orders", partition: 0, lag: 12 },
      { topic: "orders", partition: 1, lag: 3 },
    ]);
    const kafka = { getConsumerLag } as any;

    otelLagGauge(kafka, { meter: meter as any, groupId: "billing" });

    const gauge = gauges.get("kafka.client.consumer.lag")!;
    expect(meter.createObservableGauge).toHaveBeenCalledWith(
      "kafka.client.consumer.lag",
      expect.anything(),
    );
    expect(gauge.addCallback).toHaveBeenCalledTimes(1);
    expect(gauge.callbacks).toHaveLength(1);

    const result: FakeObservableResult = { observe: jest.fn() };
    await gauge.callbacks[0]!(result);

    expect(getConsumerLag).toHaveBeenCalledWith("billing");
    expect(result.observe).toHaveBeenCalledTimes(2);
    expect(result.observe).toHaveBeenCalledWith(12, {
      topic: "orders",
      partition: 0,
      groupId: "billing",
    });
    expect(result.observe).toHaveBeenCalledWith(3, {
      topic: "orders",
      partition: 1,
      groupId: "billing",
    });
  });

  it("uses an empty-string groupId attribute when no groupId is provided", async () => {
    const { meter, gauges } = makeFakeMeter();
    const getConsumerLag = jest
      .fn()
      .mockResolvedValue([{ topic: "orders", partition: 0, lag: 5 }]);
    const kafka = { getConsumerLag } as any;

    otelLagGauge(kafka, { meter: meter as any });

    const result: FakeObservableResult = { observe: jest.fn() };
    await gauges.get("kafka.client.consumer.lag")!.callbacks[0]!(result);

    expect(getConsumerLag).toHaveBeenCalledWith(undefined);
    expect(result.observe).toHaveBeenCalledWith(5, {
      topic: "orders",
      partition: 0,
      groupId: "",
    });
  });

  it("swallows callback errors silently when getConsumerLag rejects", async () => {
    const { meter, gauges } = makeFakeMeter();
    const getConsumerLag = jest.fn().mockRejectedValue(new Error("broker down"));
    const kafka = { getConsumerLag } as any;

    otelLagGauge(kafka, { meter: meter as any });

    const result: FakeObservableResult = { observe: jest.fn() };
    await expect(
      gauges.get("kafka.client.consumer.lag")!.callbacks[0]!(result),
    ).resolves.toBeUndefined();
    expect(result.observe).not.toHaveBeenCalled();
  });

  it("unregister removes the observable callback", () => {
    const { meter, gauges } = makeFakeMeter();
    const kafka = { getConsumerLag: jest.fn().mockResolvedValue([]) } as any;

    const unregister = otelLagGauge(kafka, { meter: meter as any });

    const gauge = gauges.get("kafka.client.consumer.lag")!;
    expect(gauge.callbacks).toHaveLength(1);

    unregister();

    expect(gauge.removeCallback).toHaveBeenCalledTimes(1);
    expect(gauge.callbacks).toHaveLength(0);
  });
});
