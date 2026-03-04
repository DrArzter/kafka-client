import {
  TestTopicMap,
  createClient,
  mockRun,
  KafkaClient,
} from "../helpers";
import type { KafkaInstrumentation } from "../../types";
import { KafkaJS } from "@confluentinc/kafka-javascript";

// ── Helpers ────────────────────────────────────────────────────────────

function deliverMessage(clock?: number) {
  const headers: Record<string, string[]> = {};
  if (clock !== undefined) headers["x-lamport-clock"] = [String(clock)];
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    await eachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
        headers,
        offset: "0",
      },
    });
  });
}

// ── Tests ──────────────────────────────────────────────────────────────

describe("KafkaClient — KafkaInstrumentation hooks (onRetry, onDlq, onDuplicate, onMessage)", () => {
  let client: KafkaClient<TestTopicMap>;
  let inst: Required<
    Pick<
      KafkaInstrumentation,
      "onRetry" | "onDlq" | "onDuplicate" | "onMessage"
    >
  >;

  beforeEach(() => {
    jest.clearAllMocks();
    inst = {
      onRetry: jest.fn(),
      onDlq: jest.fn(),
      onDuplicate: jest.fn(),
      onMessage: jest.fn(),
    };
    client = new KafkaClient<TestTopicMap>(
      "test-client",
      "test-group",
      ["localhost:9092"],
      { instrumentation: [inst] },
    );
  });

  it("onRetry is called with envelope, attempt and maxRetries on in-process retry", async () => {
    const handler = jest
      .fn()
      .mockRejectedValueOnce(new Error("fail"))
      .mockResolvedValue(undefined);
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 2, backoffMs: 1 },
    });

    expect(inst.onRetry).toHaveBeenCalledTimes(1);
    expect(inst.onRetry).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic" }),
      1,
      2,
    );
  });

  it("onDlq is called with envelope and 'handler-error' reason", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("fail"));
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      dlq: true,
    });

    expect(inst.onDlq).toHaveBeenCalledTimes(1);
    expect(inst.onDlq).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic" }),
      "handler-error",
    );
  });

  it("onDuplicate is called with envelope and strategy when duplicate detected", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (const clock of [10, 5]) {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
            headers: { "x-lamport-clock": [String(clock)] },
            offset: "0",
          },
        });
      }
    });

    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startConsumer(["test.topic"], handler, {
      deduplication: { strategy: "drop" },
    });

    expect(inst.onDuplicate).toHaveBeenCalledTimes(1);
    expect(inst.onDuplicate).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic" }),
      "drop",
    );
  });

  it("onMessage is called with the envelope on successful processing", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    deliverMessage();

    await client.startConsumer(["test.topic"], handler);

    expect(inst.onMessage).toHaveBeenCalledTimes(1);
    expect(inst.onMessage).toHaveBeenCalledWith(
      expect.objectContaining({ topic: "test.topic" }),
    );
  });

  it("onMessage is not called when the handler fails", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("fail"));
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      dlq: true,
    });

    expect(inst.onMessage).not.toHaveBeenCalled();
  });

  it("hooks and counters update together", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("fail"));
    deliverMessage();

    await client.startConsumer(["test.topic"], handler, {
      retry: { maxRetries: 1, backoffMs: 1 },
      dlq: true,
    });

    const metrics = client.getMetrics();
    expect(metrics.retryCount).toBe(1);
    expect(metrics.dlqCount).toBe(1);
    expect(inst.onDlq).toHaveBeenCalledTimes(1);
  });
});

describe("KafkaClient — configurable transactionalId", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("uses clientId-tx as default transactionalId", async () => {
    const client = new KafkaClient<TestTopicMap>("my-service", "my-group", [
      "localhost:9092",
    ]);
    await client.transaction(async () => {});

    const kafkaInstance = (KafkaJS.Kafka as jest.Mock).mock.results[0].value;
    const producerCalls = (kafkaInstance.producer as jest.Mock).mock.calls;
    const txCall = producerCalls.find(
      ([opts]: any[]) => opts?.kafkaJS?.transactionalId !== undefined,
    );
    expect(txCall?.[0].kafkaJS.transactionalId).toBe("my-service-tx");
  });

  it("uses custom transactionalId when provided in options", async () => {
    const client = new KafkaClient<TestTopicMap>(
      "my-service",
      "my-group",
      ["localhost:9092"],
      { transactionalId: "replica-1-tx" },
    );
    await client.transaction(async () => {});

    const kafkaInstance = (KafkaJS.Kafka as jest.Mock).mock.results[0].value;
    const producerCalls = (kafkaInstance.producer as jest.Mock).mock.calls;
    const txCall = producerCalls.find(
      ([opts]: any[]) => opts?.kafkaJS?.transactionalId !== undefined,
    );
    expect(txCall?.[0].kafkaJS.transactionalId).toBe("replica-1-tx");
  });
});

describe("KafkaClient — duplicate transactionalId warning", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("logs a warning when two clients with the same transactionalId connect in the same process", async () => {
    const logger = {
      log: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    };

    const client1 = new KafkaClient<TestTopicMap>("svc", "g1", ["localhost:9092"], {
      transactionalId: "shared-tx-id",
      logger,
    });
    const client2 = new KafkaClient<TestTopicMap>("svc", "g2", ["localhost:9092"], {
      transactionalId: "shared-tx-id",
      logger,
    });

    await client1.transaction(async () => {});
    await client2.transaction(async () => {});

    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining("shared-tx-id"),
    );

    await client1.disconnect();
    await client2.disconnect();
  });

  it("does not warn when two clients have different transactionalIds", async () => {
    const logger = {
      log: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    };

    const client1 = new KafkaClient<TestTopicMap>("svc", "g1", ["localhost:9092"], {
      transactionalId: "tx-replica-1",
      logger,
    });
    const client2 = new KafkaClient<TestTopicMap>("svc", "g2", ["localhost:9092"], {
      transactionalId: "tx-replica-2",
      logger,
    });

    await client1.transaction(async () => {});
    await client2.transaction(async () => {});

    const txWarn = (logger.warn as jest.Mock).mock.calls.filter(([msg]: [string]) =>
      msg.includes("transactionalId"),
    );
    expect(txWarn).toHaveLength(0);

    await client1.disconnect();
    await client2.disconnect();
  });
});

describe("KafkaClient — per-topic metrics", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  it("getMetrics(topic) returns zero for an unseen topic", () => {
    expect(client.getMetrics("test.topic")).toEqual({
      processedCount: 0,
      retryCount: 0,
      dlqCount: 0,
      dedupCount: 0,
    });
  });

  it("getMetrics(topic) counts only events for that topic", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    deliverMessage();
    await client.startConsumer(["test.topic"], handler);

    expect(client.getMetrics("test.topic").processedCount).toBe(1);
    expect(client.getMetrics("test.other").processedCount).toBe(0);
  });

  it("getMetrics() aggregate equals sum of per-topic metrics", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (const [topic, id] of [
        ["test.topic", "1"],
        ["test.other", "2"],
        ["test.topic", "3"],
      ] as [string, string][]) {
        await eachMessage({
          topic,
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id, value: 1, name: "x" })),
            headers: {},
            offset: id,
          },
        });
      }
    });

    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startConsumer(["test.topic", "test.other"], handler);

    expect(client.getMetrics("test.topic").processedCount).toBe(2);
    expect(client.getMetrics("test.other").processedCount).toBe(1);
    expect(client.getMetrics().processedCount).toBe(3);
  });

  it("resetMetrics(topic) resets only the specified topic", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (const [topic, id] of [
        ["test.topic", "1"],
        ["test.other", "2"],
      ] as [string, string][]) {
        await eachMessage({
          topic,
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id, value: 1, name: "x" })),
            headers: {},
            offset: id,
          },
        });
      }
    });

    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startConsumer(["test.topic", "test.other"], handler);

    expect(client.getMetrics().processedCount).toBe(2);

    client.resetMetrics("test.topic");

    expect(client.getMetrics("test.topic").processedCount).toBe(0);
    expect(client.getMetrics("test.other").processedCount).toBe(1);
    expect(client.getMetrics().processedCount).toBe(1);
  });

  it("resetMetrics() resets all topics", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      for (const [topic, id] of [
        ["test.topic", "1"],
        ["test.other", "2"],
      ] as [string, string][]) {
        await eachMessage({
          topic,
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id, value: 1, name: "x" })),
            headers: {},
            offset: id,
          },
        });
      }
    });

    const handler = jest.fn().mockResolvedValue(undefined);
    await client.startConsumer(["test.topic", "test.other"], handler);

    expect(client.getMetrics().processedCount).toBe(2);

    client.resetMetrics();

    expect(client.getMetrics()).toEqual({
      processedCount: 0,
      retryCount: 0,
      dlqCount: 0,
      dedupCount: 0,
    });
  });
});
