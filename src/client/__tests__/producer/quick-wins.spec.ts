import { createClient, mockSend, topic } from "../helpers";
import { versionedSchema } from "../../message/versioned-schema";
import { KafkaClient } from "../../kafka.client";

describe("KafkaClient — typed keys via topic().key()", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("stamps the extracted key on sendMessage", async () => {
    const client = createClient();
    const Described = topic("test.topic")
      .type<{ id: string; value: number }>()
      .key((m) => m.id);

    await client.sendMessage(Described as any, { id: "k-42", value: 1 });

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "test.topic",
        messages: [expect.objectContaining({ key: "k-42" })],
      }),
    );
  });

  it("explicit SendOptions.key wins over the extractor", async () => {
    const client = createClient();
    const Described = topic("test.topic")
      .type<{ id: string; value: number }>()
      .key((m) => m.id);

    await client.sendMessage(
      Described as any,
      { id: "k-42", value: 1 },
      { key: "explicit" },
    );

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        messages: [expect.objectContaining({ key: "explicit" })],
      }),
    );
  });

  it("applies the extractor per message in sendBatch", async () => {
    const client = createClient();
    const Described = topic("test.topic")
      .type<{ id: string; value: number }>()
      .key((m) => m.id);

    await client.sendBatch(Described as any, [
      { value: { id: "a", value: 1 } },
      { value: { id: "b", value: 2 }, key: "explicit-b" },
    ]);

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        messages: [
          expect.objectContaining({ key: "a" }),
          expect.objectContaining({ key: "explicit-b" }),
        ],
      }),
    );
  });

  it("descriptor without .key() keeps null key", async () => {
    const client = createClient();
    const Described = topic("test.topic").type<{ id: string; value: number }>();

    await client.sendMessage(Described as any, { id: "x", value: 1 });

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        messages: [expect.objectContaining({ key: null })],
      }),
    );
  });
});

describe("versionedSchema", () => {
  const v1 = {
    parse: (d: any) => {
      if (typeof d.amount !== "number") throw new Error("v1: amount required");
      return d;
    },
  };
  const v2 = {
    parse: (d: any) => {
      if (typeof d.amountMinor !== "number")
        throw new Error("v2: amountMinor required");
      return d;
    },
  };

  it("dispatches to the schema matching ctx.version", async () => {
    const schema = versionedSchema({ 1: v1, 2: v2 });
    await expect(
      schema.parse(
        { orderId: "1", amount: 5 },
        { topic: "t", headers: {}, version: 1 },
      ),
    ).resolves.toEqual({ orderId: "1", amount: 5 });
    await expect(
      schema.parse(
        { orderId: "1", amount: 5 },
        { topic: "t", headers: {}, version: 2 },
      ),
    ).rejects.toThrow("v2: amountMinor required");
  });

  it("uses the latest version when no parse context is given", async () => {
    const schema = versionedSchema({ 1: v1, 2: v2 });
    await expect(
      schema.parse({ orderId: "1", amountMinor: 500 }),
    ).resolves.toEqual({ orderId: "1", amountMinor: 500 });
  });

  it("migrates older versions to the latest shape", async () => {
    const schema = versionedSchema<{ orderId: string; amountMinor: number }>(
      { 1: v1, 2: v2 },
      {
        migrate: (data, from) =>
          from === 1
            ? { orderId: data.orderId, amountMinor: Math.round(data.amount * 100) }
            : data,
      },
    );
    await expect(
      schema.parse(
        { orderId: "1", amount: 4.2 },
        { topic: "t", headers: {}, version: 1 },
      ),
    ).resolves.toEqual({ orderId: "1", amountMinor: 420 });
  });

  it("throws a clear error for an unregistered version", async () => {
    const schema = versionedSchema({ 1: v1, 2: v2 });
    await expect(
      schema.parse({}, { topic: "orders", headers: {}, version: 3 }),
    ).rejects.toThrow(
      'versionedSchema: no schema registered for version 3 (topic "orders") — registered versions: 1, 2',
    );
  });

  it("throws at construction when no versions are registered", () => {
    expect(() => versionedSchema({})).toThrow(
      "at least one schema version must be registered",
    );
  });
});

describe("KafkaClient — constructor options validation", () => {
  it("rejects empty clientId, groupId, and brokers", () => {
    expect(
      () => new KafkaClient("", "", [], undefined),
    ).toThrow(/clientId must be a non-empty string/);
    expect(() => new KafkaClient("", "", [])).toThrow(
      /groupId must be a non-empty string/,
    );
    expect(() => new KafkaClient("c", "g", [])).toThrow(
      /brokers must be a non-empty array/,
    );
  });

  it("rejects invalid numPartitions and lagThrottle values", () => {
    expect(
      () =>
        new KafkaClient("c", "g", ["localhost:9092"], { numPartitions: 0 }),
    ).toThrow(/numPartitions must be a positive integer/);
    expect(
      () =>
        new KafkaClient("c", "g", ["localhost:9092"], {
          lagThrottle: { maxLag: -1 },
        }),
    ).toThrow(/lagThrottle\.maxLag must be >= 0/);
    expect(
      () =>
        new KafkaClient("c", "g", ["localhost:9092"], {
          lagThrottle: { maxLag: 10, pollIntervalMs: 0 },
        }),
    ).toThrow(/lagThrottle\.pollIntervalMs must be > 0/);
  });

  it("rejects invalid clockRecovery.timeoutMs and lists ALL problems at once", () => {
    let message = "";
    try {
      new KafkaClient("c", "g", [], {
        numPartitions: -2,
        clockRecovery: { topics: ["t"], timeoutMs: 0 },
      });
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toMatch(/brokers must be a non-empty array/);
    expect(message).toMatch(/numPartitions must be a positive integer/);
    expect(message).toMatch(/clockRecovery\.timeoutMs must be > 0/);
  });

  it("accepts a valid configuration", () => {
    expect(
      () =>
        new KafkaClient("c", "g", ["localhost:9092"], {
          numPartitions: 3,
          lagThrottle: { maxLag: 100, pollIntervalMs: 1000, maxWaitMs: 0 },
          clockRecovery: { topics: [], timeoutMs: 5000 },
        }),
    ).not.toThrow();
  });
});
