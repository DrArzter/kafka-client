/**
 * Tests that use FakeTransport instead of jest.mock('@confluentinc/kafka-javascript').
 *
 * Each test creates its own isolated transport instance — no global mock state,
 * no jest.clearAllMocks(), no need to understand what the manual mock returns.
 * The transport is injected via KafkaClientOptions.transport.
 */

import { KafkaClient } from "../kafka.client";
import { FakeTransport } from "../../testing/fake-transport";

// ── Topic map used in these tests ─────────────────────────────────────────────

type Topics = {
  "orders.created": { orderId: string; amount: number };
  "payments.processed": { orderId: string; status: "ok" | "failed" };
};

function makeClient(transport: FakeTransport) {
  return new KafkaClient<Topics>("svc", "svc-group", [], { transport });
}

// ── Producer tests ────────────────────────────────────────────────────────────

describe("FakeTransport — producer", () => {
  it("captures sent messages with envelope headers", async () => {
    const transport = new FakeTransport();
    const client = makeClient(transport);
    await client.connectProducer();

    await client.sendMessage("orders.created", { orderId: "o1", amount: 100 });

    const sent = transport.mainProducer.sentTo("orders.created");
    expect(sent).toHaveLength(1);
    expect(JSON.parse(sent[0].value as string)).toEqual({ orderId: "o1", amount: 100 });
    expect(sent[0].headers).toMatchObject({
      "x-event-id": expect.any(String),
      "x-timestamp": expect.any(String),
    });
  });

  it("captures batch sends", async () => {
    const transport = new FakeTransport();
    const client = makeClient(transport);
    await client.connectProducer();

    await client.sendBatch("orders.created", [
      { message: { orderId: "o1", amount: 10 } },
      { message: { orderId: "o2", amount: 20 } },
    ]);

    expect(transport.mainProducer.sentTo("orders.created")).toHaveLength(2);
  });

  it("captures tombstones as null-value messages", async () => {
    const transport = new FakeTransport();
    const client = makeClient(transport);
    await client.connectProducer();

    await client.sendTombstone("orders.created", "o1");

    const sent = transport.mainProducer.sentTo("orders.created");
    expect(sent).toHaveLength(1);
    expect(sent[0].value).toBeNull();
    expect(sent[0].key).toBe("o1");
  });

  it("each client has its own isolated transport", async () => {
    const t1 = new FakeTransport();
    const t2 = new FakeTransport();
    const c1 = makeClient(t1);
    const c2 = makeClient(t2);
    await c1.connectProducer();
    await c2.connectProducer();

    await c1.sendMessage("orders.created", { orderId: "a", amount: 1 });

    expect(t1.mainProducer.sent).toHaveLength(1);
    expect(t2.mainProducer.sent).toHaveLength(0);
  });
});

// ── Consumer tests ────────────────────────────────────────────────────────────

describe("FakeTransport — consumer", () => {
  it("delivers a message to the handler via transport.deliver()", async () => {
    const transport = new FakeTransport();
    const client = makeClient(transport);
    await client.connectProducer();

    const received: Array<{ orderId: string; amount: number }> = [];

    await client.startConsumer(
      ["orders.created"],
      async (envelope) => {
        received.push(envelope.payload);
      },
    );

    await transport.deliver("orders.created", { orderId: "o1", amount: 99 });

    expect(received).toEqual([{ orderId: "o1", amount: 99 }]);
  });

  it("forwards headers to the handler", async () => {
    const transport = new FakeTransport();
    const client = makeClient(transport);
    await client.connectProducer();

    let receivedHeaders: Record<string, any> = {};

    await client.startConsumer(["orders.created"], async (envelope) => {
      receivedHeaders = envelope.headers;
    });

    await transport.deliver("orders.created", { orderId: "o2", amount: 5 }, {
      headers: { "x-correlation-id": "corr-123" },
    });

    expect(receivedHeaders["x-correlation-id"]).toBeDefined();
  });

  it("calls onMessageLost when handler throws and dlq is disabled", async () => {
    const transport = new FakeTransport();
    const lost: string[] = [];

    const client = new KafkaClient<Topics>("svc", "svc-group", [], {
      transport,
      onMessageLost: (ctx) => { lost.push(ctx.topic); },
    });
    await client.connectProducer();

    await client.startConsumer(
      ["orders.created"],
      async () => { throw new Error("processing failed"); },
    );

    await transport.deliver("orders.created", { orderId: "o3", amount: 1 });

    expect(lost).toEqual(["orders.created"]);
  });

  it("per-consumer onMessageLost overrides client-wide", async () => {
    const transport = new FakeTransport();
    const globalLost: string[] = [];
    const consumerLost: string[] = [];

    const client = new KafkaClient<Topics>("svc", "svc-group", [], {
      transport,
      onMessageLost: () => { globalLost.push("global"); },
    });
    await client.connectProducer();

    await client.startConsumer(
      ["orders.created"],
      async () => { throw new Error("fail"); },
      { onMessageLost: () => { consumerLost.push("consumer"); } },
    );

    await transport.deliver("orders.created", { orderId: "o4", amount: 1 });

    expect(consumerLost).toEqual(["consumer"]);
    expect(globalLost).toHaveLength(0);
  });
});

// ── Transaction tests ─────────────────────────────────────────────────────────

describe("FakeTransport — transactions", () => {
  it("commits staged sends atomically", async () => {
    const transport = new FakeTransport();
    const client = makeClient(transport);
    await client.connectProducer();

    await client.transaction(async (tx) => {
      await tx.send("orders.created", { orderId: "t1", amount: 50 });
      await tx.send("payments.processed", { orderId: "t1", status: "ok" });
    });

    // After commit, sends appear in mainProducer (tx producer is separate but both go through FakeProducer)
    const txProducer = transport.producers.find((p) => p.options?.transactionalId);
    expect(txProducer).toBeDefined();
    expect(txProducer!.lastTransaction.committed).toBe(true);
    expect(txProducer!.sentTo("orders.created")).toHaveLength(1);
    expect(txProducer!.sentTo("payments.processed")).toHaveLength(1);
  });

  it("aborts on handler error — no sends committed", async () => {
    const transport = new FakeTransport();
    const client = makeClient(transport);
    await client.connectProducer();

    await expect(
      client.transaction(async (tx) => {
        await tx.send("orders.created", { orderId: "t2", amount: 10 });
        throw new Error("handler crashed");
      }),
    ).rejects.toThrow("handler crashed");

    const txProducer = transport.producers.find((p) => p.options?.transactionalId);
    expect(txProducer!.lastTransaction.aborted).toBe(true);
    expect(txProducer!.sent).toHaveLength(0);
  });
});
