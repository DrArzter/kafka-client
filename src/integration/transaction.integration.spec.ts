import "reflect-metadata";
import { TestTopics, createClient } from "./helpers";

describe("Integration — Transaction", () => {
  it("should commit transaction atomically", async () => {
    const client = createClient("tx-ok");
    await client.connectProducer();

    const orders: TestTopics["test.tx-ok"][] = [];
    const audits: TestTopics["test.tx-audit"][] = [];

    await client.startConsumer(
      ["test.tx-ok"],
      async (envelope) => {
        orders.push(envelope.payload as TestTopics["test.tx-ok"]);
      },
      { fromBeginning: true },
    );

    const client2 = createClient("tx-audit");
    await client2.connectProducer();

    await client2.startConsumer(
      ["test.tx-audit"],
      async (envelope) => {
        audits.push(envelope.payload as TestTopics["test.tx-audit"]);
      },
      { fromBeginning: true },
    );

    await client.transaction(async (tx) => {
      await tx.send("test.tx-ok", { orderId: "order-1" });
      await tx.send("test.tx-audit", { action: "CREATED" });
    });

    await new Promise((r) => setTimeout(r, 5000));

    expect(orders).toContainEqual({ orderId: "order-1" });
    expect(audits).toContainEqual({ action: "CREATED" });

    await client.disconnect();
    await client2.disconnect();
  });

  it("should abort transaction on error", async () => {
    const client = createClient("tx-abort");
    await client.connectProducer();

    const received: TestTopics["test.tx-abort"][] = [];

    await client.startConsumer(
      ["test.tx-abort"],
      async (envelope) => {
        received.push(envelope.payload);
      },
      { fromBeginning: true },
    );

    await expect(
      client.transaction(async (tx) => {
        await tx.send("test.tx-abort", { orderId: "should-not-arrive" });
        throw new Error("Simulated failure");
      }),
    ).rejects.toThrow("Simulated failure");

    await new Promise((r) => setTimeout(r, 5000));
    expect(received).toHaveLength(0);

    await client.disconnect();
  });
});
