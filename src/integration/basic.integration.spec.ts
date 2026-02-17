import "reflect-metadata";
import {
  TestTopics,
  TestDescriptor,
  createClient,
  waitForMessages,
  KafkaHealthIndicator,
} from "./helpers";

describe("Integration — Basic", () => {
  it("should send and receive a message", async () => {
    const client = createClient("basic");
    await client.connectProducer();

    const { messages, promise } = waitForMessages<TestTopics["test.basic"]>(1);

    await client.startConsumer(
      ["test.basic"],
      async (envelope) => {
        messages.push(envelope.payload);
      },
      { fromBeginning: true },
    );

    await client.sendMessage("test.basic", { text: "hello", seq: 1 });

    const result = await promise;
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ text: "hello", seq: 1 });

    await client.disconnect();
  });

  it("should send and receive a batch of messages", async () => {
    const client = createClient("batch");
    await client.connectProducer();

    const { messages, promise } = waitForMessages<TestTopics["test.batch"]>(3);

    await client.startConsumer(
      ["test.batch"],
      async (envelope) => {
        messages.push(envelope.payload);
      },
      { fromBeginning: true },
    );

    await client.sendBatch("test.batch", [
      { value: { id: 1 } },
      { value: { id: 2 } },
      { value: { id: 3 } },
    ]);

    const result = await promise;
    expect(result).toHaveLength(3);
    expect(result.map((m) => m.id).sort()).toEqual([1, 2, 3]);

    await client.disconnect();
  });

  it("should read old messages with fromBeginning", async () => {
    const client = createClient("beginning");
    await client.connectProducer();

    await client.sendMessage("test.beginning", { msg: "old-message" });
    await new Promise((r) => setTimeout(r, 2000));

    const received: TestTopics["test.beginning"][] = [];

    await client.startConsumer(
      ["test.beginning"],
      async (envelope) => {
        received.push(envelope.payload);
      },
      { fromBeginning: true },
    );

    await new Promise((r) => setTimeout(r, 5000));

    expect(received).toContainEqual({ msg: "old-message" });

    await client.disconnect();
  });

  it("should send and receive via TopicDescriptor", async () => {
    const client = createClient("descriptor");
    await client.connectProducer();

    const { messages, promise } =
      waitForMessages<TestTopics["test.descriptor"]>(2);

    await client.startConsumer(
      [TestDescriptor] as any,
      async (envelope) => {
        messages.push(envelope.payload);
      },
      { fromBeginning: true },
    );

    await client.sendMessage(TestDescriptor, { label: "one", num: 1 });
    await client.sendBatch(TestDescriptor, [
      { value: { label: "two", num: 2 } },
    ]);

    const result = await promise;
    expect(result).toHaveLength(2);
    expect(result).toContainEqual({ label: "one", num: 1 });
    expect(result).toContainEqual({ label: "two", num: 2 });

    await client.disconnect();
  });

  it("should preserve partition key ordering", async () => {
    const client = createClient("key-order");
    await client.connectProducer();

    const received: TestTopics["test.key-order"][] = [];

    await client.startConsumer(
      ["test.key-order"],
      async (envelope) => {
        received.push(envelope.payload);
      },
      { fromBeginning: true },
    );

    // Send messages with same key — should arrive in order
    for (let i = 1; i <= 5; i++) {
      await client.sendMessage(
        "test.key-order",
        { seq: i },
        { key: "same-key" },
      );
    }

    await new Promise((r) => setTimeout(r, 5000));

    expect(received).toHaveLength(5);
    expect(received.map((m) => m.seq)).toEqual([1, 2, 3, 4, 5]);

    await client.disconnect();
  });

  it("should pass message headers through send and consume", async () => {
    const client = createClient("headers");
    await client.connectProducer();

    const { messages, promise } =
      waitForMessages<TestTopics["test.headers"]>(1);

    await client.startConsumer(
      ["test.headers"],
      async (envelope) => {
        messages.push(envelope.payload);
      },
      { fromBeginning: true },
    );

    await client.sendMessage(
      "test.headers",
      { body: "with-headers" },
      {
        key: "h1",
        headers: { "x-trace-id": "trace-123", "x-source": "test" },
      },
    );

    const result = await promise;
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ body: "with-headers" });

    await client.disconnect();
  });
});
