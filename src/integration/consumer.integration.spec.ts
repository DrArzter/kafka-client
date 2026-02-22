import "reflect-metadata";
import {
  TestTopics,
  createClient,
  KafkaClient,
  getBrokers,
  KafkaHealthIndicator,
  ConsumerInterceptor,
} from "./helpers";

describe("Integration — Consumer Features", () => {
  it("should call consumer interceptors", async () => {
    const client = createClient("intercept");
    await client.connectProducer();

    const beforeCalls: string[] = [];
    const afterCalls: string[] = [];

    const interceptor: ConsumerInterceptor<TestTopics> = {
      before: (envelope) => {
        beforeCalls.push(envelope.topic);
      },
      after: (envelope) => {
        afterCalls.push(envelope.topic);
      },
    };

    const received: TestTopics["test.intercept"][] = [];

    await client.startConsumer(
      ["test.intercept"],
      async (envelope) => {
        received.push(envelope.payload);
      },
      { fromBeginning: true, interceptors: [interceptor] },
    );

    await client.sendMessage("test.intercept", { data: "intercepted" });

    await new Promise((r) => setTimeout(r, 5000));

    expect(received).toHaveLength(1);
    expect(beforeCalls).toContain("test.intercept");
    expect(afterCalls).toContain("test.intercept");

    await client.disconnect();
  });

  it("should consume messages in batch with startBatchConsumer", async () => {
    const client = createClient("batch-consume");
    await client.connectProducer();

    const batches: Array<{ messages: any[]; topic: string }> = [];
    const batchReady = new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 15_000);
      const check = setInterval(() => {
        if (batches.length >= 1) {
          clearTimeout(timer);
          clearInterval(check);
          resolve();
        }
      }, 100);
    });

    await client.startBatchConsumer(
      ["test.batch-consume"] as Array<keyof TestTopics>,
      async (envelopes, meta) => {
        batches.push({
          messages: envelopes.map((e) => e.payload),
          topic: envelopes[0]?.topic ?? "",
        });
      },
      { fromBeginning: true },
    );

    // Send 3 messages
    for (let i = 0; i < 3; i++) {
      await client.sendMessage("test.batch-consume", {
        id: i,
        text: `msg-${i}`,
      });
    }

    await batchReady;

    const allMessages = batches.flatMap((b) => b.messages);
    expect(allMessages.length).toBeGreaterThanOrEqual(3);
    expect(allMessages).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: 0, text: "msg-0" }),
        expect.objectContaining({ id: 1, text: "msg-1" }),
        expect.objectContaining({ id: 2, text: "msg-2" }),
      ]),
    );

    await client.disconnect();
  });

  it("should support multiple consumer groups on same topic", async () => {
    const client = createClient("multi-group");
    await client.connectProducer();

    const groupAMessages: any[] = [];
    const groupBMessages: any[] = [];

    const waitBoth = new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 15_000);
      const check = setInterval(() => {
        if (groupAMessages.length >= 1 && groupBMessages.length >= 1) {
          clearTimeout(timer);
          clearInterval(check);
          resolve();
        }
      }, 100);
    });

    await client.startConsumer(
      ["test.multi-group"] as Array<keyof TestTopics>,
      async (envelope) => {
        groupAMessages.push(envelope.payload);
      },
      { fromBeginning: true, groupId: `group-a-${Date.now()}` },
    );

    await client.startConsumer(
      ["test.multi-group"] as Array<keyof TestTopics>,
      async (envelope) => {
        groupBMessages.push(envelope.payload);
      },
      { fromBeginning: true, groupId: `group-b-${Date.now()}` },
    );

    await client.sendMessage("test.multi-group", { seq: 1 });

    await waitBoth;

    expect(groupAMessages).toHaveLength(1);
    expect(groupBMessages).toHaveLength(1);
    expect(groupAMessages[0]).toEqual({ seq: 1 });
    expect(groupBMessages[0]).toEqual({ seq: 1 });

    await client.disconnect();
  });

  it("should return topic list via checkStatus", async () => {
    const client = createClient("health");
    await client.connectProducer();

    await client.sendMessage("test.health", { probe: true });

    const status = await client.checkStatus();
    expect(status.status).toBe("up");
    if (status.status === "up") expect(status.topics).toContain("test.health");

    const health = new KafkaHealthIndicator();
    const result = await health.check(client);
    expect(result.status).toBe("up");
    if (result.status === "up") expect(result.topics).toContain("test.health");

    await client.disconnect();
  });

  it("should auto-create topics when autoCreateTopics is enabled", async () => {
    const topicName = `test.autocreate-${Date.now()}`;
    type AutoTopics = { [K: string]: { data: string } };

    const client = new KafkaClient<AutoTopics>(
      "integration-autocreate",
      `group-autocreate-${Date.now()}`,
      getBrokers(),
      { autoCreateTopics: true },
    );
    await client.connectProducer();

    await client.sendMessage(topicName, { data: "hello" });

    const status = await client.checkStatus();
    if (status.status === "up") expect(status.topics).toContain(topicName);

    await client.disconnect();
  });
});
