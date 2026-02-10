import "reflect-metadata";
import { KafkaContainer, StartedKafkaContainer } from "@testcontainers/kafka";
import { Kafka } from "kafkajs";
import { KafkaClient } from "../client/kafka.client";
import { KafkaHealthIndicator } from "../health/kafka.health";
import type { ConsumerInterceptor } from "../client/kafka.client";

type TestTopics = {
  "test.basic": { text: string; seq: number };
  "test.batch": { id: number };
  "test.tx-ok": { orderId: string };
  "test.tx-audit": { action: string };
  "test.tx-abort": { orderId: string };
  "test.retry": { value: string };
  "test.retry.dlq": { value: string };
  "test.intercept": { data: string };
  "test.beginning": { msg: string };
};

const ALL_TOPICS = Object.keys({
  "test.basic": 0,
  "test.batch": 0,
  "test.tx-ok": 0,
  "test.tx-audit": 0,
  "test.tx-abort": 0,
  "test.retry": 0,
  "test.retry.dlq": 0,
  "test.intercept": 0,
  "test.beginning": 0,
} satisfies Record<keyof TestTopics, unknown>);

let container: StartedKafkaContainer;
let brokers: string[];

beforeAll(async () => {
  container = await new KafkaContainer("confluentinc/cp-kafka:7.7.0")
    .withKraft()
    .withExposedPorts(9093)
    .withEnvironment({
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    })
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(9093);
  brokers = [`${host}:${port}`];

  const kafka = new Kafka({ clientId: "setup", brokers });

  // Pre-create topics
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: ALL_TOPICS.map((topic) => ({ topic, numPartitions: 1 })),
  });
  await admin.disconnect();

  // Wait for transaction coordinator to be ready (retries on the Kafka
  // client level, not producer — findGroupCoordinator is a cluster op)
  const warmupKafka = new Kafka({
    clientId: "warmup",
    brokers,
    retry: { retries: 30, initialRetryTime: 500, maxRetryTime: 30000 },
  });
  const txProducer = warmupKafka.producer({
    transactionalId: "warmup-tx",
    idempotent: true,
    maxInFlightRequests: 1,
  });
  await txProducer.connect();
  const tx = await txProducer.transaction();
  await tx.abort();
  await txProducer.disconnect();
}, 120_000);

afterAll(async () => {
  await container?.stop();
}, 30_000);

function createClient(name: string): KafkaClient<TestTopics> {
  return new KafkaClient<TestTopics>(
    `integration-${name}`,
    `group-${name}-${Date.now()}`,
    brokers,
  );
}

function waitForMessages<T>(
  count: number,
  timeout = 30_000,
): { messages: T[]; promise: Promise<T[]> } {
  const messages: T[] = [];
  const promise = new Promise<T[]>((res, rej) => {
    const timer = setTimeout(
      () => rej(new Error(`Timeout waiting for ${count} messages`)),
      timeout,
    );
    const interval = setInterval(() => {
      if (messages.length >= count) {
        clearTimeout(timer);
        clearInterval(interval);
        res(messages);
      }
    }, 100);
  });
  return { messages, promise };
}

describe("KafkaClient Integration", () => {
  it("should send and receive a message", async () => {
    const client = createClient("basic");
    await client.connectProducer();

    const { messages, promise } = waitForMessages<TestTopics["test.basic"]>(1);

    await client.startConsumer(
      ["test.basic"],
      async (msg) => {
        messages.push(msg);
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
      async (msg) => {
        messages.push(msg);
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

  it("should commit transaction atomically", async () => {
    const client = createClient("tx-ok");
    await client.connectProducer();

    const orders: TestTopics["test.tx-ok"][] = [];
    const audits: TestTopics["test.tx-audit"][] = [];

    await client.startConsumer(
      ["test.tx-ok"],
      async (msg) => {
        orders.push(msg as TestTopics["test.tx-ok"]);
      },
      { fromBeginning: true },
    );

    const client2 = createClient("tx-audit");
    await client2.connectProducer();

    await client2.startConsumer(
      ["test.tx-audit"],
      async (msg) => {
        audits.push(msg as TestTopics["test.tx-audit"]);
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
      async (msg) => {
        received.push(msg);
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

  it("should retry failed messages and send to DLQ", async () => {
    const client = createClient("retry");
    await client.connectProducer();

    let attempts = 0;

    await client.startConsumer(
      ["test.retry"],
      async () => {
        attempts++;
        throw new Error("Processing failed");
      },
      {
        fromBeginning: true,
        retry: { maxRetries: 2, backoffMs: 100 },
        dlq: true,
      },
    );

    const dlqMessages: TestTopics["test.retry"][] = [];
    const dlqClient = createClient("retry-dlq");
    await dlqClient.connectProducer();

    await dlqClient.startConsumer(
      ["test.retry.dlq" as keyof TestTopics],
      async (msg) => {
        dlqMessages.push(msg as TestTopics["test.retry"]);
      },
      { fromBeginning: true },
    );

    await client.sendMessage("test.retry", { value: "fail-me" });

    await new Promise((r) => setTimeout(r, 10000));

    // maxRetries: 2 → 1 original + 2 retries = 3 total
    expect(attempts).toBe(3);
    expect(dlqMessages.length).toBeGreaterThanOrEqual(1);
    expect(dlqMessages[0]).toEqual({ value: "fail-me" });

    await client.disconnect();
    await dlqClient.disconnect();
  });

  it("should call consumer interceptors", async () => {
    const client = createClient("intercept");
    await client.connectProducer();

    const beforeCalls: string[] = [];
    const afterCalls: string[] = [];

    const interceptor: ConsumerInterceptor<TestTopics> = {
      before: (_msg, topic) => {
        beforeCalls.push(topic as string);
      },
      after: (_msg, topic) => {
        afterCalls.push(topic as string);
      },
    };

    const received: TestTopics["test.intercept"][] = [];

    await client.startConsumer(
      ["test.intercept"],
      async (msg) => {
        received.push(msg);
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

  it("should return topic list via checkStatus", async () => {
    const client = createClient("health");
    await client.connectProducer();

    await client.sendMessage("test.basic", { text: "probe", seq: 0 });

    const status = await client.checkStatus();
    expect(status.topics).toContain("test.basic");

    const health = new KafkaHealthIndicator();
    const result = await health.check(client);
    expect(result.status).toBe("up");
    expect(result.topics).toContain("test.basic");

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
      async (msg) => {
        received.push(msg);
      },
      { fromBeginning: true },
    );

    await new Promise((r) => setTimeout(r, 5000));

    expect(received).toContainEqual({ msg: "old-message" });

    await client.disconnect();
  });
});
