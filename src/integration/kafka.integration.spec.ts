import "reflect-metadata";
import { KafkaContainer, StartedKafkaContainer } from "@testcontainers/kafka";
import { Kafka, logLevel as KafkaLogLevel } from "kafkajs";
import { KafkaClient } from "../client/kafka.client";
import { KafkaHealthIndicator } from "../nest/kafka.health";
import { topic, SchemaLike } from "../client/topic";
import {
  KafkaRetryExhaustedError,
  KafkaValidationError,
} from "../client/errors";
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
  "test.descriptor": { label: string; num: number };
  "test.retry-error": { value: string };
  "test.retry-error.dlq": { value: string };
  "test.key-order": { seq: number };
  "test.headers": { body: string };
  "test.schema-valid": { name: string; age: number };
  "test.schema-invalid": { name: string; age: number };
  "test.schema-invalid.dlq": { name: string; age: number };
  "test.schema-send": { name: string; age: number };
  "test.batch-consume": { id: number; text: string };
  "test.multi-group": { seq: number };
};

const TestDescriptor = topic("test.descriptor")<{
  label: string;
  num: number;
}>();

const personSchema: SchemaLike<{ name: string; age: number }> = {
  parse(data: unknown) {
    const d = data as any;
    if (typeof d?.name !== "string") throw new Error("name must be a string");
    if (typeof d?.age !== "number") throw new Error("age must be a number");
    return { name: d.name, age: d.age };
  },
};

const SchemaValidTopic = topic("test.schema-valid").schema(personSchema);
const SchemaInvalidTopic = topic("test.schema-invalid").schema(personSchema);
const SchemaSendTopic = topic("test.schema-send").schema(personSchema);

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
  "test.descriptor": 0,
  "test.retry-error": 0,
  "test.retry-error.dlq": 0,
  "test.key-order": 0,
  "test.headers": 0,
  "test.schema-valid": 0,
  "test.schema-invalid": 0,
  "test.schema-invalid.dlq": 0,
  "test.schema-send": 0,
  "test.batch-consume": 0,
  "test.multi-group": 0,
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

  const kafka = new Kafka({
    clientId: "setup",
    brokers,
    logLevel: KafkaLogLevel.NOTHING,
  });

  // Pre-create topics
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: ALL_TOPICS.map((topic) => ({ topic, numPartitions: 1 })),
  });
  await admin.disconnect();

  // Warmup: trigger transaction coordinator initialization.
  // Without this, the first transactional producer hangs waiting for the coordinator.
  const warmupKafka = new Kafka({
    clientId: "warmup",
    brokers,
    logLevel: KafkaLogLevel.NOTHING,
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

  it("should send and receive via TopicDescriptor", async () => {
    const client = createClient("descriptor");
    await client.connectProducer();

    const { messages, promise } =
      waitForMessages<TestTopics["test.descriptor"]>(2);

    await client.startConsumer(
      [TestDescriptor] as any,
      async (msg) => {
        messages.push(msg);
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

  it("should auto-create topics when autoCreateTopics is enabled", async () => {
    const topicName = `test.autocreate-${Date.now()}`;
    type AutoTopics = { [K: string]: { data: string } };

    const client = new KafkaClient<AutoTopics>(
      "integration-autocreate",
      `group-autocreate-${Date.now()}`,
      brokers,
      { autoCreateTopics: true },
    );
    await client.connectProducer();

    // Send to a topic that doesn't exist yet — autoCreateTopics should create it
    await client.sendMessage(topicName, { data: "hello" });

    // Verify the topic was created by listing topics
    const status = await client.checkStatus();
    expect(status.topics).toContain(topicName);

    await client.disconnect();
  });

  it("should pass KafkaRetryExhaustedError to onError interceptor", async () => {
    const client = createClient("retry-error");
    await client.connectProducer();

    let capturedError: unknown = null;
    let attempts = 0;

    const interceptor: ConsumerInterceptor<TestTopics> = {
      onError: (_msg, _topic, error) => {
        capturedError = error;
      },
    };

    await client.startConsumer(
      ["test.retry-error"],
      async () => {
        attempts++;
        throw new Error("always fails");
      },
      {
        fromBeginning: true,
        retry: { maxRetries: 2, backoffMs: 100 },
        dlq: true,
        interceptors: [interceptor],
      },
    );

    await client.sendMessage("test.retry-error", { value: "test" });

    await new Promise((r) => setTimeout(r, 10000));

    expect(attempts).toBe(3);
    expect(capturedError).toBeInstanceOf(KafkaRetryExhaustedError);
    expect((capturedError as KafkaRetryExhaustedError).attempts).toBe(3);
    expect((capturedError as KafkaRetryExhaustedError).topic).toBe(
      "test.retry-error",
    );
    expect((capturedError as KafkaRetryExhaustedError).cause).toBeInstanceOf(
      Error,
    );

    await client.disconnect();
  });

  it("should preserve partition key ordering", async () => {
    const client = createClient("key-order");
    await client.connectProducer();

    const received: TestTopics["test.key-order"][] = [];

    await client.startConsumer(
      ["test.key-order"],
      async (msg) => {
        received.push(msg);
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
    // Same partition key → messages arrive in send order
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
      async (msg) => {
        messages.push(msg);
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

  it("should validate and deliver valid messages with schema descriptor", async () => {
    const client = createClient("schema-valid");
    await client.connectProducer();

    const { messages, promise } =
      waitForMessages<TestTopics["test.schema-valid"]>(1);

    await client.startConsumer(
      [SchemaValidTopic] as any,
      async (msg) => {
        messages.push(msg);
      },
      { fromBeginning: true },
    );

    await client.sendMessage(SchemaValidTopic as any, {
      name: "Alice",
      age: 30,
    });

    const result = await promise;
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ name: "Alice", age: 30 });

    await client.disconnect();
  });

  it("should reject invalid messages on consumer with schema and send to DLQ", async () => {
    const client = createClient("schema-invalid");
    await client.connectProducer();

    const handlerCalls: any[] = [];
    let capturedError: unknown = null;

    const interceptor: ConsumerInterceptor<TestTopics> = {
      onError: (_msg, _topic, error) => {
        capturedError = error;
      },
    };

    await client.startConsumer(
      [SchemaInvalidTopic] as any,
      async (msg) => {
        handlerCalls.push(msg);
      },
      { fromBeginning: true, dlq: true, interceptors: [interceptor] },
    );

    // Send a raw invalid message via a separate client with strictSchemas disabled
    // so it bypasses send-side validation and reaches the consumer
    const sender = new KafkaClient<TestTopics>(
      "integration-schema-invalid-sender",
      "group-sender",
      brokers,
      { strictSchemas: false },
    );
    await sender.connectProducer();
    await sender.sendMessage("test.schema-invalid", {
      name: 123 as any,
      age: "not-a-number" as any,
    });

    await new Promise((r) => setTimeout(r, 5000));

    // Handler should NOT be called — message failed schema validation
    expect(handlerCalls).toHaveLength(0);

    // onError should have received KafkaValidationError
    expect(capturedError).toBeInstanceOf(KafkaValidationError);

    // DLQ should have the message
    const dlqMessages: any[] = [];
    const dlqClient = createClient("schema-invalid-dlq");
    await dlqClient.connectProducer();

    await dlqClient.startConsumer(
      ["test.schema-invalid.dlq" as keyof TestTopics],
      async (msg) => {
        dlqMessages.push(msg);
      },
      { fromBeginning: true },
    );

    await new Promise((r) => setTimeout(r, 5000));
    expect(dlqMessages.length).toBeGreaterThanOrEqual(1);

    await client.disconnect();
    await sender.disconnect();
    await dlqClient.disconnect();
  });

  it("should reject invalid messages on send with schema descriptor", async () => {
    const client = createClient("schema-send");
    await client.connectProducer();

    await expect(
      client.sendMessage(SchemaSendTopic as any, {
        name: 42 as any,
        age: "bad" as any,
      }),
    ).rejects.toThrow();

    await client.disconnect();
  });

  it("should reject string topic send via strictSchemas after descriptor registers schema", async () => {
    const client = createClient("strict-schema");
    await client.connectProducer();

    // First send via descriptor — registers the schema in the internal registry
    await client.sendMessage(SchemaSendTopic as any, {
      name: "Alice",
      age: 30,
    });

    // Now sending via string topic with invalid data should throw
    await expect(
      client.sendMessage("test.schema-send", {
        name: 42 as any,
        age: "bad" as any,
      }),
    ).rejects.toThrow("name must be a string");

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
      async (messages, topic) => {
        batches.push({ messages: [...messages], topic });
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

    // Handler should have received messages as array(s)
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
      async (msg) => {
        groupAMessages.push(msg);
      },
      { fromBeginning: true, groupId: `group-a-${Date.now()}` },
    );

    await client.startConsumer(
      ["test.multi-group"] as Array<keyof TestTopics>,
      async (msg) => {
        groupBMessages.push(msg);
      },
      { fromBeginning: true, groupId: `group-b-${Date.now()}` },
    );

    await client.sendMessage("test.multi-group", { seq: 1 });

    await waitBoth;

    // Both groups should receive the same message
    expect(groupAMessages).toHaveLength(1);
    expect(groupBMessages).toHaveLength(1);
    expect(groupAMessages[0]).toEqual({ seq: 1 });
    expect(groupBMessages[0]).toEqual({ seq: 1 });

    await client.disconnect();
  });
});
