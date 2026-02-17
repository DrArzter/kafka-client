import "reflect-metadata";
import {
  TestTopics,
  createClient,
  waitForMessages,
  getBrokers,
  SchemaValidTopic,
  SchemaInvalidTopic,
  SchemaSendTopic,
  KafkaClient,
  KafkaValidationError,
  ConsumerInterceptor,
} from "./helpers";

describe("Integration — Schema Validation", () => {
  it("should validate and deliver valid messages with schema descriptor", async () => {
    const client = createClient("schema-valid");
    await client.connectProducer();

    const { messages, promise } =
      waitForMessages<TestTopics["test.schema-valid"]>(1);

    await client.startConsumer(
      [SchemaValidTopic] as any,
      async (envelope) => {
        messages.push(envelope.payload);
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
      onError: (envelope, error) => {
        capturedError = error;
      },
    };

    await client.startConsumer(
      [SchemaInvalidTopic] as any,
      async (envelope) => {
        handlerCalls.push(envelope.payload);
      },
      { fromBeginning: true, dlq: true, interceptors: [interceptor] },
    );

    // Send a raw invalid message via a separate client with strictSchemas disabled
    const sender = new KafkaClient<TestTopics>(
      "integration-schema-invalid-sender",
      "group-sender",
      getBrokers(),
      { strictSchemas: false },
    );
    await sender.connectProducer();
    await sender.sendMessage("test.schema-invalid", {
      name: 123 as any,
      age: "not-a-number" as any,
    });

    await new Promise((r) => setTimeout(r, 5000));

    // Handler should NOT be called
    expect(handlerCalls).toHaveLength(0);

    // onError should have received KafkaValidationError
    expect(capturedError).toBeInstanceOf(KafkaValidationError);

    // DLQ should have the message
    const dlqMessages: any[] = [];
    const dlqClient = createClient("schema-invalid-dlq");
    await dlqClient.connectProducer();

    await dlqClient.startConsumer(
      ["test.schema-invalid.dlq" as keyof TestTopics],
      async (envelope) => {
        dlqMessages.push(envelope.payload);
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
});
