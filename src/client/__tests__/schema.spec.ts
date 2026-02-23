import {
  TestTopicMap,
  createClient,
  envelopeWith,
  mockSend,
  mockRun,
  mockTxAbort,
  KafkaClient,
  topic,
  SchemaLike,
  KafkaValidationError,
} from "./helpers";

describe("KafkaClient — Schema Validation", () => {
  let client: KafkaClient<TestTopicMap>;

  const validMessage = { id: "1", value: 42 };
  const invalidMessage = { id: 123, value: "not a number" };

  const strictSchema: SchemaLike<{ id: string; value: number }> = {
    parse(data: unknown) {
      const d = data as any;
      if (typeof d?.id !== "string" || typeof d?.value !== "number") {
        throw new Error("Validation failed");
      }
      return d as { id: string; value: number };
    },
  };

  const TestDescriptor = topic("test.topic").schema(strictSchema);

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("sendMessage", () => {
    it("should send valid message through schema", async () => {
      await client.sendMessage(TestDescriptor as any, validMessage);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: "test.topic",
          messages: [
            expect.objectContaining({
              value: JSON.stringify(validMessage),
            }),
          ],
        }),
      );
    });

    it("should throw on invalid message", async () => {
      await expect(
        client.sendMessage(TestDescriptor as any, invalidMessage),
      ).rejects.toThrow(KafkaValidationError);
      expect(mockSend).not.toHaveBeenCalled();
    });

    it("should skip validation when descriptor has no schema", async () => {
      const NoSchema = topic("test.topic").type<{ id: string; value: number }>();
      await client.sendMessage(NoSchema as any, invalidMessage as any);
      expect(mockSend).toHaveBeenCalled();
    });
  });

  describe("sendBatch", () => {
    it("should validate each message in batch", async () => {
      await expect(
        client.sendBatch(TestDescriptor as any, [
          { value: validMessage },
          { value: invalidMessage as any },
        ]),
      ).rejects.toThrow(KafkaValidationError);
    });

    it("should send valid batch through schema", async () => {
      await client.sendBatch(TestDescriptor as any, [
        { value: validMessage },
        { value: { id: "2", value: 99 } },
      ]);
      expect(mockSend).toHaveBeenCalled();
    });
  });

  describe("transaction", () => {
    it("should validate messages in transaction send", async () => {
      await expect(
        client.transaction(async (ctx) => {
          await ctx.send(TestDescriptor as any, invalidMessage);
        }),
      ).rejects.toThrow(KafkaValidationError);
      expect(mockTxAbort).toHaveBeenCalled();
    });

    it("should validate messages in transaction sendBatch", async () => {
      await expect(
        client.transaction(async (ctx) => {
          await ctx.sendBatch(TestDescriptor as any, [
            { value: invalidMessage as any },
          ]);
        }),
      ).rejects.toThrow(KafkaValidationError);
      expect(mockTxAbort).toHaveBeenCalled();
    });
  });

  describe("startConsumer", () => {
    it("should deliver validated message as envelope to handler", async () => {
      const handler = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify(validMessage)),
          },
        });
      });

      await client.startConsumer([TestDescriptor] as any, handler);
      expect(handler).toHaveBeenCalledWith(envelopeWith(validMessage));
    });

    it("should skip invalid message and log error", async () => {
      const handler = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify(invalidMessage)),
          },
        });
      });

      await client.startConsumer([TestDescriptor] as any, handler);
      expect(handler).not.toHaveBeenCalled();
    });

    it("should send invalid message to DLQ when dlq is enabled", async () => {
      const handler = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify(invalidMessage)),
          },
        });
      });

      await client.startConsumer([TestDescriptor] as any, handler, {
        dlq: true,
      });
      expect(handler).not.toHaveBeenCalled();
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ topic: "test.topic.dlq" }),
      );
    });

    it("should call onError interceptor with envelope and KafkaValidationError", async () => {
      const onError = jest.fn();
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify(invalidMessage)),
          },
        });
      });

      await client.startConsumer([TestDescriptor] as any, jest.fn(), {
        interceptors: [{ onError }],
      });
      expect(onError).toHaveBeenCalledTimes(1);
      // onError(envelope, error) — error is second arg
      expect(onError.mock.calls[0][1]).toBeInstanceOf(KafkaValidationError);
    });

    it("should use schemas from options (decorator path)", async () => {
      const handler = jest.fn();
      const schemas = new Map([["test.topic", strictSchema]]);

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify(invalidMessage)),
          },
        });
      });

      await client.startConsumer(["test.topic"] as any, handler, { schemas });
      expect(handler).not.toHaveBeenCalled();
    });

    it("should pass through without schema as envelope", async () => {
      const handler = jest.fn();
      const NoSchema = topic("test.topic").type<{ id: string; value: number }>();

      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify(invalidMessage)),
          },
        });
      });

      await client.startConsumer([NoSchema] as any, handler);
      // No schema = no validation = handler receives envelope with raw parsed payload
      expect(handler).toHaveBeenCalledWith(envelopeWith(invalidMessage));
    });
  });
});

describe("KafkaClient — schema registry conflict", () => {
  const schemaA: SchemaLike<{ id: string; value: number }> = {
    parse: (d: unknown) => d as any,
  };
  const schemaB: SchemaLike<{ id: string; value: number }> = {
    parse: (d: unknown) => d as any,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should warn when two different schemas are registered for the same topic via sendMessage", async () => {
    const DescA = topic("test.topic").schema(schemaA);
    const DescB = topic("test.topic").schema(schemaB);
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    const client = createClient();
    await client.sendMessage(DescA as any, { id: "1", value: 1 });
    await client.sendMessage(DescB as any, { id: "2", value: 2 });

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining(`Schema conflict for topic "test.topic"`),
    );
    warnSpy.mockRestore();
  });

  it("should NOT warn when the same schema object is re-registered", async () => {
    const Desc = topic("test.topic").schema(schemaA);
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    const client = createClient();
    await client.sendMessage(Desc as any, { id: "1", value: 1 });
    await client.sendMessage(Desc as any, { id: "2", value: 2 });

    const schemaWarnings = warnSpy.mock.calls.filter((args) =>
      String(args[0]).includes("Schema conflict"),
    );
    expect(schemaWarnings).toHaveLength(0);
    warnSpy.mockRestore();
  });

  it("should warn when conflicting schemas arrive via startConsumer options.schemas", async () => {
    const schemasFirst = new Map([["test.topic", schemaA]]);
    const schemasSecond = new Map([["test.topic", schemaB]]);
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    const client = createClient();
    await client.startConsumer(["test.topic"] as any, jest.fn(), {
      schemas: schemasFirst,
    });
    await client.startConsumer(["test.topic"] as any, jest.fn(), {
      groupId: "group-2",
      schemas: schemasSecond,
    });

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining(`Schema conflict for topic "test.topic"`),
    );
    warnSpy.mockRestore();
  });
});

describe("KafkaClient — strictSchemas", () => {
  const strictSchema: SchemaLike<{ id: string; value: number }> = {
    parse(data: unknown) {
      const d = data as any;
      if (typeof d?.id !== "string" || typeof d?.value !== "number") {
        throw new Error("Validation failed");
      }
      return d as { id: string; value: number };
    },
  };

  const Descriptor = topic("test.topic").schema(strictSchema);

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should validate string topic against registry after descriptor was used (default strictSchemas=true)", async () => {
    const client = createClient();
    // First, register the schema by sending via descriptor
    await client.sendMessage(Descriptor as any, { id: "1", value: 42 });

    // Now, string topic should be validated against the same schema
    await expect(
      client.sendMessage("test.topic", { id: 123 as any, value: "bad" as any }),
    ).rejects.toThrow(KafkaValidationError);
  });

  it("should allow string topic bypass when strictSchemas is false", async () => {
    const lenientClient = new KafkaClient<TestTopicMap>(
      "lenient-client",
      "lenient-group",
      ["localhost:9092"],
      { strictSchemas: false },
    );

    // Register schema via descriptor
    await lenientClient.sendMessage(Descriptor as any, { id: "1", value: 42 });

    // String topic should NOT be validated
    await lenientClient.sendMessage("test.topic", {
      id: 123 as any,
      value: "bad" as any,
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("should not validate string topic if schema was never registered", async () => {
    const client = createClient();
    // No descriptor was used, so registry is empty
    await client.sendMessage("test.topic", {
      id: 123 as any,
      value: "bad" as any,
    });
    expect(mockSend).toHaveBeenCalled();
  });
});
