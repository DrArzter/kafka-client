import {
  TestTopicMap,
  createClient,
  KafkaClient,
  mockConsumer,
  mockRun,
} from "./helpers";

describe("KafkaClient — consume() AsyncIterator", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
    mockRun.mockReset().mockResolvedValue(undefined);
  });

  it("delivers messages in order via next()", async () => {
    const received: any[] = [];

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
          offset: "0",
        },
      });
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify({ id: "2", value: 20 })),
          offset: "1",
        },
      });
    });

    const iter = client.consume("test.topic");

    const r1 = await iter.next();
    const r2 = await iter.next();

    expect(r1.done).toBe(false);
    expect(r1.value.payload).toMatchObject({ id: "1", value: 10 });

    expect(r2.done).toBe(false);
    expect(r2.value.payload).toMatchObject({ id: "2", value: 20 });
  });

  it("return() closes the iterator and stops the consumer", async () => {
    const iter = client.consume("test.topic");

    const result = await iter.return!();

    expect(result.done).toBe(true);
    expect(mockConsumer.disconnect).toHaveBeenCalled();
  });

  it("is usable with for-await and break stops the consumer", async () => {
    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      await eachMessage({
        topic: "test.topic",
        partition: 0,
        message: {
          value: Buffer.from(JSON.stringify({ id: "1", value: 10 })),
          offset: "0",
        },
      });
    });

    const payloads: any[] = [];

    for await (const envelope of client.consume("test.topic")) {
      payloads.push(envelope.payload);
      break; // triggers return()
    }

    expect(payloads).toHaveLength(1);
    expect(payloads[0]).toMatchObject({ id: "1", value: 10 });
    expect(mockConsumer.disconnect).toHaveBeenCalled();
  });

  it("closed iterator returns done:true immediately on next()", async () => {
    const iter = client.consume("test.topic");
    await iter.return!();

    const r = await iter.next();
    expect(r.done).toBe(true);
  });
});
