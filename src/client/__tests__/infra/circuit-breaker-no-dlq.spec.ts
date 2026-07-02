import {
  TestTopicMap,
  createClient,
  KafkaClient,
  mockRun,
  mockConsumerPause,
} from "../helpers";

function makeMsg(payload = { id: "1", value: 1 }) {
  return {
    value: Buffer.from(JSON.stringify(payload)),
    offset: "0",
  };
}

/**
 * The circuit breaker is now driven by `deps.onFailure` — fired once per failed
 * handler attempt in executeWithRetry — instead of the DLQ hook. This means it
 * must trip even when dlq: false, and each retried attempt of the same message
 * counts toward the threshold.
 */
describe("KafkaClient — Circuit Breaker (no DLQ)", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
    client = createClient();
    mockRun.mockReset().mockResolvedValue(undefined);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("trips the breaker after threshold failures with dlq: false", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("handler fail"));
    let capturedEachMessage: ((p: any) => Promise<void>) | undefined;

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      capturedEachMessage = eachMessage;
    });

    await client.startConsumer(["test.topic"], handler, {
      dlq: false,
      circuitBreaker: { threshold: 2, recoveryMs: 100 },
    });

    // Two failing messages (each: single attempt, no retry) → two failures → OPEN.
    for (let i = 0; i < 2; i++) {
      await capturedEachMessage!({
        topic: "test.topic",
        partition: 0,
        message: makeMsg({ id: String(i), value: i }),
      });
    }

    expect(mockConsumerPause).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({ topic: "test.topic", partitions: [0] }),
      ]),
    );
  });

  it("counts each failed retry attempt toward the threshold (1 message, 2 attempts)", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("handler fail"));
    let capturedEachMessage: ((p: any) => Promise<void>) | undefined;

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      capturedEachMessage = eachMessage;
    });

    await client.startConsumer(["test.topic"], handler, {
      dlq: false,
      retry: { maxRetries: 1, backoffMs: 0 },
      circuitBreaker: { threshold: 2, recoveryMs: 100 },
    });

    // A single message: 1 initial attempt + 1 retry = 2 failed attempts.
    // onFailure fires per attempt, so threshold 2 trips from one message.
    const deliver = capturedEachMessage!({
      topic: "test.topic",
      partition: 0,
      message: makeMsg(),
    });
    // Backoff sleep between attempts uses setTimeout — flush it.
    await jest.runAllTimersAsync();
    await deliver;

    expect(handler).toHaveBeenCalledTimes(2);
    expect(mockConsumerPause).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({ topic: "test.topic", partitions: [0] }),
      ]),
    );
  });
});
