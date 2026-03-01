import {
  TestTopicMap,
  createClient,
  KafkaClient,
  mockRun,
  mockSend,
  mockConsumerPause,
  mockConsumerResume,
} from "./helpers";

function makeMsg(payload = { id: "1", value: 1 }) {
  return {
    value: Buffer.from(JSON.stringify(payload)),
    offset: "0",
  };
}

/** Deliver N DLQ-causing messages via eachMessage (handler always fails). */
async function deliverFailingMessages(
  client: KafkaClient<TestTopicMap>,
  count: number,
  opts: any = {},
) {
  const handler = jest.fn().mockRejectedValue(new Error("handler fail"));

  let capturedEachMessage: ((p: any) => Promise<void>) | undefined;

  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    capturedEachMessage = eachMessage;
  });

  await client.startConsumer(["test.topic"], handler, {
    dlq: true,
    circuitBreaker: { threshold: 2, recoveryMs: 100, windowSize: 5 },
    ...opts,
  });

  for (let i = 0; i < count; i++) {
    await capturedEachMessage!({
      topic: "test.topic",
      partition: 0,
      message: makeMsg({ id: String(i), value: i }),
    });
  }

  return { handler };
}

describe("KafkaClient — Circuit Breaker", () => {
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

  it("CLOSED → OPEN: pauses partition after threshold DLQ failures", async () => {
    await deliverFailingMessages(client, 2); // threshold = 2

    expect(mockConsumerPause).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({ topic: "test.topic", partitions: [0] }),
      ]),
    );
  });

  it("OPEN → HALF_OPEN: resumes partition after recoveryMs", async () => {
    await deliverFailingMessages(client, 2);

    expect(mockConsumerResume).not.toHaveBeenCalled();

    jest.advanceTimersByTime(100); // recoveryMs = 100

    expect(mockConsumerResume).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({ topic: "test.topic", partitions: [0] }),
      ]),
    );
  });

  it("HALF_OPEN + success → CLOSED: no pause after recovery", async () => {
    let capturedEachMessage: ((p: any) => Promise<void>) | undefined;
    const handler = jest
      .fn()
      .mockRejectedValueOnce(new Error("fail"))
      .mockRejectedValueOnce(new Error("fail"))
      .mockResolvedValue(undefined); // success after recovery

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      capturedEachMessage = eachMessage;
    });

    await client.startConsumer(["test.topic"], handler, {
      dlq: true,
      circuitBreaker: { threshold: 2, recoveryMs: 100 },
    });

    // 2 failures → OPEN
    for (let i = 0; i < 2; i++) {
      await capturedEachMessage!({
        topic: "test.topic",
        partition: 0,
        message: makeMsg(),
      });
    }

    jest.advanceTimersByTime(100); // → HALF_OPEN

    mockConsumerPause.mockClear();

    // 1 success → CLOSED
    await capturedEachMessage!({
      topic: "test.topic",
      partition: 0,
      message: makeMsg(),
    });

    expect(mockConsumerPause).not.toHaveBeenCalled();
  });

  it("HALF_OPEN + failure → OPEN again", async () => {
    let capturedEachMessage: ((p: any) => Promise<void>) | undefined;
    const handler = jest.fn().mockRejectedValue(new Error("always fails"));

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      capturedEachMessage = eachMessage;
    });

    await client.startConsumer(["test.topic"], handler, {
      dlq: true,
      circuitBreaker: { threshold: 2, recoveryMs: 100 },
    });

    // 2 failures → OPEN
    for (let i = 0; i < 2; i++) {
      await capturedEachMessage!({
        topic: "test.topic",
        partition: 0,
        message: makeMsg(),
      });
    }

    mockConsumerPause.mockClear();
    jest.advanceTimersByTime(100); // → HALF_OPEN

    // 1 failure in HALF_OPEN — but handler fails, goes to DLQ
    // This won't immediately re-open since we're already in half-open
    // The failure in half-open will be recorded via notifyDlq
    await capturedEachMessage!({
      topic: "test.topic",
      partition: 0,
      message: makeMsg(),
    });

    // Should be paused again (OPEN)
    expect(mockConsumerPause).toHaveBeenCalled();
  });

  it("does not trigger circuit breaker without circuitBreaker option", async () => {
    const handler = jest.fn().mockRejectedValue(new Error("fail"));
    let capturedEachMessage: ((p: any) => Promise<void>) | undefined;

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      capturedEachMessage = eachMessage;
    });

    await client.startConsumer(["test.topic"], handler, { dlq: true });

    for (let i = 0; i < 5; i++) {
      await capturedEachMessage!({
        topic: "test.topic",
        partition: 0,
        message: makeMsg({ id: String(i), value: i }),
      });
    }

    expect(mockConsumerPause).not.toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(5); // all 5 went to DLQ
  });

  it("sliding window: successes push old failures out, preventing false trips", async () => {
    let capturedEachMessage: ((p: any) => Promise<void>) | undefined;
    let failNext = false;
    const handler = jest.fn().mockImplementation(async () => {
      if (failNext) throw new Error("fail");
    });

    mockRun.mockImplementation(async ({ eachMessage }: any) => {
      capturedEachMessage = eachMessage;
    });

    await client.startConsumer(["test.topic"], handler, {
      dlq: true,
      circuitBreaker: { threshold: 3, recoveryMs: 100, windowSize: 5 },
    });

    // Deliver 3 successes → window = [T, T, T]
    for (let i = 0; i < 3; i++) {
      await capturedEachMessage!({
        topic: "test.topic",
        partition: 0,
        message: makeMsg({ id: `s${i}`, value: i }),
      });
    }

    // 2 failures → window = [T, T, T, F, F] → 2 failures < threshold 3 → still CLOSED
    failNext = true;
    for (let i = 0; i < 2; i++) {
      await capturedEachMessage!({
        topic: "test.topic",
        partition: 0,
        message: makeMsg({ id: `f${i}`, value: i }),
      });
    }

    // Should NOT be paused — 2 failures < threshold of 3
    expect(mockConsumerPause).not.toHaveBeenCalled();

    // 1 more success → window = [T, T, F, F, T] → still 2 failures < 3 → CLOSED
    failNext = false;
    await capturedEachMessage!({
      topic: "test.topic",
      partition: 0,
      message: makeMsg({ id: "s3", value: 3 }),
    });

    expect(mockConsumerPause).not.toHaveBeenCalled();
  });

  describe("instrumentation hooks", () => {
    async function setupWithInstrumentation(inst: any) {
      const instrumented = new (
        await import("../kafka.client")
      ).KafkaClient<TestTopicMap>("test-client", "test-group", ["localhost:9092"], {
        instrumentation: [inst],
      });

      let capturedEachMessage: ((p: any) => Promise<void>) | undefined;
      const handler = jest.fn().mockRejectedValue(new Error("fail"));
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        capturedEachMessage = eachMessage;
      });
      await instrumented.startConsumer(["test.topic"], handler, {
        dlq: true,
        circuitBreaker: { threshold: 2, recoveryMs: 100 },
      });
      return { instrumented, capturedEachMessage: capturedEachMessage! };
    }

    it("onCircuitOpen fires when CLOSED → OPEN", async () => {
      const onCircuitOpen = jest.fn();
      const { capturedEachMessage } = await setupWithInstrumentation({ onCircuitOpen });

      for (let i = 0; i < 2; i++) {
        await capturedEachMessage({ topic: "test.topic", partition: 0, message: makeMsg() });
      }

      expect(onCircuitOpen).toHaveBeenCalledWith("test.topic", 0);
    });

    it("onCircuitHalfOpen fires after recoveryMs", async () => {
      const onCircuitHalfOpen = jest.fn();
      const { capturedEachMessage } = await setupWithInstrumentation({ onCircuitHalfOpen });

      for (let i = 0; i < 2; i++) {
        await capturedEachMessage({ topic: "test.topic", partition: 0, message: makeMsg() });
      }

      jest.advanceTimersByTime(100);
      expect(onCircuitHalfOpen).toHaveBeenCalledWith("test.topic", 0);
    });

    it("onCircuitClose fires when HALF_OPEN → CLOSED", async () => {
      const onCircuitClose = jest.fn();
      const handler = jest
        .fn()
        .mockRejectedValueOnce(new Error("fail"))
        .mockRejectedValueOnce(new Error("fail"))
        .mockResolvedValue(undefined);

      const instrumented = new (
        await import("../kafka.client")
      ).KafkaClient<TestTopicMap>("test-client", "test-group", ["localhost:9092"], {
        instrumentation: [{ onCircuitClose }],
      });

      let capturedEachMessage: ((p: any) => Promise<void>) | undefined;
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        capturedEachMessage = eachMessage;
      });
      await instrumented.startConsumer(["test.topic"], handler, {
        dlq: true,
        circuitBreaker: { threshold: 2, recoveryMs: 100 },
      });

      for (let i = 0; i < 2; i++) {
        await capturedEachMessage!({ topic: "test.topic", partition: 0, message: makeMsg() });
      }
      jest.advanceTimersByTime(100); // → HALF_OPEN

      await capturedEachMessage!({ topic: "test.topic", partition: 0, message: makeMsg() });

      expect(onCircuitClose).toHaveBeenCalledWith("test.topic", 0);
    });
  });
});
