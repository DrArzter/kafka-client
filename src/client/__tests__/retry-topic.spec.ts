jest.mock("@confluentinc/kafka-javascript");

import {
  mockSend,
  mockConnect,
  mockSubscribe,
  mockRun,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
  mockSendOffsets,
  mockCommitOffsets,
  mockConsumerPause,
  mockConsumerResume,
  mockConsumer,
  mockListTopics,
  mockTransaction,
} from "../../__mocks__/@confluentinc/kafka-javascript";
import { startRetryTopicConsumers } from "../kafka.client/retry-topic";
import type { RetryTopicDeps } from "../kafka.client/retry-topic";

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeMessage(
  value: string | null,
  headers: Record<string, string> = {},
  offset = "5",
) {
  return {
    value: value !== null ? Buffer.from(value) : null,
    headers: Object.fromEntries(
      Object.entries(headers).map(([k, v]) => [k, Buffer.from(v)]),
    ),
    offset,
  };
}

function makeDeps(
  overrides: Partial<RetryTopicDeps> = {},
): RetryTopicDeps {
  const txMock = {
    send: mockTxSend,
    commit: mockTxCommit,
    abort: mockTxAbort,
    sendOffsets: mockSendOffsets,
  };

  const txProducer = { transaction: jest.fn().mockResolvedValue(txMock) };
  return {
    logger: {
      log: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    },
    producer: { send: mockSend } as any,
    instrumentation: [],
    onMessageLost: jest.fn(),
    onRetry: jest.fn(),
    onDlq: jest.fn(),
    onMessage: jest.fn(),
    ensureTopic: jest.fn().mockResolvedValue(undefined),
    getOrCreateConsumer: jest.fn().mockReturnValue(mockConsumer),
    runningConsumers: new Map(),
    createRetryTxProducer: jest.fn().mockResolvedValue(txProducer),
    ...overrides,
  };
}

/** Run the eachMessage handler that was registered via mockRun. */
async function triggerMessage(
  msgArgs: ReturnType<typeof makeMessage>,
  topic = "test.topic.retry.1",
  partition = 0,
) {
  const call = (mockRun as jest.Mock).mock.calls[0]?.[0];
  if (!call?.eachMessage) throw new Error("eachMessage not registered");
  await call.eachMessage({ topic, partition, message: msgArgs });
}

const TOPIC = "test.topic";
const RETRY = { maxRetries: 2, backoffMs: 100, maxBackoffMs: 1000 };

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("startRetryTopicConsumers / startLevelConsumer", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockListTopics.mockResolvedValue([
      "test.topic",
      "test.topic.dlq",
      "test.topic.retry.1",
      "test.topic.retry.2",
    ]);
    mockConsumer.assignment = jest
      .fn()
      .mockReturnValue([{ topic: "test.topic.retry.1", partition: 0 }]);
  });

  // 1. Null message value → commit offset, skip handler
  it("skips null-value messages and commits offset", async () => {
    const handler = jest.fn();
    const deps = makeDeps();

    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      false,
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    await triggerMessage(makeMessage(null));

    expect(handler).not.toHaveBeenCalled();
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: "test.topic.retry.1", partition: 0, offset: "6" },
    ]);
    expect(mockTxSend).not.toHaveBeenCalled();
  });

  // 2. Future x-retry-after → pause partition, sleep, resume
  it("pauses and resumes partition when x-retry-after is in the future", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const deps = makeDeps();

    jest.useFakeTimers();

    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      false,
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    const retryAfter = Date.now() + 500;
    const msgPromise = triggerMessage(
      makeMessage(JSON.stringify({ id: "1" }), {
        "x-retry-after": String(retryAfter),
        "x-retry-max-retries": "2",
        "x-retry-original-topic": TOPIC,
        "x-retry-attempt": "1",
      }),
    );

    // Advance past the delay
    jest.advanceTimersByTime(600);
    await msgPromise;

    jest.useRealTimers();

    expect(mockConsumerPause).toHaveBeenCalledWith([
      { topic: "test.topic.retry.1", partitions: [0] },
    ]);
    expect(mockConsumerResume).toHaveBeenCalledWith([
      { topic: "test.topic.retry.1", partitions: [0] },
    ]);
  });

  // 3. Handler success → commitOffsets called directly (no EOS tx)
  it("commits offset directly on success without using a transaction", async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    const deps = makeDeps();

    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      false,
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    await triggerMessage(
      makeMessage(JSON.stringify({ id: "1" }), {
        "x-retry-attempt": "1",
        "x-retry-max-retries": "2",
        "x-retry-original-topic": TOPIC,
      }),
    );

    expect(handler).toHaveBeenCalled();
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: "test.topic.retry.1", partition: 0, offset: "6" },
    ]);
    expect(mockTxSend).not.toHaveBeenCalled();
    expect(mockTxCommit).not.toHaveBeenCalled();
  });

  // 4. Handler fails, not exhausted → EOS tx: send to retry.2 + sendOffsets + commit
  it("routes to next retry level via EOS transaction on non-exhausted failure", async () => {
    const handler = jest
      .fn()
      .mockRejectedValue(new Error("transient error"));
    const deps = makeDeps();

    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      false,
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    await triggerMessage(
      makeMessage(JSON.stringify({ id: "1" }), {
        "x-retry-attempt": "1",
        "x-retry-max-retries": "2",
        "x-retry-original-topic": TOPIC,
      }),
    );

    // EOS path used (tx.send instead of direct producer.send)
    expect(mockTxSend).toHaveBeenCalledTimes(1);
    const sentTo: string = mockTxSend.mock.calls[0][0].topic;
    expect(sentTo).toBe("test.topic.retry.2");
    expect(mockSendOffsets).toHaveBeenCalledTimes(1);
    expect(mockTxCommit).toHaveBeenCalledTimes(1);
    // Offset NOT committed directly
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });

  // 5. Handler fails, exhausted, DLQ enabled → EOS tx: send to DLQ + sendOffsets + commit
  it("routes exhausted failures to DLQ via EOS transaction when dlq: true", async () => {
    const handler = jest
      .fn()
      .mockRejectedValue(new Error("final error"));
    const deps = makeDeps();

    // Level 2 = retry.2 = final level (maxRetries: 2)
    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      true, // dlq: true
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    // Simulate message arriving at retry.2 (exhausted)
    const level2Call = (mockRun as jest.Mock).mock.calls[1]?.[0];
    if (!level2Call?.eachMessage) throw new Error("level 2 eachMessage not registered");
    await level2Call.eachMessage({
      topic: "test.topic.retry.2",
      partition: 0,
      message: makeMessage(JSON.stringify({ id: "1" }), {
        "x-retry-attempt": "2",
        "x-retry-max-retries": "2",
        "x-retry-original-topic": TOPIC,
      }),
    });

    expect(mockTxSend).toHaveBeenCalledTimes(1);
    const sentTo: string = mockTxSend.mock.calls[0][0].topic;
    expect(sentTo).toBe("test.topic.dlq");
    expect(mockSendOffsets).toHaveBeenCalledTimes(1);
    expect(mockTxCommit).toHaveBeenCalledTimes(1);
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });

  // 6. Handler fails, exhausted, no DLQ → onMessageLost called + commitOffsets called
  it("calls onMessageLost and commits offset when exhausted with no DLQ", async () => {
    const handler = jest
      .fn()
      .mockRejectedValue(new Error("final error"));
    const onMessageLost = jest.fn();
    const deps = makeDeps({ onMessageLost });

    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      false, // dlq: false
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    const level2Call = (mockRun as jest.Mock).mock.calls[1]?.[0];
    await level2Call.eachMessage({
      topic: "test.topic.retry.2",
      partition: 0,
      message: makeMessage(JSON.stringify({ id: "1" }), {
        "x-retry-attempt": "2",
        "x-retry-max-retries": "2",
        "x-retry-original-topic": TOPIC,
      }),
    });

    expect(onMessageLost).toHaveBeenCalledTimes(1);
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: "test.topic.retry.2", partition: 0, offset: "6" },
    ]);
    expect(mockTxSend).not.toHaveBeenCalled();
  });

  // 7. EOS transaction send throws → aborts, no commitOffsets (redelivery)
  it("aborts transaction and skips offset commit when EOS tx send fails", async () => {
    const handler = jest
      .fn()
      .mockRejectedValue(new Error("handler error"));
    mockTxSend.mockRejectedValueOnce(new Error("broker error"));
    const deps = makeDeps();

    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      false,
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    await triggerMessage(
      makeMessage(JSON.stringify({ id: "1" }), {
        "x-retry-attempt": "1",
        "x-retry-max-retries": "2",
        "x-retry-original-topic": TOPIC,
      }),
    );

    expect(mockTxAbort).toHaveBeenCalledTimes(1);
    expect(mockTxCommit).not.toHaveBeenCalled();
    expect(mockCommitOffsets).not.toHaveBeenCalled();
  });

  // 8. JSON parse failure → skips handler, commits offset
  it("skips handler and commits offset for malformed JSON", async () => {
    const handler = jest.fn();
    const deps = makeDeps();

    await startRetryTopicConsumers(
      [TOPIC],
      "test-group",
      handler,
      RETRY,
      false,
      [],
      new Map(),
      deps,
      0, // skip partition-assignment wait in tests
    );

    await triggerMessage(makeMessage("not-valid-json"));

    expect(handler).not.toHaveBeenCalled();
    expect(mockCommitOffsets).toHaveBeenCalledWith([
      { topic: "test.topic.retry.1", partition: 0, offset: "6" },
    ]);
    expect(mockTxSend).not.toHaveBeenCalled();
  });
});
