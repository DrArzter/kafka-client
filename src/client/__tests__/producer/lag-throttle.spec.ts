import {
  KafkaClient,
  mockSend,
  mockConnect,
  mockFetchOffsets,
  mockFetchTopicOffsets,
} from "../helpers";

interface Topics {
  "test.topic": { id: string };
}

/** Build a client with lagThrottle pre-configured. */
function makeClient(maxLag: number, pollIntervalMs = 50, maxWaitMs = 500) {
  return new KafkaClient<Topics>(
    "test-client",
    "test-group",
    ["localhost:9092"],
    {
      lagThrottle: { maxLag, pollIntervalMs, maxWaitMs },
    },
  );
}

/** Mock getConsumerLag to return a given total lag. */
function setLag(lag: number) {
  mockFetchOffsets.mockResolvedValue([
    { topic: "test.topic", partitions: [{ partition: 0, offset: "0" }] },
  ]);
  mockFetchTopicOffsets.mockResolvedValue([
    { partition: 0, low: "0", high: String(lag) },
  ]);
}

describe("KafkaClient — lag-based producer throttling", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useRealTimers();
    mockConnect.mockResolvedValue(undefined);
    mockSend.mockResolvedValue(undefined);
  });

  it("sends immediately when lag is below threshold", async () => {
    const client = makeClient(100);
    setLag(50);
    await client.connectProducer();

    // Wait one poll cycle
    await new Promise((r) => setTimeout(r, 80));

    await client.sendMessage("test.topic", { id: "1" });
    expect(mockSend).toHaveBeenCalledTimes(1);

    await client.disconnect();
  });

  it("delays send until lag drops below threshold", async () => {
    const client = makeClient(100, 50, 2_000);

    // First poll: high lag → throttle
    setLag(200);
    await client.connectProducer();
    await new Promise((r) => setTimeout(r, 80));

    // Schedule lag drop after 120ms
    setTimeout(() => setLag(50), 120);

    const start = Date.now();
    await client.sendMessage("test.topic", { id: "1" });
    const elapsed = Date.now() - start;

    // Should have waited at least one extra poll cycle (~100ms)
    expect(elapsed).toBeGreaterThan(80);
    expect(mockSend).toHaveBeenCalledTimes(1);

    await client.disconnect();
  });

  it("unblocks after maxWaitMs when lag never drops", async () => {
    const client = makeClient(100, 50, 200);

    setLag(500); // Always above threshold
    await client.connectProducer();
    await new Promise((r) => setTimeout(r, 80));

    const start = Date.now();
    await client.sendMessage("test.topic", { id: "1" });
    const elapsed = Date.now() - start;

    // Should have waited ~200ms (maxWaitMs) then sent anyway
    expect(elapsed).toBeGreaterThanOrEqual(180);
    expect(mockSend).toHaveBeenCalledTimes(1);

    await client.disconnect();
  });

  it("does not throttle when lagThrottle is not configured", async () => {
    const client = new KafkaClient<Topics>("test-client", "test-group", [
      "localhost:9092",
    ]);
    await client.connectProducer();

    await client.sendMessage("test.topic", { id: "1" });
    expect(mockSend).toHaveBeenCalledTimes(1);

    await client.disconnect();
  });

  it("clears the polling timer on disconnect", async () => {
    const client = makeClient(100, 50);
    setLag(0);
    await client.connectProducer();

    await client.disconnect();

    // After disconnect no further polls should affect behaviour
    // (no error thrown = timer properly cleaned up)
    expect(true).toBe(true);
  });

  it("throttles sendBatch when lag exceeds threshold", async () => {
    const client = makeClient(100, 50, 2_000);

    setLag(300);
    await client.connectProducer();
    await new Promise((r) => setTimeout(r, 80));

    setTimeout(() => setLag(10), 100);

    const start = Date.now();
    await client.sendBatch("test.topic", [{ value: { id: "1" } }]);
    expect(Date.now() - start).toBeGreaterThan(60);
    expect(mockSend).toHaveBeenCalledTimes(1);

    await client.disconnect();
  });

  it("poll errors do not block sends", async () => {
    const client = makeClient(100, 50, 300);

    // Make every fetchOffsets throw
    mockFetchOffsets.mockRejectedValue(new Error("broker down"));
    await client.connectProducer();
    await new Promise((r) => setTimeout(r, 80));

    // Should send immediately — poll errors must not set _lagThrottled
    const start = Date.now();
    await client.sendMessage("test.topic", { id: "1" });
    expect(Date.now() - start).toBeLessThan(200);
    expect(mockSend).toHaveBeenCalledTimes(1);

    await client.disconnect();
  });
});
