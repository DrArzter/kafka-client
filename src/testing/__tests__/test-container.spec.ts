import { KafkaTestContainer } from "../test-container";

// Mock @testcontainers/kafka and kafkajs to avoid Docker dependency in unit tests
const mockStop = jest.fn();
const mockGetHost = jest.fn().mockReturnValue("localhost");
const mockGetMappedPort = jest.fn().mockReturnValue(55555);
const mockStart = jest.fn().mockResolvedValue({
  getHost: mockGetHost,
  getMappedPort: mockGetMappedPort,
  stop: mockStop,
});
const mockWithEnvironment = jest.fn().mockReturnThis();
const mockWithExposedPorts = jest.fn().mockReturnThis();
const mockWithKraft = jest.fn().mockReturnThis();

jest.mock("@testcontainers/kafka", () => ({
  KafkaContainer: jest.fn().mockImplementation(() => ({
    withKraft: mockWithKraft,
    withExposedPorts: mockWithExposedPorts,
    withEnvironment: mockWithEnvironment,
    start: mockStart,
  })),
}));

const mockAdminConnect = jest.fn();
const mockAdminDisconnect = jest.fn();
const mockCreateTopics = jest.fn();
const mockTxAbort = jest.fn();
const mockProducerConnect = jest.fn();
const mockProducerDisconnect = jest.fn();
const mockProducerTransaction = jest
  .fn()
  .mockResolvedValue({ abort: mockTxAbort });

jest.mock("kafkajs", () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    admin: jest.fn().mockReturnValue({
      connect: mockAdminConnect,
      createTopics: mockCreateTopics,
      disconnect: mockAdminDisconnect,
    }),
    producer: jest.fn().mockReturnValue({
      connect: mockProducerConnect,
      disconnect: mockProducerDisconnect,
      transaction: mockProducerTransaction,
    }),
  })),
  logLevel: { NOTHING: 0 },
}));

describe("KafkaTestContainer", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("starts a KRaft container and returns brokers", async () => {
    const tc = new KafkaTestContainer();
    const brokers = await tc.start();

    expect(brokers).toEqual(["localhost:55555"]);
    expect(mockWithKraft).toHaveBeenCalled();
    expect(mockWithExposedPorts).toHaveBeenCalledWith(9093);
  });

  it("pre-creates topics (string and object forms)", async () => {
    const tc = new KafkaTestContainer({
      topics: ["orders", { topic: "payments", numPartitions: 3 }],
    });
    await tc.start();

    expect(mockCreateTopics).toHaveBeenCalledWith({
      topics: [
        { topic: "orders", numPartitions: 1 },
        { topic: "payments", numPartitions: 3 },
      ],
    });
  });

  it("skips topic creation when no topics provided", async () => {
    const tc = new KafkaTestContainer();
    await tc.start();

    expect(mockAdminConnect).not.toHaveBeenCalled();
  });

  it("warms up transaction coordinator by default", async () => {
    const tc = new KafkaTestContainer();
    await tc.start();

    expect(mockProducerConnect).toHaveBeenCalled();
    expect(mockProducerTransaction).toHaveBeenCalled();
    expect(mockTxAbort).toHaveBeenCalled();
    expect(mockProducerDisconnect).toHaveBeenCalled();
  });

  it("skips transaction warmup when disabled", async () => {
    const tc = new KafkaTestContainer({ transactionWarmup: false });
    await tc.start();

    expect(mockProducerConnect).not.toHaveBeenCalled();
  });

  it("stops the container", async () => {
    const tc = new KafkaTestContainer();
    await tc.start();
    await tc.stop();

    expect(mockStop).toHaveBeenCalled();
  });

  it("exposes brokers getter after start", async () => {
    const tc = new KafkaTestContainer();
    await tc.start();

    expect(tc.brokers).toEqual(["localhost:55555"]);
  });

  it("throws if brokers accessed before start", () => {
    const tc = new KafkaTestContainer();
    expect(() => tc.brokers).toThrow("not started");
  });

  it("uses custom image", async () => {
    const { KafkaContainer } = require("@testcontainers/kafka");
    const tc = new KafkaTestContainer({ image: "custom/kafka:latest" });
    await tc.start();

    expect(KafkaContainer).toHaveBeenCalledWith("custom/kafka:latest");
  });
});
