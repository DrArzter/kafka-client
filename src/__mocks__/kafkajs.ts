const mockSend = jest.fn().mockResolvedValue(undefined);
const mockConnect = jest.fn().mockResolvedValue(undefined);
const mockDisconnect = jest.fn().mockResolvedValue(undefined);
const mockSubscribe = jest.fn().mockResolvedValue(undefined);
const mockRun = jest.fn().mockResolvedValue(undefined);
const mockListTopics = jest.fn().mockResolvedValue(["topic1", "topic2"]);

const mockTxSend = jest.fn().mockResolvedValue(undefined);
const mockTxCommit = jest.fn().mockResolvedValue(undefined);
const mockTxAbort = jest.fn().mockResolvedValue(undefined);
const mockTransaction = jest.fn().mockResolvedValue({
  send: mockTxSend,
  commit: mockTxCommit,
  abort: mockTxAbort,
});

const mockProducer = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  send: mockSend,
  transaction: mockTransaction,
};

const mockConsumer = {
  connect: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
  subscribe: mockSubscribe,
  run: mockRun,
};

const mockCreateTopics = jest.fn().mockResolvedValue(true);

const mockAdmin = {
  connect: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
  listTopics: mockListTopics,
  createTopics: mockCreateTopics,
};

const Kafka = jest.fn().mockImplementation(() => ({
  producer: jest.fn().mockReturnValue(mockProducer),
  consumer: jest.fn().mockReturnValue(mockConsumer),
  admin: jest.fn().mockReturnValue(mockAdmin),
}));

const Partitioners = {
  DefaultPartitioner: jest.fn(),
};

const logLevel = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5,
};

export {
  Kafka,
  Partitioners,
  logLevel,
  mockProducer,
  mockConsumer,
  mockAdmin,
  mockSend,
  mockConnect,
  mockDisconnect,
  mockSubscribe,
  mockRun,
  mockListTopics,
  mockCreateTopics,
  mockTransaction,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
};
