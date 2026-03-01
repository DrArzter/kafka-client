const mockSend = jest.fn().mockResolvedValue(undefined);
const mockConnect = jest.fn().mockResolvedValue(undefined);
const mockDisconnect = jest.fn().mockResolvedValue(undefined);
const mockSubscribe = jest.fn().mockResolvedValue(undefined);
const mockRun = jest.fn().mockResolvedValue(undefined);
const mockListTopics = jest
  .fn()
  .mockResolvedValue([
    "topic1",
    "topic2",
    "test.topic",
    "test.other",
    "test.topic.dlq",
    "test.other.dlq",
    "test.topic.duplicates",
  ]);

const mockTxSend = jest.fn().mockResolvedValue(undefined);
const mockTxCommit = jest.fn().mockResolvedValue(undefined);
const mockTxAbort = jest.fn().mockResolvedValue(undefined);
const mockSendOffsets = jest.fn().mockResolvedValue(undefined);
const mockTransaction = jest.fn().mockResolvedValue({
  send: mockTxSend,
  commit: mockTxCommit,
  abort: mockTxAbort,
  sendOffsets: mockSendOffsets,
});

const mockProducer = {
  connect: mockConnect,
  disconnect: mockDisconnect,
  send: mockSend,
  transaction: mockTransaction,
};

const mockConsumerPause = jest.fn();
const mockConsumerResume = jest.fn();
const mockCommitOffsets = jest.fn().mockResolvedValue(undefined);
const mockAssignment = jest.fn().mockReturnValue([]);

const mockConsumer = {
  connect: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
  subscribe: mockSubscribe,
  run: mockRun,
  pause: mockConsumerPause,
  resume: mockConsumerResume,
  commitOffsets: mockCommitOffsets,
  assignment: mockAssignment,
};

const mockCreateTopics = jest.fn().mockResolvedValue(true);
const mockFetchOffsets = jest.fn().mockResolvedValue([]);
const mockFetchTopicOffsets = jest.fn().mockResolvedValue([]);
const mockFetchTopicOffsetsByTime = jest.fn().mockResolvedValue([]);
const mockSetOffsets = jest.fn().mockResolvedValue(undefined);

const mockAdmin = {
  connect: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
  listTopics: mockListTopics,
  createTopics: mockCreateTopics,
  fetchOffsets: mockFetchOffsets,
  fetchTopicOffsets: mockFetchTopicOffsets,
  fetchTopicOffsetsByTime: mockFetchTopicOffsetsByTime,
  setOffsets: mockSetOffsets,
};

const Kafka = jest.fn().mockImplementation(() => ({
  producer: jest.fn().mockReturnValue(mockProducer),
  consumer: jest.fn().mockReturnValue(mockConsumer),
  admin: jest.fn().mockReturnValue(mockAdmin),
}));

const logLevel = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 3,
  DEBUG: 4,
};

const KafkaJS = {
  Kafka,
  logLevel,
};

export {
  KafkaJS,
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
  mockFetchOffsets,
  mockFetchTopicOffsets,
  mockFetchTopicOffsetsByTime,
  mockSetOffsets,
  mockTransaction,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
  mockSendOffsets,
  mockConsumerPause,
  mockConsumerResume,
  mockCommitOffsets,
  mockAssignment,
};
