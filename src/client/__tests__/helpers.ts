jest.mock("@confluentinc/kafka-javascript");

import { KafkaClient, TTopicMessageMap } from "../kafka.client";
import {
  mockSend,
  mockConnect,
  mockDisconnect,
  mockConsumer,
  mockConsumerPause,
  mockConsumerResume,
  mockSubscribe,
  mockRun,
  mockListTopics,
  mockCreateTopics,
  mockAdmin,
  mockProducer,
  mockTransaction,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
  mockSendOffsets,
  mockSetOffsets,
  mockFetchOffsets,
  mockFetchTopicOffsets,
  mockFetchTopicOffsetsByTimestamp,
  mockSeek,
  mockCommitOffsets,
  mockAssignment,
} from "../../__mocks__/@confluentinc/kafka-javascript";

export interface TestTopicMap extends TTopicMessageMap {
  "test.topic": { id: string; value: number };
  "test.other": { name: string };
}

/** Match an envelope whose payload equals the given object. */
export function envelopeWith(payload: any, topic = "test.topic") {
  return expect.objectContaining({ payload, topic });
}

/** Setup mockRun to deliver a single message. */
export function setupMessage() {
  mockRun.mockImplementation(async ({ eachMessage }: any) => {
    await eachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify({ id: "1", value: 1 })),
      },
    });
  });
}

/** Returns a fully-mocked logger whose methods are jest.fn(). */
export function makeTestLogger() {
  return {
    log: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };
}

export function createClient(
  options?: import("../kafka.client").KafkaClientOptions,
) {
  return new KafkaClient<TestTopicMap>("test-client", "test-group", [
    "localhost:9092",
  ], options);
}

/** Mark a consumer group as running (for testing guard clauses). */
export function setRunningConsumer(
  client: KafkaClient<TestTopicMap>,
  groupId: string,
  mode: "eachMessage" | "eachBatch" = "eachMessage",
) {
  (client as any).ctx.runningConsumers.set(groupId, mode);
}

export {
  mockSend,
  mockConnect,
  mockDisconnect,
  mockConsumer,
  mockConsumerPause,
  mockConsumerResume,
  mockSubscribe,
  mockRun,
  mockListTopics,
  mockCreateTopics,
  mockAdmin,
  mockProducer,
  mockTransaction,
  mockTxSend,
  mockTxCommit,
  mockTxAbort,
  mockSendOffsets,
  mockSetOffsets,
  mockFetchOffsets,
  mockFetchTopicOffsets,
  mockFetchTopicOffsetsByTimestamp,
  mockSeek,
  mockCommitOffsets,
  mockAssignment,
};

export { KafkaClient } from "../kafka.client";
export type { MessageLostContext, KafkaClientOptions } from "../kafka.client";
export { topic } from "../message/topic";
export type { SchemaLike } from "../message/topic";
export { KafkaRetryExhaustedError, KafkaValidationError } from "../errors";
