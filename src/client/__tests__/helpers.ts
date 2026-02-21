jest.mock("@confluentinc/kafka-javascript");

import { KafkaClient, TTopicMessageMap } from "../kafka.client";
import {
  mockSend,
  mockConnect,
  mockConsumer,
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

export function createClient() {
  return new KafkaClient<TestTopicMap>("test-client", "test-group", [
    "localhost:9092",
  ]);
}

export {
  mockSend,
  mockConnect,
  mockConsumer,
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
};

export { KafkaClient } from "../kafka.client";
export type { MessageLostContext } from "../kafka.client";
export { topic, SchemaLike } from "../message/topic";
export { KafkaRetryExhaustedError, KafkaValidationError } from "../errors";
