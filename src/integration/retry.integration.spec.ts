import "reflect-metadata";
import {
  TestTopics,
  createClient,
  KafkaRetryExhaustedError,
  ConsumerInterceptor,
} from "./helpers";

describe("Integration — Retry & DLQ", () => {
  it("should retry failed messages and send to DLQ", async () => {
    const client = createClient("retry");
    await client.connectProducer();

    let attempts = 0;

    await client.startConsumer(
      ["test.retry"],
      async () => {
        attempts++;
        throw new Error("Processing failed");
      },
      {
        fromBeginning: true,
        retry: { maxRetries: 2, backoffMs: 100 },
        dlq: true,
      },
    );

    type DlqCapture = {
      payload: TestTopics["test.retry"];
      headers: Record<string, string>;
    };
    const dlqCaptures: DlqCapture[] = [];
    const dlqClient = createClient("retry-dlq");
    await dlqClient.connectProducer();

    await dlqClient.startConsumer(
      ["test.retry.dlq" as keyof TestTopics],
      async (envelope) => {
        dlqCaptures.push({
          payload: envelope.payload as TestTopics["test.retry"],
          headers: envelope.headers,
        });
      },
      { fromBeginning: true },
    );

    await client.sendMessage("test.retry", { value: "fail-me" });

    await new Promise((r) => setTimeout(r, 10000));

    // maxRetries: 2 → 1 original + 2 retries = 3 total
    expect(attempts).toBe(3);
    expect(dlqCaptures.length).toBeGreaterThanOrEqual(1);
    expect(dlqCaptures[0].payload).toEqual({ value: "fail-me" });
    expect(dlqCaptures[0].headers["x-dlq-original-topic"]).toBe("test.retry");
    expect(dlqCaptures[0].headers["x-dlq-attempt-count"]).toBe("3");
    expect(dlqCaptures[0].headers["x-dlq-error-message"]).toBe(
      "Processing failed",
    );
    expect(dlqCaptures[0].headers["x-dlq-failed-at"]).toBeDefined();

    await client.disconnect();
    await dlqClient.disconnect();
  });

  it("should pass KafkaRetryExhaustedError to onError interceptor", async () => {
    const client = createClient("retry-error");
    await client.connectProducer();

    let capturedError: unknown = null;
    let attempts = 0;

    const interceptor: ConsumerInterceptor<TestTopics> = {
      onError: (_envelope, error) => {
        capturedError = error;
      },
    };

    await client.startConsumer(
      ["test.retry-error"],
      async () => {
        attempts++;
        throw new Error("always fails");
      },
      {
        fromBeginning: true,
        retry: { maxRetries: 2, backoffMs: 100 },
        dlq: true,
        interceptors: [interceptor],
      },
    );

    await client.sendMessage("test.retry-error", { value: "test" });

    await new Promise((r) => setTimeout(r, 10000));

    expect(attempts).toBe(3);
    expect(capturedError).toBeInstanceOf(KafkaRetryExhaustedError);
    expect((capturedError as KafkaRetryExhaustedError).attempts).toBe(3);
    expect((capturedError as KafkaRetryExhaustedError).topic).toBe(
      "test.retry-error",
    );
    expect((capturedError as KafkaRetryExhaustedError).cause).toBeInstanceOf(
      Error,
    );

    await client.disconnect();
  });
});
