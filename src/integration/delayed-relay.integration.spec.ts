import "reflect-metadata";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { KafkaClient, getBrokers } from "./helpers";

const { Kafka, logLevel: KafkaLogLevel } = KafkaJS;

/**
 * Integration tests for delayed delivery:
 *   sendMessage(topic, payload, { deliverAfterMs }) stages the message on
 *   `<topic>.delayed`; startDelayedRelay([topic]) forwards it to the target
 *   topic once the deadline passes, preserving key + envelope headers and
 *   stripping the `x-delayed-*` control headers.
 *
 * Topics are auto-created (they are not in global-setup's pre-created list),
 * so all clients here use `autoCreateTopics: true`.
 */

// Unique topic per test run to avoid cross-run offset/relay collisions.
function uniqueTopic(prefix: string): string {
  return `${prefix}.${Date.now()}.${Math.floor(Math.random() * 1e6)}`;
}

type RelayTopics = Record<string, { value: string; seq?: number }>;

function createRelayClient(name: string): KafkaClient<RelayTopics> {
  return new KafkaClient<RelayTopics>(
    `integration-${name}`,
    `group-${name}-${Date.now()}`,
    getBrokers(),
    { autoCreateTopics: true },
  );
}

/**
 * Raw confluent consumer used to read the *key* off the relayed message —
 * EventEnvelope does not surface the Kafka message key, so we inspect it
 * directly at the driver level.
 */
async function readRawKey(
  targetTopic: string,
  timeoutMs: number,
): Promise<{ key: string | null; headers: Record<string, string> } | null> {
  const kafka = new Kafka({
    kafkaJS: {
      clientId: `raw-key-reader-${Date.now()}`,
      brokers: getBrokers(),
      logLevel: KafkaLogLevel.NOTHING,
    },
  });
  const consumer = kafka.consumer({
    kafkaJS: {
      groupId: `raw-key-reader-${Date.now()}-${Math.random()}`,
      fromBeginning: true,
    },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: targetTopic });

  return new Promise((resolve) => {
    const timer = setTimeout(async () => {
      await consumer.disconnect().catch(() => {});
      resolve(null);
    }, timeoutMs);

    void consumer.run({
      eachMessage: async ({ message }: any) => {
        clearTimeout(timer);
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) {
          headers[k] = Buffer.isBuffer(v) ? v.toString() : String(v);
        }
        const result = {
          key: message.key ? message.key.toString() : null,
          headers,
        };
        await consumer.disconnect().catch(() => {});
        resolve(result);
      },
    });
  });
}

describe("Integration — Delayed relay", () => {
  it("holds a message until ~deliverAfterMs, then delivers it with envelope headers preserved and x-delayed-* stripped", async () => {
    const deliverAfterMs = 3_000;
    const target = uniqueTopic("test.delayed-basic");

    const producer = createRelayClient("delayed-basic-prod");
    const relay = createRelayClient("delayed-basic-relay");
    const consumer = createRelayClient("delayed-basic-cons");

    await producer.connectProducer();
    await relay.connectProducer();
    await consumer.connectProducer();

    let receivedAt: number | null = null;
    let receivedEventId: string | undefined;
    let receivedCorrelationId: string | undefined;
    let receivedHeaders: Record<string, string> = {};

    const done = new Promise<void>((resolve) => {
      void consumer
        .startConsumer(
          [target],
          async (envelope) => {
            receivedAt = Date.now();
            receivedEventId = envelope.eventId;
            receivedCorrelationId = envelope.correlationId;
            receivedHeaders = envelope.headers as Record<string, string>;
            resolve();
          },
          { fromBeginning: true } as any,
        )
        .then((h) => h.ready());
    });

    // Start relay before sending so it is ready to forward on deadline.
    const relayHandle = await relay.startDelayedRelay([target]);
    await relayHandle.ready();

    const sentAt = Date.now();
    await producer.sendMessage(
      target,
      { value: "delayed-payload", seq: 1 },
      { deliverAfterMs, correlationId: "corr-delayed-123" },
    );

    // Give the consumer time to subscribe before the deadline fires.
    await done;

    expect(receivedAt).not.toBeNull();
    const elapsed = (receivedAt as unknown as number) - sentAt;

    // Must NOT arrive appreciably before the deadline. Allow a small tolerance
    // for clock skew between send-time and relay's Date.now() checkpoint.
    expect(elapsed).toBeGreaterThanOrEqual(deliverAfterMs - 500);

    // Envelope metadata survived the hop.
    expect(receivedEventId).toBeTruthy();
    expect(receivedCorrelationId).toBe("corr-delayed-123");

    // x-delayed-* control headers stripped by the relay.
    expect(receivedHeaders["x-delayed-until"]).toBeUndefined();
    expect(receivedHeaders["x-delayed-target"]).toBeUndefined();
    // Correlation/event id headers survived.
    expect(receivedHeaders["x-correlation-id"]).toBe("corr-delayed-123");
    expect(receivedHeaders["x-event-id"]).toBeTruthy();

    await consumer.disconnect();
    await relay.disconnect();
    await producer.disconnect();
  }, 40_000);

  it("preserves the explicit message key through the relay", async () => {
    const deliverAfterMs = 2_000;
    const target = uniqueTopic("test.delayed-key");

    const producer = createRelayClient("delayed-key-prod");
    const relay = createRelayClient("delayed-key-relay");

    await producer.connectProducer();
    await relay.connectProducer();

    const relayHandle = await relay.startDelayedRelay([target]);
    await relayHandle.ready();

    // Raw reader started before send so it catches the relayed message.
    const rawPromise = readRawKey(target, 20_000);

    await producer.sendMessage(
      target,
      { value: "keyed-payload" },
      { deliverAfterMs, key: "order-42" },
    );

    const raw = await rawPromise;
    expect(raw).not.toBeNull();
    expect(raw!.key).toBe("order-42");
    // Control headers stripped even on the raw view.
    expect(raw!.headers["x-delayed-until"]).toBeUndefined();
    expect(raw!.headers["x-delayed-target"]).toBeUndefined();

    await relay.disconnect();
    await producer.disconnect();
  }, 40_000);

  it("relay handle ready() resolves and stop() stops it cleanly", async () => {
    const target = uniqueTopic("test.delayed-handle");
    const relay = createRelayClient("delayed-handle");
    await relay.connectProducer();

    const handle = await relay.startDelayedRelay([target]);
    await expect(handle.ready()).resolves.toBeUndefined();

    await expect(handle.stop()).resolves.toBeUndefined();

    // After stop, the same group can be started again (no "already consuming").
    const handle2 = await relay.startDelayedRelay([target]);
    await handle2.ready();
    await handle2.stop();

    await relay.disconnect();
  }, 40_000);
});
