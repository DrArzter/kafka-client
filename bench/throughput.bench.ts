/**
 * Standalone throughput / latency benchmark.
 *
 *   npm run bench
 *
 * Spins up a real Kafka broker via Testcontainers, then measures produce and
 * consume throughput for:
 *   (a) the raw @confluentinc/kafka-javascript KafkaJS-compat API, and
 *   (b) @drarzter/kafka-client (imported from src/core.ts).
 *
 * Reports msg/s and p50/p95 latency per mode as a small table on stdout.
 * Not a Jest test — run directly with ts-node.
 */
import { KafkaContainer } from "@testcontainers/kafka";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { KafkaClient } from "../src/core";

const { Kafka, logLevel: KafkaLogLevel } = KafkaJS;

// ── Config ────────────────────────────────────────────────────────────
const N = 5_000; // measured messages
const WARMUP = 500; // warmup messages (not measured)
const PAYLOAD_BYTES = 200;
const CONSUME_TIMEOUT_MS = 60_000;

// A ~200 byte payload. { seq, ts, pad } serialised is close enough; we pad the
// filler string so the serialised value lands near PAYLOAD_BYTES.
function makePayload(seq: number): { seq: number; ts: number; pad: string } {
  const overhead = 60; // rough JSON overhead for seq + ts + keys/quotes
  const padLen = Math.max(0, PAYLOAD_BYTES - overhead);
  return { seq, ts: Date.now(), pad: "x".repeat(padLen) };
}

// ── Stats ─────────────────────────────────────────────────────────────
interface ModeResult {
  mode: string;
  produceMsgPerSec: number;
  consumeMsgPerSec: number;
  p50Ms: number;
  p95Ms: number;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(
    sorted.length - 1,
    Math.floor((p / 100) * sorted.length),
  );
  return sorted[idx];
}

function summarizeLatencies(latencies: number[]): {
  p50: number;
  p95: number;
} {
  const sorted = [...latencies].sort((a, b) => a - b);
  return { p50: percentile(sorted, 50), p95: percentile(sorted, 95) };
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}

// ── Raw confluent KafkaJS-compat benchmark ──────────────────────────────
async function benchRaw(brokers: string[], topic: string): Promise<ModeResult> {
  const kafka = new Kafka({
    kafkaJS: {
      clientId: "bench-raw",
      brokers,
      logLevel: KafkaLogLevel.NOTHING,
    },
  });

  const producer = kafka.producer({ kafkaJS: { acks: -1 } });
  await producer.connect();

  // Ensure the topic exists before the consumer subscribes — the raw driver
  // does not auto-create topics on subscribe.
  const admin = kafka.admin();
  await admin.connect();
  await admin
    .createTopics({ topics: [{ topic, numPartitions: 1 }] })
    .catch(() => {});
  await admin.disconnect();

  // Consumer set up first, from beginning, so it catches every measured message.
  const consumer = kafka.consumer({
    kafkaJS: { groupId: `bench-raw-${Date.now()}`, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topic });

  const latencies: number[] = [];
  let consumed = 0;
  let consumeStart = 0;
  let consumeEnd = 0;

  const consumeDone = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("raw consume timeout")),
      CONSUME_TIMEOUT_MS,
    );
    void consumer.run({
      eachMessage: async ({ message }: any) => {
        const parsed = JSON.parse(message.value.toString());
        // Only count measured (post-warmup) messages.
        if (parsed.seq < 0) return;
        if (consumed === 0) consumeStart = Date.now();
        latencies.push(Date.now() - parsed.ts);
        consumed++;
        if (consumed >= N) {
          consumeEnd = Date.now();
          clearTimeout(timer);
          resolve();
        }
      },
    });
  });

  // Give the consumer time to join the group before producing.
  await new Promise((r) => setTimeout(r, 8_000));

  // Warmup (seq < 0 → skipped by consumer count).
  for (let i = 0; i < WARMUP; i++) {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(makePayload(-1 - i)) }],
    });
  }

  const produceStart = Date.now();
  for (let i = 0; i < N; i++) {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(makePayload(i)) }],
    });
  }
  const produceEnd = Date.now();

  await consumeDone;

  await consumer.disconnect();
  await producer.disconnect();

  const { p50, p95 } = summarizeLatencies(latencies);
  return {
    mode: "raw confluent",
    produceMsgPerSec: round((N / (produceEnd - produceStart)) * 1000),
    consumeMsgPerSec: round((N / Math.max(1, consumeEnd - consumeStart)) * 1000),
    p50Ms: round(p50),
    p95Ms: round(p95),
  };
}

// ── @drarzter/kafka-client benchmark ────────────────────────────────────
type BenchTopics = Record<string, { seq: number; ts: number; pad: string }>;

async function benchClient(
  brokers: string[],
  topic: string,
): Promise<ModeResult> {
  const client = new KafkaClient<BenchTopics>(
    "bench-client",
    `bench-client-group-${Date.now()}`,
    brokers,
    { autoCreateTopics: true, strictSchemas: false },
  );
  await client.connectProducer();

  const latencies: number[] = [];
  let consumed = 0;
  let consumeStart = 0;
  let consumeEnd = 0;

  const consumeDone = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("client consume timeout")),
      CONSUME_TIMEOUT_MS,
    );
    void client
      .startConsumer(
        [topic],
        async (envelope) => {
          if (envelope.payload.seq < 0) return;
          if (consumed === 0) consumeStart = Date.now();
          latencies.push(Date.now() - envelope.payload.ts);
          consumed++;
          if (consumed >= N) {
            consumeEnd = Date.now();
            clearTimeout(timer);
            resolve();
          }
        },
        { fromBeginning: true } as any,
      )
      .then((h) => h.ready());
  });

  // Give the consumer time to join the group.
  await new Promise((r) => setTimeout(r, 8_000));

  for (let i = 0; i < WARMUP; i++) {
    await client.sendMessage(topic, makePayload(-1 - i));
  }

  const produceStart = Date.now();
  for (let i = 0; i < N; i++) {
    await client.sendMessage(topic, makePayload(i));
  }
  const produceEnd = Date.now();

  await consumeDone;

  await client.disconnect();

  const { p50, p95 } = summarizeLatencies(latencies);
  return {
    mode: "@drarzter/kafka-client",
    produceMsgPerSec: round((N / (produceEnd - produceStart)) * 1000),
    consumeMsgPerSec: round((N / Math.max(1, consumeEnd - consumeStart)) * 1000),
    p50Ms: round(p50),
    p95Ms: round(p95),
  };
}

// ── Table printer ───────────────────────────────────────────────────────
function printTable(results: ModeResult[]): void {
  const headers = [
    "mode",
    "produce msg/s",
    "consume msg/s",
    "p50 (ms)",
    "p95 (ms)",
  ];
  const rows = results.map((r) => [
    r.mode,
    String(r.produceMsgPerSec),
    String(r.consumeMsgPerSec),
    String(r.p50Ms),
    String(r.p95Ms),
  ]);
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((row) => row[i].length)),
  );
  const fmt = (cells: string[]) =>
    "| " + cells.map((c, i) => c.padEnd(widths[i])).join(" | ") + " |";
  const sep =
    "|" + widths.map((w) => "-".repeat(w + 2)).join("|") + "|";

  console.log("");
  console.log(`Throughput benchmark — N=${N}, warmup=${WARMUP}, ~${PAYLOAD_BYTES}B payload`);
  console.log(fmt(headers));
  console.log(sep);
  for (const row of rows) console.log(fmt(row));
  console.log("");
}

// ── Main ─────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  console.log("Starting Kafka container (Testcontainers)…");
  const container = await new KafkaContainer("confluentinc/cp-kafka:7.7.0")
    .withKraft()
    .withExposedPorts(9093)
    .withEnvironment({
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    })
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(9093);
  const brokers = [`${host}:${port}`];
  console.log(`Kafka ready at ${brokers[0]}`);

  const results: ModeResult[] = [];
  try {
    console.log("Running raw confluent benchmark…");
    results.push(await benchRaw(brokers, `bench.raw.${Date.now()}`));

    console.log("Running @drarzter/kafka-client benchmark…");
    results.push(await benchClient(brokers, `bench.client.${Date.now()}`));

    printTable(results);
  } finally {
    console.log("Stopping Kafka container…");
    await container.stop();
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Benchmark failed:", err);
    process.exit(1);
  });
