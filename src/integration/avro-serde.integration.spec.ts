import "reflect-metadata";
import type { StartedRedpandaContainer } from "@testcontainers/redpanda";
import { avroSerde } from "../serde";
import { SchemaRegistryClient } from "../client/message/schema-registry";
import { topic } from "../client/message/topic";
import type { KafkaClient } from "../client/kafka.client";
import { JsonSerde } from "../client/message/serde";
import {
  startRedpanda,
  brokersOf,
  makeClient,
  uniqueTopic,
  readFirstRawValue,
  fetchLatestSchemaId,
  fetchSchemaById,
} from "./serde-redpanda.helpers";

/**
 * FIELD TRIAL — Avro serde against a REAL broker + REAL Schema Registry.
 *
 * apache/kafka (the shared integration broker) has no Schema Registry, so this
 * spec owns its own Redpanda container (Kafka API + Confluent-compatible SR in
 * one) and does NOT depend on global-setup's broker.
 *
 * Proves three things:
 *   1. Round-trip through real Kafka + SR — produce & consume via this library.
 *   2. Interop / wire-format correctness — the exact bytes are Confluent-framed
 *      (`[0x00][id BE][avro]`), the id matches the registry's `<topic>-value`
 *      id, and the tail decodes with a raw `avsc` type built from the schema the
 *      registry serves for that id. A Java/Go Avro consumer could read them.
 *   3. Per-topic serde — one client, an Avro topic and a JSON topic, each
 *      round-tripping independently with the right on-wire encoding.
 */
type AvroTopics = Record<string, { orderId: string; amount: number; note: string }>;

const AVRO_SCHEMA = {
  type: "record",
  name: "Order",
  namespace: "com.acme.orders",
  fields: [
    { name: "orderId", type: "string" },
    { name: "amount", type: "double" },
    { name: "note", type: "string" },
  ],
};

describe("Integration — Avro serde (real Kafka + Schema Registry)", () => {
  jest.setTimeout(240_000);

  let rp: StartedRedpandaContainer;
  let registry: SchemaRegistryClient;
  let registryUrl: string;

  beforeAll(async () => {
    rp = await startRedpanda();
    registryUrl = rp.getSchemaRegistryAddress();
    registry = new SchemaRegistryClient({ baseUrl: registryUrl });
  }, 180_000);

  afterAll(async () => {
    try {
      await rp?.stop();
    } catch {
      // best-effort teardown; ryuk reaps any orphan
    }
  }, 60_000);

  it("round-trips an Avro-encoded object through real Kafka + SR", async () => {
    const t = uniqueTopic("serde.avro.roundtrip");
    const serde = avroSerde({ registry, schema: AVRO_SCHEMA, autoRegister: true });

    const producer = makeClient<AvroTopics>(rp, "avro-rt-prod", { serde });
    const consumer = makeClient<AvroTopics>(rp, "avro-rt-cons", { serde });

    await producer.connectProducer();

    const payload = { orderId: "o-42", amount: 199.95, note: "first" };
    const received: AvroTopics[string][] = [];

    const handle = await consumer.startConsumer(
      [t],
      async (env) => {
        received.push(env.payload);
      },
      { fromBeginning: true },
    );
    await handle.ready();

    await producer.sendMessage(t, payload);

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("timed out waiting for consumed Avro message")),
        60_000,
      );
      const iv = setInterval(() => {
        if (received.length >= 1) {
          clearTimeout(timer);
          clearInterval(iv);
          resolve();
        }
      }, 100);
    });

    expect(received[0]).toEqual(payload);

    // Schema landed in the registry under <topic>-value as AVRO.
    const registered = await fetchLatestSchemaId(registryUrl, `${t}-value`);
    expect(registered.id).toBeGreaterThan(0);

    await handle.stop();
    await producer.disconnect();
    await consumer.disconnect();
  });

  it("produces Confluent-interop bytes a raw Avro consumer can decode", async () => {
    const t = uniqueTopic("serde.avro.interop");
    const serde = avroSerde({ registry, schema: AVRO_SCHEMA, autoRegister: true });

    const producer = makeClient<AvroTopics>(rp, "avro-io-prod", { serde });
    await producer.connectProducer();

    const payload = { orderId: "o-777", amount: 42.5, note: "interop" };
    await producer.sendMessage(t, payload);

    // Read the SAME message with a RAW driver consumer — bytes only.
    const raw = await readFirstRawValue(brokersOf(rp), t);

    // (a) Confluent framing: magic byte 0x00 + 4-byte BE schema id.
    expect(raw.length).toBeGreaterThan(5);
    expect(raw[0]).toBe(0x00);
    const wireId = raw.readInt32BE(1);

    // (b) The on-wire id matches the registry id for <topic>-value, resolved
    //     independently of this library's client.
    const registered = await fetchLatestSchemaId(registryUrl, `${t}-value`);
    expect(wireId).toBe(registered.id);

    // (c) An EXTERNAL Avro consumer decodes the tail: fetch the writer schema by
    //     id straight from the registry, build an avsc Type, decode the bytes
    //     after the 5-byte header — must equal the original object.
    const byId = await fetchSchemaById(registryUrl, wireId);
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const avsc = require("avsc");
    const type = avsc.Type.forSchema(JSON.parse(byId.schema));
    const decoded = type.fromBuffer(raw.subarray(5));
    expect(decoded).toEqual(payload);

    // eslint-disable-next-line no-console
    console.log(
      `[avro-interop] framing OK: magic=0x00 id=${wireId} ` +
        `bytes=${raw.length} — external avsc decode matched`,
    );

    await producer.disconnect();
  });

  it("supports per-topic serde: Avro topic and JSON topic on one client", async () => {
    const avroTopic = uniqueTopic("serde.avro.pertopic-avro");
    const jsonTopic = uniqueTopic("serde.avro.pertopic-json");

    const AvroT = topic(avroTopic)
      .type<{ orderId: string; amount: number; note: string }>()
      .serde(avroSerde({ registry, schema: AVRO_SCHEMA, autoRegister: true }));
    const JsonT = topic(jsonTopic)
      .type<{ orderId: string; amount: number; note: string }>()
      .serde(new JsonSerde());

    // Client default serde is JSON; the Avro topic overrides per-topic.
    const producer: KafkaClient<AvroTopics> = makeClient<AvroTopics>(
      rp,
      "avro-pt-prod",
    );
    const consumer: KafkaClient<AvroTopics> = makeClient<AvroTopics>(
      rp,
      "avro-pt-cons",
    );

    await producer.connectProducer();

    const avroPayload = { orderId: "a-1", amount: 10, note: "avro" };
    const jsonPayload = { orderId: "j-1", amount: 20, note: "json" };

    const gotAvro: AvroTopics[string][] = [];
    const gotJson: AvroTopics[string][] = [];

    // Two consumers on one client need distinct groupIds — a KafkaClient
    // rejects a second startConsumer on the same (default) group.
    const hA = await consumer.startConsumer(
      [AvroT],
      async (env) => {
        gotAvro.push(env.payload);
      },
      { fromBeginning: true, groupId: `${avroTopic}-cg` },
    );
    const hJ = await consumer.startConsumer(
      [JsonT],
      async (env) => {
        gotJson.push(env.payload);
      },
      { fromBeginning: true, groupId: `${jsonTopic}-cg` },
    );
    await hA.ready();
    await hJ.ready();

    await producer.sendMessage(AvroT, avroPayload);
    await producer.sendMessage(JsonT, jsonPayload);

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("timed out waiting for per-topic messages")),
        60_000,
      );
      const iv = setInterval(() => {
        if (gotAvro.length >= 1 && gotJson.length >= 1) {
          clearTimeout(timer);
          clearInterval(iv);
          resolve();
        }
      }, 100);
    });

    // Each topic round-trips independently and correctly.
    expect(gotAvro[0]).toEqual(avroPayload);
    expect(gotJson[0]).toEqual(jsonPayload);

    // Raw bytes prove the encodings differ: Avro is Confluent-framed (0x00),
    // JSON is UTF-8 text.
    const avroRaw = await readFirstRawValue(brokersOf(rp), avroTopic);
    const jsonRaw = await readFirstRawValue(brokersOf(rp), jsonTopic);

    expect(avroRaw[0]).toBe(0x00);
    expect(JSON.parse(jsonRaw.toString("utf8"))).toEqual(jsonPayload);
    // The JSON bytes are NOT Confluent-framed (start with '{' = 0x7b).
    expect(jsonRaw[0]).toBe(0x7b);

    await hA.stop();
    await hJ.stop();
    await producer.disconnect();
    await consumer.disconnect();
  });
});
