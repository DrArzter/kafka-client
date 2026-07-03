import "reflect-metadata";
import type { StartedRedpandaContainer } from "@testcontainers/redpanda";
import { protobufSerde } from "../serde";
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
 * FIELD TRIAL — Protobuf serde against a REAL broker + REAL Schema Registry.
 *
 * Mirrors the Avro spec but for Protobuf, and additionally asserts the extra
 * message-index byte Confluent inserts for Protobuf:
 *
 *   [magic 0x00][id BE][0x00 message-index][protobuf binary]
 *
 * Owns its own Redpanda container (Kafka + Confluent-compatible SR), does NOT
 * depend on global-setup's broker.
 *
 * Proves:
 *   1. Round-trip through real Kafka + SR.
 *   2. Interop / wire-format correctness — framing incl. the 0x00 message-index,
 *      id matches the registry `<topic>-value` id, and the protobuf tail decodes
 *      with `protobufjs` using the schema the registry serves for that id.
 *   3. Per-topic serde — a Protobuf topic and a JSON topic on one client.
 */
type ProtoTopics = Record<string, { orderId: string; amount: number; note: string }>;

const ORDER_PROTO = `
  syntax = "proto3";
  package com.acme.orders;
  message Order {
    string orderId = 1;
    double amount = 2;
    string note = 3;
  }
`;
const MESSAGE_TYPE = "com.acme.orders.Order";

describe("Integration — Protobuf serde (real Kafka + Schema Registry)", () => {
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

  it("round-trips a Protobuf-encoded object through real Kafka + SR", async () => {
    const t = uniqueTopic("serde.pb.roundtrip");
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: MESSAGE_TYPE,
      autoRegister: true,
    });

    const producer = makeClient<ProtoTopics>(rp, "pb-rt-prod", { serde });
    const consumer = makeClient<ProtoTopics>(rp, "pb-rt-cons", { serde });

    await producer.connectProducer();

    const payload = { orderId: "o-42", amount: 199.95, note: "first" };
    const received: ProtoTopics[string][] = [];

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
        () =>
          reject(new Error("timed out waiting for consumed Protobuf message")),
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

    // protobufjs toObject with defaults:true returns every field; deep-equals
    // the produced object.
    expect(received[0]).toEqual(payload);

    const registered = await fetchLatestSchemaId(registryUrl, `${t}-value`);
    expect(registered.id).toBeGreaterThan(0);

    await handle.stop();
    await producer.disconnect();
    await consumer.disconnect();
  });

  it("produces Confluent-interop bytes a raw Protobuf consumer can decode", async () => {
    const t = uniqueTopic("serde.pb.interop");
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: MESSAGE_TYPE,
      autoRegister: true,
    });

    const producer = makeClient<ProtoTopics>(rp, "pb-io-prod", { serde });
    await producer.connectProducer();

    const payload = { orderId: "o-777", amount: 42.5, note: "interop" };
    await producer.sendMessage(t, payload);

    const raw = await readFirstRawValue(brokersOf(rp), t);

    // (a) Confluent Protobuf framing: [0x00][id BE][0x00 msg-index][protobuf].
    expect(raw.length).toBeGreaterThan(6);
    expect(raw[0]).toBe(0x00); // magic
    const wireId = raw.readInt32BE(1);
    expect(raw[5]).toBe(0x00); // message-index for the top-level type [0]

    // (b) On-wire id matches the registry id for <topic>-value.
    const registered = await fetchLatestSchemaId(registryUrl, `${t}-value`);
    expect(wireId).toBe(registered.id);

    // (c) EXTERNAL Protobuf consumer decodes the tail (bytes after the 6-byte
    //     header): fetch the writer .proto by id from the registry, parse it
    //     with protobufjs, decode — must equal the original object.
    const byId = await fetchSchemaById(registryUrl, wireId);
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const protobuf = require("protobufjs");
    const type = protobuf.parse(byId.schema).root.lookupType(MESSAGE_TYPE);
    const decoded = type.decode(raw.subarray(6));
    const obj = type.toObject(decoded, {
      longs: String,
      enums: String,
      bytes: Buffer,
      defaults: true,
    });
    expect(obj).toEqual(payload);

    // eslint-disable-next-line no-console
    console.log(
      `[pb-interop] framing OK: magic=0x00 id=${wireId} msgIndex=0x00 ` +
        `bytes=${raw.length} — external protobufjs decode matched`,
    );

    await producer.disconnect();
  });

  it("supports per-topic serde: Protobuf topic and JSON topic on one client", async () => {
    const pbTopic = uniqueTopic("serde.pb.pertopic-pb");
    const jsonTopic = uniqueTopic("serde.pb.pertopic-json");

    const PbT = topic(pbTopic)
      .type<{ orderId: string; amount: number; note: string }>()
      .serde(
        protobufSerde({
          registry,
          schema: ORDER_PROTO,
          messageType: MESSAGE_TYPE,
          autoRegister: true,
        }),
      );
    const JsonT = topic(jsonTopic)
      .type<{ orderId: string; amount: number; note: string }>()
      .serde(new JsonSerde());

    const producer: KafkaClient<ProtoTopics> = makeClient<ProtoTopics>(
      rp,
      "pb-pt-prod",
    );
    const consumer: KafkaClient<ProtoTopics> = makeClient<ProtoTopics>(
      rp,
      "pb-pt-cons",
    );

    await producer.connectProducer();

    const pbPayload = { orderId: "p-1", amount: 10, note: "proto" };
    const jsonPayload = { orderId: "j-1", amount: 20, note: "json" };

    const gotPb: ProtoTopics[string][] = [];
    const gotJson: ProtoTopics[string][] = [];

    // Two consumers on one client need distinct groupIds — a KafkaClient
    // rejects a second startConsumer on the same (default) group.
    const hP = await consumer.startConsumer(
      [PbT],
      async (env) => {
        gotPb.push(env.payload);
      },
      { fromBeginning: true, groupId: `${pbTopic}-cg` },
    );
    const hJ = await consumer.startConsumer(
      [JsonT],
      async (env) => {
        gotJson.push(env.payload);
      },
      { fromBeginning: true, groupId: `${jsonTopic}-cg` },
    );
    await hP.ready();
    await hJ.ready();

    await producer.sendMessage(PbT, pbPayload);
    await producer.sendMessage(JsonT, jsonPayload);

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("timed out waiting for per-topic messages")),
        60_000,
      );
      const iv = setInterval(() => {
        if (gotPb.length >= 1 && gotJson.length >= 1) {
          clearTimeout(timer);
          clearInterval(iv);
          resolve();
        }
      }, 100);
    });

    expect(gotPb[0]).toEqual(pbPayload);
    expect(gotJson[0]).toEqual(jsonPayload);

    // Raw bytes: Protobuf topic is Confluent-framed (0x00 + 0x00 msg-index),
    // JSON topic is UTF-8 text.
    const pbRaw = await readFirstRawValue(brokersOf(rp), pbTopic);
    const jsonRaw = await readFirstRawValue(brokersOf(rp), jsonTopic);

    expect(pbRaw[0]).toBe(0x00);
    expect(pbRaw[5]).toBe(0x00); // message-index
    expect(JSON.parse(jsonRaw.toString("utf8"))).toEqual(jsonPayload);
    expect(jsonRaw[0]).toBe(0x7b); // '{' — plain JSON, not framed

    await hP.stop();
    await hJ.stop();
    await producer.disconnect();
    await consumer.disconnect();
  });
});
