import { avroSerde } from "../../../serde";
import { SchemaRegistryClient } from "../../message/schema-registry";
import type { SerdeContext } from "../../message/serde";

/**
 * Hand-rolled stub of `SchemaRegistryClient` — no HTTP. Serves a canned
 * `{ id, schema }` and counts how often each method is hit so we can assert
 * the id-cache behaviour (getSchemaById once per id across repeated
 * deserializes).
 */
function fakeRegistry(schema: string, id = 42) {
  const calls = { getLatest: 0, register: 0, getById: 0 };
  const stub = {
    async getLatestSchema(_subject: string) {
      calls.getLatest++;
      return { id, version: 1, schema };
    },
    async registerSchema(_subject: string, _schema: string, _type?: string) {
      calls.register++;
      return { id };
    },
    async getSchemaById(byId: number) {
      calls.getById++;
      return { id: byId, schema, schemaType: "AVRO" };
    },
  } as unknown as SchemaRegistryClient;
  return { registry: stub, calls };
}

const ORDER_SCHEMA = {
  type: "record",
  name: "Order",
  fields: [
    { name: "orderId", type: "string" },
    { name: "amount", type: "double" },
  ],
};

const ctx = (over: Partial<SerdeContext> = {}): SerdeContext => ({
  topic: "orders",
  headers: {},
  ...over,
});

describe("avroSerde", () => {
  it("round-trips an object through serialize → deserialize", async () => {
    const { registry } = fakeRegistry(JSON.stringify(ORDER_SCHEMA));
    const serde = avroSerde({ registry, schema: ORDER_SCHEMA });
    const value = { orderId: "o-1", amount: 19.99 };

    const wire = (await serde.serialize(value, ctx())) as Buffer;
    const back = await serde.deserialize(wire, ctx());

    expect(back).toEqual(value);
  });

  it("frames with magic byte 0x00 and a 4-byte big-endian schema id", async () => {
    const { registry } = fakeRegistry(JSON.stringify(ORDER_SCHEMA), 12345);
    const serde = avroSerde({ registry, schema: ORDER_SCHEMA });

    const wire = (await serde.serialize(
      { orderId: "o-1", amount: 1 },
      ctx(),
    )) as Buffer;

    expect(Buffer.isBuffer(wire)).toBe(true);
    expect(wire[0]).toBe(0x00);
    expect(wire.readInt32BE(1)).toBe(12345);
    // Payload follows the 5-byte header.
    expect(wire.length).toBeGreaterThan(5);
  });

  it("resolves the subject via TopicNameStrategy by default (<topic>-value)", async () => {
    const { registry } = fakeRegistry(JSON.stringify(ORDER_SCHEMA));
    const spy = jest.spyOn(registry, "getLatestSchema");
    const serde = avroSerde({ registry, schema: ORDER_SCHEMA });

    await serde.serialize({ orderId: "o-1", amount: 1 }, ctx());
    expect(spy).toHaveBeenCalledWith("orders-value");

    await serde.serialize({ orderId: "o-2", amount: 2 }, ctx({ isKey: true }));
    expect(spy).toHaveBeenCalledWith("orders-key");
  });

  it("uses registerSchema when autoRegister is set, getLatestSchema otherwise", async () => {
    const auto = fakeRegistry(JSON.stringify(ORDER_SCHEMA));
    const autoSerde = avroSerde({
      registry: auto.registry,
      schema: ORDER_SCHEMA,
      autoRegister: true,
    });
    await autoSerde.serialize({ orderId: "a", amount: 1 }, ctx());
    expect(auto.calls.register).toBe(1);
    expect(auto.calls.getLatest).toBe(0);

    const latest = fakeRegistry(JSON.stringify(ORDER_SCHEMA));
    const latestSerde = avroSerde({
      registry: latest.registry,
      schema: ORDER_SCHEMA,
    });
    await latestSerde.serialize({ orderId: "b", amount: 2 }, ctx());
    expect(latest.calls.getLatest).toBe(1);
    expect(latest.calls.register).toBe(0);
  });

  it("deserializes repeatedly, resolving the writer schema by id each time", async () => {
    // The id → schema cache lives on SchemaRegistryClient (asserted in the next
    // test with a real client). This bare stub does not cache, so both calls hit
    // getSchemaById — here we just prove repeated deserialize is stable.
    const { registry, calls } = fakeRegistry(JSON.stringify(ORDER_SCHEMA), 7);
    const serde = avroSerde({ registry, schema: ORDER_SCHEMA });

    const wire = (await serde.serialize(
      { orderId: "o-1", amount: 5 },
      ctx(),
    )) as Buffer;

    const a = await serde.deserialize(wire, ctx());
    const b = await serde.deserialize(wire, ctx());
    expect(a).toEqual(b);
    expect(calls.getById).toBe(2);
  });

  it("SchemaRegistryClient.getSchemaById caches ids forever (one HTTP call per id)", async () => {
    // Prove the real client-side id cache: two deserializes → one GET /schemas/ids/{id}.
    const fetchFn = jest.fn(async () => ({
      ok: true,
      status: 200,
      statusText: "OK",
      text: async () => "",
      json: async () => ({ schema: JSON.stringify(ORDER_SCHEMA) }),
    })) as unknown as typeof fetch;
    const registry = new SchemaRegistryClient({
      baseUrl: "http://sr:8081",
      fetchFn,
    });
    const serde = avroSerde({ registry, schema: ORDER_SCHEMA });

    // Frame a wire message manually with id 99 so deserialize resolves via getSchemaById.
    const avsc = require("avsc");
    const type = avsc.Type.forSchema(ORDER_SCHEMA);
    const header = Buffer.alloc(5);
    header.writeUInt8(0x00, 0);
    header.writeInt32BE(99, 1);
    const wire = Buffer.concat([header, type.toBuffer({ orderId: "z", amount: 3 })]);

    await serde.deserialize(wire, ctx());
    await serde.deserialize(wire, ctx());

    expect(fetchFn).toHaveBeenCalledTimes(1);
    expect(String((fetchFn as jest.Mock).mock.calls[0][0])).toContain(
      "/schemas/ids/99",
    );
  });

  it("rejects a non-0x00 magic byte with a clear error", async () => {
    const { registry } = fakeRegistry(JSON.stringify(ORDER_SCHEMA));
    const serde = avroSerde({ registry, schema: ORDER_SCHEMA });
    const bad = Buffer.from([0x01, 0x00, 0x00, 0x00, 0x2a, 0xde, 0xad]);
    await expect(serde.deserialize(bad, ctx())).rejects.toThrow(
      /unexpected magic byte 0x01 .*expected 0x00/,
    );
  });

  it("throws if serialize is called without a schema", async () => {
    const { registry } = fakeRegistry(JSON.stringify(ORDER_SCHEMA));
    const serde = avroSerde({ registry });
    await expect(serde.serialize({ orderId: "x", amount: 1 }, ctx())).rejects.toThrow(
      /`schema` is required to serialize/,
    );
  });

  it("gives a helpful error when the optional peer `avsc` is missing", async () => {
    const { registry } = fakeRegistry(JSON.stringify(ORDER_SCHEMA));
    const importFn = jest.fn(async () => {
      throw new Error("Cannot find module 'avsc'");
    });
    const serde = avroSerde({ registry, schema: ORDER_SCHEMA, importFn });
    await expect(
      serde.serialize({ orderId: "x", amount: 1 }, ctx()),
    ).rejects.toThrow(/avroSerde: package 'avsc' is not installed/);
  });
});
