import { protobufSerde } from "../../../serde";
import { SchemaRegistryClient } from "../../message/schema-registry";
import type { SerdeContext } from "../../message/serde";

/** Hand-rolled stub of `SchemaRegistryClient` — see avro-serde.spec.ts. */
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
      return { id: byId, schema, schemaType: "PROTOBUF" };
    },
  } as unknown as SchemaRegistryClient;
  return { registry: stub, calls };
}

const ORDER_PROTO = `
  syntax = "proto3";
  message Order {
    string orderId = 1;
    double amount = 2;
  }
`;

const ctx = (over: Partial<SerdeContext> = {}): SerdeContext => ({
  topic: "orders",
  headers: {},
  ...over,
});

describe("protobufSerde", () => {
  it("round-trips an object through serialize → deserialize", async () => {
    const { registry } = fakeRegistry(ORDER_PROTO);
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: "Order",
    });
    const value = { orderId: "o-1", amount: 19.5 };

    const wire = (await serde.serialize(value, ctx())) as Buffer;
    const back = (await serde.deserialize(wire, ctx())) as typeof value;

    expect(back.orderId).toBe(value.orderId);
    expect(back.amount).toBe(value.amount);
  });

  it("frames [magic 0x00][id BE][0x00 message-index][protobuf]", async () => {
    const { registry } = fakeRegistry(ORDER_PROTO, 54321);
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: "Order",
    });

    const wire = (await serde.serialize(
      { orderId: "o-1", amount: 1 },
      ctx(),
    )) as Buffer;

    expect(wire[0]).toBe(0x00); // magic
    expect(wire.readInt32BE(1)).toBe(54321); // id big-endian
    expect(wire[5]).toBe(0x00); // message-index for top-level type [0]
    expect(wire.length).toBeGreaterThan(6);
  });

  it("uses registerSchema when autoRegister is set, getLatestSchema otherwise", async () => {
    const auto = fakeRegistry(ORDER_PROTO);
    await protobufSerde({
      registry: auto.registry,
      schema: ORDER_PROTO,
      messageType: "Order",
      autoRegister: true,
    }).serialize({ orderId: "a", amount: 1 }, ctx());
    expect(auto.calls.register).toBe(1);
    expect(auto.calls.getLatest).toBe(0);

    const latest = fakeRegistry(ORDER_PROTO);
    await protobufSerde({
      registry: latest.registry,
      schema: ORDER_PROTO,
      messageType: "Order",
    }).serialize({ orderId: "b", amount: 2 }, ctx());
    expect(latest.calls.getLatest).toBe(1);
    expect(latest.calls.register).toBe(0);
  });

  it("SchemaRegistryClient.getSchemaById caches ids forever (one HTTP call per id)", async () => {
    const fetchFn = jest.fn(async () => ({
      ok: true,
      status: 200,
      statusText: "OK",
      text: async () => "",
      json: async () => ({ schema: ORDER_PROTO }),
    })) as unknown as typeof fetch;
    const registry = new SchemaRegistryClient({
      baseUrl: "http://sr:8081",
      fetchFn,
    });
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: "Order",
    });

    // Build a wire message with id 88 so deserialize resolves via getSchemaById.
    const protobuf = require("protobufjs");
    const type = protobuf.parse(ORDER_PROTO).root.lookupType("Order");
    const body = Buffer.from(
      type.encode(type.create({ orderId: "z", amount: 3 })).finish(),
    );
    const header = Buffer.alloc(5);
    header.writeUInt8(0x00, 0);
    header.writeInt32BE(88, 1);
    const wire = Buffer.concat([header, Buffer.from([0x00]), body]);

    await serde.deserialize(wire, ctx());
    await serde.deserialize(wire, ctx());

    expect(fetchFn).toHaveBeenCalledTimes(1);
    expect(String((fetchFn as jest.Mock).mock.calls[0][0])).toContain(
      "/schemas/ids/88",
    );
  });

  it("rejects a non-0x00 magic byte with a clear error", async () => {
    const { registry } = fakeRegistry(ORDER_PROTO);
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: "Order",
    });
    const bad = Buffer.from([0x02, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x08]);
    await expect(serde.deserialize(bad, ctx())).rejects.toThrow(
      /unexpected magic byte 0x02 .*expected 0x00/,
    );
  });

  it("rejects a non-zero message-index (nested/multiple types) in v1", async () => {
    const { registry } = fakeRegistry(ORDER_PROTO);
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: "Order",
    });
    // magic + id + a non-0x00 message-index byte (0x02 → multi-element index array)
    const header = Buffer.alloc(5);
    header.writeUInt8(0x00, 0);
    header.writeInt32BE(1, 1);
    const wire = Buffer.concat([header, Buffer.from([0x02, 0x00]), Buffer.from([0x08])]);
    await expect(serde.deserialize(wire, ctx())).rejects.toThrow(
      /nested\/multiple message types are not supported in v1/,
    );
  });

  it("throws if serialize is called without a schema", async () => {
    const { registry } = fakeRegistry(ORDER_PROTO);
    const serde = protobufSerde({ registry, messageType: "Order" });
    await expect(
      serde.serialize({ orderId: "x", amount: 1 }, ctx()),
    ).rejects.toThrow(/`schema` is required to serialize/);
  });

  it("gives a helpful error when the optional peer `protobufjs` is missing", async () => {
    const { registry } = fakeRegistry(ORDER_PROTO);
    const importFn = jest.fn(async () => {
      throw new Error("Cannot find module 'protobufjs'");
    });
    const serde = protobufSerde({
      registry,
      schema: ORDER_PROTO,
      messageType: "Order",
      importFn,
    });
    await expect(
      serde.serialize({ orderId: "x", amount: 1 }, ctx()),
    ).rejects.toThrow(/protobufSerde: package 'protobufjs' is not installed/);
  });
});
