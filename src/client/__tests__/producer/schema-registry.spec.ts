import {
  SchemaRegistryClient,
  registrySchema,
} from "../../message/schema-registry";

function fakeFetch(
  routes: Record<string, { status?: number; body: unknown }>,
) {
  const calls: Array<{ url: string; init?: any }> = [];
  const fn = jest.fn(async (url: any, init?: any) => {
    calls.push({ url: String(url), init });
    const route = Object.entries(routes).find(([path]) =>
      String(url).includes(path),
    );
    if (!route) {
      return {
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: async () => "no route",
        json: async () => ({}),
      } as any;
    }
    const { status = 200, body } = route[1];
    return {
      ok: status >= 200 && status < 300,
      status,
      statusText: "OK",
      text: async () => JSON.stringify(body),
      json: async () => body,
    } as any;
  });
  return { fn: fn as unknown as typeof fetch, calls };
}

describe("SchemaRegistryClient", () => {
  it("fetches and caches the latest schema for a subject", async () => {
    const { fn, calls } = fakeFetch({
      "/subjects/orders-value/versions/latest": {
        body: { id: 7, version: 3, schema: "{}" },
      },
    });
    const client = new SchemaRegistryClient({
      baseUrl: "http://sr:8081",
      fetchFn: fn,
    });

    const first = await client.getLatestSchema("orders-value");
    const second = await client.getLatestSchema("orders-value");

    expect(first).toEqual({ id: 7, version: 3, schema: "{}" });
    expect(second).toEqual(first);
    expect(calls).toHaveLength(1); // second call served from cache
  });

  it("registers a schema and invalidates the subject cache", async () => {
    const { fn, calls } = fakeFetch({
      "/subjects/orders-value/versions/latest": {
        body: { id: 7, version: 3, schema: "{}" },
      },
      "/subjects/orders-value/versions": { body: { id: 8 } },
    });
    const client = new SchemaRegistryClient({
      baseUrl: "http://sr:8081",
      fetchFn: fn,
    });

    await client.getLatestSchema("orders-value");
    const result = await client.registerSchema("orders-value", "{}", "JSON");
    expect(result).toEqual({ id: 8 });

    await client.getLatestSchema("orders-value");
    // 1 latest + 1 register + 1 latest (cache invalidated by register)
    expect(calls).toHaveLength(3);
    const registerCall = calls[1];
    expect(registerCall.init.method).toBe("POST");
    expect(JSON.parse(registerCall.init.body)).toEqual({
      schema: "{}",
      schemaType: "JSON",
    });
  });

  it("sends Basic auth when credentials are configured", async () => {
    const { fn, calls } = fakeFetch({
      "/subjects/s/versions/latest": {
        body: { id: 1, version: 1, schema: "{}" },
      },
    });
    const client = new SchemaRegistryClient({
      baseUrl: "http://sr:8081",
      auth: { username: "key", password: "secret" },
      fetchFn: fn,
    });
    await client.getLatestSchema("s");
    expect(calls[0].init.headers.Authorization).toBe(
      "Basic " + Buffer.from("key:secret").toString("base64"),
    );
  });

  it("throws a descriptive error on non-2xx responses", async () => {
    const { fn } = fakeFetch({
      "/subjects/missing/versions/latest": {
        status: 404,
        body: { error_code: 40401, message: "Subject not found" },
      },
    });
    const client = new SchemaRegistryClient({
      baseUrl: "http://sr:8081",
      fetchFn: fn,
    });
    await expect(client.getLatestSchema("missing")).rejects.toThrow(
      /SchemaRegistry GET .*missing.* failed: 404/,
    );
  });

  it("checkCompatibility returns the registry verdict", async () => {
    const { fn } = fakeFetch({
      "/compatibility/subjects/orders-value/versions/latest": {
        body: { is_compatible: false },
      },
    });
    const client = new SchemaRegistryClient({
      baseUrl: "http://sr:8081",
      fetchFn: fn,
    });
    await expect(
      client.checkCompatibility("orders-value", "{}"),
    ).resolves.toBe(false);
  });
});

describe("registrySchema — SchemaLike bridge", () => {
  function clientWithLatest(version: number) {
    const { fn } = fakeFetch({
      "/versions/latest": { body: { id: 1, version, schema: "{}" } },
    });
    return new SchemaRegistryClient({ baseUrl: "http://sr:8081", fetchFn: fn });
  }

  it("delegates structural validation to the local validator", async () => {
    const validator = {
      parse: jest.fn((d: any) => ({ ...d, validated: true })),
    };
    const schema = registrySchema(clientWithLatest(2), "orders-value", {
      validator,
    });
    const ctx = { topic: "orders", headers: {}, version: 1 };
    await expect(schema.parse({ a: 1 }, ctx)).resolves.toEqual({
      a: 1,
      validated: true,
    });
    expect(validator.parse).toHaveBeenCalledWith({ a: 1 }, ctx);
  });

  it("rejects a message version newer than the registered latest", async () => {
    const schema = registrySchema(clientWithLatest(2), "orders-value");
    await expect(
      schema.parse({}, { topic: "orders", headers: {}, version: 3 }),
    ).rejects.toThrow(/version 3 .* newer than the latest registered version 2/);
  });

  it("passes data through when no validator and version is within range", async () => {
    const schema = registrySchema(clientWithLatest(2), "orders-value");
    await expect(
      schema.parse({ ok: 1 }, { topic: "orders", headers: {}, version: 2 }),
    ).resolves.toEqual({ ok: 1 });
  });

  it("enforceVersion: false skips the version guard", async () => {
    const schema = registrySchema(clientWithLatest(1), "orders-value", {
      enforceVersion: false,
    });
    await expect(
      schema.parse({ ok: 1 }, { topic: "orders", headers: {}, version: 5 }),
    ).resolves.toEqual({ ok: 1 });
  });
});

describe("ConsumerOptions.groupInstanceId — static group membership", () => {
  it("passes group.instance.id to the underlying consumer config", async () => {
    const { KafkaJS } = jest.requireMock("@confluentinc/kafka-javascript");
    const { createClient, mockRun } = jest.requireActual("../helpers");
    mockRun.mockResolvedValue(undefined);

    const client = createClient();
    await client.startConsumer(["test.topic"], async () => {}, {
      groupInstanceId: "pod-0",
      groupId: "static-group",
    });

    const kafkaInstances = (KafkaJS.Kafka as jest.Mock).mock.results;
    const consumerCalls = kafkaInstances.flatMap(
      (r: any) => r.value.consumer.mock.calls,
    );
    const withInstanceId = consumerCalls.find(
      (c: any[]) => c[0]?.["group.instance.id"] === "pod-0",
    );
    expect(withInstanceId).toBeDefined();
    expect(withInstanceId[0].kafkaJS.groupId).toBe("static-group");
  });
});
