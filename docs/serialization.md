# Serialization

How message **values** are turned into wire bytes on produce and back into
objects on consume. This is the serde layer — a second pluggable seam alongside
the transport, added in v0.11. It covers the `MessageSerde` contract, the default
`JsonSerde`, the registry-backed Avro/Protobuf serdes and their Confluent wire
format, and the current limits.

Public API and usage: see `README.md`. Where the serde is invoked on each path:
[`producer.md`](./producer.md#serialization-via-serde) and
[`consumer.md`](./consumer.md#stage-1--parse--validate-parsesinglemessage). Files
referenced here live under `src/client/message/` and `src/serde.ts`.

---

## The `MessageSerde` contract (`message/serde.ts`)

A serde converts a validated payload object to the wire form on produce and back
to an object on consume:

```ts
interface MessageSerde {
  serialize(value: unknown, ctx: SerdeContext): Buffer | string | Promise<Buffer | string>;
  deserialize(data: Buffer, ctx: SerdeContext): unknown | Promise<unknown>;
}

interface SerdeContext {
  topic: string;            // topic produced to / consumed from
  headers: MessageHeaders;  // decoded headers (envelope headers included)
  isKey?: boolean;          // false/omitted → value (default); true → key
}
```

Both methods may be sync or async — the client always `await`s them.

Three invariants hold across every serde:

1. **Value only.** A serde touches **only the message value**. Envelope metadata
   (`x-event-id`, `x-correlation-id`, `x-timestamp`, `x-schema-version`,
   `x-lamport-clock`, `traceparent`) always travels in Kafka **headers** and never
   passes through the serde. Whether the value is JSON, Avro, or Protobuf, the
   metadata wire contract is identical.
2. **Validation is object-level.** On produce, the `SchemaLike` runs on the
   *object* **before** `serialize`. On consume, `deserialize` runs first and the
   schema validates the *deserialized object* after. A serde never sees a schema.
3. **Deserialize receives the raw `Buffer`.** The consumer hands `deserialize` the
   exact `message.value` bytes — never a `.toString()` — so binary formats survive
   intact. (`JsonSerde` does the UTF-8 decode itself.)

### Resolution order

The active serde is chosen **per topic** by `resolveSerde` (`producer/ops.ts`) on
produce, and by `serdeMap.get(topic) ?? deps.serde` on consume:

```
per-topic  topic(...).serde(...)  (TopicDescriptor.__serde)
   >  client-wide  KafkaClientOptions.serde
   >  JsonSerde   (built-in default)
```

- A `topic('orders').serde(avroSerde({...}))` override always wins for `orders`.
- Otherwise the client-wide `serde` from the constructor options applies.
- With neither set, the default is `JsonSerde`.

The per-topic override is declared with the chainable `.serde()` builder on
`KeyableTopicDescriptor` (`message/topic.ts`), stored as `TopicDescriptor.__serde`
and composable with `.key(...)`:

```ts
const Orders = topic('orders')
  .schema(OrderSchema)
  .serde(avroSerde({ registry, schema: orderAvroSchema }))
  .key((o) => o.orderId);
```

---

## `JsonSerde` — the default (`message/serde.ts`)

```ts
class JsonSerde implements MessageSerde {
  serialize(value)   { return JSON.stringify(value); }        // UTF-8 string
  deserialize(data)  { return JSON.parse(data.toString('utf8')); }
}
```

Byte-for-byte identical to the client's historical `JSON.stringify` /
`JSON.parse`, so it is a **zero-behaviour-change default** — upgrading to v0.11
changes nothing on the wire until you opt into another serde.

---

## Registry-backed binary serdes (`src/serde.ts`, `/serde` entry point)

`avroSerde` and `protobufSerde` produce and consume the exact byte layout that
Confluent's Java/Go clients use, so this library interoperates with them through a
shared Schema Registry. They live in the separate `@drarzter/kafka-client/serde`
entry point and depend on optional peers loaded via **dynamic import** — install
only the one you use:

| Serde | Optional peer | Install |
|---|---|---|
| `avroSerde` | `avsc` | `npm install avsc` |
| `protobufSerde` | `protobufjs` | `npm install protobufjs` |

A clear error is thrown on first use if the peer is missing.

Both are factory functions returning a `MessageSerde`, and both take a
`SchemaRegistryClient` plus the local schema. On serialize they resolve the schema
id (via `registry.getLatestSchema(subject)`, or `registerSchema` when
`autoRegister: true`), encode the value, and frame the bytes. The subject defaults
to Confluent's `TopicNameStrategy` — `<topic>-value` / `<topic>-key` — overridable
via a `subject` string or a function of the `SerdeContext`.

### Confluent wire format

**Avro** frames a fixed 5-byte prefix followed by the Avro binary:

```
[magic 0x00][schema id : 4-byte big-endian][avro binary]
```

**Protobuf** inserts a **message-index** between the id and the payload:

```
[magic 0x00][schema id : 4-byte big-endian][message-index][protobuf binary]
```

The message-index identifies which message type within the `.proto` file was
used. For the first / top-level message type (index `[0]`) Confluent writes the
single byte `0x00`. This serde implements that top-level case only (see limits).

On deserialize, both serdes:

1. Read the magic byte (must be `0x00`, else a clear "not Confluent wire-format"
   error) and the 4-byte big-endian schema id via `readInt32BE(1)`.
2. Resolve the **writer** schema through `registry.getSchemaById(id)`
   (`schema-registry.ts`) — cached forever, since schema ids are immutable, so a
   given id costs exactly one registry round-trip.
3. Decode the remaining bytes with the parsed `avsc` type / `protobufjs` type
   (each cached per schema string so repeated messages don't re-parse).

`SchemaRegistryClient`'s role on the read path is exactly that id→schema lookup;
it is otherwise the same minimal REST client used by `registrySchema(...)` for
subject/version governance.

### Worked example

```ts
import { avroSerde } from '@drarzter/kafka-client/serde';
import { KafkaClient, SchemaRegistryClient, topic } from '@drarzter/kafka-client/core';

const registry = new SchemaRegistryClient({ baseUrl: 'http://localhost:8081' });
const orderSchema = {
  type: 'record',
  name: 'Order',
  fields: [{ name: 'orderId', type: 'string' }, { name: 'amount', type: 'double' }],
};

// Per-topic:
const Orders = topic('orders').serde(avroSerde({ registry, schema: orderSchema })).type<Order>();

// Or client-wide:
const kafka = new KafkaClient(id, group, brokers, {
  serde: avroSerde({ registry, schema: orderSchema }),
});
```

---

## Interaction with the rest of the pipeline

- **Envelope headers** are unaffected — always Kafka headers, never in the value.
- **Schema validation** (`SchemaLike` from `topic(...).schema(...)`) is orthogonal
  to the serde: it validates the *object*, before serialize / after deserialize.
  You can use a Zod schema for shape validation *and* an Avro serde for the wire
  format on the same topic.
- **DLQ / retry / duplicates / delayed forwarding** carry the **original wire
  bytes** (the raw `Buffer`) losslessly — no re-encode, no `.toString()`. A binary
  record lands in `<topic>.dlq` or `<topic>.retry.<n>` byte-for-byte identical to
  what was consumed. See [`consumer.md`](./consumer.md#stage-1--parse--validate-parsesinglemessage).
- **The retry chain** deserializes each `<topic>.retry.<n>` message through the
  same resolved serde (keyed by the original topic) before re-running the handler.

---

## v0.11 limits

The serde layer is intentionally minimal in this release:

- **Avro: reader schema == writer schema.** `deserialize` decodes with the writer
  schema resolved from the wire-format id; full reader-schema resolution / schema
  evolution (reading old data with a newer reader schema) is a future enhancement,
  not yet implemented.
- **Protobuf: top-level message only.** Only the top-level message type
  (message-index `[0]`, the single byte `0x00`) is supported. A non-`0x00`
  message-index (nested / multiple message types) throws a clear error on
  deserialize.
- **`readSnapshot` is JSON-only.** The compacted-topic snapshot reader
  (`consumer/features/snapshot.ts`) parses values with `JSON.parse(value.toString())`
  directly and does **not** go through the serde layer — snapshots of Avro/Protobuf
  topics are not supported yet.
