# Architecture

The big-picture view of how `@drarzter/kafka-client` is put together: the
layers, why the code is shaped the way it is, and the rules that keep the
layers from bleeding into each other.

This document is for contributors. For the public API and usage examples see
`README.md`; for agent-facing rules and gotchas see `CLAUDE.md`.

---

## The five layers

Everything flows top-to-bottom. Each layer only knows about the one below it.

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. Public facade — KafkaClient<T>                                    │
│     src/client/kafka.client/index.ts                                  │
│     Thin class. Holds one field (this.ctx) and delegates every        │
│     method to a stateless impl function.                              │
├─────────────────────────────────────────────────────────────────────┤
│  2. State bag — KafkaClientContext<T>                                 │
│     src/client/kafka.client/context.ts                                │
│     One plain object carrying ALL per-instance state: config,         │
│     connections, consumer maps, dedup state, Lamport clock,           │
│     sub-services (metrics / circuit breaker / inflight / admin).      │
├─────────────────────────────────────────────────────────────────────┤
│  3. Stateless impl modules — producer/ consumer/ admin/ infra/        │
│     src/client/kafka.client/**                                        │
│     Pure functions `fn(ctx, ...args)`. No hidden state — everything   │
│     they touch lives on ctx or is passed in.                          │
├─────────────────────────────────────────────────────────────────────┤
│  4. Transport interface — KafkaTransport (+ IProducer/IConsumer/...)  │
│     src/client/transport/transport.interface.ts                       │
│     The seam. Impl modules speak only this interface, never the       │
│     driver directly.                                                  │
├─────────────────────────────────────────────────────────────────────┤
│  5. Transport implementations                                         │
│     ConfluentTransport  (src/client/transport/confluent.transport.ts) │
│       → wraps @confluentinc/kafka-javascript (librdkafka)             │
│     FakeTransport       (src/testing/transport.fake.ts)               │
│       → in-memory double for unit tests                               │
└─────────────────────────────────────────────────────────────────────┘
```

Orthogonal to the client is the **NestJS layer** (`src/nest/**`), which wraps
`KafkaClient` in a DI module, and the **message layer** (`src/client/message/**`),
which defines the wire format (envelope, headers, topic descriptors) shared by
producer and consumer.

### The serde seam

Value **(de)serialization is pluggable**, sitting inside the message layer as a
second seam beside the transport. A `MessageSerde` (`message/serde.ts`) converts a
validated payload object to wire bytes on produce, and back to an object on
consume; the default is `JsonSerde` (`JSON.stringify` / `JSON.parse`, byte-for-byte
identical to the client's historical behaviour). The serde touches **only the
message value** — envelope headers (`x-event-id`, `x-lamport-clock`, `traceparent`,
…) always travel as headers, never through the serde. The active serde is resolved
per topic: a per-topic `topic(...).serde(...)` override wins over the client-wide
`KafkaClientOptions.serde`, which in turn falls back to `JsonSerde`. Registry-backed
binary serdes (`avroSerde` / `protobufSerde`, `src/serde.ts`) speak Confluent wire
format and are exposed via the `/serde` entry point; they depend on the optional
peers `avsc` / `protobufjs`. See [`producer.md`](./producer.md#the-send-pipeline)
and [`consumer.md`](./consumer.md#the-per-message-pipeline-in-order) for where the
serde is invoked on each path.

Three further leaf modules sit beside the client and are also re-exported through
`/core`: **security** (`src/client/security/**` — TLS/SASL resolution, cloud-IAM
token providers, ACL calculators), **outbox** (`src/client/outbox/**` — the
transactional-outbox relay), and **config** (`src/client/config/**` — the
`fromEnv` helpers, see [`configuration.md`](./configuration.md)). Security is the
only one wired into the client at construction time (`resolveSecurityOptions` →
`ConfluentTransport`); outbox and config are opt-in utilities the caller composes.

---

## The context-object pattern

The single most important structural decision in this codebase: **`KafkaClient`
is a facade that owns no logic.** Its methods are one-liners that forward to a
free function, passing `this.ctx`:

```ts
// index.ts
public async sendMessage(topicOrDesc, message, options = {}): Promise<void> {
  return sendMessageImpl(this.ctx, topicOrDesc, message, options);
}
```

`this.ctx` is a `KafkaClientContext<T>` (`context.ts`) — a plain object holding
every piece of mutable and readonly state the instance needs.

### Why do it this way

- **Testability & navigability.** Each concern (send, retry, snapshot, circuit
  breaking) lives in its own file with an explicit `ctx` parameter. There is no
  1500-line god-class and no `this` magic — you can read `producer/send.ts` in
  isolation and see exactly what state it reads and writes.
- **No import cycles through the class.** Impl modules import each other freely
  (`window.ts` → `start.ts` → `stop.ts`) without importing `KafkaClient`, which
  would create a cycle. They only import the `KafkaClientContext` *type*.
- **Selective narrowing.** Modules that touch only a few `ctx` fields declare a
  `Pick<>` alias as their parameter type — `SnapshotCtx<T>` in `snapshot.ts` and
  `StopCtx<T>` in `stop.ts`. This is compiler-enforced documentation: the module
  physically cannot reach a field it didn't declare. Modules that forward `ctx`
  onward (e.g. `setup.ts`, `window.ts`) take the full `KafkaClientContext<T>`.

### How the context is built

The constructor (`index.ts`) first runs `validateClientOptions` (fail-fast on bad
ids/brokers/numerics) and `resolveSecurityOptions` (secure-by-default TLS/SASL,
whose result becomes `ConfluentTransport`'s third argument). It then assembles
`ctx` in one `satisfies KafkaClientContext<T>` literal, then patches two fields
that need the fully-formed object:

1. `ctx.producerOpsDeps.nextLamportClock` — a closure `() => ++ctx._lamportClock`
   (can't reference `ctx` inside the literal that defines `ctx`).
2. `ctx.retryTopicDeps` — built by `buildRetryTopicDeps(ctx)` in `setup.ts`.

Sub-services (`MetricsManager`, `CircuitBreakerManager`, `InFlightTracker`,
`AdminOps`) are constructed here and stored on `ctx`. The circuit breaker and
metrics manager are wired together at construction: `MetricsManager.notifyFailure`
and `notifyMessage` call into `CircuitBreakerManager.onFailure`/`onSuccess`.

---

## Prebuilt "deps" objects

Some impl functions run far from `ctx` (inside a consumer callback, or inside
`replayDlqTopic` which is unit-tested with no real broker). Rather than thread
the whole context, the constructor pre-bakes small **dependency bundles**:

| Deps object          | Built where                | Consumed by                          |
|----------------------|----------------------------|--------------------------------------|
| `producerOpsDeps`    | constructor                | `buildSendPayload` (`producer/ops.ts`) |
| `consumerOpsDeps`    | constructor                | `getOrCreateConsumer` (`consumer/ops.ts`) |
| `retryTopicDeps`     | `buildRetryTopicDeps(ctx)` | retry-chain consumers (`retry-topic.ts`) |
| `MessageHandlerDeps` | `messageDepsFor(ctx, gid)` | `handleEachMessage` / `handleEachBatch` |
| `DlqReplayDeps`      | inline in `index.replayDlq`| `replayDlqTopic` (`consumer/features/dlq-replay.ts`) |

`messageDepsFor` is notable: it binds circuit-breaker and metrics callbacks to a
*specific* group id (`gid`), so `onFailure`/`onMessage`/`onDlq` carry the group
context the breaker needs. See `consumer.md` for the wiring.

---

## Entry points / exports

Five public entry points, mapped in `package.json#exports` to `tsup`-built
bundles under `dist/`:

| Import path                        | Source        | Contents                                              |
|------------------------------------|---------------|-------------------------------------------------------|
| `@drarzter/kafka-client`           | `src/index.ts`| Everything: `core` + the NestJS layer.                |
| `@drarzter/kafka-client/core`      | `src/core.ts` | `KafkaClient`, all types, `topic()`, envelope helpers, errors, `versionedSchema` / `registrySchema`, `MessageSerde` / `JsonSerde` / `SchemaRegistryClient`, the `ConfluentTransport` + the `KafkaTransport` / `IProducer` / `IConsumer` / `IAdmin` / `ITransaction` interface family, the **outbox** relay, the **security** helpers, and the **config** `fromEnv` helpers. No NestJS dependency. |
| `@drarzter/kafka-client/serde`     | `src/serde.ts` | `avroSerde` / `protobufSerde` (Confluent wire format), plus re-exports of `JsonSerde`, `MessageSerde`, `SerdeContext`, and `SchemaRegistryClient`. Optional peer deps on `avsc` / `protobufjs`. |
| `@drarzter/kafka-client/testing`   | `src/testing.ts` | `createMockKafkaClient`, `FakeTransport` (+ `FakeProducer/Consumer/Admin/Transaction`), `KafkaTestContainer`. |
| `@drarzter/kafka-client/otel`      | `src/otel.ts` | `otelInstrumentation()` factory. Peer dep on `@opentelemetry/api`. |

`src/index.ts` = `export * from "./core"` plus the five `src/nest/*` modules.
`src/core.ts` = the client (`kafka.client`), the transport interface +
`ConfluentTransport`, `topic`, `serde` (`MessageSerde` / `JsonSerde`),
`versioned-schema`, `schema-registry`, `errors`, `envelope`, `outbox`, `security`,
and `config`. Separately, `package.json#bin` exposes the `kafka-client-dlq` CLI
(`dist/cli/index.js`); it isn't a library entry point.

NestJS, OpenTelemetry, and Testcontainers are marked `external` in the tsup
config — they are peer/optional deps, never bundled. `avsc` and `protobufjs`
(needed only by the `/serde` Avro/Protobuf serdes) are likewise optional peers,
dynamically imported so they never load unless you use those serdes. Declarations
are emitted by `tsc -p tsconfig.build.json` (tsup's `dts` is disabled); the project
builds under TypeScript 6 with `module`/`moduleResolution: "nodenext"`.

---

## Dependency rules (what may import what)

These are conventions the layering relies on; breaking them reintroduces the
coupling the architecture avoids.

- **`index.ts` (facade)** may import any impl module. Impl modules must **not**
  import `index.ts` / `KafkaClient` (cycle). They import the
  `KafkaClientContext` *type* from `context.ts`.
- **Impl modules** talk to Kafka only through the `KafkaTransport` interface
  (`ctx.transport`, `ctx.producer`, `ctx.consumers`). They must **not** import
  `@confluentinc/kafka-javascript` — only `transport/confluent.transport.ts`,
  `test.container.ts`, and the integration/chaos test harnesses do.
- **`context.ts`** imports only *types* (all `import type`). It is the shared
  vocabulary; pulling runtime code into it would couple every impl module to
  that code.
- **`message/**`** (envelope, topic) is a leaf: depended on by producer,
  consumer, and NestJS layers, depends on nothing but `types`.
- **`types/**`** is the bottom of the graph. `*.types.ts` files export *only*
  types (no runtime values); `common.ts` is the shared-primitive exception.
- **`nest/**`** imports from `core`/`client` but nothing in `client/**` imports
  from `nest/**`. The core is usable without NestJS installed.
- **`testing/**`** implements the transport/client interfaces; nothing in
  production `src` imports from `testing/**`.

---

## Core design tenets

The invariants the whole library is built around:

1. **Typed topic maps.** `KafkaClient<T>` is parameterised by a
   `TopicMapConstraint<T>` — a `{ [topic]: payloadType }` map. Every
   `sendMessage`/`startConsumer` call is checked against it at compile time.
   `TopicDescriptor` / `topic()` (`message/topic.ts`) let you carry the type
   (and optional runtime schema) as a value. See `README.md` for usage.

2. **Envelope headers on every message.** The producer stamps a fixed set of
   `x-*` headers (`x-event-id`, `x-correlation-id`, `x-timestamp`,
   `x-schema-version`, `x-lamport-clock`, and optionally `traceparent`) on every
   send. The consumer decodes them back into a typed `EventEnvelope<T>`. This is
   the wire contract — see `producer.md` and `message/envelope.ts`.

3. **At-least-once by default, exactly-once on request.** The default
   `eachMessage`/`eachBatch` path is at-least-once (librdkafka auto-commit).
   Stronger guarantees are opt-in: `startTransactionalConsumer` (EOS via Kafka
   transactions) and `retryTopics: true` (EOS routing to retry topics). The
   `consume()` iterator is the one at-*most*-once path (offsets auto-commit as
   messages are pushed to the queue, before the handler drains them). See
   `consumer.md`.

4. **CorrelationId / trace propagation via AsyncLocalStorage.** The consumer
   pipeline runs each handler inside `runWithEnvelopeContext(...)`, so any
   `sendMessage` nested in a handler auto-inherits the incoming `correlationId`
   and `traceparent` with no explicit threading. See `producer.md`.

5. **Instrumentation is cross-cutting and composable.** `KafkaInstrumentation`
   hooks (`beforeSend`, `beforeConsume`, `onDlq`, `onCircuitOpen`, …) fire around
   every send/consume. Multiple instrumentations compose in declaration order
   (first = outermost wrap). OTel is just one implementation of this interface
   (`src/otel.ts`).
