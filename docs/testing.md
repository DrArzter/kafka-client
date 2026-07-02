# Testing infrastructure

The library ships three levels of test double, each for a different job:

| Double                  | Fakes                          | Use when you want to test… |
|-------------------------|--------------------------------|----------------------------|
| Jest manual mock        | `@confluentinc/kafka-javascript` (the driver) | `KafkaClient`'s own logic against a mocked driver (the default for unit specs). |
| `FakeTransport`         | the whole `KafkaTransport` seam| `KafkaClient` logic with an in-memory, inspectable broker — no `jest.mock`. |
| `createMockKafkaClient` | `IKafkaClient<T>` (the client) | services/handlers that *depend on* a client, without any Kafka at all. |

Plus `KafkaTestContainer` for real-broker integration tests.

Files referenced: `src/testing/**`, `src/__mocks__/**`.

---

## Jest manual mock (`src/__mocks__/@confluentinc/kafka-javascript.ts`)

Jest auto-loads this whenever a test imports `@confluentinc/kafka-javascript`,
which every unit test does transitively through `ConfluentTransport`. It exports:

- A `KafkaJS` object with a `Kafka` mock whose `.producer()`/`.consumer()`/
  `.admin()` return shared mock objects, plus the real-shaped `logLevel` and
  `PartitionAssigners` enums.
- Named `mock*` spies for every method — `mockSend`, `mockRun`, `mockSubscribe`,
  `mockCommitOffsets`, `mockTransaction` (+ `mockTxSend`/`mockTxCommit`/
  `mockTxAbort`/`mockSendOffsets`), `mockAssignment`, the admin spies, etc. —
  so specs can assert calls (`expect(mockSend).toHaveBeenCalledWith(...)`) and
  drive behaviour (`mockFetchTopicOffsets.mockResolvedValue([...])`).

`mockListTopics` is pre-seeded with a handful of topic names (including `.dlq`
and `.duplicates` variants) so the `autoCreateTopics: false` startup validators
in `AdminOps` pass without extra setup.

Because this mock is stateless about message flow (it doesn't actually deliver
messages to consumers), it's best for asserting *what the client does to the
driver*, not end-to-end delivery. For delivery, use `FakeTransport`.

---

## `FakeTransport` (`src/testing/transport.fake.ts`)

An in-memory implementation of the entire `KafkaTransport` interface. Inject it
via `KafkaClientOptions.transport` — no `jest.mock` needed — and it exercises the
real `KafkaClient` code paths against inspectable in-memory doubles.

```ts
const transport = new FakeTransport();
const client = new KafkaClient('svc', 'grp', [], { transport });
await client.connectProducer();
await client.sendMessage('orders', { id: '1' });
expect(transport.mainProducer.sentTo('orders')).toHaveLength(1);
```

### What it gives you

- **`FakeProducer`** — records every `send()` in `sent`; `sentTo(topic)`,
  `sentTopics()`, `lastTransaction`. Transactions are `FakeTransaction`s.
- **`FakeTransaction`** — staged sends live in `staged`, flush to the producer's
  `sent` on `commit()`, are discarded on `abort()`; `sendOffsets` calls are
  captured in `offsetsCommitted` for EOS assertions. `commit()` after `abort()`
  (and vice-versa) throws.
- **`FakeConsumer`** — `deliver(topic, message, partition?, offset?)` pushes a
  message straight through the registered `eachMessage` handler.
  `triggerRebalance(type, assignments)` fires the rebalance callback manually.
- **`FakeAdmin`** — pre-populate `existingTopics`, `topicOffsets`, `groupOffsets`
  to control query results; inspect `setOffsetsCalls`, `deletedGroups`,
  `deletedRecords`.
- **`FakeTransport`** accessors: `mainProducer` (the first producer, created at
  client construction), `producers`, `consumers`, `fakeAdmin`,
  `consumerFor(groupId)`, and a top-level `deliver(topic, payload, opts)` that
  finds the subscribed consumer, JSON-serialises the payload, and delivers it.

### Known fidelity divergences from the real driver

The fake trades fidelity for determinism. Be aware where it differs:

- **Pause/resume granularity.** `FakeConsumer` tracks paused state as a
  **`Set<topic>`** (`pausedTopics`) — it ignores the partition list. The real
  librdkafka path (and the circuit breaker / `consume()` backpressure) pauses at
  the topic-*partition* level. Tests can assert *that* a topic was paused, but not
  *which partitions*.
- **`consumerFor` prefix matching.** `consumerFor(groupId)` returns the first
  consumer whose group id **`=== groupId` or `.startsWith(groupId)`**. Convenient
  for reaching a companion (`<gid>-retry.1`) by the main gid, but with overlapping
  prefixes it can return the wrong consumer — pass the exact group id when it
  matters.
- **Headers are strings only.** The top-level `FakeTransport.deliver` wraps each
  header value as a single-element array (`[v]`) and only accepts a
  `Record<string, string>`. It doesn't reproduce librdkafka's `Buffer` values or
  genuinely multi-valued headers. `decodeHeaders` (which the client uses) handles
  both, but the fake never produces the Buffer/multi-value cases, so those code
  paths aren't exercised through it.
- **Auto-assignment on subscribe.** `FakeConsumer.subscribe()` assigns exactly
  **one partition (partition 0) per subscribed topic** and immediately fires the
  `onRebalance("assign", ...)` callback. This is what makes `handle.ready()`
  resolve synchronously in unit tests, but it means multi-partition scenarios and
  real rebalance timing aren't modelled.
- **No real offset semantics.** `commitOffsets`, `seek`, and `stop` are no-ops on
  `FakeConsumer`; `FakeAdmin.fetchTopicOffsets` defaults to a single empty
  partition. Offset-driven behaviour (snapshots, checkpoints, lag) needs the
  admin maps pre-populated.

---

## `createMockKafkaClient` (`src/testing/client.mock.ts`)

For testing code that **consumes** a `KafkaClient` — services, controllers — where
you don't care about Kafka behaviour, only that the right calls were made.

Returns a `MockKafkaClient<T>`: every method of `IKafkaClient<T>` is a spy.
Coverage:

- Send methods (`sendMessage`, `sendBatch`, `sendTombstone`) resolve `undefined`.
- `transaction(cb)` invokes your callback with a `{ send, sendBatch }` mock
  context, so code under test that sends inside a transaction is exercised.
- All `start*Consumer` methods resolve a handle `{ groupId: 'mock-group', stop,
  ready }` (all spies). `consume` returns an empty async iterator.
- Admin/metrics/lifecycle methods resolve sensible defaults (`checkStatus` →
  `{ status: 'up', clientId: 'mock-client', topics: [] }`, `getMetrics` → zeroed
  counters, `getClientId` → `'mock-client'`).

**Framework detection.** `detectMockFactory` probes for `jest` then `vi` via
`eval` (they're module-scope bindings, not on `globalThis`) and uses
`jest.fn()`/`vi.fn()`. Pass a custom `mockFactory` for anything else; it throws
if none is found.

---

## `KafkaTestContainer` (`src/testing/test.container.ts`)

Thin wrapper over `@testcontainers/kafka` for real-broker integration tests
(requires Docker). `start()`:

1. Boots a single-node **KRaft** Kafka container (`withKraft()`, default image
   `confluentinc/cp-kafka:7.7.0`), with `TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`
   and `MIN_ISR=1` so transactions work on one node.
2. Pre-creates any `topics` you pass (avoids first-use races).
3. Warms the **transaction coordinator** (default on): connects a throwaway
   transactional producer and aborts an empty transaction — this avoids the
   first real `transaction()` hanging while the coordinator initialises.

Returns the broker connection strings; `stop()` tears the container down.
`brokers` getter throws if not started.

Integration specs use `jest.integration.config.ts` with global setup/teardown in
`src/integration/global-setup.ts` / `global-teardown.ts` and helpers in
`src/integration/helpers.ts`. Run with `npm run test:integration` (uses
`--forceExit`). Unit specs (`jest.config.ts`) ignore `src/integration/`.

`global-setup.ts` pre-creates all fixture topics. Under **testcontainers 12** the
container's `start()` can resolve before the KRaft controller is ready to serve
`CreateTopics` (observed as `"Local: Timed out"`), so the setup retries the
create with a 2 s backoff up to a 90 s deadline before failing.

Beyond the Kafka broker, the integration suite includes **reference specs for the
pluggable stores** documented in `consumer.md`/`module-map.md`:

- `redis-dedup-store.integration.spec.ts` — a Redis-backed `DedupStore` surviving
  a simulated restart (alongside the in-memory `dedup-store.integration.spec.ts`).
- `postgres-outbox.integration.spec.ts` — a Postgres-backed `OutboxStore` driven
  by `startOutboxRelay`, asserting at-least-once delivery.

These stand up their own Redis/Postgres containers in addition to Kafka.

### Chaos suite

`src/chaos/**/*.chaos.spec.ts` are real-broker resilience tests (Docker required)
run separately via `npm run test:chaos` (`jest.chaos.config.ts` —
`testTimeout` 240 s, `maxWorkers: 1` so each spec owns its container, `--forceExit`).
`src/chaos/helpers.ts` provides container lifecycle plus Docker `pause`/`unpause`
to freeze the broker mid-stream. The four specs cover broker restart, poison-
message DLQ recovery, rebalance under load, and lag-throttle warnings. See
[`module-map.md`](./module-map.md#srcchaos--chaos--failure-injection-suite).

### Cleaning up leaked containers

`npm run containers:clean` force-removes any lingering testcontainers
(`docker ps -aq --filter label=org.testcontainers=true | xargs docker rm -f`) —
useful after a `--forceExit` run leaves containers behind.

### A note on declaration output

Type declarations are **not** produced by tsup (its `dts` is disabled); they are
emitted by `tsc -p tsconfig.build.json` in the `build` script. This matters for
tests only in that the published `.d.ts` surface is validated by the type build,
not the bundler.

---

## How `handle.ready()` keeps tests deterministic

Real consumers can take ~25 s to receive partition assignments, so tests that
send immediately after `startConsumer` returns would race. `handle.ready()`
resolves on the first (debounced) partition assignment — backed by the
`onRebalance("assign", ...)` callback wired in `getOrCreateConsumer`
(`consumer/ops.ts`).

In unit tests this is instant: `FakeConsumer.subscribe()` fires the assign
callback synchronously (see fidelity notes above), and for `fromBeginning: true`
the debounce window is `0 ms`, so `await handle.ready()` returns immediately.
Against the mock/fake this makes the "start consumer → send → assert" sequence
deterministic without arbitrary `sleep`s:

```ts
const handle = await kafka.startConsumer(['orders'], handler);
await handle.ready();        // resolves synchronously under FakeTransport
await kafka.sendMessage('orders', payload);
```
