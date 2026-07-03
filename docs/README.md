# Internal documentation

Developer-facing docs for contributors and maintainers of
`@drarzter/kafka-client` — a type-safe Kafka wrapper for NestJS built on
`@confluentinc/kafka-javascript`. This set explains **what lives where and what
is responsible for what**: the layering, data flow, invariants, and design
decisions behind the code. It complements — and deliberately does not duplicate —
the two other top-level docs:

- **`README.md`** (repo root) — user-facing API reference and usage examples.
  When these docs say "see the public API", that's where to look.
- **`CLAUDE.md`** (repo root) — agent instructions, the source-tree map, and the
  running list of constraints & gotchas.

The source is the ultimate authority; every factual claim here is meant to be
verifiable against a concrete file.

---

## Contents

| Doc | What it covers |
|-----|----------------|
| [`architecture.md`](./architecture.md) | The big picture: the five layers (facade → context bag → impl modules → transport interface → transport impls), the orthogonal security/outbox/config leaf modules, why the context-object pattern is used, entry-point/export map, dependency rules, and the core design tenets. |
| [`module-map.md`](./module-map.md) | Every source file with a one-line responsibility, grouped by directory (client, transport, message, security, outbox, config, cli, chaos, bench, nest, testing), plus the file-naming convention (`name.role.ts`). |
| [`producer.md`](./producer.md) | The send pipeline: `preparePayload`, automatic envelope headers, typed-key resolution, delayed delivery (`deliverAfterMs` → `<topic>.delayed`), the Lamport clock lifecycle, transactions (`_txChain`, tx-id registry, fencing, connect idempotency), the lag throttle, and tombstones. |
| [`consumer.md`](./consumer.md) | The consumer flavours (incl. the delayed-delivery relay) and how they map onto the two core pipelines; the per-message stages in order; circuit-breaker wiring; `DedupStore`-backed deduplication (fail-open); lazy producer connect for routing consumers; `consume()` backpressure; and per-flavour semantic caveats. |
| [`retry-and-dlq.md`](./retry-and-dlq.md) | In-process retry vs the durable retry-topic chain (companion consumers, EOS routing transactions, `x-retry-*` headers), DLQ headers/naming, `replayDlq` mechanics (and its `.dlq` guard), and what does **not** propagate into the retry chain. |
| [`nestjs.md`](./nestjs.md) | The NestJS layer: `KafkaModule.register`/`registerAsync`, the forwarded option subset (incl. `security`), multi-client tokens, the `KafkaExplorer` discovery flow + double-registration guard, `@SubscribeTo`, the health indicator, and the esbuild/`design:paramtypes` constraint. |
| [`testing.md`](./testing.md) | The three test doubles (Jest manual mock, `FakeTransport`, `createMockKafkaClient`), `FakeTransport`'s fidelity divergences, `KafkaTestContainer`/integration setup (incl. Redis-dedup / Postgres-outbox reference specs and the testcontainers-12 readiness retry), the chaos suite, `containers:clean`, and how `handle.ready()` keeps tests deterministic. |
| [`configuration.md`](./configuration.md) | The complete configuration reference: the three layers (defaults → env → code) and their precedence, full tables mapping every `KafkaClientOptions` / `ConsumerOptions` field to its env var (or "code only"), the `fromEnv` helpers, security/cloud-IAM rules, serde configuration, NestJS `ConfigService` vs `fromEnv`, and a "where do I configure X" decision table. |
| [`serialization.md`](./serialization.md) | The value-serialization seam: the `MessageSerde` contract and `SerdeContext`, the default `JsonSerde`, per-topic (`topic(...).serde(...)`) vs client-wide resolution, the registry-backed `avroSerde` / `protobufSerde` and their Confluent wire format (`[0x00][schema id BE][payload]`, plus Protobuf's message-index), `SchemaRegistryClient.getSchemaById` on the read path, lossless raw-byte forwarding to DLQ/retry, and the v0.11 limits. |

---

## Where do I look for X?

| I want to understand… | Start here |
|-----------------------|-----------|
| How a `KafkaClient` method reaches its logic | [architecture.md](./architecture.md#the-context-object-pattern) — facade delegates to `fn(ctx, ...)` |
| Where a specific file's responsibility is | [module-map.md](./module-map.md) |
| What headers a message gets on send | [producer.md](./producer.md#automatic-envelope-headers) |
| How schema validation runs on send | [producer.md](./producer.md#schema-handling-opsts) |
| How are values serialized (JSON default, per-topic override) | [serialization.md](./serialization.md#the-messageserde-contract-messageserdets) |
| How to use Avro or Protobuf (Confluent wire format) | [serialization.md](./serialization.md#registry-backed-binary-serdes-srcserdets-serde-entry-point) |
| How a message key is chosen (explicit vs `.key()`) | [producer.md](./producer.md#buildsendpayload--per-message-work-opsts) |
| How `deliverAfterMs` delays a message | [producer.md](./producer.md#delayed-delivery) + [consumer.md](./consumer.md#delayed-delivery-relay-featuresdelayedts) |
| How TLS/SASL/cloud-IAM auth is resolved | [configuration.md](./configuration.md#security-configuration) (rules) + [module-map.md](./module-map.md#srcclientsecurity--transport-security) (modules) |
| How the transactional-outbox relay works | [module-map.md](./module-map.md#srcclientoutbox--transactional-outbox) (`startOutboxRelay`) |
| Why the Lamport clock survives restarts | [producer.md](./producer.md#lamport-clock-lifecycle) (recovery + timeout) |
| How `transaction()` avoids interleaving | [producer.md](./producer.md#serialisation-via-_txchain-recent) (`_txChain`) |
| Why a second `connect()` doesn't error | [producer.md](./producer.md#lazy-transactional-producer) (cached connect promise) |
| Why a duplicate `transactionalId` warns / gets fenced | [producer.md](./producer.md#transactional-id-registry--fencing) |
| The order of parse → dedup → TTL → handler | [consumer.md](./consumer.md#the-per-message-pipeline-in-order) |
| Why the circuit breaker opens without a DLQ | [consumer.md](./consumer.md#where-failures-are-recorded-recent) (`notifyFailure`) |
| How retry-chain failures affect the main breaker | [consumer.md](./consumer.md#where-failures-are-recorded-recent) + [retry-and-dlq.md](./retry-and-dlq.md#per-level-flow-startlevelconsumer) |
| Why dedup resets after a restart (and how to persist it) | [consumer.md](./consumer.md#durability-depends-on-the-store) (`DedupStore`) |
| Why a consumer-only client can still write to a DLQ | [consumer.md](./consumer.md#lazy-producer-connect-for-routing-consumers) |
| Why a message is "lost" instead of retried | [retry-and-dlq.md](./retry-and-dlq.md#in-process-retry-executewithretry-pipelinets) → `onMessageLost` |
| Where a windowed batch goes when its flush throws | [consumer.md](./consumer.md#known-semantic-caveats) → `onMessageLost` |
| DLQ topic name and headers | [retry-and-dlq.md](./retry-and-dlq.md#dead-letter-queue) |
| How `replayDlq` picks its consumer group | [retry-and-dlq.md](./retry-and-dlq.md#ephemeral-vs-stable-group-optionsfrombeginning) |
| What `retryTopics` does NOT re-apply | [retry-and-dlq.md](./retry-and-dlq.md#what-does-not-propagate-into-the-retry-chain) |
| Why `@SubscribeTo` handlers aren't wired twice | [nestjs.md](./nestjs.md#the-double-registration-guard) |
| Why DI uses explicit `@Inject` tokens | [nestjs.md](./nestjs.md#the-esbuild--designparamtypes-constraint) |
| Which module options reach the client | [nestjs.md](./nestjs.md#which-options-forward-to-the-client) |
| How to test without a real broker | [testing.md](./testing.md#faketransport-srctestingtransportfakets) |
| Where `FakeTransport` diverges from the real driver | [testing.md](./testing.md#known-fidelity-divergences-from-the-real-driver) |
| Why `handle.ready()` is instant in tests | [testing.md](./testing.md#how-handleready-keeps-tests-deterministic) |
| How the chaos / failure-injection suite is run | [testing.md](./testing.md#chaos-suite) + [module-map.md](./module-map.md#srcchaos--chaos--failure-injection-suite) |
| Where the `kafka-client-dlq` CLI lives | [module-map.md](./module-map.md#srccli--the-kafka-client-dlq-cli) |
| How env vars map to options (and precedence) | [configuration.md](./configuration.md) + [module-map.md](./module-map.md#srcclientconfig--environment-configuration) |
| Which option maps to which env var | [configuration.md](./configuration.md#kafkaclientoptions-per-client) (client) + [ConsumerOptions](./configuration.md#consumeroptions-per-consumer) |
| Why env never overrides code options | [configuration.md](./configuration.md#the-three-layers) (precedence rule) |
| Where to configure X (message/consumer/client/env) | [configuration.md](./configuration.md#where-do-i-configure-x) (decision table) |
