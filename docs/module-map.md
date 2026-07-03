# Module map

Every source file, grouped by directory, with a one- or two-sentence
responsibility. Use this to locate the code that owns a concern; use the
topic-specific docs (`producer.md`, `consumer.md`, …) for how the pieces fit
together.

Paths are relative to the repo root. Test files (`src/**/__tests__`,
`src/integration`) are omitted — they mirror the modules they cover.

---

## `src/` — entry points

| File            | Responsibility |
|-----------------|----------------|
| `index.ts`      | Full public surface: re-exports `core.ts` plus the whole NestJS layer. Maps to `@drarzter/kafka-client`. |
| `core.ts`       | NestJS-free surface: `KafkaClient`, all types, `topic()`, envelope helpers, errors, the serde layer (`MessageSerde` / `JsonSerde` / `SerdeContext`), `SchemaRegistryClient`, and the now-public transport surface (`ConfluentTransport` + the `KafkaTransport` / `IProducer` / `IConsumer` / `IAdmin` / `ITransaction` interface family). Maps to `/core`. |
| `serde.ts`      | Registry-backed binary serdes: `avroSerde` / `protobufSerde` (Confluent wire format `[0x00][schema id BE][payload]`), plus re-exports of `JsonSerde`, `MessageSerde`, `SerdeContext`, and `SchemaRegistryClient` so `/serde` is self-contained. Optional peer deps on `avsc` / `protobufjs`, loaded via dynamic import. Maps to `/serde`. |
| `otel.ts`       | `otelInstrumentation()` factory — a `KafkaInstrumentation` that propagates W3C Trace Context. Maps to `/otel`. Peer dep on `@opentelemetry/api`. |
| `testing.ts`    | Re-export barrel for `testing/index.ts`. Maps to `/testing`. |

---

## `src/client/` — client root

| File                     | Responsibility |
|--------------------------|----------------|
| `types.ts`               | Barrel re-exporting everything under `types/`. Existing `from '../types'` imports resolve here. |
| `errors.ts`              | `KafkaProcessingError`, `KafkaValidationError`, `KafkaRetryExhaustedError` (extends `KafkaProcessingError`), and the `toError(err)` coercion helper (re-exported from `consumer/pipeline.ts`). |

### `src/client/transport/`

Both files are re-exported through `/core` (and the main entry) so callers can
inject a custom `KafkaTransport` or reuse `ConfluentTransport` directly.

| File                     | Responsibility |
|--------------------------|----------------|
| `transport.interface.ts` | The broker seam: `KafkaTransport` factory plus `IProducer`, `IConsumer`, `IAdmin`, `ITransaction` and all their payload types. Public via `/core`. |
| `confluent.transport.ts` | `ConfluentTransport` implementing `KafkaTransport`, plus the `Confluent{Producer,Consumer,Admin,Transaction}` wrappers around `@confluentinc/kafka-javascript`. The only production file importing the driver. `ConfluentTransport` is re-exported from `/core` (the interface family alongside it). Its constructor takes `(clientId, brokers, security?)` and applies the resolved TLS/SASL config (incl. OAUTHBEARER token-shape adaptation). `ConfluentProducer.connect()` caches its connect promise so `connectProducer()` and the lazy consumer-pipeline connect coexist without librdkafka's "Connect has already been called" error. |

### `src/client/message/`

| File                  | Responsibility |
|-----------------------|----------------|
| `envelope.ts`         | `EventEnvelope<T>`, header-key constants (incl. `HEADER_DELAYED_UNTIL` = `x-delayed-until`, `HEADER_DELAYED_TARGET` = `x-delayed-target`), the `AsyncLocalStorage` context (`runWithEnvelopeContext` / `getEnvelopeContext`), and the header codecs `buildEnvelopeHeaders` / `decodeHeaders` / `extractEnvelope`. |
| `topic.ts`            | `TopicDescriptor` (carries `__topic`, `__type`, optional `__schema`, optional `__key` extractor, optional `__serde` override), the `topic()` factory (`.type<T>()` / `.schema(s)`) and the chainable `.key(fn)` / `.serde(s)` builders on `KeyableTopicDescriptor`, `SchemaLike` + `SchemaParseContext`, and the `TopicsFrom<>` map-inference helper. |
| `serde.ts`            | The pluggable value-serialization seam: the `MessageSerde` interface (`serialize` / `deserialize`, both sync-or-async) and its `SerdeContext` (`{ topic, headers, isKey? }`), plus the default `JsonSerde` (`JSON.stringify` / `JSON.parse` — byte-for-byte the client's historical behaviour). Serde touches **only** the message value; envelope headers always travel as headers. See [`serialization.md`](./serialization.md). |
| `versioned-schema.ts` | `versionedSchema(versions, options?)` — composes per-version validators into one version-aware `SchemaLike` that dispatches on the `x-schema-version` header (via `SchemaParseContext.version`) and runs an optional `migrate(data, fromVersion, latestVersion)` hook to upgrade old shapes. `VersionedSchemaOptions`. |
| `schema-registry.ts`  | `SchemaRegistryClient` — a minimal HTTP client for a Confluent-compatible Schema Registry: `registerSchema` / `getLatestSchema` / `getSchemaVersion` / `checkCompatibility` (subject lookups cached for 5 min), plus `getSchemaById(id)` — the id→schema resolver the Avro/Protobuf serdes call on deserialize, cached **forever** (schema ids are immutable, so a given id costs one round-trip regardless of message count). `registrySchema(...)` wraps the client into a `SchemaLike` for a `TopicDescriptor`. The client itself is JSON-agnostic — the wire framing lives in the serdes (`src/serde.ts`). Distinct from the client's in-process `ctx.schemaRegistry` map (`Map<topic, SchemaLike>`). |

### `src/client/types/`

`.types` files export only types; `common.ts` is the shared-primitive exception;
`*.interface.ts` files are role contracts; `client.ts` is a barrel interface.

| File                    | Responsibility |
|-------------------------|----------------|
| `common.ts`             | Shared primitives: `TopicMapConstraint`, `TTopicMessageMap`, `ClientId`, `GroupId`, `MessageHeaders`, `CompressionType`, `KafkaLogger`, `KafkaMetrics`. |
| `producer.types.ts`     | `SendOptions`, `BatchMessageItem`, `BatchSendOptions`, and related send shapes. |
| `consumer.types.ts`     | `ConsumerOptions` (incl. `groupInstanceId`, `subscribeRetry`), `ConsumerHandle`, `BatchMeta`, `WindowMeta`, `RetryOptions`, `DeduplicationOptions` (incl. optional `store`), `RoutingOptions`, `ConsumerInterceptor`, `KafkaInstrumentation`, `MessageLostContext`, `TtlExpiredContext`, `CircuitBreakerOptions`, callback types. |
| `dedup.types.ts`        | `DedupStore` — the pluggable last-Lamport-clock backing store interface (`getLastClock` / `setLastClock`, sync-or-async) with documented fail-open semantics. |
| `admin.types.ts`        | Snapshot / checkpoint / DLQ-replay / health result shapes (`KafkaHealthResult`, `CheckpointResult`, `ReadSnapshotOptions`, …). |
| `config.types.ts`       | `KafkaClientOptions` — the constructor options object (incl. `security`, `transport`, and the client-wide `serde`). |
| `producer.interface.ts` | `IKafkaProducer<T>` — send-side method contract. |
| `consumer.interface.ts` | `IKafkaConsumer<T>` — consume-side method contract. |
| `admin.interface.ts`    | `IKafkaAdmin<T>` — admin/observability method contract. |
| `lifecycle.interface.ts`| `IKafkaLifecycle` — connect/disconnect/shutdown contract. |
| `client.ts`             | `IKafkaClient<T>` — the full interface, extends the four role interfaces above. |

---

## `src/client/kafka.client/` — the client implementation

| File                     | Responsibility |
|--------------------------|----------------|
| `index.ts`               | `KafkaClient<T>` facade. Runs `validateClientOptions`, resolves security (`resolveSecurityOptions` → `ConfluentTransport`'s 3rd arg), builds `ctx` in the constructor; every method delegates to an impl function passing `this.ctx`. Also exposes `startDelayedRelay` and re-exports `InMemoryDedupStore`. |
| `context.ts`             | `KafkaClientContext<T>` — the state-bag type shared by all impl modules. Types only. |
| `validate-options.ts`    | `validateClientOptions(clientId, groupId, brokers, options)` — fail-fast constructor-argument validation. Collects every problem (empty ids, empty/malformed `brokers` unless a custom `transport` is supplied, non-positive `numPartitions`, blank `transactionalId`, bad `clockRecovery`/`lagThrottle` numerics) and throws one aggregated error. Called first thing in the facade constructor. |

### `src/client/kafka.client/producer/`

| File          | Responsibility |
|---------------|----------------|
| `ops.ts`      | Pure payload helpers: `resolveTopicName`, `resolveSerde` (per-topic `__serde` override else the client-wide serde), `registerSchema`, `validateMessage`, and `buildSendPayload` (builds envelope headers, stamps Lamport clock, runs `beforeSend`, validates the object, then serialises the validated value through the resolved serde). Resolves the message key as `m.key ?? descriptor.__key?.(value) ?? null` — explicit key wins, then the `TopicDescriptor` key extractor. |
| `lifecycle.ts`| Producer connect/disconnect (`connectProducerImpl`, `disconnectImpl`), `ensureTopic` (auto-create with promise de-dup), `createRetryTxProducer`, the `_activeTransactionalIds` registry, lag-throttle poller (`startLagThrottlePoller`), Lamport clock recovery (`recoverLamportClockImpl`), and `wrapWithTimeoutWarning`. |
| `send.ts`     | `sendMessageImpl`, `sendBatchImpl`, `sendTombstoneImpl`, `transactionImpl` (with `_txChain` serialisation), the shared `preparePayload` step, `redirectToDelayed` (retargets a payload to `<topic>.delayed` + stamps `x-delayed-until`/`x-delayed-target` when `deliverAfterMs` is set), and `waitIfThrottled`. |

### `src/client/kafka.client/consumer/`

| File               | Responsibility |
|--------------------|----------------|
| `ops.ts`           | `getOrCreateConsumer` (creates/caches consumers, wires the debounced `handle.ready()` resolver via `onRebalance`, forwards `groupInstanceId` for static membership) and `buildSchemaMap`. |
| `handler.ts`       | The per-message pipeline: `parseSingleMessage` (deserialises the raw `Buffer` through the resolved serde — per-topic `serdeMap` else `deps.serde` — before schema validation, never `.toString()`ing so binary formats survive), `applyDeduplication` (`DedupStore`-backed, fail-open), `handleEachMessage`, `handleEachBatch`. Forwards the original wire bytes losslessly to DLQ/retry/duplicates. Defines `DeduplicationContext` (`{ options, store, groupId }`). Builds the EOS commit/route closures. |
| `pipeline.ts`      | The retry/DLQ engine: `executeWithRetry`, `runHandlerWithPipeline` (interceptor + instrumentation lifecycle), `sendToDlq`, `sendToRetryTopic`, `sendToDuplicatesTopic`, the `build*Payload` helpers, `validateWithSchema`, `parseJsonMessage`, and utility `sleep`. Owns `toError` (re-exported from `errors.ts`). |
| `queue.ts`         | `AsyncQueue` — a push-to-pull async queue bridging Kafka's push model to the `consume()` async iterator, with high-water-mark backpressure. |
| `setup.ts`         | Consumer bootstrap: `setupConsumer` (group-conflict guard, schema map, connect, **lazy `producer.connect()` when `dlq`/`retryTopics`/`deduplication` need to route**, subscribe), `ensureConsumerTopics`, `resolveDeduplicationContext` (picks `options.store` or an `InMemoryDedupStore` over `ctx.dedupStates`), `messageDepsFor`, `buildRetryTopicDeps`, `makeEosMainContext`, `launchRetryChain`, `validateTopicConsumerOpts`. |
| `start.ts`         | The three "start" entry impls: `startConsumerImpl`, `startBatchConsumerImpl`, `startTransactionalConsumerImpl`. Wire `consumer.run(...)` to the handler and register the group. |
| `stop.ts`          | `stopConsumerImpl` (single group or all), pause/resume (`pauseConsumerImpl`, `resumeConsumerImpl`, `pauseTopicAllPartitions`, `resumeTopicAllPartitions`). Uses the `StopCtx<T>` `Pick` alias. |
| `retry-topic.ts`   | `startRetryTopicConsumers` + `startLevelConsumer` — the durable retry chain. One consumer per `<topic>.retry.<level>`, EOS routing between levels via Kafka transactions. Defines `RetryTopicDeps`. |
| `subscribe-retry.ts`| `subscribeWithRetry` — retries `consumer.subscribe()` with jittered backoff when a topic doesn't exist yet. |

### `src/client/kafka.client/consumer/features/`

Higher-level consumer flavours and offset tooling layered on the core pipeline.

| File            | Responsibility |
|-----------------|----------------|
| `window.ts`     | `startWindowConsumerImpl` — size/time-windowed batching layered on `startConsumerImpl`. Flushes on `maxMessages` or `maxMs`; flush failures route to `onMessageLost`. |
| `routed.ts`     | `startRoutedConsumerImpl` — header-value dispatch to a `routes` map (with optional `fallback`), layered on `startConsumerImpl`. |
| `delayed.ts`    | `startDelayedRelayImpl` + `delayedTopicName` — the delayed-delivery relay. Consumes `<topic>.delayed`, pauses the partition until each message's `x-delayed-until` deadline, then forwards to `x-delayed-target` via a Kafka transaction (produce + source-offset commit atomic). Group `<groupId>-delayed-relay`, tx producer `<groupId>-delayed-relay-tx`. Strips the `x-delayed-*` control headers on forward. |
| `snapshot.ts`   | `readSnapshotImpl` (compacted-topic → `Map<key, envelope>`), `checkpointOffsetsImpl`, `restoreFromCheckpointImpl`. Uses the `SnapshotCtx<T>` `Pick` alias and ephemeral consumer groups. |
| `dlq-replay.ts` | `replayDlqTopic` — reads `<topic>.dlq`, strips `x-dlq-*` headers, re-publishes to the original (or target) topic. Rejects a `topic` that already ends in `.dlq`. Ephemeral vs stable group logic. Defines `DlqReplayDeps`. |

### `src/client/kafka.client/admin/`

| File      | Responsibility |
|-----------|----------------|
| `ops.ts`  | `AdminOps` class: lazy admin connect (`ensureConnected`), `getConsumerLag`, `checkStatus`, `listConsumerGroups`, `describeTopics`, `deleteRecords`, `resetOffsets`, `seekToOffset`, `seekToTimestamp`, `deleteGroups`, and the startup validators (`validateRetryTopicsExist`, `validateDlqTopicsExist`, `validateDuplicatesTopicsExist`). |

### `src/client/kafka.client/infra/`

| File                        | Responsibility |
|-----------------------------|----------------|
| `circuit-breaker.manager.ts`| `CircuitBreakerManager` — per `gid:topic:partition` sliding-window breaker. CLOSED → OPEN → HALF-OPEN → CLOSED; pauses/resumes partitions on transitions. |
| `metrics.manager.ts`        | `MetricsManager` — per-topic counters (`processedCount`/`retryCount`/`dlqCount`/`dedupCount`), fires instrumentation hooks, and forwards failure/success to the circuit breaker. |
| `inflight.tracker.ts`       | `InFlightTracker` — counts in-flight handler promises so `disconnect()` can drain before shutdown. |
| `dedup.store.ts`            | `InMemoryDedupStore` — the default `DedupStore`, backed by the injected `ctx.dedupStates` nested map (`groupId → "topic:partition" → clock`). Re-exported from the package root so callers can compose it. |

---

## `src/client/security/` — transport security

Exported via `/core` (and the main entry). Barrel: `index.ts`.

| File                  | Responsibility |
|-----------------------|----------------|
| `security.types.ts`   | `KafkaSecurityOptions` (`ssl?`, `sasl?`, `allowInsecure?`), `KafkaSaslOptions` (username/password for `plain`/`scram-*`, or `oauthbearer` + `oauthBearerProvider`), `OAuthBearerProvider` (async token factory) and `OAuthBearerToken`. |
| `resolve-security.ts` | `resolveSecurityOptions(security, brokers, logger)` — secure-by-default rules: SASL-set-but-SSL-unset auto-enables TLS; explicit `ssl: false` under SASL warns; no security against a non-local broker logs a one-time warning (loopback/`host.docker.internal` allowlisted; `allowInsecure` silences it). Never throws. Called in the facade constructor; its result is `ConfluentTransport`'s 3rd arg. |
| `providers.ts`        | `awsMskIamProvider({ region, … })` and `gcpAccessTokenProvider({ scopes?, … })` — `OAuthBearerProvider` factories that **dynamically import** their optional peer SDKs (`aws-msk-iam-sasl-signer-js`, `google-auth-library`) so the deps stay optional. Both return `oauthbearer` tokens. |
| `acl.ts`              | `describeRequiredAcls(input)` — enumerates the topics/groups/transactional-ids and operations a service needs (accounting for retry/DLQ/delayed/duplicates topics and ephemeral groups). `toKafkaAclCommands(resources, principal, bootstrap?)` renders `kafka-acls.sh` lines; `toMskIamPolicy(resources, cluster)` renders an AWS MSK IAM policy document. |

---

## `src/client/outbox/` — transactional outbox

Exported via `/core`. Barrel: `index.ts`.

| File              | Responsibility |
|-------------------|----------------|
| `outbox.types.ts` | `OutboxMessage` (row `id`, `topic`, `payload`, optional `key`/`headers`/`correlationId`/`eventId`), `OutboxStore` (`fetchUnpublished(limit)` oldest-first + `markPublished(ids)`), `OutboxRelayOptions` (`pollIntervalMs`, `batchSize`, `onError`, `onPublished`), `OutboxProducer<T>` (narrow `{ transaction }` surface), `OutboxRelayHandle` (`stop()`). |
| `outbox.store.ts` | `InMemoryOutboxStore` — reference `OutboxStore` for tests/docs (insertion-ordered rows, `add`, `pendingCount`/`publishedCount` helpers). Not durable. |
| `outbox.relay.ts` | `startOutboxRelay(kafka, store, options?)` — polls `fetchUnpublished` on an interval, sends each batch inside **one** Kafka transaction, and calls `markPublished` **after** the tx commits. A non-overlap guard (`running` flag) skips a tick while the previous one is in flight; a crash between commit and `markPublished` re-publishes next tick → **at-least-once** (same `eventId` for consumer dedup). `stop()` clears the timer and awaits the in-flight tick. |

---

## `src/client/config/` — environment configuration

Exported via `/core`. Barrel: `index.ts`. See [`configuration.md`](./configuration.md) for the full option-to-env-var mapping and precedence.

| File          | Responsibility |
|---------------|----------------|
| `from-env.ts` | `kafkaClientConfigFromEnv(env?, prefix?)` → `{ clientId?, groupId?, brokers?, options }`; `consumerOptionsFromEnv<T>(env?, prefix?)` → `Partial<ConsumerOptions<T>>`; `mergeConsumerOptions<T>(...layers)` (later layers win, nested blocks deep-merged). Helpers emit a key **only** for present/non-empty vars, so unset vars leave library defaults in place. Precedence: **code > env > defaults**. Pure/synchronous. |

---

## `src/cli/` — the `kafka-client-dlq` CLI

Registered as the `kafka-client-dlq` bin in `package.json`.

| File       | Responsibility |
|------------|----------------|
| `dlq.ts`   | `parseArgs(argv)` (pure) and `runDlqCommand(cmd, deps)` — the DLQ ops tool with `ls` / `peek` / `replay` / `help` subcommands. `runDlqCommand` takes an injected `DlqCliClient` so the parser and command logic are unit-testable without a broker. Also exports `USAGE`, `DlqUsageError`, and the command/deps types. |
| `index.ts` | Bin entry (`#!/usr/bin/env node`) — wires a real `KafkaClient` into `runDlqCommand`, sets exit codes on error. |

---

## `src/chaos/` — chaos / failure-injection suite

Real-broker resilience specs run via `npm run test:chaos` (config `jest.chaos.config.ts` — `testMatch **/chaos/**/*.chaos.spec.ts`, `testTimeout 240 s`, `maxWorkers 1`, `--forceExit`). Require Docker.

| File                               | Responsibility |
|------------------------------------|----------------|
| `helpers.ts`                       | Shared chaos infra: container lifecycle (`startBroker`, `brokersOf`, `createTopics`), Docker `pauseContainer`/`unpauseContainer`, `makeClient`, `capturingLogger`, `sleep`, `waitUntil`. |
| `broker-restart.chaos.spec.ts`     | Pauses the broker mid-stream, keeps producing, unpauses — asserts every acked message arrives at-least-once (loss forbidden). |
| `poison-message-recovery.chaos.spec.ts` | Injects a permanently-failing message — asserts it dead-letters after max retries without head-of-line-blocking the partition. |
| `rebalance-under-load.chaos.spec.ts` | Adds then removes a consumer under continuous load — asserts at-least-once processing across the cooperative-sticky rebalance. |
| `slow-consumer-lag.chaos.spec.ts`  | Drives a lag-throttled producer against a slow consumer — asserts the "delayed"/"resumed" throttle warnings fire. |

---

## `bench/` — throughput benchmark

| File                 | Responsibility |
|----------------------|----------------|
| `throughput.bench.ts`| Measures produce/consume throughput (msg/s) and p50/p95 latency for the raw driver vs the wrapper against a Testcontainers broker; prints a markdown table. Run via `npm run bench` (`ts-node`, not Jest). |

---

## `src/nest/` — NestJS integration

| File                | Responsibility |
|---------------------|----------------|
| `kafka.module.ts`   | `KafkaModule.register()` / `registerAsync()`. Builds a `KafkaClient` provider, calls `connectProducer()` in the factory, imports `DiscoveryModule`, registers `KafkaExplorer`. |
| `kafka.explorer.ts` | `KafkaExplorer` (`OnModuleInit`) — scans all providers for `@SubscribeTo` metadata and wires each method to `startConsumer`/`startBatchConsumer`. Holds the double-registration guard. |
| `kafka.decorator.ts`| `@SubscribeTo` (method decorator storing subscription metadata) and `@InjectKafkaClient` (parameter decorator). |
| `kafka.health.ts`   | `KafkaHealthIndicator.check(client)` → delegates to `client.checkStatus()`. |
| `kafka.constants.ts`| `KAFKA_CLIENT` DI token and `getKafkaClientToken(name)`. |

---

## `src/testing/` — test infrastructure

| File                | Responsibility |
|---------------------|----------------|
| `index.ts`          | Barrel exporting the public testing surface. |
| `client.mock.ts`    | `createMockKafkaClient<T>()` — fully-typed mock where every method is a spy. Auto-detects Jest/Vitest. |
| `transport.fake.ts` | `FakeTransport` and its `FakeProducer` / `FakeConsumer` / `FakeAdmin` / `FakeTransaction` — an in-memory `KafkaTransport` for unit tests. |
| `test.container.ts` | `KafkaTestContainer` — Testcontainers wrapper: starts a KRaft Kafka, pre-creates topics, warms the transaction coordinator. |

## `src/__mocks__/@confluentinc/kafka-javascript.ts`

Jest manual mock of the driver — auto-loaded by Jest for every unit test that
imports `@confluentinc/kafka-javascript` (i.e. everything through
`ConfluentTransport`). Exports named `mock*` spies for assertion.

---

## File-naming conventions

The rule: **hyphens join words within a name; a dot separates the name from its
role suffix.** A suffix is added only when it carries information the name alone
does not — `handler.ts`, `pipeline.ts`, `queue.ts`, `setup.ts`, `send.ts`,
`lifecycle.ts` need none because the name already states the role.

```
circuit-breaker.manager.ts
└──────┬──────┘ └───┬───┘
    name (compound)  role suffix
```

Recognised suffixes:

| Suffix              | Role                                    | Example                        |
|---------------------|-----------------------------------------|--------------------------------|
| `.types`            | Data/option shapes (types only)         | `producer.types.ts`            |
| `.interface`        | Contract interface / polymorphism point | `transport.interface.ts`       |
| `.manager`          | Stateful manager class                  | `metrics.manager.ts`           |
| `.tracker`          | Tracking/counting utility               | `inflight.tracker.ts`          |
| `.module`           | NestJS module                           | `kafka.module.ts`              |
| `.explorer`         | NestJS metadata scanner                 | `kafka.explorer.ts`            |
| `.decorator`        | NestJS decorator definitions            | `kafka.decorator.ts`           |
| `.health`           | Health indicator                        | `kafka.health.ts`              |
| `.constants`        | Constant values / DI tokens             | `kafka.constants.ts`           |
| `.mock`             | Jest/Vitest spy double                  | `client.mock.ts`               |
| `.fake`             | Standalone fake implementation          | `transport.fake.ts`            |
| `.container`        | Test infrastructure wrapper             | `test.container.ts`            |
| `.spec`             | Unit test                               | `consumer.spec.ts`             |
| `.integration.spec` | Integration test                        | `consumer.integration.spec.ts` |

Extra rules:
- `.types` files export **only** `type` / `interface` — no runtime values.
- `common.ts` is an intentional exception (shared primitives, cross-domain).
- Barrel / re-export files keep their name (`types.ts`, `client.ts`, `index.ts`)
  regardless of content.
