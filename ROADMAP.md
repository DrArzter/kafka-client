# Roadmap

> Living document — updated as priorities shift.

## 0.4.0 — EventEnvelope & Observability

- [ ] **EventEnvelope\<T\>** in core — standardized wrapper with `eventId`, `correlationId`, `schemaVersion`, `timestamp`, `traceparent`
- [ ] **Auto-wrap/unwrap** — opt-in via `envelope: true` in send/consume options
- [ ] **OpenTelemetry interceptor** — auto-inject `traceparent` header on send, extract & propagate on consume
- [ ] **correlationId propagation** — auto-forward through message headers

## 0.5.0 — Confluent client migration

- [ ] **Replace kafkajs** with `@confluentinc/kafka-javascript` — kafkajs is unmaintained, Confluent's client wraps librdkafka with a KafkaJS-compatible API
- [ ] **Benchmarks** — compare throughput/latency before and after migration
- [ ] **Migration guide** — document any breaking changes for consumers of the library

## Done

### 0.2.0

- [x] Schema validation (`SchemaLike<T>` — works with Zod, Valibot, ArkType, or any `.parse()`)
- [x] Batch consumer (`startBatchConsumer` with `eachBatch`)
- [x] Multiple consumer groups (per-call `groupId` override)
- [x] `TopicDescriptor` with type inference
- [x] `@SubscribeTo` decorator with batch & groupId support

### 0.2.1

- [x] `strictSchemas` mode (default `true`) — validates string topics against schemas registered via TopicDescriptor
- [x] Mixed eachMessage/eachBatch detection — clear error instead of kafkajs crash
- [x] `subscribeWithRetry` — configurable retry/backoff for `consumer.subscribe()`
- [x] Remove `ensureTopic()` from consumer paths (producers only)
- [x] Refactor — extract `toError`, `parseJsonMessage`, `validateWithSchema`, `executeWithRetry`, `buildSendPayload`, `setupConsumer`; eliminate duplication (721 → 681 lines)

### 0.3.1

- [x] **Testing utilities** (`@drarzter/kafka-client/testing` entrypoint)
  - [x] `createMockKafkaClient<T>()` — fully typed mock with `jest.fn()` / `vi.fn()` on every `IKafkaClient` method
  - [x] `KafkaTestContainer` — testcontainers wrapper that starts Kafka and exposes `brokers`
- [x] **Quieter kafkajs logs** — retriable broker errors (`TOPIC_ALREADY_EXISTS`, `GROUP_COORDINATOR_NOT_AVAILABLE`) downgraded from `error` to `warn`
- [x] **`typesVersions` fallback** — subpath imports (`/core`, `/testing`) work with `moduleResolution: "node"`

### 0.3.0

- [x] **Framework-agnostic core** — `@drarzter/kafka-client/core` entrypoint with zero NestJS deps
- [x] **Logger abstraction** — `KafkaLogger` interface, default `console`, NestJS `Logger` in adapter
- [x] **NestJS as optional peer** — `peerDependenciesMeta` with `optional: true`
- [x] **Adapter pattern** — NestJS code moved to `src/nest/`, core in `src/client/`
- [x] **Transaction abort safety** — `tx.abort()` failure no longer masks the original error
- [x] **Configurable partitions** — `numPartitions` option in `KafkaClientOptions`
