# Roadmap

> Living document — updated as priorities shift.

## 0.3.0 — Stabilization & Framework-agnostic core

- [ ] **Logger abstraction** — `KafkaLogger` interface, default `console`, NestJS `Logger` in adapter
- [ ] **Dual entrypoint** — `@drarzter/kafka-client/core` (zero framework deps) / `@drarzter/kafka-client` (NestJS adapter)
- [ ] **NestJS as peer dependency** — move `@nestjs/*` to `peerDependencies`
- [ ] **Transaction abort safety** — don't mask original error when `tx.abort()` throws
- [ ] **Configurable partitions** — expose `numPartitions` in `KafkaClientOptions` (currently hardcoded to `1`)

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
