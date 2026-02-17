# Roadmap

> Living document — updated as priorities shift.

## 0.5.0 — Confluent client migration

- [ ] **Replace kafkajs** with `@confluentinc/kafka-javascript` — kafkajs is unmaintained, Confluent's client wraps librdkafka with a KafkaJS-compatible API
- [ ] **Benchmarks** — compare throughput/latency before and after migration
- [ ] **Migration guide** — document any breaking changes for consumers of the library

## Done

### 0.4.0

- [x] **EventEnvelope\<T\>** — consumer handlers receive `EventEnvelope<T>` with `payload`, `eventId`, `correlationId`, `timestamp`, `schemaVersion`, `traceparent`, and Kafka metadata (`topic`, `partition`, `offset`)
- [x] **Auto envelope headers** — `x-event-id`, `x-correlation-id`, `x-timestamp`, `x-schema-version` generated on send, extracted on consume
- [x] **correlationId propagation** — auto-forward via `AsyncLocalStorage`; nested sends inherit the parent correlation ID
- [x] **KafkaInstrumentation** — client-wide hooks (`beforeSend`, `afterSend`, `beforeConsume`, `onConsumeError`) for cross-cutting concerns
- [x] **OpenTelemetry support** — `@drarzter/kafka-client/otel` entrypoint with `otelInstrumentation()` for W3C Trace Context propagation
- [x] **Breaking: consumer handler signature** — `(message, topic)` → `(envelope)` for `startConsumer`; `(messages, topic, meta)` → `(envelopes, meta)` for `startBatchConsumer`
- [x] **Breaking: interceptor signature** — `before(msg, topic)` / `onError(msg, topic, err)` → `before(envelope)` / `onError(envelope, err)`
- [x] **Send options** — `correlationId`, `schemaVersion`, `eventId` fields added to `SendOptions` and `BatchMessageItem`
- [x] **Refactor** — extracted `envelope.ts`, `consumer-pipeline.ts`, `subscribe-retry.ts` from `kafka.client.ts`; unit tests split into per-feature files under `__tests__/`

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
