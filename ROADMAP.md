# Roadmap

> Living document — updated as priorities shift.

## Upcoming

### Larger features

- **DLQ replay** — `client.replayDlq(topic, options?)` reads `{topic}.dlq`, strips DLQ metadata headers, and re-publishes messages to the original topic; supports `from` / `to` time-range filters and a dry-run mode
- **Graceful shutdown with drain** — on `SIGTERM` / `SIGINT`: pause all consumers, wait for in-flight handlers to complete, commit offsets, then disconnect; currently `disconnect()` tears down immediately
- **`SchemaContext` in `parse()`** — extend `SchemaLike.parse(data, ctx?: { topic, headers, version })` so validators can inspect message metadata; enables schema-registry adapters, version-aware migration, and header-driven parsing
- **Metrics hooks** — dedicated `metricsHook` in `KafkaClientOptions` for numeric observability (consumer lag, retry count, DLQ count, handler duration histogram); separate from OTel spans, targets Prometheus / Datadog / StatsD
- **Benchmarks** — compare throughput / latency: raw `@confluentinc/kafka-javascript` → `@drarzter/kafka-client` → `@nestjs/microservices` Kafka transport; quantify abstraction overhead
- **`retryTopics` for batch consumer** — `startBatchConsumer` currently does not support `retryTopics`; routing individual failed messages from a batch needs per-message retry topic dispatch
- **Circuit breaker** — stop retrying when downstream is consistently unavailable
- **Transport abstraction** — `KafkaTransport` interface to decouple `KafkaClient` from `@confluentinc/kafka-javascript`; swap transports without touching business code

### Fixes & small improvements

- **`checkStatus()` discriminated union** — currently returns `{ status: "up" }` or throws; align with `KafkaHealthResult` so callers get `{ status: "up"; ... } | { status: "down"; error: string }` instead of a try/catch
- **Expose `waitForPartitionAssignment` timeout** — the 10 s deadline is hardcoded; add `retryTopicAssignmentTimeoutMs` to `ConsumerOptions` so slow brokers don't silently stall `onModuleInit`

---

## Done

### Refactoring

- [x] **Split `KafkaClient` into subdirectory modules** — `kafka.client.ts` (1 034 lines) decomposed into `kafka.client/index.ts`, `producer-ops.ts`, `consumer-ops.ts`, `message-handler.ts`, `retry-topic.ts`; `topic`/`envelope` moved to `message/`, `pipeline`/`subscribe-retry` to `consumer/`; extracted `preparePayload()`, `parseSingleMessage()`, `notifyAfterSend()`, `broadcastToInterceptors()`, `buildClient()`, `buildDestroyProvider()` to eliminate repeated send/consume patterns
- [x] **Deduplicate retry consumer pipeline** — extracted `runHandlerWithPipeline` and `notifyInterceptorsOnError` from `consumer-pipeline.ts`; both `executeWithRetry` and `startRetryTopicConsumers` now share one instrumentation/interceptor lifecycle instead of two parallel copies
- [x] **`KafkaModuleBaseOptions`** — extracted shared `name` / `isGlobal` into a base interface; `KafkaModuleOptions` and `KafkaModuleAsyncOptions` both extend it; removed dead `autoCreateTopics` field from async options
- [x] **Expose `onMessageLost` / `onRebalance` in `KafkaModule`** — both options now appear in `KafkaModuleOptions` and are forwarded to `KafkaClient` in `register()` and `registerAsync()`
- [x] **Warn on `getOrCreateConsumer` option mismatch** — tracks creation options per group ID; logs a warning when `fromBeginning` or `autoCommit` differ for an existing consumer instead of silently dropping new values
- [x] **Document `transactionalId` uniqueness** — added JSDoc warning to `transaction()`: two `KafkaClient` instances with the same `clientId` share the same `transactionalId`, causing Kafka to fence one of the producers
- [x] **`beforeSend` / `afterSend` symmetry** — `afterSend` is now called once per message (N times for a batch of N), matching `beforeSend` semantics; previously it was called once per `sendMessage`/`sendBatch` invocation regardless of batch size
- [x] **`KafkaLogger` `debug` level** — added optional `debug?(message, ...args)` to the `KafkaLogger` interface; omit it to suppress debug output in production; NestJS `Logger` satisfies the interface without changes
- [x] **`eval` in `detectMockFactory` — investigated and kept intentionally** — Jest injects `jest` as a module-scope binding (not a `globalThis` property), so `globalThis["jest"]` returns `undefined`; `eval` is the only reliable way to reach this scope-level binding without a hard `@jest/globals` import; replaced the original two-eval pattern with a cleaner try/catch structure

### 0.5.5

- [x] **`getConsumerLag(groupId?)`** — query consumer group lag per partition via the admin API; returns `[{ topic, partition, lag }]`; no external tooling required
- [x] **Handler timeout warning** — `handlerTimeoutMs` option on `startConsumer` / `startBatchConsumer`; logs a `warn` if the handler has not resolved within the configured window — catches stuck handlers before they silently starve a partition
- [x] **`onRebalance` hook** — `KafkaClientOptions.onRebalance(type: 'assign' | 'revoke', partitions)` callback; wired via librdkafka's native `rebalance_cb`; callers can react to partition assignment changes without patching the consumer
- [x] **`startConsumer` / `startBatchConsumer` return `ConsumerHandle`** — `{ groupId: string; stop: () => Promise<void> }` so callers don't need to remember the group ID to call `stopConsumer(groupId)` later

### 0.5.4

- [x] **`retryTopics` validation** — throws a clear error at startup if `retryTopics: true` is set without a `retry` config, instead of silently ignoring the option
- [x] **Partition pause/resume in retry consumer** — replaced bare `sleep()` inside `eachMessage` with `consumer.pause(partition) → sleep → consumer.resume(partition)`; only the specific partition is paused during the scheduled delay, so other partitions on the same retry consumer continue processing

### 0.5.3

- [x] **`stopConsumer(groupId?)`** — selective consumer stop by group ID; `stopConsumer()` (no args) retains the existing "stop all" behaviour
- [x] **`KafkaHealthResult` discriminated union** — replaced the loose `interface` with `{ status: "up"; topics: string[] } | { status: "down"; error: string }` — narrows cleanly in `if` / `switch` on `status`
- [x] **Retry topic chain (`retryTopics: true`)** — opt-in durable retry via Kafka topics instead of in-process sleep; failed messages are routed to `<topic>.retry` with scheduling headers (`x-retry-attempt`, `x-retry-after`, `x-retry-max-retries`, `x-retry-original-topic`); a companion consumer auto-starts on `<topic>.retry` with group `<groupId>-retry`, backs off, retries, and on exhaustion routes to `<topic>.dlq` or calls `onMessageLost`
- [x] **Chaos / rebalance integration tests** — `chaos.integration.spec.ts`: rebalance test (2 consumers, one leaves mid-flight, remaining consumer resumes without loss), selective-stop test, retry topic chain happy + exhaustion paths

### 0.5.2

- [x] **Exponential backoff + full jitter** — retry delay was linear (`backoffMs * attempt`); now `random(0, min(backoffMs * 2^attempt, maxBackoffMs))` — prevents thundering herd under load
- [x] **`retry.maxBackoffMs`** — new option to cap the exponential delay (default `30000` ms)
- [x] **DLQ metadata headers** — dead letter messages now carry `x-dlq-original-topic`, `x-dlq-failed-at`, `x-dlq-error-message`, `x-dlq-error-stack`, `x-dlq-attempt-count`, plus all original message headers (preserves `x-correlation-id`, `traceparent`, etc.)
- [x] **Async `SchemaLike`** — `parse(data: unknown): T | Promise<T>` — enables async validators, remote schema registry lookup, and Confluent Schema Registry adapters
- [x] **`onMessageLost` hook** — `KafkaClientOptions.onMessageLost(ctx)` is called whenever a message is silently dropped (handler throws without DLQ, or schema validation fails without DLQ); receives `{ topic, error, attempt, headers }` — closes the "silent loss" gap

### 0.5.1 — Confluent client migration

- [x] **Replace kafkajs** with `@confluentinc/kafka-javascript` — kafkajs is unmaintained; Confluent's client wraps librdkafka with a KafkaJS-compatible API
- [x] **Separate transactional producer** — librdkafka forbids non-tx sends on a transactional producer; lazy-created `txProducer` on first `transaction()` call
- [x] **System librdkafka support** — `BUILD_LIBRDKAFKA=0` install path documented; resolves native build failures on Arch/CachyOS

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

### 0.3.1

- [x] **Testing utilities** (`@drarzter/kafka-client/testing` entrypoint)
  - [x] `createMockKafkaClient<T>()` — fully typed mock with `jest.fn()` / `vi.fn()` on every `IKafkaClient` method
  - [x] `KafkaTestContainer` — testcontainers wrapper that starts Kafka and exposes `brokers`
- [x] **Quieter logs** — retriable broker errors (`TOPIC_ALREADY_EXISTS`, `GROUP_COORDINATOR_NOT_AVAILABLE`) downgraded from `error` to `warn`
- [x] **`typesVersions` fallback** — subpath imports (`/core`, `/testing`) work with `moduleResolution: "node"`

### 0.3.0

- [x] **Framework-agnostic core** — `@drarzter/kafka-client/core` entrypoint with zero NestJS deps
- [x] **Logger abstraction** — `KafkaLogger` interface, default `console`, NestJS `Logger` in adapter
- [x] **NestJS as optional peer** — `peerDependenciesMeta` with `optional: true`
- [x] **Adapter pattern** — NestJS code moved to `src/nest/`, core in `src/client/`
- [x] **Transaction abort safety** — `tx.abort()` failure no longer masks the original error
- [x] **Configurable partitions** — `numPartitions` option in `KafkaClientOptions`

### 0.2.1

- [x] `strictSchemas` mode (default `true`) — validates string topics against schemas registered via TopicDescriptor
- [x] Mixed eachMessage/eachBatch detection — clear error instead of kafkajs crash
- [x] `subscribeWithRetry` — configurable retry/backoff for `consumer.subscribe()`
- [x] Remove `ensureTopic()` from consumer paths (producers only)
- [x] Refactor — extract `toError`, `parseJsonMessage`, `validateWithSchema`, `executeWithRetry`, `buildSendPayload`, `setupConsumer`; eliminate duplication (721 → 681 lines)

### 0.2.0

- [x] Schema validation (`SchemaLike<T>` — works with Zod, Valibot, ArkType, or any `.parse()`)
- [x] Batch consumer (`startBatchConsumer` with `eachBatch`)
- [x] Multiple consumer groups (per-call `groupId` override)
- [x] `TopicDescriptor` with type inference
- [x] `@SubscribeTo` decorator with batch & groupId support
