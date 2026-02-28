# Roadmap

> Living document — updated as priorities shift.

## Upcoming

### Larger features

- **DLQ replay** — `client.replayDlq(topic, options?)` reads `{topic}.dlq`, strips DLQ metadata headers, and re-publishes messages to the original topic; supports `from` / `to` time-range filters and a dry-run mode
- **Benchmarks** — compare throughput / latency: raw `@confluentinc/kafka-javascript` → `@drarzter/kafka-client` → `@nestjs/microservices` Kafka transport; quantify abstraction overhead
- **Circuit breaker** — stop retrying when downstream is consistently unavailable
- **Transport abstraction** — `KafkaTransport` interface to decouple `KafkaClient` from `@confluentinc/kafka-javascript`; swap transports without touching business code

---

## Done

### 0.6.7

- [x] **Configurable `transactionalId`** — `KafkaClientOptions.transactionalId` lets callers override the transactional producer ID (default: `${clientId}-tx`); useful when running multiple replicas that must have distinct IDs to avoid Kafka producer fencing
- [x] **Duplicate `transactionalId` warning** — the client tracks all active transactional IDs in a module-level registry and logs a `warn` when the same ID is registered twice within one process; cross-process conflicts are still detected by the broker (fencing)
- [x] **`highWatermark: null` in batch-retry path** — the stub `BatchMeta` passed to batch handlers during retry topic replay now sets `highWatermark: null` instead of the incorrect `env.offset`; `BatchMeta.highWatermark` is now typed `string | null`, forcing callers to guard against `null` before computing lag — a compile-time breaking change that surfaces a previously silent semantic bug
- [x] **Built-in metrics** — `getMetrics()` returns a `KafkaMetrics` snapshot (`{ processedCount, retryCount, dlqCount, dedupCount }`); `resetMetrics()` resets all counters to zero; counters are always active regardless of whether any instrumentation is configured
- [x] **`KafkaInstrumentation` lifecycle hooks** — `onMessage(envelope)`, `onRetry(envelope, attempt, maxRetries)`, `onDlq(envelope, reason)`, and `onDuplicate(envelope, strategy)` added to `KafkaInstrumentation`; fire for both in-process retries and retry topic routing; `DlqReason` type exported; `onMessage` fires on every successful handler completion (use for error-rate calculations alongside `onDlq`)

### 0.6.6

- [x] **`transaction()` race condition on concurrent first calls** — two concurrent `transaction()` calls when `txProducer` was `undefined` each created and connected their own producer; the second would overwrite `this.txProducer`, leaving the first connected but untracked (leaked); guarded by `txProducerInitPromise: Promise<Producer> | undefined` — only the first call creates the promise, subsequent concurrent calls await the same one; on connect failure the promise is cleared so the next call can retry
- [x] **Batch DLQ all-same-headers bug** — when a batch handler failed and `dlq: true`, all messages were written to DLQ with the headers from `envelopes[0]`, losing `correlationId`, `traceparent`, and custom headers for messages at indices 1…N; now passes `envelopes[i]?.headers` per message (mirrors the 0.6.3 fix for `retryTopics` batch routing)
- [x] **`onConsumeError` not called on schema validation failure** — `validateWithSchema` called `interceptor.onError` for all interceptors but never called `inst.onConsumeError` on instrumentation; OTel spans did not get `ERROR` status for schema validation failures; fixed by adding `instrumentation?: KafkaInstrumentation[]` to `validateWithSchema` deps and calling `onConsumeError` before the interceptor loop
- [x] **`ensureTopic` race condition on concurrent `sendMessage` / `sendBatch`** — two parallel sends for the same new topic both passed the `ensuredTopics.has(topic)` check, both called `createTopics`, and the second call could fail if `createTopics` is not idempotent; guarded by `ensureTopicPromises: Map<string, Promise<void>>` — concurrent calls for the same topic await the same creation promise
- [x] **`stopConsumer` disconnect errors silently swallowed** — `consumer.disconnect().catch(() => {})` discarded errors; changed to `.catch((e) => this.logger.warn(...))` on all three disconnect paths (main consumer, companion retry consumers, retry tx producers)
- [x] **`DeduplicationOptions` in-session limitation documented** — JSDoc on `DeduplicationOptions` now explicitly warns that state is in-memory and resets on process restart or `stopConsumer`; after a restart, messages with `clock ≤ N` are re-processed until offsets catch up; same applies after a rebalance
- [x] **`deduplication.strategy: 'topic'` startup validation** — when `autoCreateTopics: false`, `startConsumer` / `startBatchConsumer` now validates that `{topic}.duplicates` (or `duplicatesTopic`) exists before starting, throwing a clear error listing missing topics; with `autoCreateTopics: true` the topic is created automatically instead; symmetric with the existing DLQ and retry-topic startup checks

### 0.6.5

- [x] **Lamport Clock deduplication** — every outgoing message is stamped with a monotonically increasing `x-lamport-clock` header (counter lives in the `KafkaClient` instance, incremented per message including batch items and transaction sends); consumers opt in via `deduplication` in `ConsumerOptions`; the library tracks the last processed clock per `topic:partition` in memory and skips messages whose clock is not strictly greater than the last seen value; three routing strategies for detected duplicates — `'drop'` (silent discard, default), `'dlq'` (forward to `{topic}.dlq` with `x-dlq-reason`, `x-dlq-duplicate-incoming-clock`, `x-dlq-duplicate-last-processed-clock` headers; requires `dlq: true`), `'topic'` (forward to `{topic}.duplicates` or a custom `duplicatesTopic` with `x-duplicate-*` metadata headers); messages without the clock header pass through unchanged (backwards-compatible with any producer); dedup state is cleared on `stopConsumer(groupId)` and `disconnect()`; works with both `startConsumer` and `startBatchConsumer` (per-message filtering inside batches — duplicates are excluded before the batch handler is called)

### 0.6.4

- [x] **`retryTxProducers` memory leak on `stopConsumer`** — the EOS transactional producers created per retry level were tracked in a `Set<Producer>` and only disconnected during full `disconnect()`; calling `stopConsumer(groupId)` left them alive, accumulating on repeated start/stop cycles; changed to `Map<string, Producer>` keyed by transactionalId so each producer can be looked up and disconnected when its companion consumer is stopped
- [x] **`${token}_DESTROY` pattern replaced by `onModuleDestroy()` on `KafkaClient`** — `KafkaModule` used a separate `${token}_DESTROY` factory provider to hook the NestJS lifecycle; this created a non-standard invisible provider that confused module introspection; `KafkaClient` now exposes `onModuleDestroy()` directly, which NestJS calls on any factory-provided value that has the method; `buildDestroyProvider` removed
- [x] **Duplicate `startConsumer` / `startBatchConsumer` on same groupId now throws** — calling `startConsumer` twice with the same groupId would invoke `consumer.run()` on an already-running consumer, which is invalid in librdkafka; the existing guard only caught cross-mode conflicts (eachMessage vs eachBatch); a same-mode duplicate now throws `"startConsumer("gid") called twice"` so the mistake is caught at startup rather than producing undefined behavior
- [x] **`getConsumerLag` empty-result behavior documented** — `fetchOffsets` returns an empty array for groups that have never committed; the JSDoc now explains this is a Kafka protocol limitation (not a bug) and points users to `checkStatus()` for connectivity verification
- [x] **`Math.floor` inconsistency in in-process retry backoff** — the retry-topic and `subscribeWithRetry` paths all used `Math.floor(Math.random() * cap)` for integer-millisecond delays, but the in-process retry `sleep()` used `Math.random() * cap` (float); aligned to `Math.floor` throughout

### 0.6.3

- [x] **`transaction()` missing `afterSend` notification** — `sendMessage` and `sendBatch` call `instrumentation.afterSend` after each send, but the `send`/`sendBatch` closures inside `transaction()` did not; `afterSend` was never fired for messages sent within a transaction; fixed by extracting `payload` into a variable and calling `notifyAfterSend` after each `tx.send()`
- [x] **Batch + `retryTopics: true` headers lost for messages 2..N** — `executeWithRetry` passed only `envelopes[0].headers` to `sendToRetryTopic` for all messages in the batch; messages 2..N ended up with the first message's `x-correlation-id`, `traceparent`, and custom headers in the retry topic; fixed by passing `envelopes.map(e => e.headers)` (array) when `isBatch`; `buildRetryTopicPayload` and `sendToRetryTopic` now accept `MessageHeaders | MessageHeaders[]` and zip each message with its own headers
- [x] **`startBatchConsumer` autoCommit diagnostic downgraded to `debug`** — the warning fired on every batch consumer start even for users who never use manual offset management (`resolveOffset` / `commitOffsetsIfNecessary`); changed to `this.logger.debug?.()` so it appears only when debug logging is explicitly enabled; default logger now includes `debug: console.debug`
- [x] **DLQ topic validated at startup when `autoCreateTopics: false`** — `dlq: true` would silently succeed at startup even if `{topic}.dlq` didn't exist; the missing topic was only discovered on the first handler failure, too late to be actionable; added `validateDlqTopicsExist` (symmetrical with `validateRetryTopicsExist`) that throws a clear error listing all missing DLQ topics at consumer start time
- [x] **`highWatermark` stub in batch-retry path documented** — retry consumers use `eachMessage`, so no real batch context is available; the stub `BatchMeta` used `env.offset` as `highWatermark`; this is semantically incorrect (offset of the current message ≠ partition high-watermark); added a comment explaining the architectural limitation (properly fixed in 0.6.7 — changed to `null` with updated type)

### 0.6.2

- [x] **`handleEachBatch` rawMessages mismatch** — `rawMessages` passed to `executeWithRetry` was rebuilt from `batch.messages.filter(m => m.value)`, including messages that had already been skipped and sent to DLQ by `validateWithSchema`; on batch handler failure those schema-invalid messages would be sent to DLQ a second time; fixed by using the `rawMessages` array built in sync with `envelopes` during the parse loop, so only messages that actually passed validation are included
- [x] **`KafkaHealthIndicator.check()` accepts `IKafkaClient` instead of `KafkaClient`** — the parameter type was the concrete class, making the health check incompatible with mocks and any alternative `IKafkaClient` implementation; changed to `IKafkaClient<T>` which is the correct abstraction since only `checkStatus()` is called
- [x] **`createMockKafkaClient` missing `enableGracefulShutdown`** — `IKafkaClient` declares `enableGracefulShutdown`, but the mock factory omitted it; calling `kafka.enableGracefulShutdown()` in a test would crash at runtime; the `as unknown as MockKafkaClient<T>` cast suppressed the TypeScript error; now included as a no-op mock
- [x] **`subscribeWithRetry` fixed backoff without jitter** — all subscribe retry attempts waited a fixed `backoffMs` (default 5 s); parallel consumer starts would hit the broker simultaneously on each retry wave; changed to full jitter (`Math.random() * backoffMs`) consistent with `executeWithRetry`

### 0.6.1

- [x] **`txProducer` assigned only after successful connect** — `this.txProducer` is now set only after `p.connect()` resolves; previously, assigning before `await` left a disconnected producer in the field, causing the next `transaction()` call to skip the `if (!this.txProducer)` guard and throw on a dead producer; now `txProducer` stays `null` on connect failure and the next call retries the full connect flow
- [x] **`sendToRetryTopic` calls `onMessageLost` on send failure** — if `producer.send()` to `<topic>.retry.N` throws (broker down, topic missing), `onMessageLost` is now called with the send error; previously the error was logged and the message was silently dropped (parity fix with `sendToDlq` which already called `onMessageLost` on failure)
- [x] **`getConsumerLag` uses `Promise.all` for broker offset fetches** — `fetchTopicOffsets` calls are now issued in parallel across all topics; previously N sequential round-trips to the broker; no behavior change, latency improvement proportional to topic count
- [x] **`IKafkaClient.getClientId` declared as method** — changed from property style (`getClientId: () => ClientId`) to method style (`getClientId(): ClientId`) for consistency with all other interface members
- [x] **`decodeHeaders` uses last-wins for multi-value headers** — Kafka allows duplicate header keys; the previous `join(",")` produced unparseable strings for values containing commas; now takes the last element of the array, consistent with HTTP semantics

### 0.6.0

- [x] **Exactly-once retry routing (EOS)** — routing from retry level consumers (retry.N → retry.N+1 and retry.N → DLQ) is now wrapped in a Kafka transaction via `producer.sendOffsets({ consumer, topics })`; the produce and the consumer offset commit happen atomically, eliminating the crash-between-route-and-commit duplicate window; each retry level consumer creates its own transactional producer (`${levelGroupId}-tx`) tracked in `KafkaClient.retryTxProducers` for cleanup on `disconnect()`; if the EOS transaction fails (broker unavailable), the offset is not committed and the message is redelivered — the message stays safe in the retry chain until the broker recovers; `buildRetryTopicPayload` / `buildDlqPayload` extracted as pure payload builders for use by both the EOS path and the existing non-transactional `sendToRetryTopic` / `sendToDlq` helpers; enabled automatically when `retryTopics: true` is set — no extra configuration needed

### 0.5.9

- [x] **`retryTopics` for batch consumer** — `startBatchConsumer` now supports `retryTopics: true`; on handler failure, each envelope in the batch is routed individually to `<topic>.retry.1`; retry consumers call the batch handler with a single-element batch and a stub `BatchMeta` (no-op heartbeat/resolveOffset/commitOffsetsIfNecessary); same validation and companion-consumer lifecycle as the single-message path
- [x] **`SchemaContext` in `parse()`** — `SchemaLike.parse(data, ctx?: SchemaParseContext)` now receives `{ topic, headers, version }` as a second argument on both the consume path (`validateWithSchema`) and the send path (`buildSendPayload`); fully backward-compatible — existing Zod/Valibot/ArkType validators ignore the extra argument; enables schema-registry adapters, version-aware migration, and header-driven parsing; `SchemaParseContext` exported from all entrypoints
- [x] **Graceful shutdown with drain** — `disconnect(drainTimeoutMs?)` now waits for all in-flight `eachMessage` / `eachBatch` handlers to settle before tearing down connections; in-flight handlers are tracked via a counter incremented at the `consumer.run()` callback level; drain resolves immediately if no handlers are running, otherwise waits up to `drainTimeoutMs` (default 30 s) and logs a warning on timeout; `enableGracefulShutdown(signals?, drainTimeoutMs?)` registers SIGTERM / SIGINT handlers for non-NestJS apps; NestJS apps get drain automatically via `onModuleDestroy` → `disconnect()`
- [x] **`onMessageLost` fired on DLQ send failure** — if `producer.send()` to `{topic}.dlq` throws (broker down, topic missing), `onMessageLost` is now called with the send error so the message is never silently dropped; `sendToDlq` accepts an optional `onMessageLost` in its deps, all call sites already supplied it
- [x] **`isAdminConnected` reset on connect failure** — admin connect logic extracted to `ensureAdminConnected()`; the flag is explicitly set to `false` in the catch block, guaranteeing that a failed `admin.connect()` leaves the flag `false` and the next call will retry the connection rather than skipping it with a misleading error
- [x] **Retry topic existence validated at startup** — with `retryTopics: true` and `autoCreateTopics: false`, `startConsumer` / `startBatchConsumer` now call `admin.listTopics()` before starting retry consumers and throw a clear error listing every missing `{topic}.retry.N` topic; with `autoCreateTopics: true` the check is skipped (topics are created by the existing `ensureTopic` path)
- [x] **Schema registry conflict warning** — `registerSchema` (send path) and `buildSchemaMap` (consume path) now compare the incoming schema against the already-registered one by reference; if they differ, a `logger.warn` is emitted at startup so mismatches surface before runtime validation failures
- [x] **`autoCommit: true` + manual batch offsets warning** — `startBatchConsumer` logs a `warn` at consumer-start time when `autoCommit` is not explicitly `false`, reminding callers that mixing autoCommit with `resolveOffset()` / `commitOffsetsIfNecessary()` causes offset conflicts

### 0.5.8

- [x] **OTel span activated via `context.with`** — `BeforeConsumeResult` type added to `KafkaInstrumentation.beforeConsume`; object form supports `cleanup?()` and `wrap?(fn)` alongside the legacy `() => void` form; `otelInstrumentation()` now returns `{ cleanup, wrap }` where `wrap` calls `context.with(trace.setSpan(parentCtx, span), fn)` — the consume span is set as the active span for the handler's duration, so `trace.getActiveSpan()` works inside handlers and child spans are automatically parented; multiple wraps from different instrumentations are composed (first instrumentation = outermost)

### 0.5.7

- [x] **`topic()` API cleanup (breaking)** — removed the callable `topic('foo')<T>()` form; `topic()` now returns a plain object with only `.type<T>()` and `.schema()`; all internal usages migrated to `.type<T>()`
- [x] **`checkStatus()` discriminated union** — `checkStatus()` now catches broker errors internally and returns `{ status: "down"; clientId: string; error: string }` instead of throwing; `KafkaHealthResult` type moved to `client/types.ts` and re-exported from `kafka.health.ts`; `KafkaHealthIndicator.check()` simplified to a direct forward
- [x] **Expose `waitForPartitionAssignment` timeout** — `retryTopicAssignmentTimeoutMs` added to `ConsumerOptions`; passed through `startRetryTopicConsumers` → `startLevelConsumer` → `waitForPartitionAssignment`; default remains 10 s

### Refactoring / 0.5.6

- [x] **Multi-level retry topics (at-least-once)** — replaced single `<topic>.retry` + eager offset commit + `setTimeout` (at-most-once, retry lost on crash) with N level topics `<topic>.retry.1` … `<topic>.retry.N`; each level has its own consumer (`${groupId}-retry.1`, `${groupId}-retry.2`, …) that uses `consumer.pause → sleep(remaining) → resume` during the scheduled delay; the offset is committed only after the handler succeeds or the message is routed to the next level/DLQ — a crash during sleep or handler execution redelivers the message (at-least-once); the only remaining duplicate risk is a crash in the window between routing and the subsequent `commitOffsets`, which is a short async gap
- [x] **`stopConsumer(groupId)` stops all companion retry level consumers** — `companionGroupIds: Map<string, string[]>` tracks which level group IDs were started for each main consumer; `stopConsumer(groupId)` disconnects all of them; previously only a single `${groupId}-retry` consumer was tracked
- [x] **Send-side validation errors wrapped in `KafkaValidationError`** — `validateMessage` on the producer path now wraps raw schema errors in `KafkaValidationError` (with original error as `cause`), matching the consume-side behaviour; callers catch one consistent type on both paths
- [x] **`registerSchema` side effect removed from `buildSendPayload`** — schema registration moved to `KafkaClient.preparePayload`; `buildSendPayload` is now a pure function with no hidden state mutations
- [x] **OTel `onConsumeError` span lookup fixed** — `otelInstrumentation()` now maintains a `Map<eventId, Span>` in its closure; `beforeConsume` stores the span, the cleanup removes it, `onConsumeError` looks it up by `eventId` — span status and exception recording work correctly without requiring `trace.getActiveSpan()`
- [x] **`topic().type<T>()` alias** — added `fn.type<M>()` as an explicit alias for `topic('name')<M>()`; both forms are equivalent at runtime; improves readability when the curried generic syntax feels unfamiliar

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
