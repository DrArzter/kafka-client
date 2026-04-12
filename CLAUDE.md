# CLAUDE.md — @drarzter/kafka-client

Type-safe Kafka client wrapper for NestJS built on `@confluentinc/kafka-javascript` (librdkafka C wrapper with KafkaJS-compat API).

---

## Package identity

| Field | Value |
|---|---|
| npm name | `@drarzter/kafka-client` |
| version | `0.9.3` |
| underlying driver | `@confluentinc/kafka-javascript` ^1.8.0 |
| build tool | `tsup` (esbuild + tsc for `.d.ts`) |
| test runner | Jest + ts-jest |
| target | ES2023, CJS + ESM dual output |

---

## Entry points / exports

```
@drarzter/kafka-client          → src/index.ts   (NestJS module + KafkaClient + all types)
@drarzter/kafka-client/core     → src/core.ts    (KafkaClient only, no NestJS dep)
@drarzter/kafka-client/testing  → src/testing.ts (createMockKafkaClient, KafkaTestContainer)
@drarzter/kafka-client/otel     → src/otel.ts    (otelInstrumentation() factory)
```

`src/index.ts` re-exports everything from `core.ts` plus the NestJS layer (`KafkaModule`, `KafkaExplorer`, `KafkaHealthIndicator`, decorators, constants).

---

## Source tree

```
src/
  index.ts                        # full public surface (NestJS + core)
  core.ts                         # KafkaClient + all non-NestJS types
  otel.ts                         # otelInstrumentation() (peer: @opentelemetry/api)
  testing.ts                      # re-export of src/testing/index.ts

  client/
    types.ts                      # barrel — re-exports types/* (common, producer, consumer, admin, config, interfaces)
    errors.ts                     # KafkaProcessingError, KafkaValidationError, KafkaRetryExhaustedError
    message/
      envelope.ts                 # EventEnvelope, header constants, ALS context, buildEnvelopeHeaders, decodeHeaders, extractEnvelope
      topic.ts                    # TopicDescriptor, topic() factory, SchemaLike, TopicsFrom
    transport.interface.ts        # KafkaTransport interface + IProducer, IConsumer, IAdmin, ITransaction, payload types
    kafka.client/
      index.ts                    # KafkaClient facade (~500 lines) — delegates to impl modules via KafkaClientContext<T>
      context.ts                  # KafkaClientContext<T> — shared state object passed to all impl functions
      confluent-transport.ts      # ConfluentTransport implements KafkaTransport — wraps @confluentinc/kafka-javascript
      admin/
        ops.ts                    # AdminOps: createTopics, fetchOffsets, listGroups, deleteRecords, getConsumerLag, seekToOffset
      consumer/
        ops.ts                    # getOrCreateConsumer, buildSchemaMap
        handler.ts                # handleEachMessage, handleEachBatch, parseSingleMessage
        pipeline.ts               # executeWithRetry, runHandlerWithPipeline, sendToDlq, sendToRetryTopic, sendToDuplicatesTopic, toError, sleep
        queue.ts                  # AsyncQueue (backpressure for consume() iterator)
        retry-topic.ts            # startRetryTopicConsumers (launches retry-chain consumers)
        subscribe-retry.ts        # subscribeWithRetry (topic-exists poll on subscribe)
        dlq-replay.ts             # replayDlqTopic implementation
        setup.ts                  # setupConsumer, ensureConsumerTopics, messageDepsFor, resolveDeduplicationContext, makeEosMainContext, launchRetryChain
        start.ts                  # startConsumerImpl, startBatchConsumerImpl, startTransactionalConsumerImpl
        stop.ts                   # stopConsumerImpl, pauseConsumerImpl, resumeConsumerImpl, pauseTopicAllPartitions, resumeTopicAllPartitions
        snapshot.ts               # readSnapshotImpl, checkpointOffsetsImpl, restoreFromCheckpointImpl
        window.ts                 # startWindowConsumerImpl (size+time-windowed batching)
        routed.ts                 # startRoutedConsumerImpl (header-based dispatch)
      producer/
        ops.ts                    # buildSendPayload, registerSchema, resolveTopicName
        lifecycle.ts              # connectProducerImpl, disconnectImpl, ensureTopic, createRetryTxProducer, startLagThrottlePoller, recoverLamportClockImpl, wrapWithTimeoutWarning
        send.ts                   # sendMessageImpl, sendBatchImpl, sendTombstoneImpl, transactionImpl, waitIfThrottled, preparePayload
      infra/
        circuit-breaker.manager.ts  # CircuitBreakerManager (per-partition sliding window)
        inflight.tracker.ts         # InFlightTracker (graceful shutdown)
        metrics.manager.ts          # MetricsManager (processedCount, retryCount, dlqCount, dedupCount)

  nest/
    kafka.module.ts               # KafkaModule.register() / registerAsync()
    kafka.explorer.ts             # KafkaExplorer – OnModuleInit wires @SubscribeTo
    kafka.decorator.ts            # @SubscribeTo, @InjectKafkaClient
    kafka.health.ts               # KafkaHealthIndicator.check()
    kafka.constants.ts            # getKafkaClientToken(), KAFKA_CLIENT token

  testing/
    client.mock.ts                # createMockKafkaClient<T>()
    transport.fake.ts             # FakeTransport
    test.container.ts             # KafkaTestContainer (Testcontainers wrapper)
    index.ts

  integration/                    # Integration specs (require real Kafka via Testcontainers)
    global-setup.ts / global-teardown.ts
    helpers.ts
    *.integration.spec.ts

  __mocks__/
    @confluentinc/kafka-javascript.ts   # Jest manual mock (used by all unit tests)
```

---

## Core class: `KafkaClient<T>`

```ts
new KafkaClient<MyTopics>(clientId, groupId, brokers, options?)
```

`T` is a `TopicMapConstraint` — maps topic name strings to payload types. Can be a plain `interface`, a `type`, or inferred via `TopicsFrom<>`.

### Constructor options (`KafkaClientOptions`)

| Option | Default | Notes |
|---|---|---|
| `autoCreateTopics` | `false` | Admin creates topics on first send/subscribe |
| `strictSchemas` | `true` | Throw if sending to a topic without a registered schema when one exists |
| `numPartitions` | `1` | Partitions for auto-created topics |
| `instrumentation` | `[]` | `KafkaInstrumentation[]` — cross-cutting hooks |
| `logger` | console | Custom `KafkaLogger` |
| `transactionalId` | `${clientId}-tx` | TX producer ID — **must be unique per process** |
| `clockRecovery.topics` | `[]` | Topics to scan for max `x-lamport-clock` on `connectProducer()` |
| `lagThrottle` | — | Pause sends when consumer lag > `maxLag` |
| `onMessageLost` | — | Called when a message has no DLQ and retries are exhausted |
| `onTtlExpired` | — | Client-wide TTL expiry callback |
| `onRebalance` | — | Called on consumer group rebalance |

### Lifecycle

```ts
await kafka.connectProducer()   // connect producer + recover Lamport clock + start lag poller
await kafka.disconnect()        // stop all consumers + disconnect producer + clear tx state
kafka.enableGracefulShutdown()  // drain in-flight messages then process.exit(0) (SIGTERM/SIGINT)
```

---

## Producer API

```ts
// Single message
await kafka.sendMessage(topic, payload, options?)
await kafka.sendMessage(TopicDescriptor, payload, options?)

// Batch
await kafka.sendBatch(topic, [{ value, key?, headers?, correlationId?, schemaVersion?, eventId? }], options?)

// Tombstone (log-compacted topics)
await kafka.sendTombstone(topic, key, headers?)

// Atomic multi-send
await kafka.transaction(async (tx) => {
  await tx.send(topic, payload)
  await tx.sendBatch(topic, messages)
})
```

### `SendOptions`
`key`, `headers`, `correlationId`, `schemaVersion`, `eventId`, `compression` (`'none'|'gzip'|'snappy'|'lz4'|'zstd'`).

### Automatic envelope headers on every message
`x-event-id`, `x-correlation-id`, `x-timestamp`, `x-schema-version`, `x-lamport-clock`, `traceparent` (if OTel instrumentation active or ALS context has it).

### correlationId propagation
Uses `AsyncLocalStorage`. Consumer pipeline calls `runWithEnvelopeContext({ correlationId, traceparent })` before invoking the handler, so any `sendMessage` nested inside a handler auto-inherits the correlationId without passing it explicitly.

### Lamport clock
- Monotonically increasing counter, stamped as `x-lamport-clock` on every send.
- Recovered from broker on `connectProducer()` (via `clockRecovery.topics`) so restarts don't reset to 0.
- Used by consumer-side deduplication.

### Transaction producer
- Lazy — created on first `transaction()` call to avoid librdkafka's constraint that a transactional producer cannot do non-transactional sends.
- Default `transactionalId`: `${clientId}-tx`. Set explicitly to avoid same-process conflicts.
- Process-level registry (`_activeTransactionalIds`) detects duplicate IDs within the same process and warns.

### Lag throttle
When `lagThrottle` is configured, a background timer polls `getConsumerLag()` every `pollIntervalMs` (default 5 s). If total lag > `maxLag`, sends block in `waitIfThrottled()` for up to `maxWaitMs` (default 30 s).

---

## Consumer API

### `startConsumer`
```ts
const handle = await kafka.startConsumer(
  ['topic1', 'topic2'],             // string[], TopicDescriptor[], RegExp[], or mix
  async (envelope: EventEnvelope<T>) => { ... },
  options?
)
await handle.stop()
```

### `startBatchConsumer`
```ts
await kafka.startBatchConsumer(['topic'], async (envelopes, meta: BatchMeta) => {
  meta.resolveOffset(envelopes.at(-1)!.offset)
  await meta.commitOffsetsIfNecessary()
}, { autoCommit: false })
```

### `consume` (async iterator)
```ts
for await (const envelope of kafka.consume('my.topic', options)) {
  // break to stop the consumer
}
```
Supports `queueHighWaterMark` for backpressure — pauses partition when queue fills, resumes at 50%.

### `startWindowConsumer`
```ts
await kafka.startWindowConsumer('events', async (batch, meta: WindowMeta) => {
  // meta.trigger = 'size' | 'time'
}, { maxMessages: 100, maxMs: 5_000 })
```
Accumulates into a buffer; flushes on size or timer. Remaining buffer is flushed on `stop()`. Incompatible with `retryTopics`.

### `startRoutedConsumer`
```ts
await kafka.startRoutedConsumer(['domain.events'], {
  header: 'x-event-type',
  routes: {
    'order.created': async (e) => { ... },
  },
  fallback: async (e) => { ... },
}, options?)
```

### `startTransactionalConsumer` (EOS)
```ts
await kafka.startTransactionalConsumer(['orders.created'], async (envelope, tx) => {
  await tx.send('inventory.reserved', { ... })
  // tx.sendBatch also available
})
```
Handler, offset commit, and downstream sends are atomic. On handler failure → tx abort → message redelivered. Incompatible with `retryTopics`.

---

## `ConsumerOptions`

| Option | Default | Notes |
|---|---|---|
| `groupId` | constructor default | Override per-consumer |
| `fromBeginning` | `false` | Seek to earliest offset on subscribe |
| `autoCommit` | `true` | Set `false` for manual offset management |
| `retry` | — | `{ maxRetries, backoffMs?, maxBackoffMs? }` — exponential backoff with full jitter |
| `dlq` | `false` | Failed messages → `<topic>.dlq` |
| `retryTopics` | `false` | Durable retry via `<topic>.retry.<n>` topics; forces `autoCommit: false` |
| `retryTopicAssignmentTimeoutMs` | `10000` | Wait for retry consumer partition assignment |
| `handlerTimeoutMs` | — | Log warning if handler exceeds this duration (non-cancelling) |
| `deduplication` | — | Lamport clock dedup: `{ strategy: 'drop'|'dlq'|'topic', duplicatesTopic? }` |
| `messageTtlMs` | — | Drop messages older than N ms (by `x-timestamp` header) |
| `circuitBreaker` | — | `{ threshold, recoveryMs, windowSize, halfOpenSuccesses }` |
| `queueHighWaterMark` | — | Backpressure for `consume()` only |
| `partitionAssigner` | `'cooperative-sticky'` | `'roundrobin'`/`'range'`/`'cooperative-sticky'` |
| `interceptors` | `[]` | `ConsumerInterceptor[]` — per-consumer before/after/onError |
| `subscribeRetry` | — | Retry subscribe if topic doesn't exist yet |
| `onTtlExpired` | — | Per-consumer TTL callback (overrides client-wide) |
| `onMessageLost` | — | Per-consumer lost-message callback (overrides client-wide) |
| `onRetry` | — | Per-consumer retry callback (fires after metrics hook, not instead of it) |

---

## Retry topology

### In-process retry (default, `retry` option)
1. Handler throws → exponential backoff sleep → retry (up to `maxRetries` times).
2. On exhaustion: → DLQ (if `dlq: true`) or → `onMessageLost`.

### Retry topic chain (`retryTopics: true`, requires `retry`)
1. Handler fails → message sent to `<topic>.retry.1` (with `x-retry-*` headers).
2. A companion consumer (`<groupId>-retry`) on `<topic>.retry.1` waits for `x-retry-after` then retries.
3. On each subsequent failure → `<topic>.retry.2` … up to `maxRetries`.
4. On final exhaustion → DLQ or `onMessageLost`.

**EOS path** (single-message consumer): routing to retry topic uses a Kafka transaction (send to retry.1 + commit source offset atomically) — no duplicates even on crash.

**Retry topic naming**: `<originalTopic>.retry.<attemptNumber>` (1-indexed).

### DLQ topic naming: `<originalTopic>.dlq`

DLQ headers stamped:
- `x-dlq-original-topic`, `x-dlq-failed-at`, `x-dlq-error-message`, `x-dlq-error-stack` (max 2000 chars), `x-dlq-attempt-count`

### DLQ replay
```ts
const result = await kafka.replayDlq('orders.created', {
  targetTopic?: string,     // default: reads x-dlq-original-topic header
  dryRun?: boolean,
  filter?: (headers, value) => boolean,
  fromBeginning?: boolean,  // default true — full replay every call; false = incremental
})
// result: { replayed: number, skipped: number }
```

`fromBeginning: true` (default) uses an ephemeral group ID (`<topic>.dlq-replay-<ts>`) deleted after use — full replay on every call. `fromBeginning: false` uses a stable group ID (`<topic>.dlq-replay`) so only messages added since the last call are replayed.

---

## Circuit breaker

Per `groupId:topic:partition` sliding window.

States: **CLOSED** → **OPEN** (partition paused) → **HALF-OPEN** (probe resumes) → **CLOSED**.

- Opens when `failures >= threshold` within the window.
- After `recoveryMs` ms in OPEN → moves to HALF-OPEN.
- `halfOpenSuccesses` consecutive successes in HALF-OPEN → CLOSED. Any failure → back to OPEN.
- Instrumentation hooks: `onCircuitOpen`, `onCircuitHalfOpen`, `onCircuitClose`.

---

## Deduplication

Lamport clock-based, in-memory per `topic:partition` per consumer group.

- Skips any message whose `x-lamport-clock` ≤ last processed clock for that partition.
- **In-session only** — state resets on process restart or rebalance.
- Strategy: `'drop'` (silent), `'dlq'` (forward to `<topic>.dlq`), `'topic'` (forward to `<topic>.duplicates` or custom topic).

---

## Snapshots, checkpoints, seek

```ts
// Read entire compacted topic into a Map<key, EventEnvelope>
const snapshot = await kafka.readSnapshot('users.state', { schema?, onTombstone? })

// Save committed offsets for a group
const result = await kafka.checkpointOffsets(groupId, topics)

// Restore offsets from a saved checkpoint
await kafka.restoreFromCheckpoint(groupId, topics, { timestamp? })

// Seek to specific offset, or timestamp, or reset to earliest/latest
await kafka.seekToOffset(groupId, [{ topic, partition, offset }])
await kafka.seekToTimestamp(groupId, topics, timestamp)
await kafka.resetOffsets(groupId, topics, { strategy: 'earliest'|'latest' })
```

---

## Admin API

```ts
await kafka.checkStatus()                   // → { status: 'up'|'down', clientId, topics|error }
await kafka.listConsumerGroups()            // → { groupId, state }[]
await kafka.describeTopics(topics?)         // → { name, partitions: { partition, leader, replicas, isr }[] }[]
await kafka.getConsumerLag(groupId?)        // → { topic, partition, lag }[]
await kafka.deleteRecords(topic, [{ partition, offset }])
kafka.getCircuitState(topic, partition, groupId)  // → { status, failures, windowSize } | undefined
kafka.getMetrics()                          // → { processedCount, retryCount, dlqCount, dedupCount }
kafka.resetMetrics()
kafka.pauseConsumer(groupId, assignments)
kafka.resumeConsumer(groupId, assignments)
```

---

## `EventEnvelope<T>`

```ts
interface EventEnvelope<T> {
  payload: T           // deserialized + validated body
  topic: string
  partition: number    // -1 on send
  offset: string       // '' on send
  timestamp: string    // ISO-8601, from x-timestamp header
  eventId: string      // UUID v4
  correlationId: string
  schemaVersion: number
  traceparent?: string // W3C Trace Context
  headers: MessageHeaders  // all decoded headers
}
```

---

## `TopicDescriptor` / `topic()` factory

```ts
// Type-only (no runtime schema):
const OrderCreated = topic('order.created').type<{ orderId: string; amount: number }>()

// With runtime schema (Zod, Valibot, ArkType, or any { parse() }):
const OrderCreated = topic('order.created').schema(z.object({ orderId: z.string() }))

// Infer topic map from descriptors:
type MyTopics = TopicsFrom<typeof OrderCreated | typeof OrderCompleted>
```

`TopicDescriptor` carries `__topic` (runtime string), `__type` (phantom), `__schema` (optional).

When passed to `sendMessage` / `startConsumer`, schema is auto-extracted and validated.

---

## Schema validation

`SchemaLike<T>` — any object with `.parse(data, ctx?)`.
- `ctx?: SchemaParseContext` carries `{ topic, headers, version }` for version-aware migration.
- Schemas registered on `KafkaClient` via `registerSchema(topic, schema)` (called automatically by `TopicDescriptor`).
- `strictSchemas: true` (default): if a schema exists for a topic, all sends must pass validation.

---

## `KafkaInstrumentation` hooks

Cross-cutting — apply to all consumers/producers on the client.

| Hook | When |
|---|---|
| `beforeSend(topic, headers)` | Before producing — can mutate headers (e.g. inject traceparent) |
| `afterSend(topic)` | After successful produce |
| `beforeConsume(envelope)` | Returns `BeforeConsumeResult`: `{ cleanup?(), wrap?(fn) }` |
| `onConsumeError(envelope, error)` | When handler throws |
| `onRetry(envelope, attempt, max)` | Before each retry |
| `onDlq(envelope, reason)` | When routed to DLQ |
| `onDuplicate(envelope, strategy)` | When Lamport duplicate detected |
| `onMessage(envelope)` | After successful processing |
| `onCircuitOpen/HalfOpen/Close(topic, partition)` | Circuit state transitions |

`wrap(fn)` in `BeforeConsumeResult` runs the handler inside a specific async context (e.g. `context.with(spanCtx, fn)` for OTel). Multiple instrumentations compose in declaration order (first = outermost).

---

## OTel instrumentation

```ts
import { otelInstrumentation } from '@drarzter/kafka-client/otel'

const kafka = new KafkaClient(id, group, brokers, {
  instrumentation: [otelInstrumentation()],
})
```

- **Send**: injects `traceparent` header via W3C propagator.
- **Consume**: extracts `traceparent`, starts `SpanKind.CONSUMER` span, wraps handler in `context.with(spanCtx)`, ends span on cleanup, records error on handler throw.

---

## NestJS integration

### `KafkaModule.register()`
```ts
@Module({
  imports: [
    KafkaModule.register<MyTopics>({
      clientId: 'my-service',
      groupId: 'my-service-group',
      brokers: ['localhost:9092'],
      isGlobal: true,
      autoCreateTopics: true,
      instrumentation: [otelInstrumentation()],
    }),
  ],
})
export class AppModule {}
```

Calls `connectProducer()` inside `useFactory` — producer is ready before the app starts serving.

### `KafkaModule.registerAsync()`
```ts
KafkaModule.registerAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (cfg: ConfigService) => ({
    clientId: cfg.get('KAFKA_CLIENT_ID'),
    // ...
  }),
})
```

### Multi-client
```ts
KafkaModule.register({ name: 'analytics', ... })
// inject:
@InjectKafkaClient('analytics') private readonly kafka: KafkaClient<AnalyticsTopics>
```
Token: `KAFKA_CLIENT_analytics`.

### `@SubscribeTo`
```ts
@Injectable()
export class OrdersHandler {
  @SubscribeTo('orders.created', { groupId: 'billing-svc', retry: { maxRetries: 3 }, dlq: true })
  async handleOrder(envelope: EventEnvelope<Order>) { ... }

  @SubscribeTo(PaymentsTopic, { batch: true })
  async handleBatch(envelopes: EventEnvelope<Payment>[], meta: BatchMeta) { ... }
}
```

`KafkaExplorer` (OnModuleInit) scans all providers for `KAFKA_SUBSCRIBER_METADATA` and calls `startConsumer` / `startBatchConsumer`. Uses `@Inject(DiscoveryService)` and `@Inject(ModuleRef)` explicitly (esbuild strips `design:paramtypes`).

### `KafkaHealthIndicator`
```ts
@Injectable()
export class HealthService {
  constructor(
    private readonly health: KafkaHealthIndicator,
    @InjectKafkaClient() private readonly kafka: KafkaClient<any>,
  ) {}

  async check() {
    return this.health.check(this.kafka) // → KafkaHealthResult
  }
}
```

---

## Testing

### Unit tests (Jest + manual mock)
- **Auto-mock** at `src/__mocks__/@confluentinc/kafka-javascript.ts` — loaded automatically by Jest when `@confluentinc/kafka-javascript` is imported.
- All unit specs are in `src/client/__tests__/`, `src/nest/__tests__/`, `src/testing/__tests__/`.
- Run: `npm test`
- Jest config: `jest.config.ts` — `preset: ts-jest`, ignores `integration/`.

### `createMockKafkaClient<T>()`
```ts
import { createMockKafkaClient } from '@drarzter/kafka-client/testing'

const kafka = createMockKafkaClient<MyTopics>()
// kafka.sendMessage is jest.fn() / vi.fn()
// kafka.startConsumer resolves to { groupId: 'mock-group', stop: jest.fn() }
```
Auto-detects Jest or Vitest. Pass a `mockFactory` for other frameworks.

### Integration tests (Testcontainers)
- Require Docker. Kafka is started via `@testcontainers/kafka`.
- Config: `jest.integration.config.ts`.
- Run: `npm run test:integration` (uses `--forceExit`).
- Global setup/teardown in `src/integration/global-setup.ts` / `global-teardown.ts`.
- Helpers at `src/integration/helpers.ts`.

---

## File naming conventions

**Rule:** hyphens within a multi-word name, dot separates the name from its role suffix.

```text
circuit-breaker.manager.ts   ← "circuit-breaker" = name (compound noun)
                               ".manager"         = role suffix
```

When the filename already expresses the role — `handler.ts` is a handler, `pipeline.ts` is a pipeline — no suffix is added. A suffix is only useful when separating a *subject* from a *role* that isn't obvious from the name alone.

### Recognised suffixes

| Suffix | Role | Examples |
| --- | --- | --- |
| `.types` | Data/option shapes — configs, options, result objects, context bags | `producer.types.ts`, `consumer.types.ts` |
| `.interface` | Contract interfaces — capability/role boundaries, polymorphism points | `transport.interface.ts`, `producer.interface.ts` |
| `.manager` | Stateful manager class | `metrics.manager.ts`, `circuit-breaker.manager.ts` |
| `.tracker` | Tracking/counting utility | `inflight.tracker.ts` |
| `.module` | NestJS module | `kafka.module.ts` |
| `.explorer` | NestJS metadata scanner | `kafka.explorer.ts` |
| `.decorator` | NestJS decorator definitions | `kafka.decorator.ts` |
| `.health` | Health indicator | `kafka.health.ts` |
| `.constants` | Constant values and DI tokens | `kafka.constants.ts` |
| `.mock` | Jest/Vitest spy double | `client.mock.ts` |
| `.fake` | Standalone fake implementation | `transport.fake.ts` |
| `.container` | Test infrastructure wrapper | `test.container.ts` |
| `.spec` | Unit test | `consumer.spec.ts` |
| `.integration.spec` | Integration test | `consumer.integration.spec.ts` |

**Additional rules:**

- A suffix is added only when it carries information the name alone does not. `handler.ts`, `pipeline.ts`, `queue.ts`, `setup.ts`, `send.ts`, `lifecycle.ts` need no suffix — the name already tells you what the file does.
- `.types` files export **only** `type` / `interface` / `export type { … }` — no functions, classes, or `const`s.
- `common.ts` is an intentional exception: shared primitives that don't belong to a single domain.
- Barrel/re-export files (`types.ts`, `client.ts`, `index.ts`) keep their names regardless of content.

**Current layout:**

```text
src/client/
  transport.interface.ts            ← KafkaTransport, IProducer, IConsumer, IAdmin, ITransaction
  types.ts                          ← barrel (re-exports types/*)
  types/
    common.ts                       ← shared primitives (exception)
    producer.types.ts               ← SendOptions, BatchMessageItem, BatchSendOptions
    consumer.types.ts               ← ConsumerOptions, ConsumerHandle, BatchMeta, RetryOptions, …
    admin.types.ts                  ← snapshot/checkpoint/dlq/health types
    config.types.ts                 ← KafkaClientOptions
    producer.interface.ts           ← IKafkaProducer<T>
    consumer.interface.ts           ← IKafkaConsumer<T>
    admin.interface.ts              ← IKafkaAdmin<T>
    lifecycle.interface.ts          ← IKafkaLifecycle
    client.ts                       ← IKafkaClient<T> extends all four (barrel)
  kafka.client/
    infra/
      circuit-breaker.manager.ts    ← CircuitBreakerManager
      inflight.tracker.ts           ← InFlightTracker
      metrics.manager.ts            ← MetricsManager

src/nest/
  kafka.module.ts
  kafka.explorer.ts
  kafka.decorator.ts
  kafka.health.ts
  kafka.constants.ts

src/testing/
  client.mock.ts                    ← createMockKafkaClient
  transport.fake.ts                 ← FakeTransport
  test.container.ts                 ← KafkaTestContainer
```

---

## Build

```bash
npm run build        # tsup: CJS + ESM + .d.ts → dist/
npm run lint         # eslint --fix
npm run format       # prettier --write
npm run prepublishOnly  # runs build
```

tsup entry points: `src/index.ts`, `src/core.ts`, `src/testing.ts`, `src/otel.ts`.
NestJS, OTel, Testcontainers are all `external` (not bundled).

## Release checklist

Before bumping the version, committing, or pushing:

1. `npm test` — all unit tests must pass
2. `npm run test:integration` — all integration tests must pass (requires Docker)
3. Bump version in `package.json` and `CLAUDE.md`
4. Only then commit and push

---

## Known constraints & gotchas

### librdkafka install on Arch/CachyOS
```bash
sudo pacman -S librdkafka
BUILD_LIBRDKAFKA=0 npm install @confluentinc/kafka-javascript
```
Without system librdkafka, the native build fails on missing OpenSSL symbols.

### esbuild strips `design:paramtypes`
tsup uses esbuild → `emitDecoratorMetadata` metadata is lost. All NestJS DI in this lib uses **explicit `@Inject(Token)` decorators** — never rely on implicit constructor type inference.

### kafkaJS compat config wrapping
The confluent driver uses `kafkaJS: { ... }` blocks around all KafkaJS-style config:
```ts
new Kafka({ kafkaJS: { clientId, brokers, logLevel } })
kafka.producer({ kafkaJS: { acks: -1 } })
kafka.consumer({ kafkaJS: { groupId } })
```

### Transactional producer vs regular producer
librdkafka forbids calling `producer.send()` outside a transaction when `transactionalId` is set. This lib keeps two separate producers:
- `this.producer` — default, `acks: -1` only.
- `this.txProducer` — lazy, created on first `transaction()` call, `idempotent: true` + `transactionalId`.

### DLQ `x-dlq-*` headers
When replaying from DLQ, the `replayDlq` method strips these headers before re-publishing so they don't leak into the destination topic.

### Retry topic consumers add companion group IDs
`startConsumer({ retryTopics: true })` internally starts `N` companion consumers in parallel (via `Promise.all`) with group IDs `<groupId>-retry.1`, `<groupId>-retry.2`, etc. Each level registers itself immediately via `onLevelStarted` so partial startup failures still leave already-started consumers tracked. `stopConsumer(groupId)` tears all of them down.

### `consume()` does not support `retryTopics`
Throws at call time. Use `startConsumer()` for durable retry.

### `startWindowConsumer` and `startTransactionalConsumer` do not support `retryTopics`
Both throw at startup if `retryTopics: true` is set.

### `highWatermark` is `null` inside retry-topic consumer path
When `startBatchConsumer` uses `retryTopics: true`, the retry consumers wrap the batch handler for single-message delivery and pass `highWatermark: null`. Don't do lag calculations from `meta.highWatermark` in the retry path.

### Deduplication is in-memory only
Lamport clock state resets on process restart or rebalance. Only provides within-session deduplication.

### `interface extends TTopicMessageMap` doesn't work as generic constraint
Use `type` aliases or plain interfaces (not extending the type). Internally `TopicMapConstraint<T>` is a self-referencing mapped type, which accepts both.

### One-shot consumers delete their groups on finish

`readSnapshot`, `restoreFromCheckpoint`, `recoverLamportClock`, and `replayDlq` (with `fromBeginning: true`) all use ephemeral group IDs. Each calls `admin.deleteGroups([groupId])` in a `finally` block after the consumer disconnects so groups don't accumulate on the broker.

### `ConsumerWithAssignment` local interface

`KafkaJS.Consumer` doesn't expose `assignment()` in its public types. The method exists at runtime (librdkafka) and is accessed via a `ConsumerWithAssignment` interface defined in `stop.ts` instead of an `as any` cast.

### `onRetry` in `ConsumerOptions` composes, not replaces

When `ConsumerOptions.onRetry` is set, it fires **in addition to** the internal metrics hook (`MetricsManager.notifyRetry`) — not instead of it. `onMessageLost` and `onTtlExpired` in `ConsumerOptions` do replace the client-wide callbacks for that consumer.

### Module-scoped context aliases in `snapshot.ts` and `stop.ts`

`SnapshotCtx<T>` and `StopCtx<T>` are `Pick<KafkaClientContext<T>, ...>` aliases used as the actual parameter type in those modules' exported functions. This enforces that the modules only touch their declared context fields. Modules that forward `ctx` to other impl functions (e.g. `window.ts`, `setup.ts`) cannot be narrowed this way and use the full `KafkaClientContext<T>` in their signatures.

### Consumers take ~25 s to join a group

After `startConsumer` returns, the consumer may not have received partition assignments yet. Use `await handle.ready()` to wait for the first partition assignment before sending messages in tests:

```ts
const handle = await kafka.startConsumer(['orders'], handler, { fromBeginning: false });
await handle.ready(); // resolves when Kafka assigns at least one partition
await kafka.sendMessage('orders', payload);
```

`handle.ready()` is backed by the `onRebalance("assign", ...)` callback and never hangs in unit tests because `FakeConsumer.subscribe()` fires the callback immediately. The `fromBeginning: true` option is an alternative when the consumer can read old messages.
