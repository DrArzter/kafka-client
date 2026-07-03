# Consumer internals

How incoming messages are turned into `EventEnvelope`s and pushed through the
handler, across all consumer flavours. Covers the per-message pipeline stages,
circuit-breaker wiring, deduplication, backpressure, and the semantic caveats
each flavour carries.

Retry and DLQ topology has its own doc: see `retry-and-dlq.md`. Public API and
options: see `README.md`. Files referenced here live under
`src/client/kafka.client/consumer/` unless noted.

---

## The consumer flavours

Most flavours ultimately drive one of two core pipelines — `handleEachMessage` or
`handleEachBatch` in `handler.ts` — via `consumer.run({ eachMessage | eachBatch })`.
Feature flavours (`window`, `routed`, `delayed`, plus `snapshot`/`dlq-replay`)
live under `consumer/features/`.

| Public method                | Impl                          | Core pipeline | Notes |
|------------------------------|-------------------------------|---------------|-------|
| `startConsumer`              | `startConsumerImpl` (`start.ts`) | `handleEachMessage` | The base case. |
| `startBatchConsumer`         | `startBatchConsumerImpl` (`start.ts`) | `handleEachBatch`   | Handler gets `envelopes[] + BatchMeta`. |
| `consume` (async iterator)   | `index.ts` (wraps `startConsumer`) | `handleEachMessage` | Handler just `queue.push(envelope)`; caller pulls via `for await`. |
| `startWindowConsumer`        | `startWindowConsumerImpl` (`features/window.ts`) | `handleEachMessage` | Handler accumulates into a buffer; flushes on size/time. |
| `startRoutedConsumer`        | `startRoutedConsumerImpl` (`features/routed.ts`) | `handleEachMessage` | Handler dispatches by a header value to a `routes` map. |
| `startDelayedRelay`          | `startDelayedRelayImpl` (`features/delayed.ts`) | *(own inline pipeline)* | Relays `<topic>.delayed` → target after each message's deadline. See below. |
| `startTransactionalConsumer` | `startTransactionalConsumerImpl` (`start.ts`) | *(own inline pipeline)* | EOS: handler + downstream sends + offset commit in one Kafka tx. Does **not** use `handleEachMessage`. |

`consume`, `startWindowConsumer`, and `startRoutedConsumer` are all thin layers
that supply a synthetic handler to `startConsumerImpl` — they inherit its full
pipeline (parse, dedup, TTL, retry, DLQ). `startTransactionalConsumer` and
`startDelayedRelay` are the exceptions: each wires its own `eachMessage` because
the transaction spans the handler/forward, the downstream send, and the offset
commit atomically.

### Delayed-delivery relay (`features/delayed.ts`)

`startDelayedRelay(topics, { groupId? })` starts a relay that delivers messages
staged by the producer's `deliverAfterMs` path (see
[`producer.md`](./producer.md#delayed-delivery)). Per message on `<topic>.delayed`:

1. Read `x-delayed-until` and `x-delayed-target` (default target: the staging
   topic minus its `.delayed` suffix).
2. If the deadline is in the future, **pause** the partition, `sleep(remaining)`,
   then **resume**. The offset stays uncommitted during the wait, so a crash
   redelivers the staged message (at-least-once for the wait window).
3. **Forward transactionally**: send to the target + commit the source offset in
   one Kafka transaction (`tx.send` + `tx.sendOffsets` + `tx.commit`) — no
   duplicates on crash. The `x-delayed-*` control headers are stripped; all other
   envelope headers (`x-event-id`, `x-correlation-id`, `x-lamport-clock`,
   `traceparent`) survive the hop.

Group: `<groupId>-delayed-relay` (default `<defaultGroupId>-delayed-relay`); tx
producer `<group>-tx`. Delivery time is a lower bound with per-partition
head-of-line waiting — the relay must be running for delayed messages to arrive.

### Shared bootstrap (`setup.ts`)

`startConsumerImpl` / `startBatchConsumerImpl` share `setupConsumer`, which:

1. Guards the group id: throws if the group already runs in the *opposite* mode
   (`eachMessage` vs `eachBatch`) or if the *same* start method was called twice
   for that group.
2. Creates/caches the consumer via `getOrCreateConsumer` (`ops.ts`) and wires the
   `handle.ready()` resolver (see below). `options.groupInstanceId` is forwarded
   here to set librdkafka's `group.instance.id` for static membership (avoids a
   rebalance when a known member briefly leaves and rejoins).
3. Builds the local schema map (`buildSchemaMap`), ensures base/DLQ/dedup topics
   exist (`ensureConsumerTopics`), connects, and subscribes with
   `subscribeWithRetry`.

#### Lazy producer connect for routing consumers

DLQ, retry-topic, and duplicates routing all *produce* — to `<topic>.dlq`,
`<topic>.retry.<n>`, or the duplicates topic. So after connecting the consumer,
`setupConsumer` calls `await ctx.producer.connect()` when `dlq`, `retryTopics`, or
`deduplication` is set. This lets a **consumer-only** client (one that never
called `connectProducer()`) still deliver routed messages instead of silently
dropping them to `onMessageLost`. The connect is idempotent — the transport
caches its connect promise (see
[`producer.md`](./producer.md#lazy-transactional-producer)).

When `retryTopics: true`, `startConsumerImpl` forces `autoCommit: false` (offset
commits become part of the EOS transaction) and, after `run()`, calls
`launchRetryChain` to start the companion retry consumers — see `retry-and-dlq.md`.

### `handle.ready()` — deterministic readiness (`ops.ts`)

`getOrCreateConsumer` installs an `onRebalance` callback that resolves the
`ready()` promise on a **debounced** basis: it fires `SETTLE_MS` after the *last*
assign/revoke event with no follow-up.

- `fromBeginning: true` → `SETTLE_MS = 0` (seeks to offset 0 are synchronous).
- `fromBeginning: false` → `SETTLE_MS = 500` — covers the async broker round-trip
  that establishes the initial "latest" fetch position, and multi-consumer
  rebalances where a solo assign is immediately followed by a revoke/re-assign as
  peers join.

The resolver only fires once an `assign` has been seen (guards against a
pure-revoke cycle). In unit tests, `FakeConsumer.subscribe()` fires the assign
synchronously, so `ready()` resolves immediately.

---

## The per-message pipeline (in order)

`handleEachMessage` (`handler.ts`) is the canonical path. Stages run in this
exact order; any stage can short-circuit and (under EOS) commit the offset so the
consumer still advances.

```
eachMessage(payload)                                         [start.ts]
  └─ ctx.inFlight.track(...)          ── count for graceful-shutdown drain
       └─ handleEachMessage(payload, opts, deps)             [handler.ts]

   1. parseSingleMessage                                     [handler.ts]
        · empty value?           → warn, skip
        · decodeHeaders          → Record<string,string>     [envelope.ts]
        · serde.deserialize(raw Buffer) → null/throw → skip  [message/serde.ts]
        · validateWithSchema     → null on failure (→ DLQ or onMessageLost) [pipeline.ts]
        · extractEnvelope        → EventEnvelope<T>           [envelope.ts]

   2. deduplication (if configured)                          [applyDeduplication]
        · compare x-lamport-clock vs last-seen per topic:partition
        · duplicate → strategy (drop | dlq | topic), then skip

   3. TTL (if messageTtlMs set)
        · age = now − x-timestamp; if age > ttl → DLQ or onTtlExpired, then skip

   4. executeWithRetry(handler)                              [pipeline.ts]
        · runs the handler inside runWithEnvelopeContext(...) (ALS)
        · optional wrapWithTimeout (handlerTimeoutMs — warning only)
        · interceptors: before → handler → after
        · instrumentation: beforeConsume(wrap/cleanup) → onConsumeError
        · retry loop, DLQ / retry-topic routing, circuit-breaker onFailure
```

### Stage 1 — parse & validate (`parseSingleMessage`)

Returns `null` (skip) for empty payloads, a serde `deserialize` that throws or
yields `null`, or schema-validation failures. Deserialization goes through the
**resolved serde** — the per-topic override from `serdeMap` (built from each
subscribed `TopicDescriptor.__serde`) if present, else the client-wide
`deps.serde`, defaulting to `JsonSerde`. Crucially it hands the serde the raw
`message.value` **`Buffer`** — never a `.toString()` — so binary serdes
(Avro/Protobuf) receive the exact wire bytes. Schema validation then runs on the
deserialized *object*, unchanged. On a validation failure, `validateWithSchema`
(`pipeline.ts`) already routed the raw message to the DLQ (if enabled) or
`onMessageLost`, and fired `onConsumeError` on instrumentation and interceptors
with a minimal envelope (partition `-1`, offset `""`). `extractEnvelope`
tolerates missing envelope headers — it fabricates defaults (fresh UUIDs, `now`)
so messages from non-envelope producers still work.

**Binary-safe forwarding.** Everywhere a message is *re-produced* — DLQ
(`<topic>.dlq`), the retry chain (`<topic>.retry.<n>`), the duplicates topic, and
the delayed relay — the pipeline forwards the **original wire `Buffer`**, not a
re-encoded value. `handleEachMessage`/`handleEachBatch` capture `message.value`
as `rawBytes` up front and thread it through `executeWithRetry` into
`sendToDlq` / `sendToRetryTopic` / `sendToDuplicatesTopic`. So an Avro or
Protobuf record lands in the DLQ byte-for-byte identical to what was consumed —
no lossy `.toString()` round-trip corrupts it. (`validateWithSchema` likewise
forwards the original bytes on a validation-failure DLQ.)

The **retry-topic chain** stays consistent with this: `startLevelConsumer`
(`retry-topic.ts`) deserializes each `<topic>.retry.<n>` message through the same
resolved serde — keyed by the *original* topic (from `x-retry-original-topic`), so
the per-topic `serdeMap` override still applies — and forwards the original raw
`Buffer` to the next level or the DLQ. A binary record therefore survives the
whole retry hop unchanged. See
[`serialization.md`](./serialization.md) and `retry-and-dlq.md`.

### Stage 4 — `executeWithRetry` (`pipeline.ts`)

The heart of the consume side. Per attempt it runs the handler through
`runHandlerWithPipeline`, which layers the cross-cutting hooks:

```
beforeConsume (instrumentation, collects cleanup + wrap fns)
  → interceptor.before (per envelope)
    → [wrap₁(wrap₂(... handler ...))]   ── wraps compose, first = outermost
  → interceptor.after
  → cleanup (instrumentation)
On throw: onConsumeError (instrumentation) + cleanup, return the Error
```

- `maxAttempts` = `retryTopics ? 1 : (retry ? retry.maxRetries + 1 : 1)`. With
  `retryTopics`, the main consumer tries once and hands off to the retry chain.
- On success: fires `onMessage` (metrics + circuit-breaker success). Under EOS it
  first commits the offset via `eosCommitOnSuccess`; if that commit fails it
  logs and returns **without** `onMessage` (the message will be redelivered, so
  it must not be counted as processed).
- On each failed attempt: fires `onFailure` first (drives the breaker — see
  below), then `onConsumeError`/interceptor `onError`, then either backs off and
  retries, routes to a retry topic, or (last attempt) sends to DLQ / calls
  `onMessageLost`. Backoff is exponential with full jitter, capped at
  `maxBackoffMs`.

### Batch path differences (`handleEachBatch`)

Iterates the batch, running stages 1–3 per message; survivors are collected into
`envelopes[]` + `rawMessages[]` and passed once to the handler with a `BatchMeta`
(`resolveOffset`, `commitOffsetsIfNecessary`, `heartbeat`, `highWatermark`).
If every message is filtered out, it commits the batch offset (under EOS) and
returns. `startBatchConsumer` defaults to `autoCommit: true`; if your handler
calls `resolveOffset()`/`commitOffsetsIfNecessary()` you should pass
`autoCommit: false` (a debug warning nudges you).

---

## Circuit breaker wiring

Per `gid:topic:partition` sliding-window breaker
(`infra/circuit-breaker.manager.ts`), configured per consumer via
`ConsumerOptions.circuitBreaker`.

### Where failures are recorded (recent)

The breaker is driven at the **handler-error boundary**, not the DLQ boundary.
`executeWithRetry` calls `deps.onFailure(envelope)` on **every** failed handler
attempt, before any DLQ/retry decision. `onFailure` is
`MetricsManager.notifyFailure(envelope, gid)` (`setup.ts#messageDepsFor`), which
forwards to `CircuitBreakerManager.onFailure`.

Consequences:
- Every failed attempt counts — not only messages that end up dead-lettered. A
  consumer with `dlq: false` still opens its breaker.
- **Retry-chain failures drive the main group's breaker.** `launchRetryChain`
  binds the retry consumers' `onFailure`/`onMessage` to the **main** `gid`
  (`setup.ts`), and `startLevelConsumer` calls `deps.onFailure(envelope)` on each
  failed retry attempt (`retry-topic.ts`). So failures inside `<topic>.retry.N`
  count against the same breaker as the main topic.
- Success is recorded separately: `notifyMessage(envelope, gid)` calls
  `onSuccess`, which drives HALF-OPEN → CLOSED and fills the success window.

### State machine (`CircuitBreakerManager`)

State is keyed `${gid}:${topic}:${partition}`.

```
        failures ≥ threshold (within window)
CLOSED ───────────────────────────────────────▶ OPEN
  ▲                                              │  · window/successes reset
  │ halfOpenSuccesses consecutive successes      │  · pauseConsumer(partition)
  │                                              │  · arm recoveryMs timer
  │                                              ▼
CLOSED ◀──────────────────────────────────── HALF-OPEN
                                               │  (recoveryMs elapsed →
                                               │   resumeConsumer, status=half-open)
                                               │
        any failure in HALF-OPEN ──────────────┘ → back to OPEN
```

- OPEN pauses the offending topic-partition (`deps.pauseConsumer`), which maps to
  `pauseConsumerImpl` (`stop.ts`) on the client.
- Window: a boolean ring buffer of size `windowSize` (default `max(threshold*2, 10)`);
  `failures` = count of `false` entries. Opens when `failures >= threshold`.
- `getCircuitState(topic, partition, gid?)` exposes `{ status, failures, windowSize }`.
- State/timers are torn down per group by `removeGroup(gid)` on `stopConsumer`,
  and wholesale by `clear()` on `disconnect`.

Instrumentation hooks `onCircuitOpen` / `onCircuitHalfOpen` / `onCircuitClose`
fire on transitions.

---

## Deduplication

Lamport-clock based, per consumer group, backed by a pluggable `DedupStore`.
Enabled via `ConsumerOptions.deduplication`.

### The `DedupStore` seam

`resolveDeduplicationContext` (`setup.ts`) builds a `DeduplicationContext`
(`{ options, store, groupId }`, defined in `handler.ts`) when `deduplication` is
set. The `store` is either `options.store` (a caller-supplied `DedupStore` — e.g.
Redis) or an `InMemoryDedupStore` (`infra/dedup.store.ts`) wrapping the shared
`ctx.dedupStates` map (`groupId → Map<"topic:partition", lastClock>`). The
`DedupStore` interface (`types/dedup.types.ts`) is just two sync-or-async methods:
`getLastClock(groupId, topicPartition)` and `setLastClock(...)`.

### Semantics (`applyDeduplication` in `handler.ts`)

- For each message it reads `x-lamport-clock`. **No clock header, or a NaN
  header, → passes through** (never treated as a duplicate). This is why
  tombstones (no clock) are always delivered.
- It reads the last clock via `store.getLastClock(groupId, "topic:partition")`.
  If `incomingClock <= lastProcessedClock`, the message is a duplicate: fire
  `onDuplicate`, apply the strategy, and skip. Otherwise `store.setLastClock(...)`
  records the new clock and processing continues.
- **Fail-open.** If `getLastClock` or `setLastClock` throws/rejects, the error is
  logged and the message is treated as **not** a duplicate (processing proceeds).
  A transient store outage never silently drops a message — it only weakens
  deduplication until the store recovers, biasing towards at-least-once.
- Strategies: `'drop'` (log only), `'dlq'` (forward to `<topic>.dlq` with
  `x-dlq-reason` + clock headers, only when `dlq: true`), `'topic'` (forward to
  `deduplication.duplicatesTopic` or `<topic>.duplicates` via
  `sendToDuplicatesTopic`).

### Durability depends on the store

With the default `InMemoryDedupStore`, dedup state is process-local and resets on
**restart or rebalance** (the map lives on `ctx`, cleared by
`stopConsumer`/`disconnect`; a rebalance can move a partition to another instance
with an empty map). That provides *within-session* deduplication only. Supply a
persistent `DedupStore` (Redis, a database) to survive restarts and rebalances —
see the Redis reference in [`testing.md`](./testing.md#kafkatestcontainer-srctestingtestcontainerts).
Either way, for a hard exactly-once guarantee pair it with idempotent handlers or
`startTransactionalConsumer`.

---

## Backpressure — the `consume()` iterator

`consume(topic, options)` (`index.ts`) bridges Kafka's push model to an
`AsyncIterableIterator` via `AsyncQueue` (`queue.ts`):

- The synthetic handler does `queue.push(envelope)`; the caller's `for await`
  loop pulls via `queue.next()`.
- With `queueHighWaterMark` set: when the buffer fills, `AsyncQueue` calls
  `onFull` → `pauseTopicAllPartitions` (pauses every assigned partition of the
  topic). When the buffer drains to `<= highWaterMark / 2`, `onDrained` →
  `resumeTopicAllPartitions`. Without it, the queue is unbounded (`Infinity`).
- Breaking out of the `for await` triggers the iterator's `return()`, which
  closes the queue and calls `handle.stop()`.
- A startup failure in the underlying `startConsumer` promise fails the queue
  (`queue.fail(err)`), so `next()` rejects.

---

## Known semantic caveats

- **`consume()` is at-most-once under auto-commit.** The synthetic handler only
  pushes to the queue and returns immediately; librdkafka auto-commits the offset
  as soon as the handler returns — *before* your `for await` body has processed
  the message. A crash between push and processing loses that message. Use
  `startConsumer` (or `autoCommit: false` + manual commits) if you need
  at-least-once here.
- **`consume()` rejects `retryTopics`.** It throws at call time — durable retry
  needs the offset lifecycle that the queue path doesn't provide. Use
  `startConsumer({ retryTopics: true })`.
- **`startWindowConsumer` relies on auto-commit and rejects `retryTopics`.**
  Offsets auto-commit as messages arrive and are buffered; a flush that throws
  can't un-commit them. To avoid silently losing a window, a failing flush routes
  **every buffered envelope** to `onMessageLost` (`features/window.ts`) rather than
  swallowing the error. The remaining buffer is flushed on `stop()`. `maxMessages`
  and `maxMs` must both be `> 0`.
- **`startTransactionalConsumer` rejects `retryTopics`.** EOS already redelivers
  on failure (the tx aborts and the offset isn't committed), so a retry chain is
  redundant; it throws if `retryTopics` is set.
- **`highWatermark` is `null` in the retry-chain batch path.** When a batch
  consumer uses `retryTopics: true`, the retry consumers wrap the batch handler
  for single-message delivery and pass `highWatermark: null` in `BatchMeta`
  (`start.ts`). Don't compute lag from `meta.highWatermark` there.
- **`handlerTimeoutMs` is a warning, not a cancel.** `wrapWithTimeoutWarning`
  (`lifecycle.ts`) logs if the handler exceeds the threshold but never aborts it.
