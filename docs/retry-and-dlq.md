# Retry & DLQ topology

The two failure-handling paths — in-process retry and the durable retry-topic
chain — plus dead-lettering and DLQ replay. This is the detail behind stage 4 of
the consumer pipeline (`consumer.md#stage-4--executewithretry`).

Files referenced here live under `src/client/kafka.client/consumer/` unless
noted. Public options: see `README.md`.

---

## Two retry strategies

| | In-process retry (`retry`) | Retry-topic chain (`retryTopics: true`) |
|---|---|---|
| Where backoff happens | Inside the handler's `eachMessage` call (`sleep`) | On separate `<topic>.retry.<n>` topics, one companion consumer per level |
| Durable across restart | No — a crash mid-backoff loses the retry state | Yes — the message is persisted in the retry topic |
| Offset commit | librdkafka auto-commit after `eachMessage` returns | Manual / EOS transaction (forces `autoCommit: false`) |
| Requires | `retry.maxRetries` | `retry.maxRetries` **and** a fixed topic name (no RegExp) |
| Config guard | — | `validateTopicConsumerOpts` throws if `retryTopics` without `retry`, or with a RegExp topic |

Both are configured through the same `retry: { maxRetries, backoffMs?, maxBackoffMs? }`.
`retryTopics: true` adds durability on top.

---

## In-process retry (`executeWithRetry`, `pipeline.ts`)

The default. `maxAttempts = retry.maxRetries + 1` (the initial try plus retries).

```
attempt 1 → handler throws → onFailure (breaker) → backoff sleep → attempt 2 → …
                                                                       │
                          last attempt fails ─────────────────────────┤
                                                                       ▼
                                              dlq: true  → sendToDlq(<topic>.dlq)
                                              dlq: false → onMessageLost(...)
```

- Backoff: `min(backoffMs * 2^(attempt-1), maxBackoffMs)`, then `Math.random()`
  full jitter over that cap. Defaults: `backoffMs` 1 000, `maxBackoffMs` 30 000.
- `onFailure` fires on **every** failed attempt (drives the circuit breaker,
  independent of DLQ — see `consumer.md#circuit-breaker-wiring`).
- On the last attempt the reported error is wrapped in `KafkaRetryExhaustedError`
  (when `maxAttempts > 1`) before interceptors' `onError` see it.
- Batch path: DLQ uses **per-message** headers so each dead-lettered message
  keeps its own `correlationId`/`traceparent` rather than copying `envelopes[0]`'s.

---

## Retry-topic chain (`retry-topic.ts`)

Durable retry via dedicated topics. Turned on with `retryTopics: true`.

### Topology

With `retry.maxRetries: N`, `launchRetryChain` (`setup.ts`) →
`startRetryTopicConsumers` starts **N companion consumers in parallel**
(`Promise.all`), one per level:

```
main consumer            <groupId>            → topic
   │ handler fails once (maxAttempts = 1 in retryTopics mode)
   ▼ route to retry.1
level-1 consumer         <groupId>-retry.1    → topic.retry.1
   │ wait x-retry-after, retry; on fail route to retry.2
   ▼
level-2 consumer         <groupId>-retry.2    → topic.retry.2
   ⋮
level-N consumer         <groupId>-retry.N    → topic.retry.N
   │ final failure
   ▼ dlq: true → topic.dlq   |   dlq: false → onMessageLost
```

- **Group naming**: main `<groupId>`, companions `<groupId>-retry.<level>`
  (1-indexed). Registered in `ctx.companionGroupIds[gid]` so `stopConsumer(gid)`
  tears the whole chain down.
- **Topic naming**: `<originalTopic>.retry.<level>` (1-indexed).
- **Incremental registration**: `onLevelStarted(levelGroupId)` pushes each level
  into `companionGroupIds[gid]` as soon as it's up, so a partial startup failure
  still leaves already-started levels tracked and stoppable. `launchRetryChain`
  pre-seeds an empty array before starting, for the same reason.
- **Partition-assignment wait**: each level calls `waitForPartitionAssignment`
  (default `retryTopicAssignmentTimeoutMs` = 10 000 ms) before reporting started;
  a timeout logs a warning rather than throwing. Levels start in parallel
  specifically so this wait isn't `N × 10 s` of blocking startup.

### Per-level flow (`startLevelConsumer`)

Each level consumer runs with `autoCommit: false` and per message:

1. **Honour the delay**: read `x-retry-after`; if in the future, `pause` the
   partition, `sleep(remaining)`, then `resume`. The offset is **not** committed
   during the wait, so a crash here redelivers the message (at-least-once for the
   wait window).
2. **Validate & build envelope** (against the *original* topic's schema, resolved
   from `x-retry-original-topic`), then run the handler through
   `runHandlerWithPipeline`.
3. **On success**: `onMessage`, then `commitOffsets` directly.
4. **On failure**: `deps.onFailure(envelope)` (drives the **main** group's
   breaker), then:
   - **Not exhausted** (`level < maxRetries`): EOS transaction — send to
     `<topic>.retry.<level+1>` **and** commit the source offset in one tx.
   - **Exhausted + `dlq: true`**: EOS transaction — send to `<topic>.dlq` +
     commit source offset.
   - **Exhausted, no DLQ**: `onMessageLost`, then commit offset directly (no
     produce, no tx needed).

### EOS routing transactions

The key durability guarantee: routing a failed message to the next level (or
DLQ) and committing the source offset happen in **one Kafka transaction**
(`tx.send` + `tx.sendOffsets` + `tx.commit`). A crash at any point rolls the
transaction back — no duplicate lands in the next level or DLQ. If the tx itself
fails (broker down), the offset stays uncommitted and the message is redelivered,
retrying the routing on the next delivery. Each level owns a dedicated tx
producer `${levelGroupId}-tx` (`createRetryTxProducer`).

The **main consumer → retry.1** hop is also EOS when it can be: `makeEosMainContext`
(`setup.ts`) creates a `${gid}-main-tx` producer and, in `handleEachMessage`, the
`eosRouteToRetry` closure wraps the send-to-`retry.1` + source-offset-commit in a
transaction. The **batch** main consumer routes non-EOS via the regular producer
(`sendToRetryTopic`) — a crash between the produce and the auto-commit can leave
the message in `retry.1` *and* redelivered, so batch handlers under `retryTopics`
must be idempotent (see `handleEachBatch` / `executeWithRetry`).

### `x-retry-*` headers (`pipeline.ts`)

`buildRetryTopicPayload` stamps, and strips any stale copies from a previous hop:

| Header                    | Meaning |
|---------------------------|---------|
| `x-retry-attempt`         | The level number this message is being sent to. |
| `x-retry-after`           | `Date.now() + delay` — earliest time the level consumer should retry. |
| `x-retry-max-retries`     | The `maxRetries` in force (survives across hops). |
| `x-retry-original-topic`  | The source topic, so the level consumer validates against the right schema and reports the right topic. |

---

## Dead-letter queue

### Naming & headers

DLQ topic: `<originalTopic>.dlq`. `buildDlqPayload` (`pipeline.ts`) stamps
(on top of the original headers):

| Header                  | Value |
|-------------------------|-------|
| `x-dlq-original-topic`  | The source topic. |
| `x-dlq-failed-at`       | ISO timestamp. |
| `x-dlq-error-message`   | The error message (`"unknown"` if absent). |
| `x-dlq-error-stack`     | Stack, truncated to 2 000 chars. |
| `x-dlq-attempt-count`   | Attempt number at dead-lettering. |

Dedup-`'dlq'` and TTL-expiry dead-letters add their own reason headers
(`x-dlq-reason`, `x-dlq-duplicate-*`). `sendToDlq` falls back to `onMessageLost`
if the DLQ produce itself fails.

### `DlqReason`

`onDlq(envelope, reason)` reports why: `"handler-error"` (retry exhausted) or
`"ttl-expired"`. Note: dead-lettering does **not** itself drive the circuit
breaker — that's `notifyFailure` at the handler-error boundary.

---

## DLQ replay (`consumer/features/dlq-replay.ts`)

`replayDlq(topic, options)` reads `<topic>.dlq` and re-publishes to the original
topic (from `x-dlq-original-topic`) or `options.targetTopic`. It strips the five
`x-dlq-*` headers before re-publishing so they don't leak into the destination.

Pass the **original** topic, not the DLQ topic: `replayDlqTopic` throws if `topic`
already ends in `.dlq` (the `.dlq` suffix is appended internally, so a `.dlq`
argument would wrongly read `<topic>.dlq.dlq`). The `kafka-client-dlq` CLI
(`src/cli/`) wraps this same call for ops use.

### Ephemeral vs stable group (`options.fromBeginning`)

- `fromBeginning: true` (default) — a fresh **ephemeral** group
  `<topic>.dlq-replay-<ts>` is used, with `auto.offset.reset=earliest`, so the
  full DLQ is replayed every call. The group is **deleted from the broker** after
  the run (via `cleanupConsumer(..., deleteGroup=true)`).
- `fromBeginning: false` — a **stable** group `<topic>.dlq-replay` whose committed
  offsets persist between calls, so only messages added since the last call are
  replayed. The group is not deleted.

Only partitions with `high > low` are processed (a retention-trimmed topic can
have `high > 0` but no readable messages, which would otherwise hang the "consume
up to high − 1" loop). `dryRun` counts but doesn't produce; `filter(headers,
value)` skips non-matching messages. Returns `{ replayed, skipped }`.

---

## What does NOT propagate into the retry chain

The retry-topic consumers deliberately re-run only the handler with schema
validation. Several main-consumer behaviours are **not** re-applied at each retry
level (they run once, on the main consumer, before routing):

- **`messageTtlMs`** — TTL is checked in `handleEachMessage`/`handleEachBatch`
  only. `startLevelConsumer` has no TTL stage, so a message already routed to a
  retry topic won't be TTL-dropped there.
- **`handlerTimeoutMs`** — the timeout-warning wrapper is applied by the main
  consumer's `wrapWithTimeout`; retry-level consumers call the handler directly
  (no wrapper).
- **`deduplication`** — dedup state and the `applyDeduplication` check live in
  the main pipeline only. Retry-level consumers do not consult or update the
  Lamport-clock dedup state.

Interceptors and instrumentation **do** run in the retry chain
(`runHandlerWithPipeline` is shared), as do the metrics/breaker `onFailure` /
`onMessage` / `onRetry` / `onDlq` callbacks (bound to the main group).
