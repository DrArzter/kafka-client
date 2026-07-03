# Producer internals

How the send path works from `sendMessage()` down to `producer.send()` on the
transport. Covers payload preparation, the automatic envelope headers, the
Lamport clock lifecycle, transactions, the lag throttle, and tombstones.

Public API and options: see `README.md`. Files referenced here live under
`src/client/kafka.client/producer/` unless noted.

---

## The send pipeline

All three send methods (`sendMessage`, `sendBatch`, and both `transaction()`
sends) funnel through **one** shared step: `preparePayload` in `send.ts`.

```
sendMessage(topic, msg, opts)                        [index.ts]
   → sendMessageImpl(ctx, ...)                        [send.ts]
        1. waitIfThrottled(ctx)          ── block if lag throttle is engaged
        2. preparePayload(ctx, ...)      ── the shared builder ↓
        3. redirectToDelayed(...)?       ── if opts.deliverAfterMs > 0, retarget to <topic>.delayed
        4. ctx.producer.send(payload)    ── hand off to the transport
        5. metrics.notifyAfterSend(...)  ── fires afterSend() instrumentation

preparePayload(ctx, topicOrDesc, messages, compression)   [send.ts]
   ├─ registerSchema(...)      ── if a TopicDescriptor carries __schema, add it
   │                              to ctx.schemaRegistry (warn on conflict)     [ops.ts]
   ├─ buildSendPayload(...)    ── per-message: build headers, stamp clock,
   │                              run beforeSend, validate, serialise via serde [ops.ts]
   └─ ensureTopic(ctx, topic)  ── auto-create the topic if enabled             [lifecycle.ts]
```

> **Transport security.** Before any of this runs, the facade constructor calls
> `resolveSecurityOptions(options.security, brokers, logger)` and hands the result
> to `ConfluentTransport` as its third argument, so the producer connects over the
> resolved TLS/SASL settings. See [`configuration.md`](./configuration.md#security-configuration).

### `buildSendPayload` — per-message work (`ops.ts`)

For each message in the batch it:

1. **Builds envelope headers** via `buildEnvelopeHeaders` (see table below).
2. **Stamps the Lamport clock** — `x-lamport-clock` = `String(deps.nextLamportClock())`,
   but only when `deps.nextLamportClock` is provided. (The producer path always
   provides it via `ctx.producerOpsDeps.nextLamportClock`.)
3. **Runs `beforeSend` instrumentation** so tracing can inject `traceparent` etc.
   into the header object (mutated in place).
4. **Validates** the value against the attached-or-registry schema
   (`validateMessage`), passing a `SchemaParseContext` (`{ topic, headers, version }`).
5. **Serialises** the validated value into the message `value` via the resolved
   serde (see [Serialization](#serialization-via-serde) below). The default
   `JsonSerde` produces exactly the same bytes as the old hard-coded
   `JSON.stringify`, so this is a zero-behaviour-change default.
6. **Resolves the key**: `m.key ?? topicOrDesc.__key?.(m.value) ?? null`. An
   explicit `options.key` always wins; otherwise, if the topic was passed as a
   `TopicDescriptor` with a `.key(fn)` extractor, the extractor runs on the
   original (pre-validation) payload; failing both, the key is `null`.

Note the ordering: headers (including the clock) are built *before* validation,
and validation runs on the **object** *before* serialisation — so a
`SchemaParseContext`-aware validator can read the version/headers, and the serde
always receives an already-validated value.

### Schema handling (`ops.ts`)

- `registerSchema` copies a `TopicDescriptor.__schema` into the client-wide
  `ctx.schemaRegistry`. If a *different* schema is already registered for that
  topic it logs a warning and overwrites (last-write-wins).
- `validateMessage` priority: (1) inline schema on the descriptor is **always**
  applied; (2) a registry schema is applied only when `strictSchemasEnabled`
  (default `true`) *and* the topic was passed as a plain string; (3) no schema →
  value passes through unchanged. A validation failure throws
  `KafkaValidationError`.

### `ensureTopic` (`lifecycle.ts`)

No-op unless `autoCreateTopics` is on and the topic isn't already in
`ctx.ensuredTopics`. Concurrent calls for the same topic share one in-flight
promise (`ctx.ensureTopicPromises`) so a topic is created once even under a
burst of parallel sends.

### Serialization via serde (`ops.ts`)

Step 5 of `buildSendPayload` no longer hard-codes `JSON.stringify`. It resolves
a `MessageSerde` (`message/serde.ts`) and calls `serde.serialize(validatedValue,
{ topic, headers, isKey: false })`, using whatever it returns (a `Buffer` or a
`string`) as the message `value`.

Which serde runs is decided **per topic** by `resolveSerde(topicOrDesc,
deps.serde)`:

```
per-topic TopicDescriptor.__serde   >   client-wide KafkaClientOptions.serde   >   JsonSerde
```

- A `topic('orders').serde(avroSerde({...}))` override always wins for that topic.
- Otherwise the client-wide `serde` (from the constructor options) applies.
- With neither set, the default is `JsonSerde` — byte-for-byte identical to the
  historical behaviour, so upgrading changes nothing until you opt in.

The serde only ever touches the message **value**. Every envelope header
(`x-event-id`, `x-correlation-id`, `x-timestamp`, `x-schema-version`,
`x-lamport-clock`, `traceparent`) is still built and sent as a Kafka header
regardless of the serde — the wire contract for metadata is unchanged whether the
value is JSON, Avro, or Protobuf. Validation also stays object-level: the
`SchemaLike` runs on the object *before* the serde encodes it. See
[`serialization.md`](./serialization.md) for the `MessageSerde` contract and the
registry-backed Avro/Protobuf serdes.

---

## Automatic envelope headers

`buildEnvelopeHeaders` (`message/envelope.ts`) stamps these on **every** send.
User-supplied `headers` win on key conflict (`{ ...envelope, ...options.headers }`).

| Header              | Source / default                                             |
|---------------------|--------------------------------------------------------------|
| `x-event-id`        | `options.eventId` → else a new UUID v4.                      |
| `x-correlation-id`  | `options.correlationId` → **ALS context** → else new UUID.   |
| `x-timestamp`       | `new Date().toISOString()` at build time.                    |
| `x-schema-version`  | `String(options.schemaVersion ?? 1)`.                        |
| `x-lamport-clock`   | Stamped by `buildSendPayload` (not `buildEnvelopeHeaders`) — `String(++ctx._lamportClock)`. |
| `traceparent`       | Copied from ALS context if present; OTel's `beforeSend` may inject/override it. |

### CorrelationId & traceparent propagation (AsyncLocalStorage)

`correlationId` resolution is *explicit option → ALS → new UUID*. The ALS store
is set by the **consumer** pipeline: before invoking a handler, the consumer
runs it inside `runWithEnvelopeContext({ correlationId, traceparent })`
(see `consumer.md`). Any `sendMessage` nested inside that handler therefore
inherits the incoming correlationId automatically — no manual threading. Outside
a handler, `getEnvelopeContext()` returns `undefined` and a fresh UUID is minted.

---

## Lamport clock lifecycle

A single monotonically-increasing counter, `ctx._lamportClock`, used to give
consumer-side deduplication a total-ish order across a producer's messages.

**Stamping.** `ctx.producerOpsDeps.nextLamportClock = () => ++ctx._lamportClock`
(wired in the constructor). Every produced message gets the next value as
`x-lamport-clock`. Because it's a pre-increment, the first message is `1`.

**Recovery on connect.** `connectProducerImpl` (`lifecycle.ts`) calls
`recoverLamportClockImpl(ctx, ctx.clockRecoveryTopics)`. If `clockRecovery.topics`
is configured, it:

1. Fetches offsets for each topic, collecting every non-empty partition and its
   last offset (`high − 1`).
2. Spins up an **ephemeral** consumer (`<clientId>-clock-recovery-<ts>`), seeks
   each partition to its last message, reads one message per partition, and
   takes the maximum `x-lamport-clock` seen.
3. Sets `ctx._lamportClock = maxClock`, so the next send continues from
   `maxClock + 1` — restarts don't reset the clock to 0.
4. Cleans up: disconnects the consumer and deletes the ephemeral group in a
   `finally`.

**Timeout bound (recent).** A seeked partition may never deliver its last
message (compaction/retention trimmed it between the offset fetch and the seek).
Without a bound, `connectProducer()` would hang forever. `recoverLamportClockImpl`
arms a `clockRecoveryTimeoutMs` timer (default 30 000 ms, from
`clockRecovery.timeoutMs`); on timeout it logs a warning, cleans up, and
**proceeds with the partial `maxClock` gathered so far** rather than hanging.

**Use in dedup.** The consumer compares each message's `x-lamport-clock` against
the last processed clock per `topic:partition` and skips those `<=` it. See
`consumer.md#deduplication`.

---

## Transactions

`transaction(fn)` gives atomic multi-topic sends: either every `tx.send` inside
`fn` commits or none do.

### Lazy transactional producer

librdkafka forbids a `transactionalId`-configured producer from doing
non-transactional `send()`s, so the library keeps **two** producers:

- `ctx.producer` — the default producer (`acks: -1`), used by all normal sends.
- `ctx.txProducer` — created **lazily** on the first `transaction()` call, with
  `idempotent: true` + `transactionalId`.

`transactionImpl` (`send.ts`) guards concurrent lazy init with
`ctx.txProducerInitPromise`, so only one tx producer is ever created; on init
failure the promise is reset so a later call can retry.

Separately, at the transport level `ConfluentProducer.connect()` caches its
connect promise (`transport/confluent.transport.ts`). librdkafka's compat layer —
unlike KafkaJS — throws *"Connect has already been called elsewhere"* on a second
`connect()`. Caching one in-flight/settled promise lets `connectProducer()` and
the lazy consumer-pipeline connect (DLQ / retry / duplicates routing, see
[`consumer.md`](./consumer.md#lazy-producer-connect-for-routing-consumers)) both
call `connect()` idempotently. The promise is reset on a failed connect (so a
retry can reconnect) and on `disconnect()`.

### Serialisation via `_txChain` (recent)

A transactional producer supports only **one open transaction at a time**.
Overlapping `transaction()` calls would otherwise interleave
begin/send/commit/abort on the shared producer and corrupt state.
`transactionImpl` serialises them with a promise chain:

```ts
const prev = ctx._txChain;
let release;
ctx._txChain = new Promise(r => (release = r));  // next caller awaits us
await prev;                                        // wait for the previous tx
try { await runTransaction(ctx, fn); }
finally { release(); }                             // let the next caller proceed
```

Each `transaction()` call waits for the previous one to finish before opening its
own transaction. `runTransaction` builds the `TransactionContext` (`tx.send` /
`tx.sendBatch`, each going through `preparePayload`), calls `fn`, then
`tx.commit()` — or `tx.abort()` on any throw (abort failures are logged, the
original error is re-thrown).

### Transactional-id registry & fencing

`_activeTransactionalIds` (`lifecycle.ts`) is a **process-level `Set`** of active
transactional IDs. When a tx producer is created (`transactionImpl` or
`createRetryTxProducer`) and the ID is already present, a warning is logged:
Kafka will *fence* one of the two producers (the loser fails on its next
transaction). The default ID is `${clientId}-tx`; set `transactionalId`
explicitly (unique per process/replica) to avoid same-process collisions.
Cross-process collisions can't be detected here — they surface as broker fencing
errors. IDs are removed from the set on `disconnect()` and, for retry/EOS
producers, on `stopConsumer()`.

Related transactional producers created elsewhere:
- `${gid}-main-tx` — EOS routing for the main consumer when `retryTopics: true`.
- `${levelGroupId}-tx` — one per retry level (`retry-topic.ts`).
- `${gid}-txc` — `startTransactionalConsumer` EOS producer.

---

## Lag throttle

Optional back-pressure on the producer keyed to consumer lag. Enabled by
`KafkaClientOptions.lagThrottle`.

- **Poller.** `startLagThrottlePoller` (`lifecycle.ts`), started by
  `connectProducer()`, runs a background `setInterval` (`pollIntervalMs`, default
  5 000 ms, `unref`'d). Each tick calls `getConsumerLag(groupId)`, sums total lag,
  and flips `ctx._lagThrottled` true when `total > maxLag`, false when it drops
  back to `<= maxLag`. Poll errors are swallowed so they never block sends.
- **Blocking.** Every send calls `waitIfThrottled(ctx)` first (`send.ts`). While
  `_lagThrottled`, it polls the flag every 100 ms up to `maxWaitMs` (default
  30 000 ms). On timeout it logs a warning and **sends anyway** — this is
  best-effort throttling, not a hard gate.
- The timer is cleared on `disconnect()` / `disconnectProducer()`.

---

## Delayed delivery

`SendOptions.deliverAfterMs` (and `BatchSendOptions.deliverAfterMs`) defers a
message rather than sending it to the target topic immediately. It is a producer-
side staging step paired with the consumer-side `startDelayedRelay` (see
[`consumer.md`](./consumer.md#delayed-delivery-relay-featuresdelayedts)).

When `deliverAfterMs > 0`, `sendMessageImpl`/`sendBatchImpl` build the normal
payload, then call `redirectToDelayed` (`send.ts`), which:

1. Stamps each message with `x-delayed-until = String(Date.now() + deliverAfterMs)`
   and `x-delayed-target = <originalTopic>`.
2. Rewrites the payload topic to `<originalTopic>.delayed`.
3. `ensureTopic`s the `.delayed` staging topic.

The message is then produced to `<topic>.delayed` as usual (all envelope headers,
Lamport clock, and `beforeSend`/`afterSend` hooks still apply — this is a normal
send with two extra control headers). It sits there until a running
`startDelayedRelay` forwards it to the target once the deadline passes. Delivery
time is a **lower bound**, and the relay must be running for delayed messages to
be delivered at all.

---

## Tombstones

`sendTombstone(topic, key, headers?)` (`sendTombstoneImpl` in `send.ts`) produces
a `{ value: null, key }` message for log-compacted topics.

It deliberately **bypasses `buildSendPayload`**: no envelope headers, no schema
validation, and — importantly — **no Lamport clock stamping**. A tombstone
therefore carries no `x-lamport-clock`, which means it is exempt from
consumer-side deduplication (the dedup check passes any message without a clock
header straight through). It still honours `waitIfThrottled`, `ensureTopic`, and
the `beforeSend`/`afterSend` instrumentation hooks.
