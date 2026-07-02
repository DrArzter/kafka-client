# Configuration reference

Everything that can be tuned on `@drarzter/kafka-client` — and **where** each
knob lives. The library is code-first: every option is a plain constructor or
consumer argument. Environment variables are an optional convenience layer on
top, surfaced through two helpers exported from `@drarzter/kafka-client/core`:

- `kafkaClientConfigFromEnv(env?, prefix?)` → `{ clientId?, groupId?, brokers?, options }`
- `consumerOptionsFromEnv<T>(env?, prefix?)` → `Partial<ConsumerOptions<T>>`
- `mergeConsumerOptions(...layers)` → `ConsumerOptions<T>` (later layers win)

The `.env.example` at the repo root is the exhaustive, commented variable list;
this document is the mapping from those variables (and their code-level
equivalents) to their effect, plus the precedence rules that govern how the two
combine.

Public API and usage examples: see `README.md`. The source is the ultimate
authority — the helper implementation lives in
`src/client/config/from-env.ts`.

---

## The three layers

Configuration is resolved from three sources, highest priority first:

```
   explicit code options   >   environment variables   >   built-in defaults
```

1. **Built-in defaults** — the values documented in the tables below (e.g.
   `strictSchemas` is `true`, `autoCommit` is `true`). Apply when neither of the
   layers above set the field.
2. **Environment variables** — read only when you call `kafkaClientConfigFromEnv`
   / `consumerOptionsFromEnv`. Each helper emits a key **only** when the variable
   is present and non-empty; unset variables leave the default in place.
3. **Explicit code options** — anything you pass directly to the constructor or a
   consumer call. Because the helpers only *produce* the argument objects, code
   values you spread afterwards (or merge as a later layer) always win.

### The library never reads `.env` itself

There is no built-in dotenv step. Load the file **before** constructing the
client, using either:

- Node 20.6+ built-in flag: `node --env-file=.env dist/main.js`
- the `dotenv` package: `import 'dotenv/config'` at the top of your entrypoint.

### Worked example

```ts
import { KafkaClient, kafkaClientConfigFromEnv } from '@drarzter/kafka-client/core';

// env: KAFKA_CLIENT_ID=svc  KAFKA_BROKERS=b1:9092,b2:9092  KAFKA_STRICT_SCHEMAS=false
const { clientId, groupId, brokers, options } = kafkaClientConfigFromEnv();

const kafka = new KafkaClient(
  clientId ?? 'fallback-svc',   // env wins over the fallback
  groupId  ?? 'fallback-grp',   // env absent → fallback used
  brokers  ?? ['localhost:9092'],
  {
    ...options,                 // strictSchemas: false comes from env
    strictSchemas: true,        // …but this code line overrides it back to true
    onMessageLost: alerting,    // not env-configurable — code only
  },
);
```

Effective result: `clientId` from env, `groupId` from the fallback,
`strictSchemas` from the **code** line (not env), `onMessageLost` from code, and
every other option at its library default.

---

## `KafkaClientOptions` (per client)

Passed as the 4th constructor argument (or `KafkaModule.register(...)`). Prefix
for env vars: **`KAFKA_`**.

| Option | Type | Default | Env var | Effect |
|---|---|---|---|---|
| *(clientId)* | string | — | `KAFKA_CLIENT_ID` | 1st constructor arg — client identity to the broker. |
| *(groupId)* | string | — | `KAFKA_GROUP_ID` | 2nd constructor arg — default consumer group. |
| *(brokers)* | string[] | — | `KAFKA_BROKERS` (list) | 3rd constructor arg — `host:port` bootstrap servers. |
| `autoCreateTopics` | boolean | `false` | `KAFKA_AUTO_CREATE_TOPICS` | Admin-creates topics before first send/subscribe. Dev only. |
| `strictSchemas` | boolean | `true` | `KAFKA_STRICT_SCHEMAS` | Validate string topic keys against a registered schema. |
| `numPartitions` | number | `1` | `KAFKA_NUM_PARTITIONS` | Partitions for auto-created topics. |
| `transactionalId` | string | `${clientId}-tx` | `KAFKA_TRANSACTIONAL_ID` | Tx producer id. Must be unique per process/replica. |
| `clockRecovery.topics` | string[] | — | `KAFKA_CLOCK_RECOVERY_TOPICS` (list) | Topics scanned for the max Lamport clock on connect. Enables the block. |
| `clockRecovery.timeoutMs` | number | `30000` | `KAFKA_CLOCK_RECOVERY_TIMEOUT_MS` | Bound on recovery before proceeding with a partial result. |
| `lagThrottle.maxLag` | number | — | `KAFKA_LAG_THROTTLE_MAX_LAG` | Lag threshold above which sends block. Enables the block. |
| `lagThrottle.groupId` | string | client group | `KAFKA_LAG_THROTTLE_GROUP_ID` | Which group's lag is monitored. |
| `lagThrottle.pollIntervalMs` | number | `5000` | `KAFKA_LAG_THROTTLE_POLL_INTERVAL_MS` | Lag poll cadence. |
| `lagThrottle.maxWaitMs` | number | `30000` | `KAFKA_LAG_THROTTLE_MAX_WAIT_MS` | Max time a send waits while throttled. |
| `security.ssl` | boolean | auto | `KAFKA_SSL` | Enable TLS. Auto-`true` when `sasl` is set. See below. |
| `security.sasl.mechanism` | enum | — | `KAFKA_SASL_MECHANISM` | `plain` / `scram-sha-256` / `scram-sha-512`. |
| `security.sasl.username` | string | — | `KAFKA_SASL_USERNAME` | SASL username. |
| `security.sasl.password` | string | — | `KAFKA_SASL_PASSWORD` | SASL password. |
| `security.allowInsecure` | boolean | `false` | `KAFKA_ALLOW_INSECURE` | Acknowledge a plaintext-to-remote setup, silence the warning. |
| `logger` | `KafkaLogger` | console | **code only** | Custom logger. |
| `instrumentation` | `KafkaInstrumentation[]` | `[]` | **code only** | Cross-cutting hooks (e.g. `otelInstrumentation()`). |
| `onMessageLost` | fn | — | **code only** | Called when a message is dropped without a DLQ. |
| `onTtlExpired` | fn | — | **code only** | Client-wide TTL-expiry callback. |
| `onRebalance` | fn | — | **code only** | Called on consumer group rebalance. |
| `transport` | `KafkaTransport` | `ConfluentTransport` | **code only** | Alternate/fake transport (see `docs/testing.md`). |
| `security.sasl` (oauthbearer) | `{ oauthBearerProvider }` | — | **code only** | Cloud IAM — provider is a function. See below. |

Conditional nesting emitted by `kafkaClientConfigFromEnv`:

- `clockRecovery` — only when `KAFKA_CLOCK_RECOVERY_TOPICS` is present.
- `lagThrottle` — only when `KAFKA_LAG_THROTTLE_MAX_LAG` is present (its only
  required field).
- `security` — only when at least one `KAFKA_SSL` / `KAFKA_SASL_*` /
  `KAFKA_ALLOW_INSECURE` variable is present.

Cross-references: producer-side clock recovery and the lag throttle are
explained in [`producer.md`](./producer.md#lamport-clock-lifecycle); the NestJS
module option subset is in [`nestjs.md`](./nestjs.md#which-options-forward-to-the-client).

---

## `ConsumerOptions` (per consumer)

Passed as the 3rd argument to `startConsumer` / `startBatchConsumer` (and via
`@SubscribeTo`). Prefix for env vars: **`KAFKA_CONSUMER_`** (note the different
prefix — these are consumer defaults, not client options).

| Option | Type | Default | Env var | Effect |
|---|---|---|---|---|
| `groupId` | string | client group | `KAFKA_CONSUMER_GROUP_ID` | Override the group for this consumer. |
| `fromBeginning` | boolean | `false` | `KAFKA_CONSUMER_FROM_BEGINNING` | Seek to earliest on subscribe. |
| `autoCommit` | boolean | `true` | `KAFKA_CONSUMER_AUTO_COMMIT` | Auto-commit offsets. |
| `dlq` | boolean | `false` | `KAFKA_CONSUMER_DLQ` | Route exhausted/invalid messages to `<topic>.dlq`. |
| `retry.maxRetries` | number | — | `KAFKA_CONSUMER_RETRY_MAX_RETRIES` | Max attempts. Enables the retry block. |
| `retry.backoffMs` | number | `1000` | `KAFKA_CONSUMER_RETRY_BACKOFF_MS` | Base backoff (exponential + jitter). |
| `retry.maxBackoffMs` | number | `30000` | `KAFKA_CONSUMER_RETRY_MAX_BACKOFF_MS` | Backoff cap. |
| `retryTopics` | boolean | `false` | `KAFKA_CONSUMER_RETRY_TOPICS` | Durable retry via `<topic>.retry.<n>`. Requires `retry`; forces `autoCommit: false`. |
| `retryTopicAssignmentTimeoutMs` | number | `10000` | `KAFKA_CONSUMER_RETRY_TOPIC_ASSIGNMENT_TIMEOUT_MS` | Wait for retry-level partition assignment. |
| `handlerTimeoutMs` | number | — | `KAFKA_CONSUMER_HANDLER_TIMEOUT_MS` | Warn if a handler exceeds this (non-cancelling). |
| `messageTtlMs` | number | — | `KAFKA_CONSUMER_MESSAGE_TTL_MS` | Drop messages older than N ms (`x-timestamp`). |
| `deduplication.strategy` | enum | — | `KAFKA_CONSUMER_DEDUPLICATION_STRATEGY` | `drop` / `dlq` / `topic`. Enables the block. |
| `deduplication.duplicatesTopic` | string | `<topic>.duplicates` | `KAFKA_CONSUMER_DEDUPLICATION_TOPIC` | Destination when `strategy: 'topic'`. |
| `deduplication.store` | `DedupStore` | in-memory | **code only** | Persistent dedup store (e.g. Redis). |
| `circuitBreaker.threshold` | number | `5` | `KAFKA_CONSUMER_CIRCUIT_BREAKER_THRESHOLD` | Failures to open the circuit. Enables the block. |
| `circuitBreaker.recoveryMs` | number | `30000` | `KAFKA_CONSUMER_CIRCUIT_BREAKER_RECOVERY_MS` | Time OPEN before probing HALF-OPEN. |
| `circuitBreaker.windowSize` | number | `threshold*2` (min 10) | `KAFKA_CONSUMER_CIRCUIT_BREAKER_WINDOW_SIZE` | Sliding-window size. |
| `circuitBreaker.halfOpenSuccesses` | number | `1` | `KAFKA_CONSUMER_CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES` | Consecutive HALF-OPEN successes to close. |
| `queueHighWaterMark` | number | unbounded | `KAFKA_CONSUMER_QUEUE_HIGH_WATER_MARK` | Backpressure for `consume()` only. |
| `partitionAssigner` | enum | `cooperative-sticky` | `KAFKA_CONSUMER_PARTITION_ASSIGNER` | `roundrobin` / `range` / `cooperative-sticky`. |
| `groupInstanceId` | string | — | `KAFKA_CONSUMER_GROUP_INSTANCE_ID` | Static membership (`group.instance.id`). |
| `subscribeRetry.retries` | number | `5` | `KAFKA_CONSUMER_SUBSCRIBE_RETRY_RETRIES` | Subscribe attempts when the topic is missing. Enables the block. |
| `subscribeRetry.backoffMs` | number | `5000` | `KAFKA_CONSUMER_SUBSCRIBE_RETRY_DELAY_MS` | Delay between subscribe attempts. |
| `interceptors` | `ConsumerInterceptor[]` | `[]` | **code only** | Per-consumer before/after/onError. |
| `schemas` | `Map` | — | **code only** | `@internal` — populated by `@SubscribeTo`. |
| `onTtlExpired` | fn | — | **code only** | Per-consumer TTL callback (overrides client-wide). |
| `onMessageLost` | fn | — | **code only** | Per-consumer lost-message callback. |
| `onRetry` | fn | — | **code only** | Per-consumer retry callback (composes with metrics hook). |

Conditional nesting emitted by `consumerOptionsFromEnv`:

- `retry` — only when `KAFKA_CONSUMER_RETRY_MAX_RETRIES` is present.
- `deduplication` — only when `KAFKA_CONSUMER_DEDUPLICATION_STRATEGY` is present.
- `circuitBreaker` — only when `KAFKA_CONSUMER_CIRCUIT_BREAKER_THRESHOLD` is
  present.
- `subscribeRetry` — only when `KAFKA_CONSUMER_SUBSCRIBE_RETRY_RETRIES` is
  present.

Cross-references: retry topology and DLQ mechanics are in
[`retry-and-dlq.md`](./retry-and-dlq.md); the per-message pipeline (parse →
dedup → TTL → handler) and circuit-breaker wiring are in
[`consumer.md`](./consumer.md#the-per-message-pipeline-in-order).

### Merging env defaults with code

`consumerOptionsFromEnv` gives you a *base layer*; combine it with per-call
overrides via `mergeConsumerOptions` (later layers win, and the four nested
objects — `retry`, `deduplication`, `circuitBreaker`, `subscribeRetry` — are
**deep-merged**):

```ts
const envDefaults = consumerOptionsFromEnv(); // e.g. { retry: { maxRetries: 3, backoffMs: 500 } }

await kafka.startConsumer(
  ['orders'],
  handler,
  mergeConsumerOptions(envDefaults, {
    dlq: true,
    retry: { maxRetries: 5 }, // maxRetries → 5, backoffMs (500) preserved from env
  }),
);
```

---

## Security configuration

`security` follows secure-by-default rules (implemented in
`src/client/security/resolve-security.ts`):

- **SASL set, SSL unset → TLS auto-enabled.** Credentials never travel over
  plaintext by accident. Opting out requires an explicit `ssl: false`, which
  logs a warning.
- **No security against a remote broker → one-time warning.** Local brokers
  (`localhost`, `127.0.0.0/8`, `::1`, `host.docker.internal`, …) never warn. Set
  `allowInsecure: true` (or `KAFKA_ALLOW_INSECURE=true`) to acknowledge an
  intentionally insecure setup (e.g. a trusted mesh terminating mTLS).

### Env-configurable vs code-only

Only the username/password mechanisms are env-configurable
(`plain`, `scram-sha-256`, `scram-sha-512`). `KAFKA_SASL_MECHANISM`,
`KAFKA_SASL_USERNAME`, and `KAFKA_SASL_PASSWORD` must be set **together** — a
partial set throws at parse time.

**Cloud providers are code-only.** AWS MSK IAM (`awsMskIamProvider`) and Google
Cloud Managed Kafka (`gcpAccessTokenProvider`) use the `oauthbearer` mechanism,
which requires a token-**provider function** — it cannot be expressed as an
environment variable. Set the connection/TLS via env and wire the provider in
code:

```ts
import { KafkaClient, kafkaClientConfigFromEnv, awsMskIamProvider }
  from '@drarzter/kafka-client/core';

const { clientId, groupId, brokers, options } = kafkaClientConfigFromEnv();
const kafka = new KafkaClient(clientId!, groupId!, brokers!, {
  ...options,                       // ssl: true from KAFKA_SSL
  security: {
    ...options.security,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: awsMskIamProvider({ region: 'eu-west-1' }),
    },
  },
});
```

---

## NestJS: `registerAsync` + `ConfigService` vs `fromEnv`

Two idioms reach the same place; pick by how the rest of your app is wired.

- **`ConfigService`-driven `registerAsync`.** If your app already centralises
  config in `@nestjs/config`, keep doing that — read each value from
  `ConfigService` inside `useFactory` and return a `KafkaModuleOptions`. This
  keeps validation and typing in one place and doesn't need the `fromEnv`
  helpers at all.

  ```ts
  KafkaModule.registerAsync({
    imports: [ConfigModule],
    inject: [ConfigService],
    useFactory: (cfg: ConfigService) => ({
      clientId: cfg.getOrThrow('KAFKA_CLIENT_ID'),
      groupId:  cfg.getOrThrow('KAFKA_GROUP_ID'),
      brokers:  cfg.getOrThrow<string>('KAFKA_BROKERS').split(','),
      autoCreateTopics: cfg.get('NODE_ENV') !== 'production',
    }),
  });
  ```

- **`kafkaClientConfigFromEnv` inside `useFactory`.** If you'd rather not
  hand-map each field, call the helper and spread its result — you still get the
  precedence and conditional-nesting logic for free, and can override per field:

  ```ts
  KafkaModule.registerAsync({
    useFactory: () => {
      const { clientId, groupId, brokers, options } = kafkaClientConfigFromEnv();
      return {
        clientId: clientId ?? 'my-svc',
        groupId:  groupId  ?? 'my-grp',
        brokers:  brokers  ?? ['localhost:9092'],
        ...options,
        autoCreateTopics: process.env.NODE_ENV !== 'production',
      };
    },
  });
  ```

Note that `KafkaModuleOptions` is a curated subset of `KafkaClientOptions` (see
[`nestjs.md`](./nestjs.md#which-options-forward-to-the-client)); options outside
that subset are ignored by the module regardless of how you supply them.

For `@SubscribeTo` handlers, pass `consumerOptionsFromEnv()` merged with the
decorator options at wiring time, or set the decorator options directly — the
same precedence applies.

---

## Where do I configure X?

| Scope of the setting | Configure it in… | Example |
|---|---|---|
| One message | `SendOptions` (4th arg of `sendMessage`) | `key`, `headers`, `correlationId`, `compression` |
| One consumer | `ConsumerOptions` (3rd arg of `startConsumer`) | `retry`, `dlq`, `deduplication`, `circuitBreaker` |
| The whole client | `KafkaClientOptions` (4th constructor arg) | `strictSchemas`, `security`, `instrumentation`, `lagThrottle` |
| One environment (dev/stage/prod) | env vars via `kafkaClientConfigFromEnv` / `consumerOptionsFromEnv` | broker list, TLS, per-env retry/DLQ defaults |
| A cross-cutting concern (tracing/metrics) | `instrumentation` (client) or `interceptors` (consumer) — code only | `otelInstrumentation()` |
| Cloud IAM auth | `security.sasl` with a provider function — code only | `awsMskIamProvider`, `gcpAccessTokenProvider` |

Rule of thumb: **environment variables set the defaults for an environment;
code sets the intent.** When both apply, code wins.
