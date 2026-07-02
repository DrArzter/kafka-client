# NestJS layer

The `src/nest/**` modules wrap `KafkaClient` in a NestJS dynamic module with DI,
declarative `@SubscribeTo` handlers, and a health indicator. Nothing in
`src/client/**` depends on NestJS — this layer sits on top and is only pulled in
via the main `@drarzter/kafka-client` entry point (not `/core`).

Public usage examples: see `README.md`.

---

## `KafkaModule` (`kafka.module.ts`)

A `@Module({})` with two static factory methods, both returning a `DynamicModule`.

### `register(options)`

Synchronous config. Builds a provider under the client's DI token and a
`KafkaExplorer`, imports `DiscoveryModule`, and exports the client provider.

### `registerAsync(options)`

Same, but the client options come from a `useFactory` (with `imports`/`inject`
for DI) — use it to pull config from `ConfigService` etc.

### `buildClient` — the shared factory

Both paths funnel into `buildClient`, which:

1. Constructs `new KafkaClient(clientId, groupId, brokers, {...})`.
2. **Calls `await client.connectProducer()` inside the factory** — so the
   producer (and Lamport-clock recovery, and the lag-throttle poller) is ready
   *before* Nest finishes bootstrapping and the app starts serving.
3. Injects a NestJS `Logger("KafkaClient:<clientId>")` as the client's logger.

### Which options forward to the client

`KafkaModuleOptions` is a curated subset of `KafkaClientOptions`. `buildClient`
forwards exactly these:

`autoCreateTopics`, `strictSchemas`, `numPartitions`, `instrumentation`,
`onMessageLost`, `onRebalance`, `transactionalId`, `clockRecovery`,
`lagThrottle`, `onTtlExpired`, `transport`, `security`.

`security` (TLS + SASL, including MSK IAM / GCP OAUTHBEARER via a provider
function) is forwarded straight through, so a NestJS app authenticates to the
broker the same way a bare `KafkaClient` does — see
[`configuration.md`](./configuration.md#security-configuration).

The `logger` is always overridden with a Nest `Logger` (you can't pass your own
through the module). `clientId`, `groupId`, `brokers` are top-level module
options mapped to the constructor's positional args.

---

## Multi-client & tokens (`kafka.constants.ts`)

- Default token: `KAFKA_CLIENT` (the string).
- Named clients: `getKafkaClientToken(name)` → `KAFKA_CLIENT_<name>`.

Register several clients by passing `name` to each `register()`/`registerAsync()`,
then inject the one you want with `@InjectKafkaClient('analytics')`
(`kafka.decorator.ts`), which is just `Inject(getKafkaClientToken(name))`.

`isGlobal: true` sets `global` on the dynamic module so the client provider is
available app-wide without re-importing `KafkaModule` in every feature module.

---

## `@SubscribeTo` & the explorer

### The decorator (`kafka.decorator.ts`)

`@SubscribeTo(topics, options?)` is a **method decorator** that only stores
metadata — it does not start a consumer itself. It:

- Unwraps `TopicDescriptor`s to their `__topic` strings for the `topics` array,
  and extracts any `__schema` into a per-topic `schemas` map.
- Splits out the non-`ConsumerOptions` fields (`clientName`, `batch`) from the
  rest of the consumer options.
- Appends a `KafkaSubscriberMetadata` entry to `KAFKA_SUBSCRIBER_METADATA` on the
  class constructor (via `Reflect.defineMetadata`), keyed with the method name.
  Multiple `@SubscribeTo` on one class accumulate.

### The explorer (`kafka.explorer.ts`)

`KafkaExplorer` implements `OnModuleInit`. On startup it:

1. Enumerates all providers via `DiscoveryService.getProviders()`.
2. For each provider instance, reads `KAFKA_SUBSCRIBER_METADATA` off its
   constructor.
3. For each metadata entry: resolves the target client from the DI container by
   token (`moduleRef.get(token, { strict: false })`), binds the decorated method,
   and calls `client.startBatchConsumer(...)` if `batch: true`, else
   `client.startConsumer(...)`, forwarding the stored options (+ `schemas`).

### The double-registration guard

Every `KafkaModule.register()` contributes **its own** `KafkaExplorer`, and each
explorer scans **all** providers. In a multi-client app that means every
`@SubscribeTo` handler would be wired once per registered module — producing
duplicate consumers and the "startConsumer called twice" error from
`setupConsumer`.

The guard is a module-level `WeakMap<providerInstance, Set<entryKey>>`
(`wiredSubscriptions`). Before wiring an entry, the explorer computes
`entryKey = "<token>:<methodName>"`; if it's already in the set for that instance
it skips. Keyed by **instance** (not constructor) so separate Nest apps in one
process — e.g. two test apps — wire their own instances independently.

---

## Health indicator (`kafka.health.ts`)

`KafkaHealthIndicator.check(client)` is a thin `@Injectable()` that delegates to
`client.checkStatus()` and returns a `KafkaHealthResult`
(`{ status: 'up'|'down', clientId, topics|error }`). `checkStatus` never throws —
it returns the discriminated union — so this composes cleanly with
`@nestjs/terminus`-style health checks. The indicator is not auto-registered by
`KafkaModule`; provide it yourself where needed.

---

## The esbuild / `design:paramtypes` constraint

`tsup` builds with **esbuild**, which does not emit `emitDecoratorMetadata`
(`design:paramtypes`). NestJS's implicit constructor-type DI relies on that
metadata, so it is unavailable in the published bundle.

Consequence: **every DI point in this layer uses an explicit `@Inject(Token)`
decorator.** `KafkaExplorer` injects `@Inject(DiscoveryService)` and
`@Inject(ModuleRef)` explicitly rather than relying on parameter-type inference;
client injection goes through the string token via `@InjectKafkaClient`. If you
add providers here, never rely on implicit constructor type inference — always
name the token.
