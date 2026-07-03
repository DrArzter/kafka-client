# @drarzter/kafka-client

[![npm version](https://img.shields.io/npm/v/@drarzter/kafka-client)](https://www.npmjs.com/package/@drarzter/kafka-client)
[![CI](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml/badge.svg)](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Type-safe Kafka client for Node.js. Framework-agnostic core with a first-class NestJS adapter. Built on top of [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript) (librdkafka).

> **🕹️ Try it live — [kafka-playground](https://github.com/DrArzter/kafka-playground)**: an interactive sandbox that spins up Kafka + this library in Docker and lets you create producers/consumers from a dashboard and watch retries, DLQ, circuit breaker, delayed delivery, dedup (Redis), transactional outbox (Postgres), and Avro/Protobuf serde work in real time. `git clone && docker compose up`.

## Table of contents

- [What is this?](#what-is-this)
- [Why?](#why)
- [How it compares](#how-it-compares)
- [Installation](#installation)
- [Standalone usage](#standalone-usage-no-nestjs)
- [Quick start (NestJS)](#quick-start-nestjs)
- [Usage](#usage)
  - [1. Define your topic map](#1-define-your-topic-map)
  - [2. Register the module](#2-register-the-module)
  - [3. Inject and use](#3-inject-and-use)
- [Consuming messages](#consuming-messages)
  - [Declarative: @SubscribeTo()](#declarative-subscribeto)
  - [Imperative: startConsumer()](#imperative-startconsumer)
  - [Regex topic subscription](#regex-topic-subscription)
  - [Iterator: consume()](#iterator-consume)
- [Multiple consumer groups](#multiple-consumer-groups)
- [Partition key](#partition-key)
  - [Typed partition keys](#typed-partition-keys)
- [Message headers](#message-headers)
- [Batch sending](#batch-sending)
- [Delayed delivery](#delayed-delivery)
- [Batch consuming](#batch-consuming)
- [Tombstone messages](#tombstone-messages)
- [Compression](#compression)
- [Transactions](#transactions)
- [Consumer interceptors](#consumer-interceptors)
- [Instrumentation](#instrumentation)
  - [OpenTelemetry metrics](#opentelemetry-metrics)
- [Transport security](#transport-security)
  - [AWS MSK IAM & GCP authentication](#aws-msk-iam--gcp-authentication)
  - [ACL requirements](#acl-requirements)
- [Environment configuration](#environment-configuration)
- [Options reference](#options-reference)
- [Error classes](#error-classes)
- [Deduplication (Lamport Clock)](#deduplication-lamport-clock)
  - [Pluggable deduplication store](#pluggable-deduplication-store)
- [Retry topic chain](#retry-topic-chain)
- [stopConsumer](#stopconsumer)
- [Pause and resume](#pause-and-resume)
- [Circuit breaker](#circuit-breaker)
  - [getCircuitState](#getcircuitstate)
- [Reset consumer offsets](#reset-consumer-offsets)
- [Seek to offset](#seek-to-offset)
- [Seek to timestamp](#seek-to-timestamp)
- [Message TTL](#message-ttl)
- [DLQ replay](#dlq-replay)
- [Read snapshot](#read-snapshot)
- [Offset checkpointing](#offset-checkpointing)
  - [checkpointOffsets](#checkpointoffsets)
  - [restoreFromCheckpoint](#restorefromcheckpoint)
- [Windowed batch consumer](#windowed-batch-consumer)
- [Header-based routing](#header-based-routing)
- [Lag-based producer throttling](#lag-based-producer-throttling)
- [Transactional consumer](#transactional-consumer)
- [Transactional outbox](#transactional-outbox)
- [Serialization: JSON, Avro, Protobuf](#serialization-json-avro-protobuf)
- [Schema Registry client](#schema-registry-client)
- [Admin API](#admin-api)
- [DLQ CLI](#dlq-cli)
- [Graceful shutdown](#graceful-shutdown)
- [Consumer handles](#consumer-handles)
- [onMessageLost](#onmessagelost)
- [onTtlExpired](#onttlexpired)
- [onRebalance](#onrebalance)
- [Consumer lag](#consumer-lag)
- [Handler timeout warning](#handler-timeout-warning)
- [Static group membership](#static-group-membership)
- [Schema validation](#schema-validation)
  - [Versioned schemas](#versioned-schemas)
  - [Context-aware validators](#context-aware-validators-schemaparsecontext)
- [Constructor options validation](#constructor-options-validation)
- [Health check](#health-check)
- [Testing](#testing)
- [Project structure](#project-structure)

## What is this?

An opinionated, type-safe abstraction over `@confluentinc/kafka-javascript` (librdkafka). Works standalone (Express, Fastify, raw Node) or as a NestJS DynamicModule. Not a full-featured framework — just a clean, typed layer for producing and consuming Kafka messages.

**This library exists so you don't have to think about:**

- rebalance edge cases
- retry loops and backoff scheduling
- dead letter queue wiring
- transaction coordinator warmup
- graceful shutdown and offset commit pitfalls
- silent message loss

Safe by default. Configurable when you need it. Escape hatches for when you know what you're doing.

## Why?

- **Typed topics** — you define a map of topic -> message shape, and the compiler won't let you send wrong data to wrong topic
- **Topic descriptors** — `topic()` DX sugar lets you define topics as standalone typed objects instead of string keys
- **Framework-agnostic** — use standalone or with NestJS (`register()` / `registerAsync()`, DI, lifecycle hooks)
- **Idempotent producer** — `acks: -1`, `idempotent: true` by default
- **Lamport Clock deduplication** — every outgoing message is stamped with a monotonically increasing `x-lamport-clock` header; the consumer tracks the last processed value per `topic:partition` and silently drops (or routes to DLQ / a dedicated topic) any message whose clock is not strictly greater than the last seen value
- **Retry + DLQ** — exponential backoff with full jitter; dead letter queue with error metadata headers (original topic, error message, stack, attempt count)
- **Batch sending** — send multiple messages in a single request
- **Batch consuming** — `startBatchConsumer()` for high-throughput `eachBatch` processing
- **Partition key support** — route related messages to the same partition
- **Custom headers** — attach metadata headers to messages
- **Transactions** — exactly-once semantics with `producer.transaction()`
- **EventEnvelope** — every consumed message is wrapped in `EventEnvelope<T>` with `eventId`, `correlationId`, `timestamp`, `schemaVersion`, `traceparent`, and Kafka metadata
- **Correlation ID propagation** — auto-generated on send, auto-propagated through `AsyncLocalStorage` so nested sends inherit the same correlation ID
- **OpenTelemetry support** — `@drarzter/kafka-client/otel` entrypoint with `otelInstrumentation()` for W3C Trace Context propagation
- **Consumer interceptors** — before/after/onError hooks with `EventEnvelope` access
- **Client-wide instrumentation** — `KafkaInstrumentation` hooks for cross-cutting concerns (tracing, metrics)
- **Auto-create topics** — `autoCreateTopics: true` for dev mode — no need to pre-create topics
- **Error classes** — `KafkaProcessingError` and `KafkaRetryExhaustedError` with topic, message, and attempt metadata
- **Health check** — built-in health indicator for monitoring
- **Multiple consumer groups** — named clients for different bounded contexts
- **Declarative & imperative** — use `@SubscribeTo()` decorator or `startConsumer()` directly
- **Async iterator** — `consume<K>()` returns an `AsyncIterableIterator<EventEnvelope<T[K]>>` for `for await` consumption; breaking out of the loop stops the consumer automatically
- **Message TTL** — `messageTtlMs` drops or DLQs messages older than a configurable threshold, preventing stale events from poisoning downstream systems after a lag spike
- **Circuit breaker** — `circuitBreaker` option applies a sliding-window breaker per topic-partition; pauses delivery on repeated handler failures and resumes after a configurable recovery window
- **Seek to offset** — `seekToOffset(groupId, assignments)` seeks individual partitions to explicit offsets for fine-grained replay
- **Tombstone messages** — `sendTombstone(topic, key)` sends a null-value record to compact a key out of a log-compacted topic; all instrumentation hooks still fire
- **Regex topic subscription** — `startConsumer([/^orders\..+/], handler)` subscribes using a pattern; the broker routes matching topics to the consumer dynamically
- **Compression** — per-send `compression` option (`gzip`, `snappy`, `lz4`, `zstd`) in `SendOptions` and `BatchSendOptions`
- **Partition assignment strategy** — `partitionAssigner` in `ConsumerOptions` chooses between `cooperative-sticky` (default), `roundrobin`, and `range`
- **Admin API** — `listConsumerGroups()`, `describeTopics()`, `deleteRecords()` for group inspection, partition metadata, and message deletion
- **Typed partition keys** — `topic('orders').type<T>().key(m => m.orderId)` binds a partition-key extractor to a descriptor so related messages land on the same partition without passing `key` at every call site
- **Versioned schemas** — `versionedSchema({ 1: v1, 2: v2 }, { migrate })` dispatches validation on the `x-schema-version` header and upgrades old shapes to the latest
- **Constructor validation** — the `KafkaClient` constructor fails fast, throwing a single aggregated error that lists every invalid config value instead of surfacing a confusing driver error on first use
- **Pluggable deduplication store** — swap the in-memory Lamport-clock store for a `DedupStore` (e.g. Redis-backed) so deduplication survives restarts and rebalances; fail-open on store errors
- **Delayed delivery** — `sendMessage(..., { deliverAfterMs })` stages messages in `<topic>.delayed`; a `startDelayedRelay()` consumer forwards them transactionally once the deadline passes
- **OpenTelemetry metrics** — `otelMetricsInstrumentation()` records send/consume counters and a handler-duration histogram; `otelLagGauge()` reports per-partition consumer lag as an observable gauge
- **Transport security** — `security: { ssl, sasl }` with secure-by-default rules: SASL auto-enables TLS, plaintext to non-local brokers warns once (silenceable via `allowInsecure: true`); SASL mechanisms `plain`, `scram-sha-256`, `scram-sha-512`, `oauthbearer`
- **AWS MSK / GCP auth** — `awsMskIamProvider({ region })` and `gcpAccessTokenProvider()` supply OAUTHBEARER tokens from the standard AWS / Google credential chains (IRSA, task roles, ADC)
- **ACL requirements helper** — `describeRequiredAcls()` enumerates every derived topic, companion group, ephemeral group, and transactional id a service needs; render them as `kafka-acls.sh` commands or an MSK IAM policy
- **Environment configuration** — `kafkaClientConfigFromEnv()`, `consumerOptionsFromEnv()`, and `mergeConsumerOptions()` build config from env vars with `code > env > defaults` precedence
- **Transactional outbox** — `startOutboxRelay()` publishes rows from a DB outbox table to Kafka inside a transaction; at-least-once with stable `eventId` for downstream dedup
- **Pluggable serialization** — JSON by default; drop in `avroSerde()` / `protobufSerde()` (`@drarzter/kafka-client/serde`) for **Confluent wire-format** Avro/Protobuf and interop with Java/Go via a Schema Registry, client-wide or per-topic
- **Schema Registry client** — `SchemaRegistryClient` + `registrySchema()` keep locally-defined schemas in lockstep with a Confluent-compatible registry
- **Static group membership** — `groupInstanceId` (`group.instance.id`) skips rebalance on k8s rolling restarts within `session.timeout.ms`
- **DLQ CLI** — `kafka-client-dlq ls | peek | replay` for inspecting and re-publishing dead letter queues from the terminal

See the [Roadmap](./ROADMAP.md) for upcoming features and version history.

## How it compares

Kafka in Node.js is usually one of: the low-level driver [`kafkajs`](https://github.com/tulios/kafkajs) (and things built on it, like the NestJS Kafka transport), or the native [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript) driver. Those give you a solid **transport** — produce, consume, commit — and leave the reliability patterns (retry topologies, DLQ, circuit breaking, dedup, delayed delivery, serde) for you to build and maintain yourself. This library is that reliability layer, batteries-included, on top of the Confluent/librdkafka driver.

The table is about what ships **out of the box** — not what's theoretically buildable. `kafkajs` is a driver, so most rows below are "do it yourself" there; that's the point.

| Capability | `@drarzter/kafka-client` | `kafkajs` | `@nestjs/microservices` (Kafka) |
|---|:---:|:---:|:---:|
| Compile-time typed topic → payload map | ✅ | ❌ | ❌ |
| Produce / consume / batch / admin | ✅ | ✅ | ✅ |
| Exactly-once transactions | ✅ | ✅ | ⚠️ limited |
| Retry with backoff + jitter | ✅ built-in | 🔨 DIY | 🔨 DIY |
| Durable retry-topic chains (`<topic>.retry.N`) | ✅ | 🔨 DIY | 🔨 DIY |
| Dead-letter queue + metadata headers | ✅ | 🔨 DIY | 🔨 DIY |
| Circuit breaker (per partition) | ✅ | 🔨 DIY | 🔨 DIY |
| Deduplication (Lamport clock, pluggable store) | ✅ | 🔨 DIY | 🔨 DIY |
| Delayed delivery + transactional relay | ✅ | 🔨 DIY | 🔨 DIY |
| Transactional outbox relay | ✅ | 🔨 DIY | 🔨 DIY |
| Avro / Protobuf serde (Confluent wire format) | ✅ `/serde` | 🔨 DIY | ❌ |
| OpenTelemetry traces **and** metrics | ✅ | 🔨 DIY | 🔨 DIY |
| Envelope (eventId / correlationId / trace) + ALS propagation | ✅ | ❌ | ❌ |
| Security helpers (MSK IAM / GCP OAUTHBEARER, ACL generator) | ✅ | ⚠️ manual | ⚠️ manual |
| First-class NestJS integration | ✅ | ❌ | ✅ |

✅ built-in · ⚠️ partial / manual config · 🔨 possible but you build & maintain it · ❌ not available

`@nestjs/microservices` Kafka transport is itself built on `kafkajs` and targets request-response / event **messaging** patterns rather than data-streaming reliability — so it inherits the same "build it yourself" gaps.

**And it's nearly free.** The [throughput benchmark](#running-tests) (`npm run bench`) measures this wrapper against the raw `@confluentinc/kafka-javascript` driver on a real broker: **~2% overhead** with identical p50/p95 latency. The typed envelope, Lamport clock, and instrumentation hooks cost almost nothing on the hot path — you get every row above without trading away throughput.

> The comparison reflects built-in capabilities as of this version and isn't a knock on the driver-level libraries — `kafkajs` is a fine transport; this just saves you from re-implementing the layer above it.

## Installation

```bash
npm install @drarzter/kafka-client
```

`@confluentinc/kafka-javascript` uses a native librdkafka addon. On most systems it builds automatically. For faster installs (skips compilation), install the system library first:

```bash
# Arch / CachyOS
sudo pacman -S librdkafka

# Debian / Ubuntu
sudo apt-get install librdkafka-dev

# macOS
brew install librdkafka
```

Then install with `BUILD_LIBRDKAFKA=0 npm install`.

For NestJS projects, install peer dependencies: `@nestjs/common`, `@nestjs/core`, `reflect-metadata`, `rxjs`.

For standalone usage (Express, Fastify, raw Node), no extra dependencies needed — import from `@drarzter/kafka-client/core`.

## Standalone usage (no NestJS)

```typescript
import { KafkaClient, topic } from '@drarzter/kafka-client/core';

const OrderCreated = topic('order.created').type<{ orderId: string; amount: number }>();

const kafka = new KafkaClient('my-app', 'my-group', ['localhost:9092']);
await kafka.connectProducer();

// Send
await kafka.sendMessage(OrderCreated, { orderId: '123', amount: 100 });

// Consume — handler receives an EventEnvelope
await kafka.startConsumer([OrderCreated], async (envelope) => {
  console.log(`${envelope.topic}:`, envelope.payload.orderId);
});

// Custom logger (winston, pino, etc.)
const kafka2 = new KafkaClient('my-app', 'my-group', ['localhost:9092'], {
  logger: myWinstonLogger,
});

// All module options work in standalone mode too
const kafka3 = new KafkaClient('my-app', 'my-group', ['localhost:9092'], {
  autoCreateTopics: true,   // auto-create topics on first use
  numPartitions: 3,         // partitions for auto-created topics
  strictSchemas: false,     // disable schema enforcement for string topic keys
  instrumentation: [...],   // client-wide tracing/metrics hooks
});

// Health check — available directly, no NestJS needed
const status = await kafka.checkStatus();
// { status: 'up', clientId: 'my-app', topics: ['order.created', ...] }

// Stop all consumers without disconnecting the producer or admin
// Useful when you want to re-subscribe with different options
await kafka.stopConsumer();
```

## Quick start (NestJS)

Send and receive a message in 3 files:

```typescript
// types.ts
export interface MyTopics {
  'hello': { text: string };
}
```

```typescript
// app.module.ts
import { Module } from '@nestjs/common';
import { KafkaModule } from '@drarzter/kafka-client';
import { MyTopics } from './types';
import { AppService } from './app.service';

@Module({
  imports: [
    KafkaModule.register<MyTopics>({
      clientId: 'my-app',
      groupId: 'my-group',
      brokers: ['localhost:9092'],
    }),
  ],
  providers: [AppService],
})
export class AppModule {}
```

```typescript
// app.service.ts
import { Injectable } from '@nestjs/common';
import { InjectKafkaClient, KafkaClient, SubscribeTo, EventEnvelope } from '@drarzter/kafka-client';
import { MyTopics } from './types';

@Injectable()
export class AppService {
  constructor(
    @InjectKafkaClient() private readonly kafka: KafkaClient<MyTopics>,
  ) {}

  async send() {
    await this.kafka.sendMessage('hello', { text: 'Hello, Kafka!' });
  }

  @SubscribeTo('hello')
  async onHello(envelope: EventEnvelope<MyTopics['hello']>) {
    console.log('Received:', envelope.payload.text);
  }
}
```

## Usage

### 1. Define your topic map

Both `interface` and `type` work — pick whichever you prefer:

```typescript
// Explicit: extends TTopicMessageMap — IDE hints that values must be Record<string, any>
import { TTopicMessageMap } from '@drarzter/kafka-client';

export interface OrdersTopicMap extends TTopicMessageMap {
  'order.created': {
    orderId: string;
    userId: string;
    amount: number;
  };
  'order.completed': {
    orderId: string;
    completedAt: string;
  };
}
```

```typescript
// Minimal: plain interface or type — works just the same
export interface OrdersTopicMap {
  'order.created': { orderId: string; userId: string; amount: number };
  'order.completed': { orderId: string; completedAt: string };
}

// or
export type OrdersTopicMap = {
  'order.created': { orderId: string; userId: string; amount: number };
  'order.completed': { orderId: string; completedAt: string };
};
```

#### Alternative: `topic()` descriptors

Instead of a centralized topic map, define each topic as a standalone typed object:

```typescript
import { topic, TopicsFrom } from '@drarzter/kafka-client';

export const OrderCreated = topic('order.created').type<{
  orderId: string;
  userId: string;
  amount: number;
}>();

export const OrderCompleted = topic('order.completed').type<{
  orderId: string;
  completedAt: string;
}>();

// Combine into a topic map for KafkaModule generics
export type OrdersTopicMap = TopicsFrom<typeof OrderCreated | typeof OrderCompleted>;
```

Topic descriptors work everywhere strings work — `sendMessage`, `sendBatch`, `transaction`, `startConsumer`, and `@SubscribeTo()`:

```typescript
// Sending
await kafka.sendMessage(OrderCreated, { orderId: '123', userId: '456', amount: 100 });
await kafka.sendBatch(OrderCreated, [{ value: { orderId: '1', userId: '10', amount: 50 } }]);

// Transactions
await kafka.transaction(async (tx) => {
  await tx.send(OrderCreated, { orderId: '123', userId: '456', amount: 100 });
});

// Consuming (decorator)
@SubscribeTo(OrderCreated)
async handleOrder(envelope: EventEnvelope<OrdersTopicMap['order.created']>) { ... }

// Consuming (imperative)
await kafka.startConsumer([OrderCreated], handler);
```

### 2. Register the module

```typescript
import { KafkaModule } from '@drarzter/kafka-client';
import { OrdersTopicMap } from './orders.types';

@Module({
  imports: [
    KafkaModule.register<OrdersTopicMap>({
      clientId: 'my-service',
      groupId: 'my-consumer-group',
      brokers: ['localhost:9092'],
      autoCreateTopics: true, // auto-create topics on first use (dev mode)
    }),
  ],
})
export class OrdersModule {}
```

`autoCreateTopics` calls `admin.createTopics()` (idempotent — no-op if topic already exists) before the first send **and** before each `startConsumer` / `startBatchConsumer` call. librdkafka errors on unknown topics at subscribe time, so consumer-side creation is required. Useful in development, not recommended for production.

Or with `ConfigService`:

```typescript
KafkaModule.registerAsync<OrdersTopicMap>({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    clientId: 'my-service',
    groupId: 'my-consumer-group',
    brokers: config.get<string>('KAFKA_BROKERS').split(','),
  }),
})
```

#### Global module

By default, `KafkaModule` is scoped — you need to import it in every module that uses `@InjectKafkaClient()`. Pass `isGlobal: true` to make the client available everywhere:

```typescript
// app.module.ts — register once
KafkaModule.register<OrdersTopicMap>({
  clientId: 'my-service',
  groupId: 'my-consumer-group',
  brokers: ['localhost:9092'],
  isGlobal: true,
})

// any other module — no need to import KafkaModule
@Injectable()
export class SomeService {
  constructor(@InjectKafkaClient() private readonly kafka: KafkaClient<OrdersTopicMap>) {}
}
```

Works with `registerAsync()` too:

```typescript
KafkaModule.registerAsync<OrdersTopicMap>({
  isGlobal: true,
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({ ... }),
})
```

### 3. Inject and use

```typescript
import { Injectable } from '@nestjs/common';
import { InjectKafkaClient, KafkaClient } from '@drarzter/kafka-client';
import { OrdersTopicMap } from './orders.types';

@Injectable()
export class OrdersService {
  constructor(
    @InjectKafkaClient()
    private readonly kafka: KafkaClient<OrdersTopicMap>,
  ) {}

  async createOrder() {
    await this.kafka.sendMessage('order.created', {
      orderId: '123',
      userId: '456',
      amount: 100,
    });
  }
}
```

## Consuming messages

Three ways — choose what fits your style.

### Declarative: @SubscribeTo()

```typescript
import { Injectable } from '@nestjs/common';
import { SubscribeTo } from '@drarzter/kafka-client';

@Injectable()
export class OrdersHandler {
  @SubscribeTo('order.created')
  async handleOrderCreated(envelope: EventEnvelope<OrdersTopicMap['order.created']>) {
    console.log('New order:', envelope.payload.orderId);
  }

  @SubscribeTo('order.completed', { retry: { maxRetries: 3 }, dlq: true })
  async handleOrderCompleted(envelope: EventEnvelope<OrdersTopicMap['order.completed']>) {
    console.log('Order completed:', envelope.payload.orderId);
  }
}
```

The module auto-discovers `@SubscribeTo()` methods on startup and subscribes them.

### Imperative: startConsumer()

```typescript
@Injectable()
export class OrdersService implements OnModuleInit {
  constructor(
    @InjectKafkaClient()
    private readonly kafka: KafkaClient<OrdersTopicMap>,
  ) {}

  async onModuleInit() {
    await this.kafka.startConsumer(
      ['order.created', 'order.completed'],
      async (envelope) => {
        console.log(`${envelope.topic}:`, envelope.payload);
      },
      {
        retry: { maxRetries: 3, backoffMs: 1000 },
        dlq: true,
      },
    );
  }
}
```

### Regex topic subscription

Subscribe to multiple topics matching a pattern — the broker dynamically routes any topic whose name matches the regex to this consumer:

```typescript
// Subscribe to all topics starting with "orders."
await kafka.startConsumer([/^orders\..+/], handler);

// Mix regexes and literal strings
await kafka.startConsumer([/^payments\..+/, 'audit.global'], handler);
```

Works with `startBatchConsumer` and `@SubscribeTo` too:

```typescript
@SubscribeTo(/^events\..+/)
async handleEvent(envelope: EventEnvelope<any>) { ... }
```

> **Limitation:** `retryTopics: true` is incompatible with regex subscriptions — the library cannot derive static retry topic names from a pattern. An error is thrown at startup if both are combined.

### Iterator: consume()

Stream messages from a single topic as an `AsyncIterableIterator` — useful for scripts, one-off tasks, or any context where you prefer `for await` over a callback:

```typescript
for await (const envelope of kafka.consume('order.created')) {
  console.log('Order:', envelope.payload.orderId);
}

// Breaking out of the loop stops the consumer automatically
for await (const envelope of kafka.consume('order.created')) {
  if (envelope.payload.orderId === targetId) break;
}
```

`consume()` accepts the same `ConsumerOptions` as `startConsumer()`:

```typescript
for await (const envelope of kafka.consume('orders', {
  retry: { maxRetries: 3 },
  dlq: true,
  messageTtlMs: 60_000,
})) {
  await processOrder(envelope.payload);
}
```

`break`, `return`, or any early exit from the loop calls the iterator's `return()` method, which closes the internal queue and calls `handle.stop()` on the background consumer.

**Backpressure** — use `queueHighWaterMark` to prevent unbounded queue growth when processing is slower than the message rate:

```typescript
for await (const envelope of kafka.consume('orders', {
  queueHighWaterMark: 100, // pause partition when queue reaches 100 messages
})) {
  await slowProcessing(envelope.payload); // resumes when queue drains below 50
}
```

The partition is paused when the internal queue reaches `queueHighWaterMark` and automatically resumed when it drains below 50%. Without this option the queue is unbounded.

**Error propagation** — if the consumer fails to start (e.g. broker unreachable), the error surfaces on the next `next()` / `for await` iteration rather than being silently swallowed.

## Multiple consumer groups

### Per-consumer groupId

Override the default consumer group for specific consumers. Each unique `groupId` creates a separate librdkafka Consumer internally:

```typescript
// Default group from constructor
await kafka.startConsumer(['orders'], handler);

// Custom group — receives its own copy of messages
await kafka.startConsumer(['orders'], auditHandler, { groupId: 'orders-audit' });

// Works with @SubscribeTo too
@SubscribeTo('orders', { groupId: 'orders-audit' })
async auditOrders(envelope) { ... }
```

**Important:** You cannot mix `eachMessage` and `eachBatch` consumers on the same `groupId`, and you cannot call `startConsumer` (or `startBatchConsumer`) twice on the same `groupId` without stopping it first. The library throws a clear error in both cases:

```text
Cannot use eachBatch on consumer group "my-group" — it is already running with eachMessage.
Use a different groupId for this consumer.

startConsumer("my-group") called twice — this group is already consuming.
Call stopConsumer("my-group") first or pass a different groupId.
```

### Named clients

Register multiple named clients for different bounded contexts:

```typescript
@Module({
  imports: [
    KafkaModule.register<OrdersTopicMap>({
      name: 'orders',
      clientId: 'orders-service',
      groupId: 'orders-consumer',
      brokers: ['localhost:9092'],
    }),
    KafkaModule.register<PaymentsTopicMap>({
      name: 'payments',
      clientId: 'payments-service',
      groupId: 'payments-consumer',
      brokers: ['localhost:9092'],
    }),
  ],
})
export class AppModule {}
```

Inject by name — the string in `@InjectKafkaClient()` must match the `name` from `register()`:

```typescript
@Injectable()
export class OrdersService {
  constructor(
    @InjectKafkaClient('orders')    // ← matches name: 'orders' above
    private readonly kafka: KafkaClient<OrdersTopicMap>,
  ) {}
}
```

Same with `@SubscribeTo()` — use `clientName` to target a specific named client:

```typescript
@SubscribeTo('payment.received', { clientName: 'payments' })  // ← matches name: 'payments'
async handlePayment(envelope: EventEnvelope<PaymentsTopicMap['payment.received']>) {
  // ...
}
```

## Partition key

Route all events for the same order to the same partition:

```typescript
await this.kafka.sendMessage(
  'order.created',
  { orderId: '123', userId: '456', amount: 100 },
  { key: '123' },
);
```

### Typed partition keys

Instead of passing `key` at every call site, bind a partition-key extractor to the topic descriptor with `.key()`. The extractor runs on every send through that descriptor, so messages with the same logical key always land on the same partition — you never forget to set it. Available on both `.type<T>()` and `.schema()` descriptors:

```typescript
import { topic } from '@drarzter/kafka-client';

const OrderCreated = topic('order.created')
  .type<{ orderId: string; userId: string; amount: number }>()
  .key((m) => m.orderId);

// Key is derived automatically from the payload — no `key` needed
await kafka.sendMessage(OrderCreated, { orderId: '123', userId: '456', amount: 100 });
// → produced with key '123'

// Works with schema descriptors too
const PaymentTaken = topic('payment.taken')
  .schema(z.object({ paymentId: z.string(), orderId: z.string() }))
  .key((m) => m.orderId);
```

The extractor runs on the **original (pre-validation) payload**. An explicit `key` in `SendOptions` — or a batch item's `key` — always wins over the descriptor's extractor:

```typescript
// Explicit key overrides the extractor
await kafka.sendMessage(OrderCreated, { orderId: '123', userId: '456', amount: 100 }, {
  key: 'custom-partition-key',
});
```

## Message headers

Attach metadata to messages:

```typescript
await this.kafka.sendMessage(
  'order.created',
  { orderId: '123', userId: '456', amount: 100 },
  {
    key: '123',
    headers: { 'x-correlation-id': 'abc-def', 'x-source': 'api-gateway' },
  },
);
```

Headers work with batch sending too:

```typescript
await this.kafka.sendBatch('order.created', [
  {
    value: { orderId: '1', userId: '10', amount: 50 },
    key: '1',
    headers: { 'x-correlation-id': 'req-1' },
  },
]);
```

## Batch sending

```typescript
await this.kafka.sendBatch('order.created', [
  { value: { orderId: '1', userId: '10', amount: 50 }, key: '1' },
  { value: { orderId: '2', userId: '20', amount: 75 }, key: '2' },
  { value: { orderId: '3', userId: '30', amount: 100 }, key: '3' },
]);
```

## Delayed delivery

Schedule a message for future delivery with `deliverAfterMs`. Instead of going straight to the target topic, the message is produced to a `<topic>.delayed` staging topic carrying `x-delayed-until` (deadline) and `x-delayed-target` headers. A **relay consumer** started via `startDelayedRelay()` holds each message until its deadline passes, then forwards it to the target topic:

```typescript
// 1. Start the relay once (per process) for the topics you delay-deliver to
await kafka.startDelayedRelay(['order.reminder']);

// 2. Send a message that should arrive in ~1 hour
await kafka.sendMessage(
  'order.reminder',
  { orderId: '123', channel: 'email' },
  { deliverAfterMs: 60 * 60 * 1000 },
);
// → staged in order.reminder.delayed, forwarded to order.reminder ~1 h later
```

`deliverAfterMs` also works on `sendBatch` — it applies to the whole batch:

```typescript
await kafka.sendBatch('order.reminder', messages, { deliverAfterMs: 30_000 });
```

The relay defaults to a `<defaultGroupId>-delayed-relay` consumer group; override it with `startDelayedRelay(topics, { groupId })`. Forwarding is **transactional** — the produce to the target topic and the source-offset commit happen atomically, so no duplicates are relayed even if the relay crashes mid-forward. The original key, value, and envelope headers (`x-event-id`, `x-correlation-id`, `x-lamport-clock`, `traceparent`) all survive the hop; only the `x-delayed-*` control headers are stripped.

> **Delivery time is a lower bound.** The relay pauses a partition until the head-of-line message's deadline, so later messages on the same partition wait behind it (at-least semantics). Delayed messages are only delivered while the relay is running — treat it as a long-lived consumer, not a fire-and-forget scheduler.

## Batch consuming

Process messages in batches for higher throughput. The handler receives an array of `EventEnvelope`s and a `BatchMeta` object with offset management controls:

```typescript
await this.kafka.startBatchConsumer(
  ['order.created'],
  async (envelopes, meta) => {
    // envelopes: EventEnvelope<OrdersTopicMap['order.created']>[]
    for (const env of envelopes) {
      await processOrder(env.payload);
      meta.resolveOffset(env.offset);

      // Call heartbeat() during long-running batch processing to prevent
      // the broker from considering the consumer dead (session.timeout.ms)
      await meta.heartbeat();
    }
    await meta.commitOffsetsIfNecessary();
  },
  { retry: { maxRetries: 3 }, dlq: true },
);
```

With `autoCommit: false` for full manual offset control:

```typescript
await this.kafka.startBatchConsumer(
  ['order.created'],
  async (envelopes, meta) => {
    for (const env of envelopes) {
      await processOrder(env.payload);
      meta.resolveOffset(env.offset);
    }
    // commitOffsetsIfNecessary() commits only when autoCommit is off
    // or when the commit interval has elapsed
    await meta.commitOffsetsIfNecessary();
  },
  { autoCommit: false },
);
```

> **Note:** If your handler calls `resolveOffset()` or `commitOffsetsIfNecessary()` without setting `autoCommit: false`, a `debug` message is logged at consumer-start time — mixing autoCommit with manual offset control causes offset conflicts. Set `autoCommit: false` to suppress the message and take full control of offset management.

With `@SubscribeTo()`:

```typescript
@SubscribeTo('order.created', { batch: true })
async handleOrders(envelopes: EventEnvelope<OrdersTopicMap['order.created']>[], meta: BatchMeta) {
  for (const env of envelopes) { ... }
}
```

Schema validation runs per-message — invalid messages are skipped (DLQ'd if enabled), valid ones are passed to the handler. Retry applies to the whole batch.

`retryTopics: true` is also supported on `startBatchConsumer`. On handler failure, each envelope in the batch is routed individually to `<topic>.retry.1`; the companion retry consumers call the batch handler one message at a time with a stub `BatchMeta` (no-op `heartbeat`/`resolveOffset`/`commitOffsetsIfNecessary`):

```typescript
await kafka.startBatchConsumer(
  ['orders.created'],
  async (envelopes, meta) => { /* same handler */ },
  {
    retry: { maxRetries: 3, backoffMs: 1000 },
    dlq: true,
    retryTopics: true, // ← now supported for batch consumers too
  },
);
```

`BatchMeta` exposes:

| Property/Method | Description |
| --------------- | ----------- |
| `partition` | Partition number for this batch |
| `highWatermark` | Latest offset in the partition (`string`). `null` when the message is replayed via a retry topic consumer — in that path the broker high-watermark is not available. Guard against `null` before computing lag |
| `heartbeat()` | Send a heartbeat to keep the consumer session alive — call during long processing loops |
| `resolveOffset(offset)` | Mark offset as processed (required before `commitOffsetsIfNecessary`) |
| `commitOffsetsIfNecessary()` | Commit resolved offsets; respects `autoCommit` setting |

## Tombstone messages

Send a null-value Kafka record to compact a specific key out of a log-compacted topic:

```typescript
// Delete the record with key "user-123" from the log-compacted "users" topic
await kafka.sendTombstone('users', 'user-123');

// With custom headers
await kafka.sendTombstone('users', 'user-123', { 'x-reason': 'gdpr-deletion' });
```

`sendTombstone` skips envelope headers, schema validation, and the Lamport clock — the record value is literally `null`, as required by Kafka's log compaction protocol. Both `beforeSend` and `afterSend` instrumentation hooks still fire so tracing works correctly.

## Compression

Reduce network bandwidth with per-send compression. Supported codecs: `'gzip'`, `'snappy'`, `'lz4'`, `'zstd'`:

```typescript
import { CompressionType } from '@drarzter/kafka-client/core';

// Single message
await kafka.sendMessage('events', payload, { compression: 'gzip' });

// Batch
await kafka.sendBatch('events', messages, { compression: 'snappy' });
```

Compression is applied at the Kafka message-set level — the broker decompresses transparently on the consumer side. `'snappy'` and `'lz4'` offer the best throughput/CPU trade-off for most workloads; `'gzip'` gives the highest compression ratio; `'zstd'` balances both.

## Transactions

Send multiple messages atomically with exactly-once semantics:

```typescript
await this.kafka.transaction(async (tx) => {
  await tx.send('order.created', {
    orderId: '123',
    userId: '456',
    amount: 100,
  });
  await tx.send('order.completed', {
    orderId: '123',
    completedAt: new Date().toISOString(),
  });
  // if anything throws, all messages are rolled back
});
```

`tx.sendBatch()` is also available inside transactions:

```typescript
await this.kafka.transaction(async (tx) => {
  await tx.sendBatch('order.created', [
    { value: { orderId: '1', userId: '10', amount: 50 }, key: '1' },
    { value: { orderId: '2', userId: '20', amount: 75 }, key: '2' },
  ]);
  // if anything throws, all messages are rolled back
});
```

## Consumer interceptors

Add before/after/onError hooks to message processing. Interceptors receive the full `EventEnvelope`:

```typescript
import { ConsumerInterceptor } from '@drarzter/kafka-client';

const loggingInterceptor: ConsumerInterceptor<OrdersTopicMap> = {
  before: (envelope) => {
    console.log(`Processing ${envelope.topic}`, envelope.payload);
  },
  after: (envelope) => {
    console.log(`Done ${envelope.topic}`);
  },
  onError: (envelope, error) => {
    console.error(`Failed ${envelope.topic}:`, error.message);
  },
};

await this.kafka.startConsumer(['order.created'], handler, {
  interceptors: [loggingInterceptor],
});
```

Multiple interceptors run in order. All hooks are optional.

## Instrumentation

For client-wide cross-cutting concerns (tracing, metrics), use `KafkaInstrumentation` hooks instead of per-consumer interceptors:

```typescript
import { otelInstrumentation } from '@drarzter/kafka-client/otel';

const kafka = new KafkaClient('my-app', 'my-group', brokers, {
  instrumentation: [otelInstrumentation()],
});
```

`otelInstrumentation()` injects `traceparent` on send, extracts it on consume, and creates `CONSUMER` spans automatically. The span is set as the **active OTel context** for the handler's duration via `context.with()` — so `trace.getActiveSpan()` works inside your handler and any child spans are automatically parented to the consume span. Requires `@opentelemetry/api` as a peer dependency.

### OpenTelemetry metrics

`otelInstrumentation()` handles **traces**. For **metrics**, the same entrypoint exports `otelMetricsInstrumentation()` (counters + a duration histogram) and `otelLagGauge()` (an observable consumer-lag gauge). They share nothing with the tracing instrumentation and compose with it in any order:

```typescript
import {
  otelInstrumentation,
  otelMetricsInstrumentation,
  otelLagGauge,
} from '@drarzter/kafka-client/otel';

const kafka = new KafkaClient('my-app', 'my-group', brokers, {
  instrumentation: [otelInstrumentation(), otelMetricsInstrumentation()],
});
```

`otelMetricsInstrumentation()` registers seven instruments under the meter `@drarzter/kafka-client` (created once per instance, not per message):

| Instrument | Type | Attributes | Recorded when |
| ---------- | ---- | ---------- | ------------- |
| `kafka.client.messages.sent` | Counter | `topic` | a message is sent |
| `kafka.client.messages.processed` | Counter | `topic` | a handler succeeds |
| `kafka.client.messages.retried` | Counter | `topic` | a message is queued for retry |
| `kafka.client.messages.dlq` | Counter | `topic`, `reason` | a message is routed to a DLQ |
| `kafka.client.messages.duplicate` | Counter | `topic`, `strategy` | a Lamport-clock duplicate is detected |
| `kafka.client.consume.errors` | Counter | `topic` | a handler throws |
| `kafka.client.consume.duration` | Histogram (ms) | `topic` | measured across the handler's execution |

Pass a custom meter with `otelMetricsInstrumentation({ meter })` to route instruments through your own `MeterProvider`; it defaults to `metrics.getMeter('@drarzter/kafka-client')`.

`otelLagGauge()` registers an observable gauge `kafka.client.consumer.lag` (attributes `topic`, `partition`, `groupId`) that polls `getConsumerLag()` on each metric-collection cycle. It returns an **unregister disposer** — call it on shutdown to stop observing:

```typescript
const unregisterLag = otelLagGauge(kafka, { groupId: 'billing-service' });

// ...later, on shutdown:
unregisterLag();
```

`groupId` defaults to the client's constructor group (reported as an empty-string attribute), and `meter` overrides the meter as above. Lag-query failures during a collection cycle are swallowed silently — a broker hiccup reports no samples for that cycle rather than breaking metric collection. Both helpers require `@opentelemetry/api` as a peer dependency.

### Custom instrumentation

`beforeConsume` can return a `BeforeConsumeResult` — either the legacy `() => void` cleanup function, or an object with `cleanup` and/or `wrap`:

```typescript
import { KafkaInstrumentation, BeforeConsumeResult } from '@drarzter/kafka-client';

const myInstrumentation: KafkaInstrumentation = {
  beforeSend(topic, headers) { /* inject headers, start timer */ },
  afterSend(topic) { /* record send latency */ },

  beforeConsume(envelope): BeforeConsumeResult {
    const span = startMySpan(envelope.topic);
    return {
      // cleanup() is called after the handler completes (success or error)
      cleanup() { span.end(); },
      // wrap(fn) runs the handler inside the desired async context
      // call fn() wherever you need it in the context scope
      wrap(fn) { return runWithSpanActive(span, fn); },
    };
  },

  onConsumeError(envelope, error) { /* record error metric */ },
};
```

The legacy `() => void` form is still fully supported — return a function directly if you only need cleanup:

```typescript
beforeConsume(envelope) {
  const timer = startTimer();
  return () => timer.end(); // cleanup only, no context wrapping
},
```

`BeforeConsumeResult` is a union:

```typescript
type BeforeConsumeResult =
  | (() => void)                     // legacy: cleanup only
  | { cleanup?(): void;              // called after handler (success or error)
      wrap?(fn: () => Promise<void>): Promise<void>; // wraps handler execution
    };
```

When multiple instrumentations each provide a `wrap`, they compose in declaration order — the first instrumentation's `wrap` is the outermost.

### Lifecycle event hooks

Three additional hooks fire for specific events in the consume pipeline:

| Hook | When called | Arguments |
| ---- | ----------- | --------- |
| `onMessage` | Handler successfully processed a message | `(envelope)` — use as a success counter for error-rate calculations |
| `onRetry` | A message is queued for another attempt (in-process backoff or routed to a retry topic) | `(envelope, attempt, maxRetries)` |
| `onDlq` | A message is routed to the dead letter queue | `(envelope, reason)` — reason is `'handler-error'`, `'validation-error'`, or `'lamport-clock-duplicate'` |
| `onDuplicate` | A duplicate is detected via Lamport Clock | `(envelope, strategy)` — strategy is `'drop'`, `'dlq'`, or `'topic'` |

```typescript
const myInstrumentation: KafkaInstrumentation = {
  onMessage(envelope) {
    metrics.increment('kafka.processed', { topic: envelope.topic });
  },
  onRetry(envelope, attempt, maxRetries) {
    console.warn(`Retrying ${envelope.topic} — attempt ${attempt}/${maxRetries}`);
  },
  onDlq(envelope, reason) {
    alertingSystem.send({ topic: envelope.topic, reason });
  },
  onDuplicate(envelope, strategy) {
    metrics.increment('kafka.duplicate', { topic: envelope.topic, strategy });
  },
};
```

### Built-in metrics

`KafkaClient` maintains lightweight in-process event counters independently of any instrumentation:

```typescript
// Global snapshot — aggregate across all topics
const snapshot = kafka.getMetrics();
// { processedCount: number; retryCount: number; dlqCount: number; dedupCount: number }

// Per-topic snapshot
const orderMetrics = kafka.getMetrics('order.created');
// { processedCount: 5, retryCount: 1, dlqCount: 0, dedupCount: 0 }

kafka.resetMetrics();                // reset all counters
kafka.resetMetrics('order.created'); // reset only one topic's counters
```

Passing a topic name that has not seen any events returns a zero-valued snapshot — it never throws.

Counters are incremented in the same code paths that fire the corresponding hooks — they are always active regardless of whether any instrumentation is configured.

## Transport security

Configure TLS and SASL through the `security` option on `KafkaClientOptions`. The library applies **secure-by-default** rules so credentials never leak onto plaintext connections by accident:

- **SASL auto-enables TLS.** When `sasl` is set and `ssl` is left unset, `ssl` is turned on automatically — SASL credentials always travel over TLS unless you explicitly opt out.
- **Explicit `ssl: false` with SASL warns.** Setting `sasl` together with `ssl: false` logs a warning that credentials will cross the wire in plaintext — only safe on fully trusted networks.
- **Plaintext to non-local brokers warns once.** With no `ssl`/`sasl` at all and at least one non-local broker (anything outside `localhost`, `127.0.0.0/8`, `::1`, `0.0.0.0`, `host.docker.internal`), a single warning is logged per client. Acknowledge and silence it with `allowInsecure: true`.

Nothing here ever throws or blocks a connection — the defaults protect, you stay in control.

```typescript
import { KafkaClient } from '@drarzter/kafka-client/core';

// SASL/SCRAM over TLS — ssl auto-enabled because sasl is set
const kafka = new KafkaClient('billing-svc', 'billing-group', ['broker.example.com:9093'], {
  security: {
    sasl: {
      mechanism: 'scram-sha-512',
      username: 'billing-svc',
      password: process.env.KAFKA_PASSWORD!,
    },
    // ssl: true — inferred automatically; set explicitly if you prefer
  },
});
```

`KafkaSecurityOptions`:

| Field | Default | Description |
| ----- | ------- | ----------- |
| `ssl` | `true` when `sasl` set, else `false` | Enable TLS |
| `sasl` | — | SASL authentication (see below) |
| `allowInsecure` | `false` | Acknowledge an intentionally insecure (plaintext, non-local) setup and silence the warning. No effect when `ssl`/`sasl` are set |

`sasl` is a discriminated union on `mechanism`:

```typescript
// Username / password mechanisms
{ mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512', username: string, password: string }

// Token-based (AWS MSK IAM, GCP, custom)
{ mechanism: 'oauthbearer', oauthBearerProvider: () => Promise<OAuthBearerToken> }
```

An `OAuthBearerProvider` is an async factory the driver calls on connect and before each token expiry; it returns `{ value, principal?, lifetimeMs?, extensions? }`.

### AWS MSK IAM & GCP authentication

Two ready-made `oauthbearer` providers cover the common managed-Kafka cases. Both resolve credentials from the platform's standard chain — nothing to hard-code — and rely on an **optional** peer dependency you install alongside this library.

**AWS MSK IAM** — `awsMskIamProvider({ region })` delegates token signing to `aws-msk-iam-sasl-signer-js`. Credentials come from the standard AWS provider chain, so EKS IRSA, ECS task roles, and env credentials all work unchanged. Authorisation is then governed by IAM policies (`kafka-cluster:*` actions) — see [ACL requirements](#acl-requirements) to generate one:

```bash
npm install aws-msk-iam-sasl-signer-js
```

```typescript
import { KafkaClient, awsMskIamProvider } from '@drarzter/kafka-client/core';

const kafka = new KafkaClient('orders-svc', 'orders-group', brokers, {
  security: {
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: awsMskIamProvider({ region: 'eu-west-1' }),
    },
  },
});
```

**GCP** — `gcpAccessTokenProvider()` delegates to `google-auth-library` using Application Default Credentials, so GKE Workload Identity, attached service accounts, and `GOOGLE_APPLICATION_CREDENTIALS` all work unchanged. It supplies a raw ADC access token; verify the exact token format your cluster expects against current Google documentation:

```bash
npm install google-auth-library
```

```typescript
import { KafkaClient, gcpAccessTokenProvider } from '@drarzter/kafka-client/core';

const kafka = new KafkaClient('events-svc', 'events-group', brokers, {
  security: {
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: gcpAccessTokenProvider(),
    },
  },
});
```

| Provider | Options | Optional peer dep |
| -------- | ------- | ----------------- |
| `awsMskIamProvider` | `{ region }` | `aws-msk-iam-sasl-signer-js` |
| `gcpAccessTokenProvider` | `{ scopes?, principal?, tokenTtlMs? }` (defaults: `cloud-platform` scope, principal `'gcp'`, 50 min TTL) | `google-auth-library` |

Neither package is a hard dependency — they are dynamically imported on first token fetch. If the package is missing, the provider throws a clear install hint rather than failing at build time.

### ACL requirements

The features that make this library convenient — retry topics, DLQ, delayed delivery, deduplication routing, DLQ replay, snapshots, clock recovery — quietly create **extra topics and consumer groups** (`<topic>.retry.N`, `<topic>.dlq`, `<topic>.delayed`, `<topic>.duplicates`, `<groupId>-retry.N`, timestamped ephemeral groups, transactional ids). On a locked-down cluster every one of them needs an ACL, and the last place you want to discover a missing grant is production at 3 a.m.

`describeRequiredAcls()` enumerates the complete set from a declarative usage profile. Feed the result to `toKafkaAclCommands()` for `kafka-acls.sh` commands, or `toMskIamPolicy()` for an AWS MSK IAM policy document:

```typescript
import {
  describeRequiredAcls,
  toKafkaAclCommands,
  toMskIamPolicy,
} from '@drarzter/kafka-client/core';

const resources = describeRequiredAcls({
  clientId: 'billing-svc',
  groupIds: ['billing-svc-group'],
  produceTopics: ['invoices.created'],
  consumeTopics: ['orders.created'],
  features: {
    retryTopics: { maxRetries: 3 },
    dlq: true,
    dlqReplay: true,
    transactions: true,
  },
});

// Render kafka-acls.sh commands for a principal
for (const cmd of toKafkaAclCommands(resources, 'User:billing-svc', 'broker:9092')) {
  console.log(cmd);
}
// kafka-acls.sh --bootstrap-server broker:9092 --add --allow-principal 'User:billing-svc' \
//   --operation READ --operation DESCRIBE --topic 'orders.created'  # startConsumer
// kafka-acls.sh ... --topic 'orders.created.dlq'   # dlq: true — failed messages routed to DLQ
// kafka-acls.sh ... --topic 'orders.created.retry.1' ... --topic 'orders.created.retry.3'
// kafka-acls.sh ... --group 'billing-svc-group-retry.' --resource-pattern-type prefixed
// kafka-acls.sh ... --transactional-id 'billing-svc-group-' --resource-pattern-type prefixed
// kafka-acls.sh ... --group 'orders.created.dlq-replay' --operation DELETE --resource-pattern-type prefixed
// ...

// Or an MSK IAM policy document
const policy = toMskIamPolicy(resources, {
  region: 'eu-west-1',
  accountId: '123456789012',
  clusterName: 'prod',
  clusterUuid: 'abcd-1234',
});
```

`describeRequiredAcls()` returns `AclResource[]`, each carrying `resourceType` (`topic` | `group` | `transactional-id` | `cluster`), `patternType` (`literal` | `prefixed`), `name`, `operations`, and a `reason` naming the feature that requires it. Ephemeral-group features (`dlqReplay`, `snapshots`, `clockRecovery`) request `DELETE` on a **prefixed** pattern, because those groups are timestamped and cleaned up after use.

| Feature flag | Adds |
| ------------ | ---- |
| `dlq` | `<topic>.dlq` WRITE per consumed topic |
| `retryTopics: { maxRetries }` | `<topic>.retry.1…N` topics; `<groupId>-retry.` prefixed groups; `<groupId>-` prefixed transactional ids |
| `delayedDelivery` | `<topic>.delayed` topics; `<groupId>-delayed-relay` group + `-tx` id |
| `duplicatesTopic` | `<topic>.duplicates` (or a custom topic name) WRITE |
| `dlqReplay` | `<topic>.dlq-replay` prefixed groups (READ, DESCRIBE, **DELETE**) + DLQ READ |
| `snapshots` | `<clientId>-snapshot-` prefixed groups (READ, DESCRIBE, **DELETE**) |
| `clockRecovery` | `<clientId>-clock-recovery-` prefixed groups (READ, DESCRIBE, **DELETE**) |
| `transactions` | `<clientId>-tx` transactional id |
| `autoCreateTopics` | cluster `CREATE` (avoid in production) |

`toMskIamPolicy()` maps Kafka operations to `kafka-cluster:*` actions, turns prefixed patterns into `name*` ARN wildcards, and always includes `kafka-cluster:Connect`. **Review both outputs against your organisation's least-privilege standards and current AWS documentation before applying** — they are a starting point, not a rubber stamp.

## Environment configuration

Build client and consumer configuration from environment variables with a strict precedence rule: **explicit code options > env vars > built-in library defaults**. The helpers only *feed* values in — anything you hard-code always wins, and any variable left unset keeps the library default.

The library never reads a `.env` file itself. Load one first with Node's built-in `node --env-file=.env` (Node 20.6+) or the `dotenv` package, then call the helpers:

```typescript
import { KafkaClient, kafkaClientConfigFromEnv } from '@drarzter/kafka-client/core';

const { clientId, groupId, brokers, options } = kafkaClientConfigFromEnv();

const kafka = new KafkaClient(
  clientId ?? 'my-svc',           // env value or your fallback
  groupId ?? 'my-grp',
  brokers ?? ['localhost:9092'],
  {
    ...options,                   // only the keys whose env vars were present
    onMessageLost: alerting,      // code-level value — always applied, not env-configurable
  },
);
```

`kafkaClientConfigFromEnv(env?, prefix?)` reads `KAFKA_`-prefixed variables (`CLIENT_ID`, `GROUP_ID`, `BROKERS`, `AUTO_CREATE_TOPICS`, `STRICT_SCHEMAS`, `NUM_PARTITIONS`, `TRANSACTIONAL_ID`, `CLOCK_RECOVERY_*`, `LAG_THROTTLE_*`, and the security vars `SSL`, `SASL_MECHANISM`, `SASL_USERNAME`, `SASL_PASSWORD`, `ALLOW_INSECURE`). It returns `{ clientId?, groupId?, brokers?, options }`, emitting only the keys whose variables were set. Malformed booleans/numbers/enums throw with the offending variable named. `oauthbearer` cannot come from env — token providers are functions, so configure them in code.

`consumerOptionsFromEnv(env?, prefix?)` reads `KAFKA_CONSUMER_`-prefixed variables into a `Partial<ConsumerOptions>` (retry, DLQ, deduplication, circuit breaker, TTL, `GROUP_INSTANCE_ID`, and more). Merge it under your code-level options with `mergeConsumerOptions()`, which applies the precedence rule — later layers win, and the nested objects (`retry`, `deduplication`, `circuitBreaker`, `subscribeRetry`) are deep-merged so a code layer can override a single field:

```typescript
import { consumerOptionsFromEnv, mergeConsumerOptions } from '@drarzter/kafka-client/core';

const envDefaults = consumerOptionsFromEnv();
await kafka.startConsumer(
  ['orders'],
  handler,
  mergeConsumerOptions(envDefaults, { dlq: true }), // code layer wins on conflict
);
```

Both helpers accept an explicit `env` object (handy in tests) and a custom variable `prefix`. See [`docs/configuration.md`](./docs/configuration.md) for the full variable reference and [`.env.example`](./.env.example) for a ready-to-copy template.

## Options reference

### Send options

Options for `sendMessage()` — the third argument:

| Option | Default | Description |
| ------ | ------- | ----------- |
| `key` | — | Partition key for message routing |
| `headers` | — | Custom metadata headers (merged with auto-generated envelope headers) |
| `correlationId` | auto | Override the auto-propagated correlation ID (default: inherited from ALS context or new UUID) |
| `schemaVersion` | `1` | Schema version for the payload |
| `eventId` | auto | Override the auto-generated event ID (UUID v4) |
| `compression` | — | Compression codec for the message set: `'gzip'`, `'snappy'`, `'lz4'`, `'zstd'`; omit to send uncompressed |
| `deliverAfterMs` | — | Delay delivery by at least this many milliseconds via a `<topic>.delayed` staging topic; requires a running `startDelayedRelay()` (see [Delayed delivery](#delayed-delivery)) |

`sendBatch()` accepts `compression` and `deliverAfterMs` as top-level options (not per-message); all other options are per-message inside the array items.

### Consumer options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `groupId` | constructor value | Override consumer group for this subscription |
| `fromBeginning` | `false` | Read from the beginning of the topic |
| `autoCommit` | `true` | Auto-commit offsets |
| `retry.maxRetries` | — | Number of retry attempts |
| `retry.backoffMs` | `1000` | Base delay for exponential backoff in ms |
| `retry.maxBackoffMs` | `30000` | Maximum delay cap for exponential backoff in ms |
| `dlq` | `false` | Send to `{topic}.dlq` after all retries exhausted — message carries `x-dlq-*` metadata headers |
| `retryTopics` | `false` | Route failed messages through per-level topics (`{topic}.retry.1`, `{topic}.retry.2`, …) instead of sleeping in-process; exactly-once routing semantics within the retry chain; requires `retry` (see [Retry topic chain](#retry-topic-chain)) |
| `interceptors` | `[]` | Array of before/after/onError hooks |
| `retryTopicAssignmentTimeoutMs` | `10000` | Timeout (ms) to wait for each retry level consumer to receive partition assignments after connecting; increase for slow brokers |
| `handlerTimeoutMs` | — | Log a warning if the handler hasn't resolved within this window (ms) — does not cancel the handler |
| `deduplication.strategy` | `'drop'` | What to do with duplicate messages: `'drop'` silently discards, `'dlq'` forwards to `{topic}.dlq` (requires `dlq: true`), `'topic'` forwards to `{topic}.duplicates` |
| `deduplication.duplicatesTopic` | `{topic}.duplicates` | Custom destination for `strategy: 'topic'` |
| `deduplication.store` | in-memory | Pluggable `DedupStore` for the per-partition last-processed clock; supply a persistent store (e.g. Redis) so dedup survives restarts/rebalances (see [Pluggable deduplication store](#pluggable-deduplication-store)) |
| `messageTtlMs` | — | Drop (or DLQ) messages older than this many milliseconds at consumption time; evaluated against the `x-timestamp` header; see [Message TTL](#message-ttl) |
| `circuitBreaker` | — | Enable circuit breaker with `{}` for zero-config defaults; see [Circuit breaker](#circuit-breaker) |
| `circuitBreaker.threshold` | `5` | Failed handler attempts within `windowSize` that open the circuit |
| `circuitBreaker.recoveryMs` | `30_000` | Milliseconds to wait in OPEN state before entering HALF_OPEN |
| `circuitBreaker.windowSize` | `threshold × 2, min 10` | Sliding window size in messages |
| `circuitBreaker.halfOpenSuccesses` | `1` | Consecutive successes in HALF_OPEN required to close the circuit |
| `queueHighWaterMark` | unbounded | Max messages buffered in the `consume()` iterator queue before the partition is paused; resumes at 50% drain. Only applies to `consume()` |
| `batch` | `false` | (decorator only) Use `startBatchConsumer` instead of `startConsumer` |
| `partitionAssigner` | `'cooperative-sticky'` | Partition assignment strategy: `'cooperative-sticky'` (minimal movement on rebalance, best for horizontal scaling), `'roundrobin'` (even distribution), `'range'` (contiguous partition ranges) |
| `groupInstanceId` | — | Static group membership (`group.instance.id`) — a member that restarts within `session.timeout.ms` rejoins with the same partitions and no rebalance. Must be unique per member; not propagated to retry companions. See [Static group membership](#static-group-membership) |
| `onTtlExpired` | — | Per-consumer override of the client-level `onTtlExpired` callback; takes precedence when set. Receives `TtlExpiredContext` — same shape as the client-level hook |
| `onMessageLost` | — | Per-consumer override of the client-level `onMessageLost` callback; takes precedence when set. Use for consumer-specific dead-message alerting or structured logging |
| `onRetry` | — | Per-consumer retry callback; fires **in addition to** the built-in metrics hook (does not replace it). Same signature as `KafkaInstrumentation.onRetry` |
| `subscribeRetry.retries` | `5` | Max attempts for `consumer.subscribe()` when topic doesn't exist yet |
| `subscribeRetry.backoffMs` | `5000` | Delay between subscribe retry attempts (ms) |

### Module options

Passed to `KafkaModule.register()` or returned from `registerAsync()` factory:

| Option | Default | Description |
| ------ | ------- | ----------- |
| `clientId` | — | Kafka client identifier (required) |
| `groupId` | — | Default consumer group ID (required) |
| `brokers` | — | Array of broker addresses (required) |
| `name` | — | Named client identifier for multi-client setups |
| `isGlobal` | `false` | Make the client available in all modules without re-importing |
| `autoCreateTopics` | `false` | Auto-create topics on first send (dev only) |
| `numPartitions` | `1` | Number of partitions for auto-created topics |
| `strictSchemas` | `true` | Validate string topic keys against schemas registered via TopicDescriptor |
| `security` | — | TLS + SASL transport security with secure-by-default rules (`{ ssl, sasl, allowInsecure }`); see [Transport security](#transport-security) |
| `instrumentation` | `[]` | Client-wide instrumentation hooks (e.g. OTel). Applied to both send and consume paths |
| `transactionalId` | `${clientId}-tx` | Transactional producer ID for `transaction()` calls. Must be unique per producer instance across the cluster — two instances sharing the same ID will be fenced by Kafka. The client logs a warning when the same ID is registered twice within one process |
| `onMessageLost` | — | Called when a message is silently dropped without DLQ — use to alert, log to external systems, or trigger fallback logic |
| `onTtlExpired` | — | Called when a message is dropped due to TTL expiration (`messageTtlMs`) and `dlq` is not enabled; receives `{ topic, ageMs, messageTtlMs, headers }` |
| `onRebalance` | — | Called on every partition assign/revoke event across all consumers created by this client |
| `clockRecovery.topics` | — | Topics to scan on `connectProducer()` to recover the highest `x-lamport-clock`, so the clock stays monotonic across restarts (see [Deduplication](#deduplication-lamport-clock)) |
| `clockRecovery.timeoutMs` | `30000` | Max time (ms) to wait for clock recovery before proceeding with a partial result |
| `lagThrottle` | — | Delay sends when a consumer group's lag exceeds `maxLag` (see [Lag-based producer throttling](#lag-based-producer-throttling)) |
| `lagThrottle.maxLag` | — | Lag threshold (messages) above which sends are delayed (required when `lagThrottle` is set) |
| `lagThrottle.groupId` | default group | Consumer group whose lag is monitored |
| `lagThrottle.pollIntervalMs` | `5000` | How often (ms) to poll `getConsumerLag()` in the background |
| `lagThrottle.maxWaitMs` | `30000` | Max time (ms) a send waits while throttled before proceeding anyway (best-effort, not hard back-pressure) |
| `transport` | `ConfluentTransport` | Custom `KafkaTransport` implementation — target an alternative broker library or inject a deterministic fake in tests |

> **Advanced — direct transport access.** `ConfluentTransport` and the full
> `KafkaTransport` interface family (`IProducer`, `IConsumer`, `IAdmin`, …) are
> exported from `@drarzter/kafka-client/core`. When you need low-level admin
> operations the facade does not expose (e.g. per-partition watermarks), build a
> transport instead of deep-importing the raw driver:
>
> ```typescript
> import { ConfluentTransport } from '@drarzter/kafka-client/core';
>
> const admin = new ConfluentTransport('ops-cli', brokers).admin();
> await admin.connect();
> const watermarks = await admin.fetchTopicOffsets('orders'); // [{ partition, low, high }]
> ```

**Module-scoped** (default) — import `KafkaModule` in each module that needs it:

```typescript
// orders.module.ts
@Module({
  imports: [
    KafkaModule.register<OrdersTopicMap>({
      clientId: 'orders',
      groupId: 'orders-group',
      brokers: ['localhost:9092'],
    }),
  ],
})
export class OrdersModule {}
```

**App-wide** — register once in `AppModule` with `isGlobal: true`, inject anywhere:

```typescript
// app.module.ts
@Module({
  imports: [
    KafkaModule.register<MyTopics>({
      clientId: 'my-app',
      groupId: 'my-group',
      brokers: ['localhost:9092'],
      isGlobal: true,
    }),
  ],
})
export class AppModule {}

// any module — no KafkaModule import needed
@Injectable()
export class PaymentService {
  constructor(@InjectKafkaClient() private readonly kafka: KafkaClient<MyTopics>) {}
}
```

## Error classes

When a consumer message handler fails after all retries, the library throws typed error objects:

```typescript
import { KafkaProcessingError, KafkaRetryExhaustedError } from '@drarzter/kafka-client';
```

**`KafkaProcessingError`** — base class for processing failures. Has `topic`, `originalMessage`, and supports `cause`:

```typescript
const err = new KafkaProcessingError('handler failed', 'order.created', rawMessage, { cause: originalError });
err.topic;            // 'order.created'
err.originalMessage;  // the parsed message object
err.cause;            // the original error
```

**`KafkaRetryExhaustedError`** — thrown after all retries are exhausted. Extends `KafkaProcessingError` and adds `attempts`:

```typescript
// In an onError interceptor:
const interceptor: ConsumerInterceptor<MyTopics> = {
  onError: (envelope, error) => {
    if (error instanceof KafkaRetryExhaustedError) {
      console.log(`Failed after ${error.attempts} attempts on ${error.topic}`);
      console.log('Last error:', error.cause);
    }
  },
};
```

When `retry.maxRetries` is set and all attempts fail, `KafkaRetryExhaustedError` is passed to `onError` interceptors automatically.

**`KafkaValidationError`** — thrown when schema validation fails on the consumer side. Has `topic`, `originalMessage`, and `cause`:

```typescript
import { KafkaValidationError } from '@drarzter/kafka-client';

const interceptor: ConsumerInterceptor<MyTopics> = {
  onError: (envelope, error) => {
    if (error instanceof KafkaValidationError) {
      console.log(`Bad message on ${error.topic}:`, error.cause?.message);
    }
  },
};
```

## Deduplication (Lamport Clock)

Every outgoing message produced by this library is stamped with a monotonically increasing logical clock — the `x-lamport-clock` header. The counter lives in the `KafkaClient` instance and increments by one per message (including individual messages inside `sendBatch` and `transaction`).

On the consumer side, enable deduplication by passing `deduplication` to `startConsumer` or `startBatchConsumer`. The library checks the incoming clock against the last processed value for that `topic:partition` combination and skips any message whose clock is not strictly greater.

```typescript
await kafka.startConsumer(['orders.created'], handler, {
  deduplication: {}, // 'drop' strategy — silently discard duplicates
});
```

### How duplicates happen

The most common scenario: a producer service restarts. Its in-memory clock resets to `0`. The consumer already processed messages with clocks `1…N`. All new messages from the restarted producer (clocks `1`, `2`, `3`, …) have clocks ≤ `N` and are treated as duplicates.

```text
Producer A (running): sends clock 1, 2, 3, 4, 5  → consumer processes all 5
Producer A (restarts): sends clock 1, 2, 3         → consumer sees 1 ≤ 5 — duplicate!
```

### Strategies

| Strategy | Behaviour |
| -------- | --------- |
| `'drop'` *(default)* | Log a warning and silently discard the message |
| `'dlq'` | Forward to `{topic}.dlq` with reason metadata headers (`x-dlq-reason`, `x-dlq-duplicate-incoming-clock`, `x-dlq-duplicate-last-processed-clock`). Requires `dlq: true` |
| `'topic'` | Forward to `{topic}.duplicates` (or `duplicatesTopic` if set) with reason metadata headers (`x-duplicate-reason`, `x-duplicate-incoming-clock`, `x-duplicate-last-processed-clock`, `x-duplicate-detected-at`) |

```typescript
// Strategy: drop (default)
await kafka.startConsumer(['orders'], handler, {
  deduplication: {},
});

// Strategy: DLQ — inspect duplicates from {topic}.dlq
await kafka.startConsumer(['orders'], handler, {
  dlq: true,
  deduplication: { strategy: 'dlq' },
});

// Strategy: dedicated topic — consume from {topic}.duplicates
await kafka.startConsumer(['orders'], handler, {
  deduplication: { strategy: 'topic' },
});

// Strategy: custom topic name
await kafka.startConsumer(['orders'], handler, {
  deduplication: {
    strategy: 'topic',
    duplicatesTopic: 'ops.orders.duplicates',
  },
});
```

### Startup validation

When `autoCreateTopics: false` and `strategy: 'topic'`, `startConsumer` / `startBatchConsumer` validates that the destination topic (`{topic}.duplicates` or `duplicatesTopic`) exists before starting the consumer. A clear error is thrown at startup listing every missing topic, rather than silently failing on the first duplicate.

With `autoCreateTopics: true` the check is skipped — the topic is created automatically instead.

### Backwards compatibility

Messages without an `x-lamport-clock` header pass through unchanged. Producers not using this library are unaffected.

### Limitations

Deduplication state is **in-memory and per-consumer-instance**. Understand what that means:

- **Consumer restart** — state is cleared on restart. The first batch of messages after restart is accepted regardless of their clock values, so duplicates spanning a restart window are not caught.
- **Multiple consumer instances** (same group, different machines) — each instance tracks its own partition subset. Partitions are reassigned on rebalance, so a rebalance can reset the state for moved partitions.
- **Cross-session duplicates** — this guards against duplicates from a **producer that restarted within the same consumer session**. For durable, cross-restart deduplication, persist the clock state externally (Redis, database) and implement idempotent handlers.

Use this feature as a lightweight first line of defence — not as a substitute for idempotent business logic.

### Pluggable deduplication store

The in-memory limitation above is only the **default**. Pass a `store` in `deduplication` to back the per-partition clock with any external system — Redis, a database, anything — so deduplication survives process restarts and rebalances. The store implements the `DedupStore` interface:

```typescript
import { DedupStore } from '@drarzter/kafka-client';

interface DedupStore {
  // Return the last processed clock for a group + "topic:partition", or undefined.
  getLastClock(groupId: string, topicPartition: string): number | undefined | Promise<number | undefined>;
  // Persist the last processed clock for a group + "topic:partition".
  setLastClock(groupId: string, topicPartition: string, clock: number): void | Promise<void>;
}
```

Both methods may be synchronous or return a promise. A minimal Redis-backed store:

```typescript
class RedisDedupStore implements DedupStore {
  constructor(private readonly redis: RedisClient) {}

  private key(groupId: string, topicPartition: string) {
    return `dedup:${groupId}:${topicPartition}`;
  }

  async getLastClock(groupId: string, topicPartition: string) {
    const raw = await this.redis.get(this.key(groupId, topicPartition));
    return raw === null ? undefined : Number(raw);
  }

  async setLastClock(groupId: string, topicPartition: string, clock: number) {
    await this.redis.set(this.key(groupId, topicPartition), String(clock));
  }
}

await kafka.startConsumer(['payments'], handler, {
  deduplication: { strategy: 'drop', store: new RedisDedupStore(redis) },
});
```

**Failure semantics (fail-open):** if `getLastClock` or `setLastClock` throws or rejects, the error is logged and the message is treated as **not** a duplicate. A transient store outage never silently drops messages — it only weakens deduplication until the store recovers, biasing towards at-least-once delivery.

When `store` is omitted, the built-in `InMemoryDedupStore` is used — the in-session behaviour described above.

## Retry topic chain

> **tl;dr — recommended production setup:**
>
> ```typescript
> await kafka.startConsumer(['orders.created'], handler, {
>   retry: { maxRetries: 3, backoffMs: 1_000, maxBackoffMs: 30_000 },
>   dlq: true,          // ← messages never silently disappear
>   retryTopics: true,  // ← retries survive restarts; routing is exactly-once
> });
> ```
>
> Just `retry` + `dlq: true` is already safe for most workloads — failed messages land in `{topic}.dlq` after all retries and are never silently dropped. Add `retryTopics: true` for crash-durable retries and exactly-once routing guarantees within the retry chain.
>
> | Configuration | What happens to a message that always fails | Process crash mid-retry |
> | --- | --- | --- |
> | `retry` only | Dropped — `onMessageLost` fires | Lost if crash between attempts |
> | `retry` + `dlq` | Lands in `{topic}.dlq` after all attempts | DLQ write may duplicate (rare) |
> | `retry` + `dlq` + `retryTopics` | Lands in `{topic}.dlq` after all attempts | Retries survive restarts; routing is exactly-once |

By default, retry is handled in-process: the consumer sleeps between attempts while holding the partition. With `retryTopics: true`, failed messages are routed through a chain of Kafka topics instead — one topic per retry level. A companion consumer auto-starts per level, waits for the scheduled delay using partition pause/resume, then calls the same handler.

Benefits over in-process retry:

- **Durable** — retry messages survive a consumer restart; all routing (main → retry.1, level N → N+1, retry → DLQ) is exactly-once via Kafka transactions
- **Non-blocking** — the original consumer is free immediately; each level consumer only pauses its specific partition during the delay window, so other partitions continue processing
- **Isolated** — each retry level has its own consumer group, so a slow level 3 consumer never blocks a level 1 consumer

```typescript
await kafka.startConsumer(['orders.created'], handler, {
  retry: { maxRetries: 3, backoffMs: 1000, maxBackoffMs: 30_000 },
  dlq: true,
  retryTopics: true,   // ← opt in
});
```

With `maxRetries: 3`, this creates three dedicated topics and three companion consumers:

```text
orders.created.retry.1  →  consumer group: my-group-retry.1  (delay ~1 s)
orders.created.retry.2  →  consumer group: my-group-retry.2  (delay ~2 s)
orders.created.retry.3  →  consumer group: my-group-retry.3  (delay ~4 s)
```

Message flow with `maxRetries: 2` and `dlq: true`:

```text
orders.created       →  handler fails  →  orders.created.retry.1  (attempt 1, delay ~1 s)
orders.created.retry.1  →  handler fails  →  orders.created.retry.2  (attempt 2, delay ~2 s)
orders.created.retry.2  →  handler fails  →  orders.created.dlq
```

Each level consumer uses `consumer.pause → sleep(remaining) → consumer.resume` so the partition offset is never committed before the message is processed. On a process crash during sleep or handler execution, the message is redelivered on restart.

The retry topic messages carry scheduling headers (`x-retry-attempt`, `x-retry-after`, `x-retry-original-topic`, `x-retry-max-retries`) that each level consumer reads automatically — no manual configuration needed.

> **Delivery guarantee:** the entire retry chain — including the **main consumer → retry.1** boundary — is **exactly-once**. Every routing step (main → retry.1, retry.N → retry.N+1, retry.N → DLQ) is wrapped in a Kafka transaction via `sendOffsetsToTransaction`: the produce and the consumer offset commit happen atomically. A crash at any point rolls back the transaction: the message is redelivered and the routing is retried, with no duplicate in the next level. If the EOS transaction fails (broker unavailable), the offset stays uncommitted and the message is safely redelivered — it is never lost.
>
> The standard Kafka at-least-once guarantee still applies at the handler level: if your handler succeeds but the process crashes before the manual offset commit completes, the message is redelivered to the handler. Design handlers to be idempotent.
>
> **Startup validation:** `retryTopics` requires `retry` to be set — an error is thrown at startup if `retry` is missing. When `autoCreateTopics: false`, all `{topic}.retry.N` topics are validated to exist at startup and a clear error lists any missing ones. With `autoCreateTopics: true` the check is skipped — topics are created automatically by the `ensureTopic` path. Supported by both `startConsumer` and `startBatchConsumer`.

`stopConsumer(groupId)` automatically stops all companion retry level consumers started for that group.

## stopConsumer

Stop all consumers or a specific group:

```typescript
// Stop a specific consumer group
await kafka.stopConsumer('my-group');

// Stop all consumers
await kafka.stopConsumer();
```

`stopConsumer(groupId)` disconnects and removes only that group's consumer, leaving other groups running. Useful when you want to pause processing for a specific topic without restarting the whole client.

## Pause and resume

Temporarily stop delivering messages from specific partitions without disconnecting the consumer:

```typescript
// Pause partition 0 of 'orders' (default group)
kafka.pauseConsumer(undefined, [{ topic: 'orders', partitions: [0] }]);

// Resume it later
kafka.resumeConsumer(undefined, [{ topic: 'orders', partitions: [0] }]);

// Target a specific consumer group, multiple partitions
kafka.pauseConsumer('payments-group', [{ topic: 'payments', partitions: [0, 1] }]);
```

The first argument is the consumer group ID — pass `undefined` to target the default group. A warning is logged if the group is not found.

Pausing is non-destructive: the consumer stays connected and Kafka preserves the partition assignment for as long as the group session is alive. Messages accumulate in the topic and are delivered once the consumer resumes. Typical use: apply backpressure when a downstream dependency (e.g. a database) is temporarily overloaded.

## Circuit breaker

Automatically pause delivery from a topic-partition when its handler failure rate exceeds a threshold. After a recovery window the partition is resumed automatically.

Failures are recorded at the handler-error boundary: every failed handler attempt counts (including in-process retries and retry-topic chain levels), independent of whether the message ends up in a DLQ. `dlq` is **not** required for the breaker to work.

Zero-config start — all options have sensible defaults:

```typescript
await kafka.startConsumer(['orders'], handler, {
  dlq: true,
  circuitBreaker: {},
});
```

Full config for fine-tuning:

```typescript
await kafka.startConsumer(['orders'], handler, {
  dlq: true,
  circuitBreaker: {
    threshold: 10,         // open after 10 failures (default: 5)
    recoveryMs: 60_000,   // wait 60 s before probing (default: 30 s)
    windowSize: 50,        // track last 50 messages (default: threshold × 2, min 10)
    halfOpenSuccesses: 3,  // 3 successes to close (default: 1)
  },
});
```

State machine per `${groupId}:${topic}:${partition}`:

| State | Behaviour |
| ----- | --------- |
| **CLOSED** (normal) | Messages delivered. Failures recorded in sliding window. Opens when `failures ≥ threshold`. |
| **OPEN** | Partition paused via `pauseConsumer`. After `recoveryMs` ms transitions to HALF_OPEN. |
| **HALF_OPEN** | Partition resumed. After `halfOpenSuccesses` consecutive successes the circuit closes. Any single failure immediately re-opens it. |

Successful `onMessage` completions count as successes. The retry topic path is not subject to the breaker — it has its own backoff and EOS guarantees.

Options:

| Option | Default | Description |
| ------ | ------- | ----------- |
| `threshold` | `5` | Failed handler attempts within `windowSize` that open the circuit |
| `recoveryMs` | `30_000` | Milliseconds to wait in OPEN state before entering HALF_OPEN |
| `windowSize` | `threshold × 2, min 10` | Sliding window size in messages |
| `halfOpenSuccesses` | `1` | Consecutive successes in HALF_OPEN required to close the circuit |

### getCircuitState

Inspect the current circuit breaker state for a partition — useful for health endpoints and dashboards:

```typescript
const state = kafka.getCircuitState('orders', 0);
// undefined — circuit not configured or never tripped
// { status: 'closed', failures: 2, windowSize: 10 }
// { status: 'open',   failures: 5, windowSize: 10 }
// { status: 'half-open', failures: 0, windowSize: 0 }

// With explicit group ID:
const state = kafka.getCircuitState('orders', 0, 'payments-group');
```

Returns `undefined` when `circuitBreaker` is not configured for the group or the circuit has never been tripped (state is lazily initialised on the first DLQ event).

**Instrumentation hooks** — react to state transitions via `KafkaInstrumentation`:

```typescript
const kafka = new KafkaClient('svc', 'group', brokers, {
  instrumentation: [{
    onCircuitOpen(topic, partition) {
      metrics.increment('circuit_open', { topic, partition });
    },
    onCircuitHalfOpen(topic, partition) {
      logger.log(`Circuit probing ${topic}[${partition}]`);
    },
    onCircuitClose(topic, partition) {
      metrics.increment('circuit_close', { topic, partition });
    },
  }],
});
```

## Reset consumer offsets

Seek a consumer group's committed offsets to the beginning or end of a topic:

```typescript
// Seek to the beginning — re-process all existing messages
await kafka.resetOffsets(undefined, 'orders', 'earliest');

// Seek to the end — skip existing messages, process only new ones
await kafka.resetOffsets(undefined, 'orders', 'latest');

// Target a specific consumer group
await kafka.resetOffsets('payments-group', 'orders', 'earliest');
```

**Important:** the consumer for the specified group must be stopped before calling `resetOffsets`. An error is thrown if the group is currently running — this prevents the reset from racing with an active offset commit.

## Seek to offset

Seek individual topic-partitions to explicit offsets — useful when `resetOffsets` is too coarse and you need per-partition control:

```typescript
// Seek partition 0 of 'orders' to offset 100, partition 1 to offset 200
await kafka.seekToOffset(undefined, [
  { topic: 'orders', partition: 0, offset: '100' },
  { topic: 'orders', partition: 1, offset: '200' },
]);

// Multiple topics in one call
await kafka.seekToOffset('payments-group', [
  { topic: 'payments', partition: 0, offset: '0' },
  { topic: 'refunds',  partition: 0, offset: '500' },
]);
```

The first argument is the consumer group ID — pass `undefined` to target the default group. Assignments are grouped by topic internally so each `admin.setOffsets` call covers all partitions of one topic.

**Important:** the consumer for the specified group must be stopped before calling `seekToOffset`. An error is thrown if the group is currently running.

## Seek to timestamp

Seek partitions to the offset nearest to a specific point in time — useful for replaying events that occurred after a known incident or deployment:

```typescript
const ts = new Date('2024-06-01T12:00:00Z').getTime(); // Unix ms

await kafka.seekToTimestamp(undefined, [
  { topic: 'orders', partition: 0, timestamp: ts },
  { topic: 'orders', partition: 1, timestamp: ts },
]);

// Multiple topics in one call
await kafka.seekToTimestamp('payments-group', [
  { topic: 'payments', partition: 0, timestamp: ts },
  { topic: 'refunds',  partition: 0, timestamp: ts },
]);
```

Uses `admin.fetchTopicOffsetsByTimestamp` under the hood. If no offset exists at the requested timestamp (e.g. the partition is empty or the timestamp is in the future), the partition falls back to the current high watermark (end of topic — new messages only).

**Important:** the consumer group must be stopped before seeking. Assignments for the same topic are batched into a single `admin.setOffsets` call.

## Message TTL

Drop or route expired messages using `messageTtlMs` in `ConsumerOptions`:

```typescript
await kafka.startConsumer(['orders'], handler, {
  messageTtlMs: 60_000, // drop messages older than 60 s
  dlq: true,            // route expired messages to DLQ instead of dropping
});
```

The TTL is evaluated against the `x-timestamp` header stamped on every outgoing message by the producer. Messages whose age at consumption time exceeds `messageTtlMs` are:

- **Routed to DLQ** with `x-dlq-reason: ttl-expired` when `dlq: true`
- **Dropped** (calling `onTtlExpired` if configured) otherwise

Typical use: prevent stale events from poisoning downstream systems after a consumer lag spike — e.g. discard order events or push notifications that are no longer actionable.

## DLQ replay

Re-publish messages from a dead letter queue back to the original topic:

```typescript
// Re-publish all messages from 'orders.dlq' → 'orders'
const result = await kafka.replayDlq('orders');
// { replayed: 42, skipped: 0 }
```

Options:

| Option | Default | Description |
| ------ | ------- | ----------- |
| `targetTopic` | `x-dlq-original-topic` header | Override the destination topic |
| `dryRun` | `false` | Count messages without sending |
| `filter` | — | `(headers) => boolean` — skip messages where the callback returns `false` |
| `fromBeginning` | `true` | `true` = full replay every call; `false` = incremental (only new messages since the last call) |

```typescript
// Dry run — see how many messages would be replayed
const dry = await kafka.replayDlq('orders', { dryRun: true });

// Route to a different topic
const result = await kafka.replayDlq('orders', { targetTopic: 'orders.v2' });

// Only replay messages with a specific correlation ID
const filtered = await kafka.replayDlq('orders', {
  filter: (headers) => headers['x-correlation-id'] === 'corr-123',
});

// Incremental replay — only messages added since the last call
const incremental = await kafka.replayDlq('orders', { fromBeginning: false });
```

`replayDlq` reads the DLQ topic up to the high-watermark at the time of the call — messages published after replay starts are not included. By default (`fromBeginning: true`) an ephemeral group ID is used on each call so all messages are always replayed; the group is deleted from the broker after use. With `fromBeginning: false` a stable group ID persists committed offsets between calls, enabling incremental replay. DLQ metadata headers (`x-dlq-original-topic`, `x-dlq-error-message`, `x-dlq-error-stack`, `x-dlq-failed-at`, `x-dlq-attempt-count`) are stripped from the replayed messages; all other headers (e.g. `x-correlation-id`) are preserved.

## Read snapshot

Read any topic from the beginning to its current high-watermark and return a `Map<key, EventEnvelope<T>>` with the **latest value per key**. Useful for bootstrapping in-memory state at service startup without an external cache:

```typescript
// Build a key → latest-value index for a compacted topic
const orders = await kafka.readSnapshot('orders.state');
orders.get('order-123'); // EventEnvelope with the latest payload for that key
```

Tombstone records (null-value messages) remove the key from the map, consistent with log-compaction semantics:

```typescript
const snapshot = await kafka.readSnapshot('orders.state', {
  onTombstone: (key) => console.log(`Key deleted: ${key}`),
});
```

Optional schema validation skips invalid messages with a warning instead of throwing:

```typescript
import { z } from 'zod';

const OrderSchema = z.object({ orderId: z.string(), amount: z.number() });

const snapshot = await kafka.readSnapshot('orders.state', {
  schema: OrderSchema,
});
```

`readSnapshot` uses a short-lived temporary consumer that is **not** registered in the client's consumer map — it disconnects as soon as all partitions reach their high-watermark. The call resolves with the complete snapshot; it does not stream.

| Option | Description |
| ------ | ----------- |
| `schema` | Zod / Valibot / ArkType (any `.parse()` shape) — invalid messages are skipped with a warning |
| `onTombstone` | Called for each tombstone key before it is removed from the map |

## Offset checkpointing

Save and restore consumer group offsets via a dedicated Kafka topic. Useful for point-in-time recovery, blue/green deployments, and disaster recovery without resetting to `earliest`/`latest`.

### checkpointOffsets

Snapshot the current committed offsets of a consumer group into a Kafka topic:

```typescript
// Checkpoint the default group
const result = await kafka.checkpointOffsets(undefined, 'checkpoints');
// {
//   groupId: 'orders-group',
//   topics: ['orders', 'payments'],
//   partitionCount: 4,
//   savedAt: 1710000000000,
// }

// Checkpoint a specific group
await kafka.checkpointOffsets('payments-group', 'checkpoints');
```

Each call appends a new record to the checkpoint topic keyed by `groupId`, with `x-checkpoint-timestamp` and `x-checkpoint-group-id` headers. The checkpoint topic acts as an append-only audit log — use a **non-compacted** topic to retain history.

Requires `connectProducer()` to have been called before checkpointing.

### restoreFromCheckpoint

Restore a consumer group's committed offsets from the nearest checkpoint:

```typescript
// Restore to the latest checkpoint
const result = await kafka.restoreFromCheckpoint(undefined, 'checkpoints');
// {
//   groupId: 'orders-group',
//   offsets: [{ topic: 'orders', partition: 0, offset: '1500' }, ...],
//   restoredAt: 1710000000000,
//   checkpointAge: 3600000, // ms since the checkpoint was saved
// }

// Restore to the nearest checkpoint before a specific timestamp
const ts = new Date('2024-06-01T12:00:00Z').getTime();
await kafka.restoreFromCheckpoint(undefined, 'checkpoints', { timestamp: ts });
```

Checkpoint selection rules:

1. If `timestamp` is omitted — the **latest** checkpoint is selected.
2. If `timestamp` is given — the newest checkpoint whose `savedAt ≤ timestamp` is selected.
3. If all checkpoints are newer than `timestamp` — falls back to the **oldest** checkpoint with a warning.
4. Throws if no checkpoint exists for the group.

**Important:** the consumer group must be stopped before calling `restoreFromCheckpoint`. An error is thrown if any consumer in the group is currently running.

`restoreFromCheckpoint` uses a short-lived temporary consumer to read all checkpoint records up to the current high-watermark, then calls `admin.setOffsets` for every topic-partition in the selected checkpoint.

| Option | Description |
| ------ | ----------- |
| `timestamp` | Target Unix ms. Omit to restore the latest checkpoint |

## Windowed batch consumer

Accumulate messages into a buffer and flush a handler when either a **size** or **time** trigger fires — whichever comes first. Gives explicit control over both batch size and processing latency, unlike `startBatchConsumer` which delivers broker-sized batches of unpredictable size:

```typescript
const handle = await kafka.startWindowConsumer(
  'orders',
  async (envelopes, meta) => {
    console.log(`Flushing ${envelopes.length} orders (trigger: ${meta.trigger})`);
    await db.bulkInsert(envelopes.map((e) => e.payload));
  },
  {
    maxMessages: 100,  // flush when 100 messages accumulate
    maxMs: 5_000,      // or after 5 s, whichever fires first
  },
);
```

`WindowMeta` is passed to the handler on every flush:

| Field | Description |
| ----- | ----------- |
| `trigger` | `"size"` — buffer reached `maxMessages`; `"time"` — `maxMs` elapsed |
| `windowStart` | Unix ms of the first message in the flushed window |
| `windowEnd` | Unix ms when the flush was initiated |

On `handle.stop()` any buffered messages are flushed before the consumer disconnects — no messages are lost on clean shutdown.

`retryTopics: true` is rejected at startup with a clear error — the retry topic chain is incompatible with windowed accumulation.

| Option | Default | Description |
| ------ | ------- | ----------- |
| `maxMessages` | required | Flush when the buffer reaches this many messages |
| `maxMs` | required | Flush after this many ms since the first buffered message |
| All `ConsumerOptions` fields | — | Standard consumer options apply (`retry`, `dlq`, `deduplication`, etc.) |

## Header-based routing

Dispatch messages to different handlers based on the value of a Kafka header — no `if/switch` boilerplate in a catch-all handler. Useful when one topic carries multiple event types distinguished by a header like `x-event-type`:

```typescript
await kafka.startRoutedConsumer(['events'], {
  header: 'x-event-type',
  routes: {
    'order.created':   async (e) => handleOrderCreated(e.payload),
    'order.cancelled': async (e) => handleOrderCancelled(e.payload),
    'order.shipped':   async (e) => handleOrderShipped(e.payload),
  },
  fallback: async (e) => logger.warn('Unknown event type', e.headers),
});
```

Messages are dispatched to the handler whose key matches `envelope.headers[header]`. If the header is absent or its value has no matching route:

- The `fallback` handler is called if provided.
- The message is silently skipped if `fallback` is omitted.

All standard `ConsumerOptions` apply uniformly across every route — retry, DLQ, deduplication, circuit breaker, interceptors, etc.:

```typescript
await kafka.startRoutedConsumer(
  ['events'],
  {
    header: 'x-event-type',
    routes: {
      'payment.processed': async (e) => processPayment(e.payload),
      'payment.failed':    async (e) => handleFailure(e.payload),
    },
  },
  {
    retry: { maxRetries: 3, backoffMs: 500 },
    dlq: true,
    deduplication: { strategy: 'drop' },
  },
);
```

The returned `ConsumerHandle` works the same as `startConsumer` — `handle.stop()` stops the consumer cleanly.

## Lag-based producer throttling

Delay `sendMessage`, `sendBatch`, and `sendTombstone` automatically when a consumer group falls behind. Provides backpressure without an external store — the lag is measured via the built-in admin API:

```typescript
const kafka = new KafkaClient('my-service', 'orders-group', brokers, {
  lagThrottle: {
    maxLag: 10_000,         // delay sends when lag exceeds 10 000 messages
    pollIntervalMs: 5_000,  // check lag every 5 s (default)
    maxWaitMs: 30_000,      // give up waiting after 30 s and send anyway (default)
  },
});

await kafka.connectProducer(); // starts the background polling loop
```

While the observed lag exceeds `maxLag`, every send waits in a `100 ms` spin-loop until the lag drops or `maxWaitMs` is reached. When `maxWaitMs` is exceeded a warning is logged and the send proceeds — this is best-effort throttling, not hard backpressure.

| Option | Default | Description |
| ------ | ------- | ----------- |
| `maxLag` | required | Total lag threshold (sum across all partitions) |
| `groupId` | client default group | Consumer group whose lag is monitored |
| `pollIntervalMs` | `5000` | How often to call `getConsumerLag()` in the background |
| `maxWaitMs` | `30000` | Maximum time (ms) a single send waits before proceeding anyway |

The polling timer is started by `connectProducer()` and cleared by `disconnect()` or `disconnectProducer()`. Poll errors are silently ignored — a failing admin call never blocks sends.

## Transactional consumer

Consume messages with **exactly-once semantics** for read-process-write pipelines. Each message is processed inside a Kafka transaction: outgoing sends and the source offset commit succeed or fail atomically — no partial writes, no duplicates on restart:

```typescript
await kafka.startTransactionalConsumer(
  ['orders'],
  async (envelope, tx) => {
    // Both sends and the offset commit are part of one atomic transaction
    await tx.send('invoices', { orderId: envelope.payload.orderId, amount: envelope.payload.amount });
    await tx.send('notifications', { userId: envelope.payload.userId, message: 'Order confirmed' });
    // tx commits automatically when this function returns
  },
);
```

The handler receives a `TransactionalHandlerContext` with two methods:

| Method | Description |
| ------ | ----------- |
| `tx.send(topic, message, options?)` | Stage a single message inside the transaction |
| `tx.sendBatch(topic, messages, options?)` | Stage multiple messages inside the transaction |

**On handler success** — staged sends + source offset commit are committed atomically via `tx.sendOffsets()` + `tx.commit()`. Downstream consumers only see the messages after the commit.

**On handler failure** — `tx.abort()` is called automatically. No staged sends become visible. The source message offset is not committed, so Kafka redelivers the message on the next poll.

```typescript
await kafka.startTransactionalConsumer(
  ['payments'],
  async (envelope, tx) => {
    const result = await processPayment(envelope.payload);
    // Only route to the audit topic if payment succeeded
    await tx.send('payments.audit', { paymentId: result.id, status: 'ok' });
  },
  {
    groupId: 'payments-eos',
    deduplication: { strategy: 'drop' }, // standard ConsumerOptions apply
  },
);
```

`retryTopics: true` is rejected at startup — EOS redelivery on failure is already guaranteed by the transaction. `autoCommit` is always `false` (managed internally).

## Transactional outbox

The transactional-outbox pattern decouples "write my business state" from "publish an event" so the two can never diverge. Application code writes an event row into an outbox table **in the same DB transaction** as its business writes; a relay polls that table and publishes the rows to Kafka, marking them published only after Kafka has acked them. If the process dies after the DB commit but before the publish, the row is still there and gets published on the next poll — the event is never lost.

`startOutboxRelay()` runs that relay against any `OutboxStore` you implement. The library never touches your database — you own the schema and the queries; it only needs to read unpublished rows oldest-first and durably mark rows published:

```typescript
import { startOutboxRelay, OutboxStore } from '@drarzter/kafka-client/core';

// Pseudo-Postgres store — you own the table and the SQL.
const store: OutboxStore = {
  async fetchUnpublished(limit) {
    const { rows } = await pool.query(
      `SELECT id, topic, payload, key, correlation_id AS "correlationId",
              event_id AS "eventId", headers
         FROM outbox
        WHERE published_at IS NULL
        ORDER BY created_at ASC
        LIMIT $1`,
      [limit],
    );
    return rows;
  },
  async markPublished(ids) {
    await pool.query(`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, [ids]);
  },
};

await kafka.connectProducer();

const relay = startOutboxRelay(kafka, store, {
  pollIntervalMs: 500,   // default 1000
  batchSize: 200,        // default 100 — rows fetched & published per tick
  onPublished: (n) => metrics.increment('outbox.published', n),
  onError: (err, batch) => logger.error(`outbox batch of ${batch.length} failed`, err),
});

// On shutdown — stop() halts the timer and awaits any in-flight iteration:
await relay.stop();
await kafka.disconnect();
```

Meanwhile, application code inserts outbox rows inside its business transaction:

```typescript
// Inside a DB transaction, alongside your business INSERT/UPDATE:
await tx.query(
  `INSERT INTO outbox (id, topic, payload, key, correlation_id, event_id)
   VALUES ($1, $2, $3, $4, $5, $6)`,
  [randomUUID(), 'orders.created', JSON.stringify(order), order.id, corrId, eventId],
);
```

**Delivery guarantee: at-least-once.** Each poll publishes the whole batch inside **one Kafka transaction**, then marks the rows published. If the process crashes *after* the Kafka commit but *before* `markPublished`, those rows are re-published on the next tick — a **duplicate**. Persist a stable `eventId` on each row (surfaced as `x-event-id`) so consumers can deduplicate, either via this library's [Lamport-clock deduplication](#deduplication-lamport-clock) or an application-level idempotency check. Iterations never overlap; the loop never dies on error.

`OutboxStore` interface:

| Method | Description |
| ------ | ----------- |
| `fetchUnpublished(limit): Promise<OutboxMessage[]>` | Unpublished rows, oldest first, capped at `limit`. Empty array = nothing to do |
| `markPublished(ids): Promise<void>` | Durably mark ids published; called only after Kafka acks. Idempotent |

An `InMemoryOutboxStore` (with `.add()`, `pendingCount`, `publishedCount`) ships for tests and as executable documentation — it is **not** durable, so it does not provide the "same DB transaction as the business write" guarantee that is the whole point of the pattern. A full Postgres reference implementation lives in [`src/integration/postgres-outbox.integration.spec.ts`](./src/integration/postgres-outbox.integration.spec.ts).

## Serialization: JSON, Avro, Protobuf

By default every message value is serialized as JSON — no configuration needed.
Serialization is a pluggable seam (`MessageSerde`): swap in Avro or Protobuf
with **Confluent wire format** (`[magic 0x00][4-byte schema id][payload]`) to
interoperate with Java/Go producers and consumers through a Schema Registry.

```typescript
import { KafkaClient, topic } from '@drarzter/kafka-client/core';
import { avroSerde } from '@drarzter/kafka-client/serde';
import { SchemaRegistryClient } from '@drarzter/kafka-client/core';

const registry = new SchemaRegistryClient({ baseUrl: 'http://localhost:8081' });

const orderSchema = {
  type: 'record', name: 'Order',
  fields: [{ name: 'orderId', type: 'string' }, { name: 'amount', type: 'int' }],
};

// Client-wide: every value goes through Avro.
const kafka = new KafkaClient('orders-svc', 'orders-grp', ['localhost:9092'], {
  serde: avroSerde({ registry, schema: orderSchema, autoRegister: true }),
});

// …or per-topic (JSON elsewhere, Avro just here):
const OrderCreated = topic('order.created')
  .serde(avroSerde({ registry, schema: orderSchema, autoRegister: true }))
  .type<{ orderId: string; amount: number }>();
```

`protobufSerde({ registry, schema: protoSource, messageType: 'Order', autoRegister: true })`
works the same way. `avsc` / `protobufjs` are **optional peer dependencies** —
install only the one you use (`npm i avsc` or `npm i protobufjs`); a clear error
tells you if it's missing.

**Serde options.** `registry` (required); `schema` (Avro JSON / `.proto` source —
required to serialize); `subject?` (defaults to Confluent TopicNameStrategy
`<topic>-value` / `<topic>-key`); `autoRegister?` (register the schema on first
send to obtain its id — handy in dev; default `false` reads the latest registered
schema instead). Parsed schemas and id→schema lookups are cached.

**Custom serde.** Implement `MessageSerde` (`serialize(value, ctx) → Buffer | string`,
`deserialize(data, ctx) → value`) for MessagePack, CBOR, encryption, etc. `JsonSerde`
is the default and is exported for composition.

**Notes & limits (v0.11):** the envelope headers (`x-event-id`, Lamport clock,
`traceparent`, …) always travel as Kafka headers regardless of value serde. DLQ,
retry-topic, duplicates, and delayed-relay forwarding preserve the original wire
bytes losslessly, so binary formats survive every hop. Avro currently uses the
writer schema as the reader schema (no reader-schema evolution yet); Protobuf
supports the top-level message type only; `readSnapshot` remains JSON-only.

## Schema Registry client

`SchemaRegistryClient` is a minimal, dependency-free client for the Confluent Schema Registry REST API (works with Confluent Platform/Cloud, Redpanda, Karapace, and the AWS Glue SR proxy). Its scope is **subject/version management, compatibility checks, and id→schema lookups** — used both to keep your locally-defined schemas in lockstep with a central registry and as the backing lookup for the Avro/Protobuf serdes (see [Serialization: JSON, Avro, Protobuf](#serialization-json-avro-protobuf)).

```typescript
import { SchemaRegistryClient } from '@drarzter/kafka-client/core';

const registry = new SchemaRegistryClient({
  baseUrl: 'http://localhost:8081',
  auth: { username: apiKey, password: apiSecret }, // optional HTTP Basic (Confluent Cloud)
  cacheTtlMs: 300_000, // latest-version cache TTL — default 5 min
});

// Register (idempotent — re-registering the same schema returns the existing id)
const { id } = await registry.registerSchema('order.created-value', JSON.stringify(orderJsonSchema), 'JSON');

// Fetch (getLatestSchema is cached; getSchemaVersion is not)
const latest = await registry.getLatestSchema('order.created-value');
const v2 = await registry.getSchemaVersion('order.created-value', 2);

// Check compatibility against the subject's policy without registering
const ok = await registry.checkCompatibility('order.created-value', JSON.stringify(candidate));
```

| Method | Cached | Description |
| ------ | ------ | ----------- |
| `getLatestSchema(subject)` | yes (`cacheTtlMs`) | Latest `{ id, version, schema }` for a subject |
| `getSchemaVersion(subject, version)` | no | A specific registered version |
| `registerSchema(subject, schema, schemaType?)` | invalidates cache | Register (idempotent); returns `{ id }`. `schemaType` defaults to `'JSON'` |
| `checkCompatibility(subject, schema, schemaType?)` | no | `true` when the registry reports the schema compatible |

`registrySchema()` bridges a registry subject to this library's `SchemaLike` seam so you can attach it to a `TopicDescriptor` like any other schema. On each `parse` it resolves the subject's latest version (cached), optionally verifies the message's `x-schema-version` is not newer than what is registered, and delegates structural validation to a local validator:

```typescript
import { topic, registrySchema } from '@drarzter/kafka-client/core';
import { z } from 'zod';

const OrderCreated = topic('order.created').schema(
  registrySchema(registry, 'order.created-value', {
    validator: z.object({ orderId: z.string() }), // local runtime shape check
    enforceVersion: true, // default — fail loudly if the message version outruns the registry
  }),
);
```

The division of labour: the **registry governs schema evolution** (compatibility across versions); the **local validator governs runtime shape**. When `enforceVersion` is `true` (the default) a producer publishing a version newer than the latest registered version fails loudly rather than drifting silently.

## Admin API

Inspect consumer groups, topic metadata, and delete records via the built-in admin client — no separate connection needed.

### `listConsumerGroups()`

Returns all consumer groups known to the broker:

```typescript
const groups = await kafka.listConsumerGroups();
// [{ groupId: 'orders-group', state: 'Stable' }, ...]
```

`state` reflects the librdkafka group state: `'Stable'`, `'PreparingRebalance'`, `'CompletingRebalance'`, `'Dead'`, or `'Empty'`.

### `describeTopics(topics?)`

Fetch partition metadata (leader, replicas, ISR) for one or more topics. Omit `topics` to describe all topics the client knows about:

```typescript
const info = await kafka.describeTopics(['orders.created', 'payments.received']);
// [
//   {
//     name: 'orders.created',
//     partitions: [{ partition: 0, leader: 1, replicas: [1, 2], isr: [1, 2] }],
//   },
//   ...
// ]
```

### `deleteRecords(topic, partitions)`

Truncate a topic by deleting all records up to (but not including) a given offset:

```typescript
// Delete all records in partition 0 up to offset 500
await kafka.deleteRecords('orders.created', [
  { partition: 0, offset: '500' },
  { partition: 1, offset: '200' },
]);
```

Pass `offset: '-1'` to delete all records in a partition (truncate completely).

## DLQ CLI

The package ships a `kafka-client-dlq` binary for inspecting and re-publishing dead letter queues from the terminal — no code needed. It operates on `<topic>.dlq` topics and delegates replay to `KafkaClient.replayDlq`:

```bash
# List every .dlq topic with its message count (optionally filtered by base-topic prefix)
kafka-client-dlq ls     --brokers localhost:9092 [--prefix orders]

# Print up to N messages from <topic>.dlq — offset, x-dlq-* headers, and value
kafka-client-dlq peek   --brokers localhost:9092 --topic orders.created [--limit 5]

# Re-publish <topic>.dlq to its original topic (or --target), full or incremental
kafka-client-dlq replay --brokers localhost:9092 --topic orders.created [--target orders.manual] [--dry-run] [--from-beginning | --incremental]
```

| Flag | Command | Description |
| ---- | ------- | ----------- |
| `--brokers <list>` | all | Comma-separated broker addresses (**required**) |
| `--prefix <name>` | `ls` | Only show DLQ topics whose base name starts with `<name>` |
| `--topic <name>` | `peek`, `replay` | Base topic name — the CLI reads `<name>.dlq` |
| `--limit <n>` | `peek` | Max messages to print (default `10`) |
| `--target <t>` | `replay` | Override destination topic (default: `x-dlq-original-topic` header) |
| `--dry-run` | `replay` | Count what would be replayed without publishing |
| `--from-beginning` | `replay` | Full replay of all DLQ messages every call (default) |
| `--incremental` | `replay` | Only messages added since the previous replay |

`--from-beginning` and `--incremental` are mutually exclusive. Run `kafka-client-dlq --help` (or with no arguments) for the full usage text.

## Graceful shutdown

`disconnect()` now drains in-flight handlers before tearing down connections — no messages are silently cut off mid-processing.

**NestJS** apps get this automatically: `onModuleDestroy` calls `disconnect()`, which waits for all running `eachMessage` / `eachBatch` callbacks to settle first. Enable NestJS shutdown hooks in your bootstrap:

```typescript
// main.ts
const app = await NestFactory.create(AppModule);
app.enableShutdownHooks(); // lets NestJS call onModuleDestroy on SIGTERM
await app.listen(3000);
```

**Standalone** apps call `enableGracefulShutdown()` to register SIGTERM / SIGINT handlers:

```typescript
const kafka = new KafkaClient('my-app', 'my-group', brokers);
await kafka.connectProducer();

kafka.enableGracefulShutdown();
// or with custom signals and timeout:
kafka.enableGracefulShutdown(['SIGTERM', 'SIGINT'], 60_000);
```

`disconnect()` accepts an optional `drainTimeoutMs` (default `30_000` ms). If handlers haven't settled within the window, a warning is logged and the client disconnects anyway:

```typescript
await kafka.disconnect(10_000); // wait up to 10 s, then force disconnect
```

## Consumer handles

`startConsumer()` and `startBatchConsumer()` return a `ConsumerHandle` instead of `void`. Use it to stop a specific consumer or wait for it to be ready:

```typescript
const handle = await kafka.startConsumer(['orders'], handler, { fromBeginning: false });

console.log(handle.groupId); // e.g. "my-group"

// Wait until Kafka has assigned at least one partition to this consumer.
// Safe to call before sending test messages — eliminates fixed setTimeout delays.
await handle.ready();

// Later — stop only this consumer, producer stays connected
await handle.stop();
```

`handle.stop()` is equivalent to `kafka.stopConsumer(handle.groupId)`. Useful in lifecycle methods or when you need to conditionally stop one consumer while others keep running.

`handle.ready()` resolves once the broker fires the first partition-assignment event. For `fromBeginning: false` consumers it adds a 500 ms settle window so librdkafka can complete the async `latest` offset fetch before you send; for `fromBeginning: true` consumers it resolves immediately on assignment. In unit tests with `FakeTransport`, `subscribe()` fires the assignment synchronously, so `handle.ready()` resolves in the same tick.

## onMessageLost

By default, if a consumer handler throws and `dlq` is not enabled, the message is logged and dropped. Use `onMessageLost` to catch these silent losses:

```typescript
import { KafkaClient, MessageLostContext } from '@drarzter/kafka-client/core';

const kafka = new KafkaClient('my-app', 'my-group', ['localhost:9092'], {
  onMessageLost: (ctx: MessageLostContext) => {
    // ctx.topic    — topic the message came from
    // ctx.error    — what caused the failure
    // ctx.attempt  — number of attempts (0 = schema validation failed before handler ran)
    // ctx.headers  — original message headers (correlationId, traceparent, ...)
    myAlertSystem.send(`Message lost on ${ctx.topic}: ${ctx.error.message}`);
  },
});
```

`onMessageLost` fires in three cases:

1. **Handler error** — handler threw after all retries and `dlq: false`
2. **Validation error** — schema rejected the message and `dlq: false` (attempt is `0`)
3. **DLQ send failure** — `dlq: true` but `producer.send()` to `{topic}.dlq` itself threw (broker down, topic missing); the error passed to `onMessageLost` is the send error, not the original handler error

In the normal case (`dlq: true`, DLQ send succeeds), `onMessageLost` does NOT fire — the message is preserved in `{topic}.dlq`.

> **Note:** TTL-expired messages do **not** trigger `onMessageLost`. Use `onTtlExpired` to observe them separately.

## onTtlExpired

Called when a message is dropped because it exceeded `messageTtlMs` and `dlq` is not enabled. Fires instead of `onMessageLost` for expired messages:

```typescript
import { KafkaClient, TtlExpiredContext } from '@drarzter/kafka-client/core';

const kafka = new KafkaClient('my-app', 'my-group', ['localhost:9092'], {
  onTtlExpired: (ctx: TtlExpiredContext) => {
    // ctx.topic         — topic the message came from
    // ctx.ageMs         — actual age of the message at drop time
    // ctx.messageTtlMs  — the configured threshold
    // ctx.headers       — original message headers
    logger.warn(`Stale message on ${ctx.topic}: ${ctx.ageMs}ms old (limit ${ctx.messageTtlMs}ms)`);
  },
});
```

When `dlq: true`, expired messages are routed to DLQ instead and `onTtlExpired` is **not** called.

`onTtlExpired` can also be set per-consumer via `ConsumerOptions.onTtlExpired`. The consumer-level value takes precedence over the client-level one, so you can use different handlers for different topics:

```typescript
await kafka.startConsumer(['orders'], handler, {
  messageTtlMs: 30_000,
  onTtlExpired: (ctx) => {
    // overrides the client-level onTtlExpired for this consumer only
    ordersMetrics.increment('ttl_expired', { topic: ctx.topic });
  },
});
```

## onRebalance

React to partition rebalance events without patching the consumer. Useful for flushing in-flight state before partitions are revoked, or for logging/metrics:

```typescript
const kafka = new KafkaClient('my-app', 'my-group', ['localhost:9092'], {
  onRebalance: (type, partitions) => {
    // type       — 'assign' | 'revoke'
    // partitions — Array<{ topic: string; partition: number }>
    console.log(`Rebalance ${type}:`, partitions);
  },
});
```

- `'assign'` fires when this instance receives new partitions (e.g. on startup or when another consumer leaves the group).
- `'revoke'` fires when partitions are taken away (e.g. another consumer joins).

The callback is applied to **every** consumer created by this client. If you need per-consumer rebalance handling, use separate `KafkaClient` instances.

## Consumer lag

Query consumer group lag per partition via the admin API — no external tooling needed:

```typescript
const lag = await kafka.getConsumerLag();
// [{ topic: 'orders', partition: 0, lag: 12 }, ...]

// Or for a different group:
const lag2 = await kafka.getConsumerLag('another-group');
```

Lag is computed as `brokerHighWatermark − lastCommittedOffset`. A partition with a committed offset of `-1` (nothing ever committed) reports full lag equal to the high watermark.

Returns an empty array when the group has no committed offsets at all.

## Handler timeout warning

Catch stuck handlers before they silently starve a partition. Set `handlerTimeoutMs` on `startConsumer` or `startBatchConsumer`:

```typescript
await kafka.startConsumer(['orders'], handler, {
  handlerTimeoutMs: 5_000, // warn if handler hasn't resolved after 5 s
});
```

If the handler hasn't resolved within the window, a `warn` is logged:

```text
[KafkaClient:my-app] Handler for topic "orders" has not resolved after 5000ms — possible stuck handler
```

The handler is **not** cancelled — the warning is diagnostic only. Combine with `retry` to automatically give up after a fixed number of slow attempts.

## Static group membership

Set `groupInstanceId` in `ConsumerOptions` to give a consumer a **static** identity (`group.instance.id`). A member that restarts within the broker's `session.timeout.ms` rejoins the group with the same partition assignment and triggers **no rebalance** — ideal for Kubernetes rolling restarts and short redeploys where a transient rebalance would otherwise stall every consumer in the group:

```typescript
await kafka.startConsumer(['orders'], handler, {
  groupInstanceId: `orders-svc-${process.env.HOSTNAME}`,
});
```

The id must be **unique per member** within the consumer group — derive it from a stable per-pod value such as the StatefulSet ordinal or hostname. Two live members sharing the same `groupInstanceId` are fenced by the broker.

`groupInstanceId` is applied only to the consumer you set it on. It is **not** propagated to retry-chain companion consumers — those run in their own groups (`<groupId>-retry.N`) and rebalance independently. It can also be supplied via the `KAFKA_CONSUMER_GROUP_INSTANCE_ID` environment variable (see [Environment configuration](#environment-configuration)).

## Schema validation

Add runtime message validation using any library with a `.parse()` method — Zod, Valibot, ArkType, or a custom validator. No extra dependency required.

### Defining topics with schemas

```typescript
import { topic, TopicsFrom } from '@drarzter/kafka-client';
import { z } from 'zod';  // or valibot, arktype, etc.

// Schema-validated — type inferred from schema, no generic needed
export const OrderCreated = topic('order.created').schema(z.object({
  orderId: z.string(),
  userId: z.string(),
  amount: z.number().positive(),
}));

// Without schema — explicit type via .type<T>()
export const OrderAudit = topic('order.audit').type<{ orderId: string; action: string }>();

export type MyTopics = TopicsFrom<typeof OrderCreated | typeof OrderAudit>;
```

### How it works

**On send** — `sendMessage`, `sendBatch`, and `transaction` call `schema.parse(message)` before serializing. Invalid messages throw immediately as `KafkaValidationError` (the original schema error is available as `cause`):

```typescript
// This throws KafkaValidationError — amount must be positive
await kafka.sendMessage(OrderCreated, { orderId: '1', userId: '2', amount: -5 });
```

**On consume** — after `JSON.parse`, the consumer validates each message against the schema. Invalid messages are:

1. Logged as errors
2. Sent to DLQ if `dlq: true`
3. Passed to `onError` interceptors as `KafkaValidationError`
4. Skipped (handler is NOT called)

```typescript
@SubscribeTo(OrderCreated, { dlq: true })
async handleOrder(envelope) {
  // `envelope.payload` is guaranteed to match the schema
  console.log(envelope.payload.orderId); // string — validated at runtime
}
```

### Strict schema mode

By default (`strictSchemas: true`), once a schema is registered via a TopicDescriptor, string topic keys are also validated against it:

```typescript
// First call registers the schema in the internal registry
await kafka.sendMessage(OrderCreated, { orderId: '1', userId: '2', amount: 100 });

// Now this is ALSO validated — throws if data doesn't match OrderCreated's schema
await kafka.sendMessage('order.created', { orderId: 123, userId: null, amount: -5 });
```

Disable with `strictSchemas: false` in `KafkaModule.register()` options if you want the old behavior (string topics bypass validation).

### Bring your own validator

Any object with `parse(data: unknown): T | Promise<T>` works — sync and async validators are both supported:

```typescript
import { SchemaLike } from '@drarzter/kafka-client';

// Sync validator
const customValidator: SchemaLike<{ id: string }> = {
  parse(data: unknown) {
    const d = data as any;
    if (typeof d?.id !== 'string') throw new Error('id must be a string');
    return { id: d.id };
  },
};

// Async validator — e.g. remote schema registry lookup
const asyncValidator: SchemaLike<{ id: string }> = {
  async parse(data: unknown) {
    const schema = await fetchSchemaFromRegistry('my.topic');
    return schema.validate(data);
  },
};

const MyTopic = topic('my.topic').schema(customValidator);
```

### Context-aware validators (`SchemaParseContext`)

`parse()` receives an optional second argument `ctx: SchemaParseContext` on both the consume and send paths. Use it for schema-registry lookups, version-aware migration, or header-driven parsing:

```typescript
import { SchemaLike, SchemaParseContext } from '@drarzter/kafka-client';

const versionedValidator: SchemaLike<MyPayload> = {
  parse(data: unknown, ctx?: SchemaParseContext) {
    const version = ctx?.version ?? 1;
    // version comes from the x-schema-version header (send: schemaVersion option)
    if (version >= 2) return migrateV1toV2(data);
    return validateV1(data);
  },
};

// On consume: ctx = { topic: 'orders.created', headers: { ... }, version: 2 }
// On send:    ctx = { topic: 'orders.created', headers: { ... }, version: schemaVersion ?? 1 }
```

`SchemaParseContext` shape:

```typescript
interface SchemaParseContext {
  topic: string;           // topic the message was produced to / consumed from
  headers: MessageHeaders; // decoded headers (envelope headers included)
  version: number;         // x-schema-version header value, defaults to 1
}
```

Existing validators (Zod, Valibot, ArkType, custom) that only use the first argument continue to work unchanged — the second argument is silently ignored.

### Versioned schemas

`versionedSchema()` composes per-version validators into a single `SchemaLike` that dispatches on the message's `x-schema-version` header (via `SchemaParseContext.version`). Pass a map of version number → validator, plus an optional `migrate` hook that upgrades older shapes to the latest:

```typescript
import { topic, versionedSchema } from '@drarzter/kafka-client';
import { z } from 'zod';

const OrderSchema = versionedSchema<{ orderId: string; amountMinor: number }>(
  {
    1: z.object({ orderId: z.string(), amount: z.number() }),          // legacy: major units
    2: z.object({ orderId: z.string(), amountMinor: z.number().int() }), // current: minor units
  },
  {
    // migrate(data, fromVersion, latestVersion) → data in its latest shape
    migrate: (data, from) =>
      from === 1
        ? { orderId: data.orderId, amountMinor: Math.round(data.amount * 100) }
        : data,
  },
);

const OrderCreated = topic('order.created').schema(OrderSchema);
```

Dispatch rules:

- **Consume path** — the version comes from the `x-schema-version` header (defaults to `1` when absent).
- **Send path** — the version comes from `SendOptions.schemaVersion` (defaults to `1`).
- **No parse context** (a direct `schema.parse(data)` call) — the **latest** registered version is assumed.

After a non-latest version is parsed, `migrate` (if provided) is called so your handler always receives the latest shape. Without a `migrate` hook, older versions are returned as parsed and callers must handle shape differences themselves.

A message carrying a version with **no registered schema throws** — the error lists every registered version rather than validating against the wrong shape, so a misconfigured producer fails loudly:

```text
versionedSchema: no schema registered for version 3 (topic "order.created") — registered versions: 1, 2
```

## Constructor options validation

The `KafkaClient` constructor validates its arguments up front. If anything is invalid it throws a **single aggregated error** listing every problem at once, so a misconfigured client fails at construction with a clear message instead of surfacing a confusing driver error on first use:

```typescript
new KafkaClient('', '', [], { numPartitions: 0 });
// throws:
// KafkaClient: invalid configuration:
// - clientId must be a non-empty string
// - groupId must be a non-empty string
// - brokers must be a non-empty array of broker addresses
// - numPartitions must be a positive integer (got 0)
```

Checks performed:

- `clientId` and `groupId` must be non-empty strings.
- `brokers` must be a non-empty array with no empty entries — **unless** a custom `transport` is supplied (e.g. `FakeTransport` in tests), in which case an empty `brokers` array is allowed since no broker is dialled.
- `numPartitions`, when set, must be a positive integer.
- `transactionalId`, when set, must be non-empty.
- `clockRecovery.topics` must be an array; `clockRecovery.timeoutMs`, when set, must be `> 0`.
- `lagThrottle.maxLag` must be `>= 0`; `lagThrottle.pollIntervalMs` must be `> 0`; `lagThrottle.maxWaitMs` must be `>= 0` (each validated only when set).

This applies to both `new KafkaClient(...)` and `KafkaModule.register()` / `registerAsync()`, which construct the client under the hood.

## Health check

Monitor Kafka connectivity with the built-in health indicator:

```typescript
import { Injectable } from '@nestjs/common';
import { InjectKafkaClient, KafkaClient, KafkaHealthIndicator } from '@drarzter/kafka-client';
import { OrdersTopicMap } from './orders.types';

@Injectable()
export class HealthService {
  private readonly health = new KafkaHealthIndicator();

  constructor(
    @InjectKafkaClient()
    private readonly kafka: KafkaClient<OrdersTopicMap>,
  ) {}

  async checkKafka() {
    return this.health.check(this.kafka);
    // { status: 'up', clientId: 'my-service', topics: ['order.created', ...] }
    // or { status: 'down', clientId: 'my-service', error: 'Connection refused' }
  }
}
```

## Testing

### Testing utilities

Import from `@drarzter/kafka-client/testing` — zero runtime deps, only `jest` and `@testcontainers/kafka` as peer dependencies.

> Unit tests mock `@confluentinc/kafka-javascript` — no Kafka broker needed. Integration tests use Testcontainers (requires Docker).

#### `createMockKafkaClient<T>()`

Fully typed mock with `jest.fn()` on every `IKafkaClient` method. All methods resolve to sensible defaults:

```typescript
import { createMockKafkaClient } from '@drarzter/kafka-client/testing';

const kafka = createMockKafkaClient<MyTopics>();

const service = new OrdersService(kafka);
await service.createOrder();

expect(kafka.sendMessage).toHaveBeenCalledWith(
  'order.created',
  expect.objectContaining({ orderId: '123' }),
);

// Override return values
kafka.checkStatus.mockResolvedValueOnce({ status: 'up', clientId: 'mock-client', topics: ['order.created'] });

// Mock rejections
kafka.sendMessage.mockRejectedValueOnce(new Error('broker down'));
```

#### `KafkaTestContainer`

Thin wrapper around `@testcontainers/kafka` that handles common setup pain points — transaction coordinator warmup, topic pre-creation:

```typescript
import { KafkaTestContainer } from '@drarzter/kafka-client/testing';
import { KafkaClient } from '@drarzter/kafka-client/core';

let container: KafkaTestContainer;
let brokers: string[];

beforeAll(async () => {
  container = new KafkaTestContainer({
    topics: ['orders', { topic: 'payments', numPartitions: 3 }],
  });
  brokers = await container.start();
}, 120_000);

afterAll(() => container.stop());

it('sends and receives', async () => {
  const kafka = new KafkaClient('test', 'test-group', brokers);
  // ...
});
```

Options:

| Option | Default | Description |
| ------ | ------- | ----------- |
| `image` | `"confluentinc/cp-kafka:7.7.0"` | Docker image |
| `transactionWarmup` | `true` | Warm up transaction coordinator on start |
| `topics` | `[]` | Topics to pre-create (string or `{ topic, numPartitions }`) |

### Running tests

Unit tests (mocked `@confluentinc/kafka-javascript` — no broker needed):

```bash
npm test
```

Integration tests with a real Kafka broker via [testcontainers](https://node.testcontainers.org/) (requires Docker):

```bash
npm run test:integration
```

The integration suite spins up a single-node KRaft Kafka container and tests sending, consuming, batching, transactions, retry + DLQ, interceptors, health checks, and `fromBeginning` — no mocks.

Both suites run in CI on every push to `main` and on pull requests.

**Chaos suite** — fault-injection tests (broker restarts, forced rebalances) that verify redelivery and offset-commit guarantees under failure:

```bash
npm run test:chaos
```

**Benchmark** — measure the wrapper's overhead over the raw driver:

```bash
npm run bench
```

The throughput benchmark reports roughly **~2% overhead** versus using `@confluentinc/kafka-javascript` directly — the typed envelope, Lamport clock, and instrumentation hooks cost very little on the hot path.

**Clean up stray containers** — if a Testcontainers run is interrupted, remove leftover containers:

```bash
npm run containers:clean
```

## File naming conventions

Hyphens within a multi-word name; dot separates the name from its role suffix.

| Suffix | Role |
| --- | --- |
| `.types` | Data/option shapes — configs, options, result objects |
| `.interface` | Contract interfaces — capability boundaries, polymorphism points |
| `.manager` | Stateful manager class |
| `.tracker` | Tracking/counting utility |
| `.module` | NestJS module |
| `.explorer` | NestJS metadata scanner |
| `.decorator` | NestJS decorator definitions |
| `.health` | Health indicator |
| `.constants` | Constant values and DI tokens |
| `.mock` | Jest/Vitest spy double |
| `.fake` | Standalone fake implementation |
| `.container` | Test infrastructure wrapper |
| `.spec` | Unit test |
| `.integration.spec` | Integration test |

A suffix is added only when it carries information the name alone does not. When the filename already expresses the role — `handler.ts` is a handler, `pipeline.ts` is a pipeline, `queue.ts` is a queue — no suffix is needed.

---

## Project structure

```text
src/
├── index.ts                         # Full entrypoint — core + NestJS adapter
├── core.ts                          # Standalone entrypoint (@drarzter/kafka-client/core)
├── otel.ts                          # OpenTelemetry entrypoint (@drarzter/kafka-client/otel)
├── testing.ts                       # Testing entrypoint (@drarzter/kafka-client/testing)
│
├── client/                          # Core library — zero framework dependencies
│   ├── types.ts                     # All public interfaces: KafkaClientOptions, ConsumerOptions,
│   │                                #   SendOptions, EventEnvelope, ConsumerHandle, BatchMeta,
│   │                                #   RoutingOptions, TransactionalHandlerContext,
│   │                                #   KafkaInstrumentation, ConsumerInterceptor, SchemaLike, …
│   ├── errors.ts                    # KafkaProcessingError, KafkaRetryExhaustedError, KafkaValidationError
│   │
│   ├── message/
│   │   ├── envelope.ts              # extractEnvelope() — Buffer → EventEnvelope; buildHeaders()
│   │   └── topic.ts                 # topic() builder → TopicDescriptor; global schema registry;
│   │                                #   TopicsFrom<T> utility type
│   │
│   ├── kafka.client/
│   │   ├── index.ts                 # KafkaClient class — public API, producer/consumer lifecycle,
│   │   │                            #   Lamport clock, ALS correlation ID, graceful shutdown,
│   │   │                            #   clockRecovery, readSnapshot(), checkpointOffsets(),
│   │   │                            #   restoreFromCheckpoint(), startWindowConsumer(),
│   │   │                            #   startRoutedConsumer(), startTransactionalConsumer(),
│   │   │                            #   lagThrottle poller, waitIfThrottled()
│   │   │
│   │   ├── admin/
│   │   │   └── ops.ts               # AdminOps: listConsumerGroups(), describeTopics(),
│   │   │                            #   deleteRecords(), resetOffsets(), seekToOffset(),
│   │   │                            #   seekToTimestamp(), getConsumerLag(), ensureTopic()
│   │   │
│   │   ├── producer/
│   │   │   └── ops.ts               # buildPayload() — JSON serialise + schema.parse();
│   │   │                            #   sendMessage / sendBatch / transaction / sendTombstone;
│   │   │                            #   schema registry lookup for strictSchemas mode
│   │   │
│   │   ├── consumer/
│   │   │   ├── ops.ts               # setupConsumer() — librdkafka Consumer factory, rebalance
│   │   │   │                        #   hooks, subscribe with retries, autoCommit config;
│   │   │   │                        #   startConsumer() / startBatchConsumer() orchestration
│   │   │   ├── handler.ts           # handleEachMessage() / handleEachBatch() — top-level
│   │   │   │                        #   eachMessage/eachBatch callbacks wired to pipeline;
│   │   │   │                        #   EOS main-consumer context for retryTopics mode
│   │   │   ├── pipeline.ts          # executeWithRetry() — dedup → TTL → interceptors →
│   │   │   │                        #   handler → retry/DLQ/lost; sendToDlq(); sendToRetryTopic()
│   │   │   ├── retry-topic.ts       # startRetryTopicConsumers() — spins up N level consumers;
│   │   │   │                        #   startLevelConsumer() — pause/sleep/resume per partition;
│   │   │   │                        #   EOS routing via sendOffsetsToTransaction
│   │   │   ├── subscribe-retry.ts   # subscribeWithRetry() — retries consumer.subscribe() when
│   │   │   │                        #   topic doesn't exist yet (subscribeRetry option)
│   │   │   ├── dlq-replay.ts        # replayDlq() — temp consumer reads DLQ up to high-watermark,
│   │   │   │                        #   strips x-dlq-* headers, re-publishes to original topic
│   │   │   └── queue.ts             # AsyncQueue — bounded async iterator used by consume();
│   │   │                            #   backpressure via queueHighWaterMark (pause/resume)
│   │   │
│   │   └── infra/
│   │       ├── circuit-breaker.manager.ts  # CircuitBreakerManager — per groupId:topic:partition state
│   │       │                        #   machine (CLOSED → OPEN → HALF_OPEN); sliding failure window
│   │       ├── metrics.manager.ts          # MetricsManager — in-process counters (processed / retry /
│   │       │                        #   dlq / dedup) per topic; getMetrics() / resetMetrics()
│   │       └── inflight.tracker.ts         # InFlightTracker — tracks running handlers for graceful
│   │                                #   shutdown drain (disconnect waits for all to settle)
│   │
│   └── __tests__/                   # Unit tests — mocked @confluentinc/kafka-javascript
│       ├── helpers.ts               # buildMockMessage(), mock setup, spy exports (mockSend, …)
│       ├── consumer.spec.ts         # Legacy top-level consumer tests
│       ├── consumer/
│       │   ├── consumer.spec.ts          # startConsumer() core behaviour
│       │   ├── batch-consumer.spec.ts    # startBatchConsumer(), BatchMeta, autoCommit
│       │   ├── consumer-groups.spec.ts   # Multiple groupId, eachMessage/eachBatch conflict guard
│       │   ├── consumer-handle.spec.ts   # ConsumerHandle.stop()
│       │   ├── consume-iterator.spec.ts  # consume() iterator, backpressure, break/return
│       │   ├── retry.spec.ts             # In-process retry, backoff, maxRetries
│       │   ├── retry-topic.spec.ts       # Retry topic chain, EOS routing, level consumers
│       │   ├── deduplication.spec.ts     # Lamport clock dedup, strategies (drop/dlq/topic)
│       │   ├── interceptors.spec.ts      # ConsumerInterceptor before/after/onError hooks
│       │   ├── dlq-replay.spec.ts        # replayDlq(), dryRun, filter, targetTopic
│       │   ├── read-snapshot.spec.ts     # readSnapshot(), tombstones, multi-partition, schema, HWM
│       │   ├── checkpoint.spec.ts        # checkpointOffsets(), restoreFromCheckpoint(), timestamp selection
│       │   ├── window-consumer.spec.ts   # startWindowConsumer(), size/time triggers, shutdown flush
│       │   ├── router.spec.ts            # startRoutedConsumer(), route dispatch, fallback, skip
│       │   ├── transactional-consumer.spec.ts  # startTransactionalConsumer(), EOS commit/abort, tx.send
│       │   ├── ttl.spec.ts               # messageTtlMs, onTtlExpired, TTL→DLQ routing
│       │   ├── message-lost.spec.ts      # onMessageLost — handler error, validation, DLQ failure
│       │   ├── handler-timeout.spec.ts   # handlerTimeoutMs warning
│       │   └── rebalance.spec.ts         # onRebalance assign/revoke callbacks
│       ├── producer/
│       │   ├── producer.spec.ts          # sendMessage(), sendBatch(), sendTombstone(), compression
│       │   ├── transaction.spec.ts       # transaction(), tx.send(), tx.sendBatch(), rollback
│       │   ├── schema.spec.ts            # Schema validation on send/consume, strictSchemas
│       │   ├── topic.spec.ts             # topic() descriptor, TopicsFrom, schema registry
│       │   └── lag-throttle.spec.ts      # lagThrottle option, threshold, maxWaitMs, poll errors
│       ├── admin/
│       │   ├── admin.spec.ts             # listConsumerGroups(), describeTopics(), deleteRecords(),
│       │   │                             #   resetOffsets(), seekToOffset(), seekToTimestamp()
│       │   └── consumer-lag.spec.ts      # getConsumerLag()
│       └── infra/
│           ├── circuit-breaker.spec.ts       # CircuitBreaker state machine, getCircuitState()
│           ├── errors.spec.ts                # Error class hierarchy and properties
│           ├── instrumentation.spec.ts       # KafkaInstrumentation hooks, wrap/cleanup composition
│           ├── otel.spec.ts                  # otelInstrumentation(), traceparent propagation
│           ├── metrics-counters.spec.ts      # getMetrics(), resetMetrics(), per-topic counters
│           └── metrics-observability.spec.ts # onMessage/onRetry/onDlq/onDuplicate hooks
│
├── nest/                            # NestJS adapter — depends on @nestjs/common, reflect-metadata
│   ├── kafka.module.ts              # KafkaModule.register() / registerAsync(); DynamicModule,
│   │                                #   isGlobal, named clients; onModuleInit / onModuleDestroy
│   ├── kafka.explorer.ts            # Auto-discovers @SubscribeTo() methods across all providers
│   │                                #   at startup and calls startConsumer / startBatchConsumer
│   ├── kafka.decorator.ts           # @SubscribeTo(topic, options) method decorator;
│   │                                #   @InjectKafkaClient(name?) parameter decorator
│   ├── kafka.health.ts              # KafkaHealthIndicator.check() — wraps kafka.checkStatus()
│   ├── kafka.constants.ts           # DI token constants (KAFKA_CLIENT, KAFKA_OPTIONS)
│   └── __tests__/
│       ├── kafka.decorator.spec.ts  # @SubscribeTo / @InjectKafkaClient metadata
│       ├── kafka.explorer.spec.ts   # Explorer discovery and subscription wiring
│       └── kafka.health.spec.ts     # KafkaHealthIndicator up/down responses
│
├── testing/                         # Testing utilities — no runtime Kafka deps
│   ├── index.ts                     # Re-exports createMockKafkaClient, KafkaTestContainer
│   ├── client.mock.ts               # createMockKafkaClient<T>() — jest.fn() on every
│   │                                #   IKafkaClient method with sensible defaults
│   ├── test.container.ts            # KafkaTestContainer — thin @testcontainers/kafka wrapper;
│   │                                #   transaction coordinator warmup, topic pre-creation
│   └── __tests__/
│       ├── client.mock.spec.ts      # Mock client method stubs and overrides
│       └── test.container.spec.ts   # Container start/stop lifecycle
│
├── integration/                     # Integration tests — require Docker (testcontainers)
│   ├── global-setup.ts              # Start shared Kafka container before all suites
│   ├── global-teardown.ts           # Stop container after all suites
│   ├── helpers.ts                   # createClient(), waitForMessages(), unique topic names
│   ├── basic.integration.spec.ts              # Send/receive, headers, batch, fromBeginning
│   ├── consumer.integration.spec.ts           # startConsumer(), pause/resume, stopConsumer()
│   ├── transaction.integration.spec.ts        # Atomic sends, rollback on error
│   ├── retry.integration.spec.ts              # In-process retry, retryTopics chain, DLQ
│   ├── deduplication.integration.spec.ts      # Lamport clock dedup with real broker
│   ├── consumer-lag.integration.spec.ts       # getConsumerLag() against real offsets
│   ├── consumer-handle.integration.spec.ts    # ConsumerHandle.stop() lifecycle
│   ├── graceful-shutdown.integration.spec.ts  # disconnect() drains in-flight handlers
│   ├── schema.integration.spec.ts             # Schema validation send+consume round-trip
│   ├── otel.integration.spec.ts               # OpenTelemetry span propagation end-to-end
│   └── chaos.integration.spec.ts              # Fault injection — broker restarts, rebalances
│
└── __mocks__/
    └── @confluentinc/
        └── kafka-javascript.ts      # Manual Jest mock — Kafka, Producer, Consumer stubs;
                                     #   mockSend, mockTxSend, mockCommit, mockSeek, …
```

All exported types and methods have JSDoc comments — your IDE will show inline docs and autocomplete.

## License

[MIT](LICENSE)
