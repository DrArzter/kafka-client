# @drarzter/kafka-client

[![npm version](https://img.shields.io/npm/v/@drarzter/kafka-client)](https://www.npmjs.com/package/@drarzter/kafka-client)
[![CI](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml/badge.svg)](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Type-safe Kafka client for Node.js. Framework-agnostic core with a first-class NestJS adapter. Built on top of [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript) (librdkafka).

## Table of contents

- [What is this?](#what-is-this)
- [Why?](#why)
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
- [Multiple consumer groups](#multiple-consumer-groups)
- [Partition key](#partition-key)
- [Message headers](#message-headers)
- [Batch sending](#batch-sending)
- [Batch consuming](#batch-consuming)
- [Transactions](#transactions)
- [Consumer interceptors](#consumer-interceptors)
- [Instrumentation](#instrumentation)
- [Options reference](#options-reference)
- [Error classes](#error-classes)
- [Deduplication (Lamport Clock)](#deduplication-lamport-clock)
- [Retry topic chain](#retry-topic-chain)
- [stopConsumer](#stopconsumer)
- [Graceful shutdown](#graceful-shutdown)
- [Consumer handles](#consumer-handles)
- [onMessageLost](#onmessagelost)
- [onRebalance](#onrebalance)
- [Consumer lag](#consumer-lag)
- [Handler timeout warning](#handler-timeout-warning)
- [Schema validation](#schema-validation)
  - [Context-aware validators](#context-aware-validators-schemaparsecontext)
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

See the [Roadmap](./ROADMAP.md) for upcoming features and version history.

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

Two ways — choose what fits your style.

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
const snapshot = kafka.getMetrics();
// { processedCount: number; retryCount: number; dlqCount: number; dedupCount: number }

kafka.resetMetrics(); // reset all counters to zero
```

Counters are incremented in the same code paths that fire the corresponding hooks — they are always active regardless of whether any instrumentation is configured.

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

`sendBatch()` accepts the same options per message inside the array items.

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
| `batch` | `false` | (decorator only) Use `startBatchConsumer` instead of `startConsumer` |
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
| `instrumentation` | `[]` | Client-wide instrumentation hooks (e.g. OTel). Applied to both send and consume paths |
| `transactionalId` | `${clientId}-tx` | Transactional producer ID for `transaction()` calls. Must be unique per producer instance across the cluster — two instances sharing the same ID will be fenced by Kafka. The client logs a warning when the same ID is registered twice within one process |
| `onMessageLost` | — | Called when a message is silently dropped without DLQ — use to alert, log to external systems, or trigger fallback logic |
| `onRebalance` | — | Called on every partition assign/revoke event across all consumers created by this client |

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

- **Durable** — retry messages survive a consumer restart; routing between levels and to DLQ is exactly-once via Kafka transactions
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

> **Delivery guarantee:** routing within the retry chain (retry.N → retry.N+1 and retry.N → DLQ) is **exactly-once** — each routing step is wrapped in a Kafka transaction via `sendOffsetsToTransaction`, so the produce and the consumer offset commit happen atomically. A crash at any point rolls back the transaction: the message is redelivered and the routing is retried, with no duplicate in the next level. If the EOS transaction itself fails (broker unavailable), the offset is not committed and the message stays safely in the retry topic until the broker recovers.
>
> The remaining at-least-once window is at the **main consumer → retry.1** boundary: the main consumer uses `autoCommit: true` by default, so if it crashes after routing to `retry.1` but before autoCommit fires, the message may appear twice in `retry.1`. This is the standard Kafka at-least-once trade-off for any consumer using autoCommit. Design handlers to be idempotent if this edge case is unacceptable.
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

`startConsumer()` and `startBatchConsumer()` return a `ConsumerHandle` instead of `void`. Use it to stop a specific consumer without needing to remember the group ID:

```typescript
const handle = await kafka.startConsumer(['orders'], handler);

console.log(handle.groupId); // e.g. "my-group"

// Later — stop only this consumer, producer stays connected
await handle.stop();
```

`handle.stop()` is equivalent to `kafka.stopConsumer(handle.groupId)`. Useful in lifecycle methods or when you need to conditionally stop one consumer while others keep running.

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

## Project structure

```text
src/
├── client/         # Core — KafkaClient, types, envelope, consumer pipeline, topic(), errors (0 framework deps)
├── nest/           # NestJS adapter — Module, Explorer, decorators, health
├── testing/        # Testing utilities — mock client, testcontainer wrapper
├── core.ts         # Standalone entrypoint (@drarzter/kafka-client/core)
├── otel.ts         # OpenTelemetry entrypoint (@drarzter/kafka-client/otel)
├── testing.ts      # Testing entrypoint (@drarzter/kafka-client/testing)
└── index.ts        # Full entrypoint — core + NestJS adapter
```

All exported types and methods have JSDoc comments — your IDE will show inline docs and autocomplete.

## License

[MIT](LICENSE)
