# @drarzter/kafka-client

[![npm version](https://img.shields.io/npm/v/@drarzter/kafka-client)](https://www.npmjs.com/package/@drarzter/kafka-client)
[![CI](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml/badge.svg)](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Type-safe Kafka client wrapper for NestJS. Built on top of [kafkajs](https://kafka.js.org/).

## What is this?

An opinionated wrapper around kafkajs that integrates with NestJS as a DynamicModule. Not a full-featured framework — just a clean, typed abstraction for producing and consuming Kafka messages.

## Why?

- **Typed topics** — you define a map of topic -> message shape, and the compiler won't let you send wrong data to wrong topic
- **Topic descriptors** — `topic()` DX sugar lets you define topics as standalone typed objects instead of string keys
- **NestJS-native** — `register()` / `registerAsync()`, DI injection, lifecycle hooks out of the box
- **Idempotent producer** — `acks: -1`, `idempotent: true` by default
- **Retry + DLQ** — configurable retries with backoff, dead letter queue for failed messages
- **Batch sending** — send multiple messages in a single request
- **Batch consuming** — `startBatchConsumer()` for high-throughput `eachBatch` processing
- **Partition key support** — route related messages to the same partition
- **Custom headers** — attach metadata headers to messages
- **Transactions** — exactly-once semantics with `producer.transaction()`
- **Consumer interceptors** — before/after/onError hooks for message processing
- **Auto-create topics** — `autoCreateTopics: true` for dev mode — no need to pre-create topics
- **Error classes** — `KafkaProcessingError` and `KafkaRetryExhaustedError` with topic, message, and attempt metadata
- **Health check** — built-in health indicator for monitoring
- **Multiple consumer groups** — named clients for different bounded contexts
- **Declarative & imperative** — use `@SubscribeTo()` decorator or `startConsumer()` directly

## Installation

```bash
npm install @drarzter/kafka-client
# or
pnpm add @drarzter/kafka-client
```

Peer dependencies: `@nestjs/common`, `@nestjs/core`, `reflect-metadata`, `rxjs`

## Quick start

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
import { InjectKafkaClient, KafkaClient, SubscribeTo } from '@drarzter/kafka-client';
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
  async onHello(message: MyTopics['hello']) {
    console.log('Received:', message.text);
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

export const OrderCreated = topic('order.created')<{
  orderId: string;
  userId: string;
  amount: number;
}>();

export const OrderCompleted = topic('order.completed')<{
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
async handleOrder(message: OrdersTopicMap['order.created']) { ... }

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

`autoCreateTopics` calls `admin.createTopics()` (idempotent — no-op if topic already exists) before the first send/consume for each topic. Useful in development, not recommended for production.

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
  async handleOrderCreated(message: OrdersTopicMap['order.created'], topic: string) {
    console.log('New order:', message.orderId);
  }

  @SubscribeTo('order.completed', { retry: { maxRetries: 3 }, dlq: true })
  async handleOrderCompleted(message: OrdersTopicMap['order.completed'], topic: string) {
    console.log('Order completed:', message.orderId);
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
      async (message, topic) => {
        console.log(`${topic}:`, message);
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

Override the default consumer group for specific consumers. Each unique `groupId` creates a separate kafkajs Consumer internally:

```typescript
// Default group from constructor
await kafka.startConsumer(['orders'], handler);

// Custom group — receives its own copy of messages
await kafka.startConsumer(['orders'], auditHandler, { groupId: 'orders-audit' });

// Works with @SubscribeTo too
@SubscribeTo('orders', { groupId: 'orders-audit' })
async auditOrders(message) { ... }
```

**Important:** You cannot mix `eachMessage` and `eachBatch` consumers on the same `groupId`. The library throws a clear error if you try:

```text
Cannot use eachBatch on consumer group "my-group" — it is already running with eachMessage.
Use a different groupId for this consumer.
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
async handlePayment(message: PaymentsTopicMap['payment.received']) {
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

Process messages in batches for higher throughput. The handler receives an array of parsed messages and a `BatchMeta` object with offset management controls:

```typescript
await this.kafka.startBatchConsumer(
  ['order.created'],
  async (messages, topic, meta) => {
    // messages: OrdersTopicMap['order.created'][]
    for (const msg of messages) {
      await processOrder(msg);
      meta.resolveOffset(/* ... */);
    }
    await meta.commitOffsetsIfNecessary();
  },
  { retry: { maxRetries: 3 }, dlq: true },
);
```

With `@SubscribeTo()`:

```typescript
@SubscribeTo('order.created', { batch: true })
async handleOrders(messages: OrdersTopicMap['order.created'][], topic: string) {
  // messages is an array
}
```

Schema validation runs per-message — invalid messages are skipped (DLQ'd if enabled), valid ones are passed to the handler. Retry applies to the whole batch.

`BatchMeta` exposes: `partition`, `highWatermark`, `heartbeat()`, `resolveOffset(offset)`, `commitOffsetsIfNecessary()`.

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

`tx.sendBatch()` is also available inside transactions.

## Consumer interceptors

Add before/after/onError hooks to message processing:

```typescript
import { ConsumerInterceptor } from '@drarzter/kafka-client';

const loggingInterceptor: ConsumerInterceptor<OrdersTopicMap> = {
  before: (message, topic) => {
    console.log(`Processing ${topic}`, message);
  },
  after: (message, topic) => {
    console.log(`Done ${topic}`);
  },
  onError: (message, topic, error) => {
    console.error(`Failed ${topic}:`, error.message);
  },
};

await this.kafka.startConsumer(['order.created'], handler, {
  interceptors: [loggingInterceptor],
});
```

Multiple interceptors run in order. All hooks are optional.

## Options reference

### Send options

Options for `sendMessage()` — the third argument:

| Option    | Default | Description                                      |
|-----------|---------|--------------------------------------------------|
| `key`     | —       | Partition key for message routing                 |
| `headers` | —       | Custom metadata headers (`Record<string, string>`) |

`sendBatch()` accepts `key` and `headers` per message inside the array items.

### Consumer options

| Option | Default | Description |
|--------|---------|-------------|
| `groupId` | constructor value | Override consumer group for this subscription |
| `fromBeginning` | `false` | Read from the beginning of the topic |
| `autoCommit` | `true` | Auto-commit offsets |
| `retry.maxRetries` | — | Number of retry attempts |
| `retry.backoffMs` | `1000` | Base delay between retries (multiplied by attempt number) |
| `dlq` | `false` | Send to `{topic}.dlq` after all retries exhausted |
| `interceptors` | `[]` | Array of before/after/onError hooks |
| `batch` | `false` | (decorator only) Use `startBatchConsumer` instead of `startConsumer` |
| `subscribeRetry.retries` | `5` | Max attempts for `consumer.subscribe()` when topic doesn't exist yet |
| `subscribeRetry.backoffMs` | `5000` | Delay between subscribe retry attempts (ms) |

### Module options

Passed to `KafkaModule.register()` or returned from `registerAsync()` factory:

| Option | Default | Description |
|--------|---------|-------------|
| `clientId` | — | Kafka client identifier (required) |
| `groupId` | — | Default consumer group ID (required) |
| `brokers` | — | Array of broker addresses (required) |
| `name` | — | Named client identifier for multi-client setups |
| `isGlobal` | `false` | Make the client available in all modules without re-importing |
| `autoCreateTopics` | `false` | Auto-create topics on first send (dev only) |
| `strictSchemas` | `true` | Validate string topic keys against schemas registered via TopicDescriptor |

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
  onError: (message, topic, error) => {
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
  onError: (message, topic, error) => {
    if (error instanceof KafkaValidationError) {
      console.log(`Bad message on ${error.topic}:`, error.cause?.message);
    }
  },
};
```

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

// Without schema — explicit generic (still works)
export const OrderAudit = topic('order.audit')<{ orderId: string; action: string }>();

export type MyTopics = TopicsFrom<typeof OrderCreated | typeof OrderAudit>;
```

### How it works

**On send** — `sendMessage`, `sendBatch`, and `transaction` call `schema.parse(message)` before serializing. Invalid messages throw immediately (the schema library's error, e.g. `ZodError`):

```typescript
// This throws ZodError — amount must be positive
await kafka.sendMessage(OrderCreated, { orderId: '1', userId: '2', amount: -5 });
```

**On consume** — after `JSON.parse`, the consumer validates each message against the schema. Invalid messages are:

1. Logged as errors
2. Sent to DLQ if `dlq: true`
3. Passed to `onError` interceptors as `KafkaValidationError`
4. Skipped (handler is NOT called)

```typescript
@SubscribeTo(OrderCreated, { dlq: true })
async handleOrder(message) {
  // `message` is guaranteed to match the schema
  console.log(message.orderId); // string — validated at runtime
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

Any object with `parse(data: unknown): T` works:

```typescript
import { SchemaLike } from '@drarzter/kafka-client';

const customValidator: SchemaLike<{ id: string }> = {
  parse(data: unknown) {
    const d = data as any;
    if (typeof d?.id !== 'string') throw new Error('id must be a string');
    return { id: d.id };
  },
};

const MyTopic = topic('my.topic').schema(customValidator);
```

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

Unit tests (mocked kafkajs):

```bash
npm test
```

Integration tests with a real Kafka broker via [testcontainers](https://node.testcontainers.org/) (requires Docker):

```bash
npm run test:integration
```

The integration suite spins up a single-node KRaft Kafka container and tests sending, consuming, batching, transactions, retry + DLQ, interceptors, health checks, and `fromBeginning` — no mocks.

Both suites run in CI on every push to `main`.

## Project structure

```
src/
├── client/         # KafkaClient, types, topic(), error classes
├── module/         # KafkaModule, KafkaExplorer, DI constants
├── decorators/     # @InjectKafkaClient(), @SubscribeTo()
├── health/         # KafkaHealthIndicator
└── index.ts        # Public API re-exports
```

All exported types and methods have JSDoc comments — your IDE will show inline docs and autocomplete.

## License

[MIT](LICENSE)
