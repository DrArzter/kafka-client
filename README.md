# @drarzter/kafka-client

[![npm version](https://img.shields.io/npm/v/@drarzter/kafka-client)](https://www.npmjs.com/package/@drarzter/kafka-client)
[![CI](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml/badge.svg)](https://github.com/drarzter/kafka-client/actions/workflows/publish.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Type-safe Kafka client wrapper for NestJS. Built on top of [kafkajs](https://kafka.js.org/).

## What is this?

An opinionated wrapper around kafkajs that integrates with NestJS as a DynamicModule. Not a full-featured framework — just a clean, typed abstraction for producing and consuming Kafka messages.

## Why?

- **Typed topics** — you define a map of topic -> message shape, and the compiler won't let you send wrong data to wrong topic
- **NestJS-native** — `register()` / `registerAsync()`, DI injection, lifecycle hooks out of the box
- **Idempotent producer** — `acks: -1`, `idempotent: true` by default
- **Retry + DLQ** — configurable retries with backoff, dead letter queue for failed messages
- **Batch sending** — send multiple messages in a single request
- **Partition key support** — route related messages to the same partition
- **Custom headers** — attach metadata headers to messages
- **Transactions** — exactly-once semantics with `producer.transaction()`
- **Consumer interceptors** — before/after/onError hooks for message processing
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
    }),
  ],
})
export class OrdersModule {}
```

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

## Consumer options

| Option | Default | Description |
|--------|---------|-------------|
| `fromBeginning` | `false` | Read from the beginning of the topic |
| `autoCommit` | `true` | Auto-commit offsets |
| `retry.maxRetries` | — | Number of retry attempts |
| `retry.backoffMs` | `1000` | Base delay between retries (multiplied by attempt number) |
| `dlq` | `false` | Send to `{topic}.dlq` after all retries exhausted |
| `interceptors` | `[]` | Array of before/after/onError hooks |

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

## Project structure

```
src/
├── client/         # KafkaClient, types, interfaces
├── module/         # KafkaModule, KafkaExplorer, DI constants
├── decorators/     # @InjectKafkaClient(), @SubscribeTo()
├── health/         # KafkaHealthIndicator
└── index.ts        # Public API re-exports
```

All exported types and methods have JSDoc comments — your IDE will show inline docs and autocomplete.

## License

[MIT](LICENSE)
