# @drarzter/kafka-client

Type-safe Kafka client wrapper for NestJS. Built on top of [kafkajs](https://kafka.js.org/).

## What is this?

An opinionated wrapper around kafkajs that integrates with NestJS as a DynamicModule. Not a full-featured framework — just a clean, typed abstraction for producing and consuming Kafka messages.

## Why?

- **Typed topics** — you define a map of topic → message shape, and the compiler won't let you send wrong data to wrong topic
- **NestJS-native** — `register()` / `registerAsync()`, DI injection, lifecycle hooks out of the box
- **Idempotent producer** — `acks: -1`, `idempotent: true` by default
- **Minimal** — ~150 lines of code, no magic

## Installation

```bash
npm install @drarzter/kafka-client
# or
pnpm add @drarzter/kafka-client
```

Peer dependencies: `@nestjs/common`, `reflect-metadata`, `rxjs`

## Usage

### 1. Define your topic map

```typescript
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
import { Injectable, Inject } from '@nestjs/common';
import { KafkaClient, KAFKA_CLIENT } from '@drarzter/kafka-client';
import { OrdersTopicMap } from './orders.types';

@Injectable()
export class OrdersService {
  constructor(
    @Inject(KAFKA_CLIENT)
    private readonly kafka: KafkaClient<OrdersTopicMap>,
  ) {}

  async createOrder() {
    // ✅ type-safe — compiler knows the shape
    await this.kafka.sendMessage('order.created', {
      orderId: '123',
      userId: '456',
      amount: 100,
    });

    // ❌ won't compile — wrong payload for this topic
    // await this.kafka.sendMessage('order.created', { wrong: 'data' });
  }

  async listen() {
    await this.kafka.startConsumer(
      ['order.created'],
      async (message, topic) => {
        console.log(message.orderId); // typed
      },
    );
  }
}
```

### Consumer options

```typescript
await this.kafka.startConsumer(
  ['order.created'],
  handler,
  {
    fromBeginning: false, // default: false
    autoCommit: true,     // default: true
  },
);
```

## License

MIT
