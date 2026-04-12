import type { TopicMapConstraint } from "./common";
import type { IKafkaProducer } from "./producer.interface";
import type { IKafkaConsumer } from "./consumer.interface";
import type { IKafkaAdmin } from "./admin.interface";
import type { IKafkaLifecycle } from "./lifecycle.interface";

export type { IKafkaProducer } from "./producer.interface";
export type { IKafkaConsumer } from "./consumer.interface";
export type { IKafkaAdmin } from "./admin.interface";
export type { IKafkaLifecycle } from "./lifecycle.interface";

/**
 * Full Kafka client interface — the union of all role-specific sub-interfaces.
 *
 * Compose sub-interfaces directly when you need a narrower dependency:
 * ```ts
 * function sendOrder(producer: IKafkaProducer<MyTopics>) { ... }
 * function startWorker(consumer: IKafkaConsumer<MyTopics>) { ... }
 * ```
 */
export interface IKafkaClient<T extends TopicMapConstraint<T>>
  extends IKafkaProducer<T>,
    IKafkaConsumer<T>,
    IKafkaAdmin<T>,
    IKafkaLifecycle {}
