import { KafkaJS } from "@confluentinc/kafka-javascript";
type Kafka = KafkaJS.Kafka;
type Producer = KafkaJS.Producer;
type Consumer = KafkaJS.Consumer;
type Admin = KafkaJS.Admin;
const { Kafka: KafkaClass, logLevel: KafkaLogLevel } = KafkaJS;
import { TopicDescriptor, SchemaLike } from "./topic";
import {
  buildEnvelopeHeaders,
  decodeHeaders,
  extractEnvelope,
  runWithEnvelopeContext,
} from "./envelope";
import type { EventEnvelope } from "./envelope";
import {
  toError,
  sleep,
  parseJsonMessage,
  validateWithSchema,
  executeWithRetry,
  sendToDlq,
  sendToRetryTopic,
  RETRY_HEADER_ATTEMPT,
  RETRY_HEADER_AFTER,
  RETRY_HEADER_MAX_RETRIES,
  RETRY_HEADER_ORIGINAL_TOPIC,
} from "./consumer-pipeline";
import { KafkaRetryExhaustedError } from "./errors";
import { subscribeWithRetry } from "./subscribe-retry";
import type {
  ClientId,
  GroupId,
  SendOptions,
  MessageHeaders,
  BatchMessageItem,
  ConsumerOptions,
  ConsumerHandle,
  TransactionContext,
  TopicMapConstraint,
  IKafkaClient,
  KafkaClientOptions,
  KafkaInstrumentation,
  KafkaLogger,
  BatchMeta,
} from "./types";

// Re-export all types so existing `import { ... } from './kafka.client'` keeps working
export * from "./types";

/**
 * Type-safe Kafka client.
 * Wraps @confluentinc/kafka-javascript (librdkafka) with JSON serialization,
 * retries, DLQ, transactions, and interceptors.
 *
 * @typeParam T - Topic-to-message type mapping for compile-time safety.
 */
export class KafkaClient<
  T extends TopicMapConstraint<T>,
> implements IKafkaClient<T> {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private txProducer: Producer | undefined;
  private readonly consumers = new Map<string, Consumer>();
  private readonly admin: Admin;
  private readonly logger: KafkaLogger;
  private readonly autoCreateTopicsEnabled: boolean;
  private readonly strictSchemasEnabled: boolean;
  private readonly numPartitions: number;
  private readonly ensuredTopics = new Set<string>();
  private readonly defaultGroupId: string;
  private readonly schemaRegistry = new Map<string, SchemaLike>();
  private readonly runningConsumers = new Map<
    string,
    "eachMessage" | "eachBatch"
  >();
  private readonly instrumentation: KafkaInstrumentation[];
  private readonly onMessageLost: KafkaClientOptions["onMessageLost"];
  private readonly onRebalance: KafkaClientOptions["onRebalance"];

  private isAdminConnected = false;
  public readonly clientId: ClientId;

  constructor(
    clientId: ClientId,
    groupId: GroupId,
    brokers: string[],
    options?: KafkaClientOptions,
  ) {
    this.clientId = clientId;
    this.defaultGroupId = groupId;
    this.logger = options?.logger ?? {
      log: (msg) => console.log(`[KafkaClient:${clientId}] ${msg}`),
      warn: (msg, ...args) =>
        console.warn(`[KafkaClient:${clientId}] ${msg}`, ...args),
      error: (msg, ...args) =>
        console.error(`[KafkaClient:${clientId}] ${msg}`, ...args),
    };
    this.autoCreateTopicsEnabled = options?.autoCreateTopics ?? false;
    this.strictSchemasEnabled = options?.strictSchemas ?? true;
    this.numPartitions = options?.numPartitions ?? 1;
    this.instrumentation = options?.instrumentation ?? [];
    this.onMessageLost = options?.onMessageLost;
    this.onRebalance = options?.onRebalance;

    this.kafka = new KafkaClass({
      kafkaJS: {
        clientId: this.clientId,
        brokers,
        logLevel: KafkaLogLevel.ERROR,
      },
    });
    this.producer = this.kafka.producer({
      kafkaJS: {
        acks: -1,
      },
    });
    this.admin = this.kafka.admin();
  }

  // ── Send ─────────────────────────────────────────────────────────

  /** Send a single typed message. Accepts a topic key or a TopicDescriptor. */
  public async sendMessage<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(descriptor: D, message: D["__type"], options?: SendOptions): Promise<void>;
  public async sendMessage<K extends keyof T>(
    topic: K,
    message: T[K],
    options?: SendOptions,
  ): Promise<void>;
  public async sendMessage(
    topicOrDesc: any,
    message: any,
    options: SendOptions = {},
  ): Promise<void> {
    const payload = await this.buildSendPayload(topicOrDesc, [
      {
        value: message,
        key: options.key,
        headers: options.headers,
        correlationId: options.correlationId,
        schemaVersion: options.schemaVersion,
        eventId: options.eventId,
      },
    ]);
    await this.ensureTopic(payload.topic);
    await this.producer.send(payload);
    for (const inst of this.instrumentation) {
      inst.afterSend?.(payload.topic);
    }
  }

  /** Send multiple typed messages in one call. Accepts a topic key or a TopicDescriptor. */
  public async sendBatch<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    descriptor: D,
    messages: Array<BatchMessageItem<D["__type"]>>,
  ): Promise<void>;
  public async sendBatch<K extends keyof T>(
    topic: K,
    messages: Array<BatchMessageItem<T[K]>>,
  ): Promise<void>;
  public async sendBatch(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
  ): Promise<void> {
    const payload = await this.buildSendPayload(topicOrDesc, messages);
    await this.ensureTopic(payload.topic);
    await this.producer.send(payload);
    for (const inst of this.instrumentation) {
      inst.afterSend?.(payload.topic);
    }
  }

  /** Execute multiple sends atomically. Commits on success, aborts on error. */
  public async transaction(
    fn: (ctx: TransactionContext<T>) => Promise<void>,
  ): Promise<void> {
    if (!this.txProducer) {
      this.txProducer = this.kafka.producer({
        kafkaJS: {
          acks: -1,
          idempotent: true,
          transactionalId: `${this.clientId}-tx`,
          maxInFlightRequests: 1,
        },
      });
      await this.txProducer.connect();
    }
    const tx = await this.txProducer.transaction();
    try {
      const ctx: TransactionContext<T> = {
        send: async (
          topicOrDesc: any,
          message: any,
          options: SendOptions = {},
        ) => {
          const payload = await this.buildSendPayload(topicOrDesc, [
            {
              value: message,
              key: options.key,
              headers: options.headers,
              correlationId: options.correlationId,
              schemaVersion: options.schemaVersion,
              eventId: options.eventId,
            },
          ]);
          await this.ensureTopic(payload.topic);
          await tx.send(payload);
        },
        sendBatch: async (
          topicOrDesc: any,
          messages: BatchMessageItem<any>[],
        ) => {
          const payload = await this.buildSendPayload(topicOrDesc, messages);
          await this.ensureTopic(payload.topic);
          await tx.send(payload);
        },
      };
      await fn(ctx);
      await tx.commit();
    } catch (error) {
      try {
        await tx.abort();
      } catch (abortError) {
        this.logger.error(
          "Failed to abort transaction:",
          toError(abortError).message,
        );
      }
      throw error;
    }
  }

  // ── Producer lifecycle ───────────────────────────────────────────

  /** Connect the idempotent producer. Called automatically by `KafkaModule.register()`. */
  public async connectProducer(): Promise<void> {
    await this.producer.connect();
    this.logger.log("Producer connected");
  }

  public async disconnectProducer(): Promise<void> {
    await this.producer.disconnect();
    this.logger.log("Producer disconnected");
  }

  // ── Consumer: eachMessage ────────────────────────────────────────

  /** Subscribe to topics and start consuming messages with the given handler. */
  public async startConsumer<K extends Array<keyof T>>(
    topics: K,
    handleMessage: (envelope: EventEnvelope<T[K[number]]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleMessage: (envelope: EventEnvelope<D["__type"]>) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startConsumer(
    topics: any[],
    handleMessage: (envelope: EventEnvelope<any>) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<ConsumerHandle> {
    if (options.retryTopics && !options.retry) {
      throw new Error(
        "retryTopics requires retry to be configured — set retry.maxRetries to enable the retry topic chain",
      );
    }

    const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachMessage", options);

    const deps = {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
    };
    const timeoutMs = options.handlerTimeoutMs;

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) {
          this.logger.warn(`Received empty message from topic ${topic}`);
          return;
        }

        const raw = message.value.toString();
        const parsed = parseJsonMessage(raw, topic, this.logger);
        if (parsed === null) return;

        const headers = decodeHeaders(message.headers);
        const validated = await validateWithSchema(
          parsed,
          raw,
          topic,
          schemaMap,
          interceptors,
          dlq,
          { ...deps, originalHeaders: headers },
        );
        if (validated === null) return;

        const envelope = extractEnvelope(
          validated,
          headers,
          topic,
          partition,
          message.offset,
        );

        await executeWithRetry(
          () => {
            const fn = () =>
              runWithEnvelopeContext(
                {
                  correlationId: envelope.correlationId,
                  traceparent: envelope.traceparent,
                },
                () => handleMessage(envelope),
              );
            return timeoutMs
              ? this.wrapWithTimeoutWarning(fn, timeoutMs, topic)
              : fn();
          },
          {
            envelope,
            rawMessages: [raw],
            interceptors,
            dlq,
            retry,
            retryTopics: options.retryTopics,
          },
          deps,
        );
      },
    });

    this.runningConsumers.set(gid, "eachMessage");

    if (options.retryTopics && retry) {
      await this.startRetryTopicConsumers(
        topicNames,
        gid,
        handleMessage,
        retry,
        dlq,
        interceptors,
        schemaMap,
      );
    }

    return { groupId: gid, stop: () => this.stopConsumer(gid) };
  }

  // ── Consumer: eachBatch ──────────────────────────────────────────

  /** Subscribe to topics and consume messages in batches. */
  public async startBatchConsumer<K extends Array<keyof T>>(
    topics: K,
    handleBatch: (
      envelopes: EventEnvelope<T[K[number]]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startBatchConsumer<
    D extends TopicDescriptor<string & keyof T, T[string & keyof T]>,
  >(
    topics: D[],
    handleBatch: (
      envelopes: EventEnvelope<D["__type"]>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options?: ConsumerOptions<T>,
  ): Promise<ConsumerHandle>;
  public async startBatchConsumer(
    topics: any[],
    handleBatch: (
      envelopes: EventEnvelope<any>[],
      meta: BatchMeta,
    ) => Promise<void>,
    options: ConsumerOptions<T> = {},
  ): Promise<ConsumerHandle> {
    const { consumer, schemaMap, gid, dlq, interceptors, retry } =
      await this.setupConsumer(topics, "eachBatch", options);

    const deps = {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
    };
    const timeoutMs = options.handlerTimeoutMs;

    await consumer.run({
      eachBatch: async ({
        batch,
        heartbeat,
        resolveOffset,
        commitOffsetsIfNecessary,
      }) => {
        const envelopes: EventEnvelope<any>[] = [];
        const rawMessages: string[] = [];

        for (const message of batch.messages) {
          if (!message.value) {
            this.logger.warn(
              `Received empty message from topic ${batch.topic}`,
            );
            continue;
          }

          const raw = message.value.toString();
          const parsed = parseJsonMessage(raw, batch.topic, this.logger);
          if (parsed === null) continue;

          const headers = decodeHeaders(message.headers);
          const validated = await validateWithSchema(
            parsed,
            raw,
            batch.topic,
            schemaMap,
            interceptors,
            dlq,
            { ...deps, originalHeaders: headers },
          );
          if (validated === null) continue;
          envelopes.push(
            extractEnvelope(
              validated,
              headers,
              batch.topic,
              batch.partition,
              message.offset,
            ),
          );
          rawMessages.push(raw);
        }

        if (envelopes.length === 0) return;

        const meta: BatchMeta = {
          partition: batch.partition,
          highWatermark: batch.highWatermark,
          heartbeat,
          resolveOffset,
          commitOffsetsIfNecessary,
        };

        await executeWithRetry(
          () => {
            const fn = () => handleBatch(envelopes, meta);
            return timeoutMs
              ? this.wrapWithTimeoutWarning(fn, timeoutMs, batch.topic)
              : fn();
          },
          {
            envelope: envelopes,
            rawMessages: batch.messages
              .filter((m) => m.value)
              .map((m) => m.value!.toString()),
            interceptors,
            dlq,
            retry,
            isBatch: true,
          },
          deps,
        );
      },
    });

    this.runningConsumers.set(gid, "eachBatch");

    return { groupId: gid, stop: () => this.stopConsumer(gid) };
  }

  // ── Consumer lifecycle ───────────────────────────────────────────

  public async stopConsumer(groupId?: string): Promise<void> {
    if (groupId !== undefined) {
      const consumer = this.consumers.get(groupId);
      if (!consumer) {
        this.logger.warn(
          `stopConsumer: no active consumer for group "${groupId}"`,
        );
        return;
      }
      await consumer.disconnect().catch(() => {});
      this.consumers.delete(groupId);
      this.runningConsumers.delete(groupId);
      this.logger.log(`Consumer disconnected: group "${groupId}"`);
    } else {
      const tasks = Array.from(this.consumers.values()).map((c) =>
        c.disconnect().catch(() => {}),
      );
      await Promise.allSettled(tasks);
      this.consumers.clear();
      this.runningConsumers.clear();
      this.logger.log("All consumers disconnected");
    }
  }

  /**
   * Query consumer group lag per partition.
   * Lag = broker high-watermark − last committed offset.
   * A committed offset of -1 (nothing committed yet) counts as full lag.
   */
  public async getConsumerLag(
    groupId?: string,
  ): Promise<Array<{ topic: string; partition: number; lag: number }>> {
    const gid = groupId ?? this.defaultGroupId;
    if (!this.isAdminConnected) {
      await this.admin.connect();
      this.isAdminConnected = true;
    }

    const committedByTopic = await this.admin.fetchOffsets({ groupId: gid });
    const result: Array<{ topic: string; partition: number; lag: number }> = [];

    for (const { topic, partitions } of committedByTopic) {
      const brokerOffsets = await this.admin.fetchTopicOffsets(topic);

      for (const { partition, offset } of partitions) {
        const broker = brokerOffsets.find((o) => o.partition === partition);
        if (!broker) continue;

        const committed = parseInt(offset, 10);
        const high = parseInt(broker.high, 10);
        // committed === -1 means the group has never committed for this partition
        const lag = committed === -1 ? high : Math.max(0, high - committed);
        result.push({ topic, partition, lag });
      }
    }

    return result;
  }

  /** Check broker connectivity and return status, clientId, and available topics. */
  public async checkStatus(): Promise<{
    status: "up";
    clientId: string;
    topics: string[];
  }> {
    if (!this.isAdminConnected) {
      await this.admin.connect();
      this.isAdminConnected = true;
    }
    const topics = await this.admin.listTopics();
    return { status: "up", clientId: this.clientId, topics };
  }

  public getClientId(): ClientId {
    return this.clientId;
  }

  /** Gracefully disconnect producer, all consumers, and admin. */
  public async disconnect(): Promise<void> {
    const tasks: Promise<void>[] = [this.producer.disconnect()];
    if (this.txProducer) {
      tasks.push(this.txProducer.disconnect());
      this.txProducer = undefined;
    }
    for (const consumer of this.consumers.values()) {
      tasks.push(consumer.disconnect());
    }
    if (this.isAdminConnected) {
      tasks.push(this.admin.disconnect());
      this.isAdminConnected = false;
    }
    await Promise.allSettled(tasks);
    this.consumers.clear();
    this.runningConsumers.clear();
    this.logger.log("All connections closed");
  }

  // ── Retry topic chain ────────────────────────────────────────────

  /**
   * Auto-start companion consumers on `<topic>.retry` for each original topic.
   * Called by `startConsumer` when `retryTopics: true`.
   *
   * Flow per message:
   *   1. Sleep until `x-retry-after` (scheduled by the main consumer or previous retry hop)
   *   2. Call the original handler
   *   3. On failure: if retries remain → re-send to `<originalTopic>.retry` with incremented attempt
   *                  if exhausted       → DLQ or onMessageLost
   */
  private async startRetryTopicConsumers(
    originalTopics: string[],
    originalGroupId: string,
    handleMessage: (envelope: EventEnvelope<any>) => Promise<void>,
    retry: { maxRetries: number; backoffMs?: number; maxBackoffMs?: number },
    dlq: boolean,
    interceptors: any[],
    schemaMap: Map<string, SchemaLike>,
  ): Promise<void> {
    const retryTopicNames = originalTopics.map((t) => `${t}.retry`);
    const retryGroupId = `${originalGroupId}-retry`;
    const backoffMs = retry.backoffMs ?? 1_000;
    const maxBackoffMs = retry.maxBackoffMs ?? 30_000;
    const deps = {
      logger: this.logger,
      producer: this.producer,
      instrumentation: this.instrumentation,
      onMessageLost: this.onMessageLost,
    };

    for (const rt of retryTopicNames) {
      await this.ensureTopic(rt);
    }

    const consumer = this.getOrCreateConsumer(retryGroupId, false, true);
    await consumer.connect();
    await subscribeWithRetry(consumer, retryTopicNames, this.logger);

    await consumer.run({
      eachMessage: async ({ topic: retryTopic, partition, message }) => {
        if (!message.value) return;

        const raw = message.value.toString();
        const parsed = parseJsonMessage(raw, retryTopic, this.logger);
        if (parsed === null) return;

        const headers = decodeHeaders(message.headers);
        const originalTopic =
          (headers[RETRY_HEADER_ORIGINAL_TOPIC] as string | undefined) ??
          retryTopic.replace(/\.retry$/, "");
        const currentAttempt = parseInt(
          (headers[RETRY_HEADER_ATTEMPT] as string | undefined) ?? "1",
          10,
        );
        const maxRetries = parseInt(
          (headers[RETRY_HEADER_MAX_RETRIES] as string | undefined) ??
            String(retry.maxRetries),
          10,
        );
        const retryAfter = parseInt(
          (headers[RETRY_HEADER_AFTER] as string | undefined) ?? "0",
          10,
        );

        // Pause only this partition for the scheduled delay so that other
        // topic-partitions on the same retry consumer continue processing.
        const remaining = retryAfter - Date.now();
        if (remaining > 0) {
          consumer.pause([{ topic: retryTopic, partitions: [partition] }]);
          await sleep(remaining);
          consumer.resume([{ topic: retryTopic, partitions: [partition] }]);
        }

        // Validate schema against original topic's schema (if any)
        const validated = await validateWithSchema(
          parsed,
          raw,
          originalTopic,
          schemaMap,
          interceptors,
          dlq,
          { ...deps, originalHeaders: headers },
        );
        if (validated === null) return;

        // Build envelope with originalTopic so correlationId/traceparent are correct
        const envelope = extractEnvelope(
          validated,
          headers,
          originalTopic,
          partition,
          message.offset,
        );

        try {
          // Instrumentation: beforeConsume
          const cleanups: (() => void)[] = [];
          for (const inst of this.instrumentation) {
            const c = inst.beforeConsume?.(envelope);
            if (typeof c === "function") cleanups.push(c);
          }
          for (const interceptor of interceptors)
            await interceptor.before?.(envelope);

          await runWithEnvelopeContext(
            {
              correlationId: envelope.correlationId,
              traceparent: envelope.traceparent,
            },
            () => handleMessage(envelope),
          );

          for (const interceptor of interceptors)
            await interceptor.after?.(envelope);
          for (const cleanup of cleanups) cleanup();
        } catch (error) {
          const err = toError(error);
          const nextAttempt = currentAttempt + 1;
          const exhausted = currentAttempt >= maxRetries;

          for (const inst of this.instrumentation)
            inst.onConsumeError?.(envelope, err);

          const reportedError =
            exhausted && maxRetries > 1
              ? new KafkaRetryExhaustedError(
                  originalTopic,
                  [envelope.payload],
                  maxRetries,
                  { cause: err },
                )
              : err;
          for (const interceptor of interceptors) {
            await interceptor.onError?.(envelope, reportedError);
          }

          this.logger.error(
            `Retry consumer error for ${originalTopic} (attempt ${currentAttempt}/${maxRetries}):`,
            err.stack,
          );

          if (!exhausted) {
            // currentAttempt is the hop that just failed (1-based).
            // Before hop N+1 we want backoffMs * 2^N, so exponent = currentAttempt.
            const cap = Math.min(backoffMs * 2 ** currentAttempt, maxBackoffMs);
            const delay = Math.floor(Math.random() * cap);
            await sendToRetryTopic(
              originalTopic,
              [raw],
              nextAttempt,
              maxRetries,
              delay,
              headers,
              deps,
            );
          } else if (dlq) {
            await sendToDlq(originalTopic, raw, deps, {
              error: err,
              // +1 to account for the main consumer's initial attempt before
              // routing to the retry topic, making this consistent with the
              // in-process retry path where attempt counts all tries.
              attempt: currentAttempt + 1,
              originalHeaders: headers,
            });
          } else {
            await deps.onMessageLost?.({
              topic: originalTopic,
              error: err,
              attempt: currentAttempt,
              headers,
            });
          }
        }
      },
    });

    this.runningConsumers.set(retryGroupId, "eachMessage");

    // Block until the retry consumer has received at least one partition assignment.
    // consumer.run() starts the group-join protocol asynchronously; without this wait,
    // the caller may send a message to the original topic before the retry consumer
    // has established its "latest" offset on the (initially empty) retry partition,
    // causing it to skip any retry messages produced in that window.
    await this.waitForPartitionAssignment(consumer, retryTopicNames);

    this.logger.log(
      `Retry topic consumers started for: ${originalTopics.join(", ")} (group: ${retryGroupId})`,
    );
  }

  // ── Private helpers ──────────────────────────────────────────────

  /**
   * Poll `consumer.assignment()` until the consumer has received at least one
   * partition for the given topics, then return. Logs a warning and returns
   * (rather than throwing) on timeout so that a slow broker does not break
   * the caller — in the worst case a message sent immediately after would be
   * missed, which is the same behaviour as before this guard was added.
   */
  private async waitForPartitionAssignment(
    consumer: Consumer,
    topics: string[],
    timeoutMs = 10_000,
  ): Promise<void> {
    const topicSet = new Set(topics);
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      try {
        const assigned: { topic: string; partition: number }[] =
          consumer.assignment();
        if (assigned.some((a) => topicSet.has(a.topic))) return;
      } catch {
        // consumer.assignment() throws if not yet in CONNECTED state — keep polling
      }
      await sleep(200);
    }
    this.logger.warn(
      `Retry consumer did not receive partition assignments for [${topics.join(", ")}] within ${timeoutMs}ms`,
    );
  }

  private getOrCreateConsumer(
    groupId: string,
    fromBeginning: boolean,
    autoCommit: boolean,
  ): Consumer {
    if (!this.consumers.has(groupId)) {
      const config: Parameters<typeof this.kafka.consumer>[0] = {
        kafkaJS: { groupId, fromBeginning, autoCommit },
      };

      if (this.onRebalance) {
        const onRebalance = this.onRebalance;
        // rebalance_cb is called by librdkafka on every partition assign/revoke.
        // err.code -175 = ERR__ASSIGN_PARTITIONS, -174 = ERR__REVOKE_PARTITIONS.
        // The library handles the actual assign/unassign in its finally block regardless
        // of what this callback does, so we only need it for the side-effect notification.
        (config as any)["rebalance_cb"] = (err: any, assignment: any[]) => {
          const type = err.code === -175 ? "assign" : "revoke";
          try {
            onRebalance(
              type,
              assignment.map((p) => ({
                topic: p.topic,
                partition: p.partition,
              })),
            );
          } catch (e) {
            this.logger.warn(
              `onRebalance callback threw: ${(e as Error).message}`,
            );
          }
        };
      }

      this.consumers.set(groupId, this.kafka.consumer(config));
    }
    return this.consumers.get(groupId)!;
  }

  /**
   * Start a timer that logs a warning if `fn` hasn't resolved within `timeoutMs`.
   * The handler itself is not cancelled — the warning is diagnostic only.
   */
  private wrapWithTimeoutWarning<R>(
    fn: () => Promise<R>,
    timeoutMs: number,
    topic: string,
  ): Promise<R> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const promise = fn().finally(() => {
      if (timer !== undefined) clearTimeout(timer);
    });
    timer = setTimeout(() => {
      this.logger.warn(
        `Handler for topic "${topic}" has not resolved after ${timeoutMs}ms — possible stuck handler`,
      );
    }, timeoutMs);
    return promise;
  }

  private resolveTopicName(topicOrDescriptor: unknown): string {
    if (typeof topicOrDescriptor === "string") return topicOrDescriptor;
    if (
      topicOrDescriptor &&
      typeof topicOrDescriptor === "object" &&
      "__topic" in topicOrDescriptor
    ) {
      return (topicOrDescriptor as TopicDescriptor).__topic;
    }
    return String(topicOrDescriptor);
  }

  private async ensureTopic(topic: string): Promise<void> {
    if (!this.autoCreateTopicsEnabled || this.ensuredTopics.has(topic)) return;
    if (!this.isAdminConnected) {
      await this.admin.connect();
      this.isAdminConnected = true;
    }
    await this.admin.createTopics({
      topics: [{ topic, numPartitions: this.numPartitions }],
    });
    this.ensuredTopics.add(topic);
  }

  /** Register schema from descriptor into global registry (side-effect). */
  private registerSchema(topicOrDesc: any): void {
    if (topicOrDesc?.__schema) {
      const topic = this.resolveTopicName(topicOrDesc);
      this.schemaRegistry.set(topic, topicOrDesc.__schema);
    }
  }

  /** Validate message against schema. Pure — no side-effects on registry. */
  private async validateMessage(topicOrDesc: any, message: any): Promise<any> {
    if (topicOrDesc?.__schema) {
      return await topicOrDesc.__schema.parse(message);
    }
    if (this.strictSchemasEnabled && typeof topicOrDesc === "string") {
      const schema = this.schemaRegistry.get(topicOrDesc);
      if (schema) return await schema.parse(message);
    }
    return message;
  }

  /**
   * Build a kafkajs-ready send payload.
   * Handles: topic resolution, schema registration, validation, JSON serialization,
   * envelope header generation, and instrumentation hooks.
   */
  private async buildSendPayload(
    topicOrDesc: any,
    messages: Array<BatchMessageItem<any>>,
  ): Promise<{
    topic: string;
    messages: Array<{
      value: string;
      key: string | null;
      headers: MessageHeaders;
    }>;
  }> {
    this.registerSchema(topicOrDesc);
    const topic = this.resolveTopicName(topicOrDesc);
    const builtMessages = await Promise.all(
      messages.map(async (m) => {
        const envelopeHeaders = buildEnvelopeHeaders({
          correlationId: m.correlationId,
          schemaVersion: m.schemaVersion,
          eventId: m.eventId,
          headers: m.headers,
        });

        // Let instrumentation hooks mutate headers (e.g. OTel injects traceparent)
        for (const inst of this.instrumentation) {
          inst.beforeSend?.(topic, envelopeHeaders);
        }

        return {
          value: JSON.stringify(
            await this.validateMessage(topicOrDesc, m.value),
          ),
          key: m.key ?? null,
          headers: envelopeHeaders,
        };
      }),
    );
    return { topic, messages: builtMessages };
  }

  /** Shared consumer setup: groupId check, schema map, connect, subscribe. */
  private async setupConsumer(
    topics: any[],
    mode: "eachMessage" | "eachBatch",
    options: ConsumerOptions<T>,
  ) {
    const {
      groupId: optGroupId,
      fromBeginning = false,
      retry,
      dlq = false,
      interceptors = [],
      schemas: optionSchemas,
    } = options;

    const gid = optGroupId || this.defaultGroupId;
    const existingMode = this.runningConsumers.get(gid);
    const oppositeMode = mode === "eachMessage" ? "eachBatch" : "eachMessage";
    if (existingMode === oppositeMode) {
      throw new Error(
        `Cannot use ${mode} on consumer group "${gid}" — it is already running with ${oppositeMode}. ` +
          `Use a different groupId for this consumer.`,
      );
    }

    const consumer = this.getOrCreateConsumer(
      gid,
      fromBeginning,
      options.autoCommit ?? true,
    );
    const schemaMap = this.buildSchemaMap(topics, optionSchemas);

    const topicNames = (topics as any[]).map((t: any) =>
      this.resolveTopicName(t),
    );

    // Ensure topics exist before subscribing — librdkafka errors on unknown topics
    for (const t of topicNames) {
      await this.ensureTopic(t);
    }
    if (dlq) {
      for (const t of topicNames) {
        await this.ensureTopic(`${t}.dlq`);
      }
    }

    await consumer.connect();
    await subscribeWithRetry(
      consumer,
      topicNames,
      this.logger,
      options.subscribeRetry,
    );

    this.logger.log(
      `${mode === "eachBatch" ? "Batch consumer" : "Consumer"} subscribed to topics: ${topicNames.join(", ")}`,
    );

    return { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry };
  }

  private buildSchemaMap(
    topics: any[],
    optionSchemas?: Map<string, SchemaLike>,
  ): Map<string, SchemaLike> {
    const schemaMap = new Map<string, SchemaLike>();
    for (const t of topics) {
      if (t?.__schema) {
        const name = this.resolveTopicName(t);
        schemaMap.set(name, t.__schema);
        this.schemaRegistry.set(name, t.__schema);
      }
    }
    if (optionSchemas) {
      for (const [k, v] of optionSchemas) {
        schemaMap.set(k, v);
        this.schemaRegistry.set(k, v);
      }
    }
    return schemaMap;
  }
}
