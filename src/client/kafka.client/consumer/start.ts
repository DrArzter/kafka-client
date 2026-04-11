import type { KafkaClientContext } from "../context";
import type {
  TopicMapConstraint,
  ConsumerOptions,
  ConsumerHandle,
  BatchMeta,
  SendOptions,
  BatchSendOptions,
  TransactionalHandlerContext,
} from "../../types";
import type { EventEnvelope } from "../../message/envelope";
import { runWithEnvelopeContext } from "../../message/envelope";
import { handleEachMessage, handleEachBatch, parseSingleMessage } from "./handler";
import {
  setupConsumer,
  validateTopicConsumerOpts,
  resolveDeduplicationContext,
  messageDepsFor,
  makeEosMainContext,
  launchRetryChain,
} from "./setup";
import { createRetryTxProducer, wrapWithTimeoutWarning } from "../producer/lifecycle";
import { preparePayload } from "../producer/send";
import { toError } from "./pipeline";

// ── startConsumer ─────────────────────────────────────────────────────────────

export async function startConsumerImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topics: any[],
  handleMessage: (envelope: EventEnvelope<any>) => Promise<void>,
  options: ConsumerOptions<T> = {},
): Promise<ConsumerHandle> {
  validateTopicConsumerOpts(topics, options);
  const setupOptions = options.retryTopics
    ? { ...options, autoCommit: false as const }
    : options;
  const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry, readyPromise } =
    await setupConsumer(ctx, topics, "eachMessage", setupOptions);

  if (options.circuitBreaker)
    ctx.circuitBreaker.setConfig(gid, options.circuitBreaker);
  const deps = messageDepsFor(ctx, gid, options);
  const eosMainContext = await makeEosMainContext(ctx, gid, consumer, options);

  await consumer.run({
    eachMessage: (payload) =>
      ctx.inFlight.track(() =>
        handleEachMessage(
          payload,
          {
            schemaMap,
            handleMessage,
            interceptors,
            dlq,
            retry,
            retryTopics: options.retryTopics,
            timeoutMs: options.handlerTimeoutMs,
            wrapWithTimeout: (fn, ms, topic) =>
              wrapWithTimeoutWarning(ctx.logger, fn, ms, topic),
            deduplication: resolveDeduplicationContext(
              ctx,
              gid,
              options.deduplication,
            ),
            messageTtlMs: options.messageTtlMs,
            onTtlExpired: options.onTtlExpired,
            eosMainContext,
          },
          deps,
        ),
      ),
  });

  ctx.runningConsumers.set(gid, "eachMessage");
  if (options.retryTopics && retry) {
    await launchRetryChain(ctx, gid, topicNames, handleMessage, {
      retry,
      dlq,
      interceptors,
      schemaMap,
      assignmentTimeoutMs: options.retryTopicAssignmentTimeoutMs,
    });
  }
  return { groupId: gid, stop: () => stopConsumerByGid(ctx, gid), ready: () => readyPromise };
}

// ── startBatchConsumer ────────────────────────────────────────────────────────

export async function startBatchConsumerImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topics: any[],
  handleBatch: (
    envelopes: EventEnvelope<any>[],
    meta: BatchMeta,
  ) => Promise<void>,
  options: ConsumerOptions<T> = {},
): Promise<ConsumerHandle> {
  validateTopicConsumerOpts(topics, options);
  if (!options.retryTopics && options.autoCommit !== false) {
    ctx.logger.debug?.(
      `startBatchConsumer: autoCommit is enabled (default true). ` +
        `If your handler calls resolveOffset() or commitOffsetsIfNecessary(), set autoCommit: false to avoid offset conflicts.`,
    );
  }
  const setupOptions = options.retryTopics
    ? { ...options, autoCommit: false as const }
    : options;
  const { consumer, schemaMap, topicNames, gid, dlq, interceptors, retry, readyPromise } =
    await setupConsumer(ctx, topics, "eachBatch", setupOptions);

  if (options.circuitBreaker)
    ctx.circuitBreaker.setConfig(gid, options.circuitBreaker);
  const deps = messageDepsFor(ctx, gid, options);
  const eosMainContext = await makeEosMainContext(ctx, gid, consumer, options);

  await consumer.run({
    eachBatch: (payload) =>
      ctx.inFlight.track(() =>
        handleEachBatch(
          payload,
          {
            schemaMap,
            handleBatch,
            interceptors,
            dlq,
            retry,
            retryTopics: options.retryTopics,
            timeoutMs: options.handlerTimeoutMs,
            wrapWithTimeout: (fn, ms, topic) =>
              wrapWithTimeoutWarning(ctx.logger, fn, ms, topic),
            deduplication: resolveDeduplicationContext(
              ctx,
              gid,
              options.deduplication,
            ),
            messageTtlMs: options.messageTtlMs,
            onTtlExpired: options.onTtlExpired,
            eosMainContext,
          },
          deps,
        ),
      ),
  });

  ctx.runningConsumers.set(gid, "eachBatch");
  if (options.retryTopics && retry) {
    const handleMessageForRetry = (env: EventEnvelope<any>) =>
      handleBatch([env], {
        partition: env.partition,
        highWatermark: null,
        heartbeat: async () => {},
        resolveOffset: () => {},
        commitOffsetsIfNecessary: async () => {},
      });
    await launchRetryChain(ctx, gid, topicNames, handleMessageForRetry, {
      retry,
      dlq,
      interceptors,
      schemaMap,
      assignmentTimeoutMs: options.retryTopicAssignmentTimeoutMs,
    });
  }
  return { groupId: gid, stop: () => stopConsumerByGid(ctx, gid), ready: () => readyPromise };
}

// ── startTransactionalConsumer ────────────────────────────────────────────────

export async function startTransactionalConsumerImpl<
  T extends TopicMapConstraint<T>,
>(
  ctx: KafkaClientContext<T>,
  topics: any[],
  handler: (
    envelope: EventEnvelope<any>,
    tx: TransactionalHandlerContext<T>,
  ) => Promise<void>,
  options: ConsumerOptions<T> = {},
): Promise<ConsumerHandle> {
  if (options.retryTopics) {
    throw new Error(
      "startTransactionalConsumer: retryTopics is not supported. " +
        "EOS is already guaranteed by the transaction — redelivery on failure is handled automatically.",
    );
  }

  const setupOptions = { ...options, autoCommit: false as const };
  const { consumer, schemaMap, gid, readyPromise } = await setupConsumer(
    ctx,
    topics,
    "eachMessage",
    setupOptions,
  );

  const txProducer = await createRetryTxProducer(ctx, `${gid}-txc`);
  const deps = messageDepsFor(ctx, gid);

  await consumer.run({
    eachMessage: ({ topic, partition, message }) =>
      ctx.inFlight.track(async () => {
        const envelope = await parseSingleMessage(
          message,
          topic,
          partition,
          schemaMap,
          options.interceptors ?? [],
          false,
          deps,
        );

        const nextOffset = String(Number.parseInt(message.offset, 10) + 1);

        if (envelope === null) {
          await consumer.commitOffsets([
            { topic, partition, offset: nextOffset },
          ]);
          return;
        }

        const tx = await txProducer.transaction();

        const txCtx: TransactionalHandlerContext<T> = {
          send: async (t: any, msg: any, sendOpts?: SendOptions) => {
            const payload = await preparePayload(ctx, t, [
              {
                value: msg,
                key: sendOpts?.key,
                headers: sendOpts?.headers,
                correlationId: sendOpts?.correlationId,
                schemaVersion: sendOpts?.schemaVersion,
                eventId: sendOpts?.eventId,
              },
            ]);
            await tx.send(payload);
          },
          sendBatch: async (t: any, msgs: any[], batchOpts?: BatchSendOptions) => {
            const payload = await preparePayload(
              ctx,
              t,
              msgs,
              batchOpts?.compression,
            );
            await tx.send(payload);
          },
        };

        try {
          await runWithEnvelopeContext(
            {
              correlationId: envelope.correlationId,
              traceparent: envelope.traceparent,
            },
            () => handler(envelope, txCtx),
          );
          await tx.sendOffsets({
            consumer,
            topics: [
              { topic, partitions: [{ partition, offset: nextOffset }] },
            ],
          });
          await tx.commit();
          deps.onMessage?.(envelope);
        } catch (err) {
          try {
            await tx.abort();
          } catch {}
          ctx.logger.warn(
            `startTransactionalConsumer: handler failed on ${topic}[${partition}]@${message.offset} — ` +
              `tx aborted, message will be redelivered (${toError(err).message})`,
          );
          throw err;
        }
      }),
  });

  ctx.runningConsumers.set(gid, "eachMessage");
  return { groupId: gid, stop: () => stopConsumerByGid(ctx, gid), ready: () => readyPromise };
}

// ── Shared stop helper (avoids circular dep with stop.ts) ─────────────────────

import { stopConsumerImpl } from "./stop";

function stopConsumerByGid<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  gid: string,
): Promise<void> {
  return stopConsumerImpl(ctx, gid);
}
