import type { KafkaClientContext } from "../context";
import type {
  TopicMapConstraint,
  SendOptions,
  BatchMessageItem,
  BatchSendOptions,
  TransactionContext,
  MessageHeaders,
  CompressionType,
} from "../../types";
import { buildSendPayload, registerSchema } from "./ops";
import { ensureTopic, _activeTransactionalIds } from "./lifecycle";
import {
  HEADER_DELAYED_TARGET,
  HEADER_DELAYED_UNTIL,
} from "../../message/envelope";

// ── Payload builder ───────────────────────────────────────────────────────────

/**
 * Build a ProduceRequest payload: register schema, validate, stamp envelope headers.
 * Shared by sendMessage, sendBatch, and transaction sends.
 */
export async function preparePayload<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topicOrDesc: any,
  messages: Array<BatchMessageItem<any>>,
  compression?: CompressionType,
) {
  registerSchema(topicOrDesc, ctx.schemaRegistry, ctx.logger);
  const payload = await buildSendPayload(
    topicOrDesc,
    messages,
    ctx.producerOpsDeps,
    compression,
  );
  await ensureTopic(ctx, payload.topic);
  return payload;
}

// ── Delayed delivery ──────────────────────────────────────────────────────────

/**
 * Redirect a built payload to the `<topic>.delayed` staging topic, stamping
 * the `x-delayed-until` / `x-delayed-target` control headers on every message.
 * A relay started via `startDelayedRelay()` forwards it after the deadline.
 */
async function redirectToDelayed<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  payload: { topic: string; messages: Array<{ headers: MessageHeaders }> },
  deliverAfterMs: number,
): Promise<void> {
  const until = String(Date.now() + deliverAfterMs);
  for (const m of payload.messages) {
    m.headers[HEADER_DELAYED_UNTIL] = until;
    m.headers[HEADER_DELAYED_TARGET] = payload.topic;
  }
  payload.topic = `${payload.topic}.delayed`;
  await ensureTopic(ctx, payload.topic);
}

// ── Send operations ───────────────────────────────────────────────────────────

export async function sendMessageImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topicOrDesc: any,
  message: any,
  options: SendOptions = {},
): Promise<void> {
  await waitIfThrottled(ctx);
  const payload = await preparePayload(
    ctx,
    topicOrDesc,
    [
      {
        value: message,
        key: options.key,
        headers: options.headers,
        correlationId: options.correlationId,
        schemaVersion: options.schemaVersion,
        eventId: options.eventId,
      },
    ],
    options.compression,
  );
  if (options.deliverAfterMs && options.deliverAfterMs > 0) {
    await redirectToDelayed(ctx, payload, options.deliverAfterMs);
  }
  await ctx.producer.send(payload);
  ctx.metrics.notifyAfterSend(payload.topic, payload.messages.length);
}

export async function sendBatchImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topicOrDesc: any,
  messages: Array<BatchMessageItem<any>>,
  options?: BatchSendOptions,
): Promise<void> {
  await waitIfThrottled(ctx);
  const payload = await preparePayload(
    ctx,
    topicOrDesc,
    messages,
    options?.compression,
  );
  if (options?.deliverAfterMs && options.deliverAfterMs > 0) {
    await redirectToDelayed(ctx, payload, options.deliverAfterMs);
  }
  await ctx.producer.send(payload);
  ctx.metrics.notifyAfterSend(payload.topic, payload.messages.length);
}

export async function sendTombstoneImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topic: string,
  key: string,
  headers?: MessageHeaders,
): Promise<void> {
  await waitIfThrottled(ctx);
  const hdrs: MessageHeaders = { ...headers };
  for (const inst of ctx.instrumentation) inst.beforeSend?.(topic, hdrs);
  await ensureTopic(ctx, topic);
  await ctx.producer.send({
    topic,
    messages: [{ value: null, key, headers: hdrs }],
  });
  for (const inst of ctx.instrumentation) inst.afterSend?.(topic);
}

export async function transactionImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  fn: (txCtx: TransactionContext<T>) => Promise<void>,
): Promise<void> {
  if (!ctx.txProducerInitPromise) {
    if (_activeTransactionalIds.has(ctx.txId)) {
      ctx.logger.warn(
        `transactionalId "${ctx.txId}" is already in use by another KafkaClient in this process. ` +
          `Kafka will fence one of the producers. ` +
          `Set a unique \`transactionalId\` (or distinct \`clientId\`) per instance.`,
      );
    }
    const initPromise = (async () => {
      const p = ctx.transport.producer({
        idempotent: true,
        transactionalId: ctx.txId,
      });
      await p.connect();
      _activeTransactionalIds.add(ctx.txId);
      return p;
    })();
    ctx.txProducerInitPromise = initPromise.catch((err) => {
      ctx.txProducerInitPromise = undefined;
      throw err;
    });
  }
  ctx.txProducer = await ctx.txProducerInitPromise;

  // A transactional producer supports only ONE open transaction at a time —
  // serialise overlapping transaction() calls so they cannot interleave
  // begin/send/commit/abort on the shared producer.
  const prev = ctx._txChain;
  let release!: () => void;
  ctx._txChain = new Promise<void>((r) => (release = r));
  await prev;
  try {
    await runTransaction(ctx, fn);
  } finally {
    release();
  }
}

async function runTransaction<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  fn: (txCtx: TransactionContext<T>) => Promise<void>,
): Promise<void> {
  const tx = await ctx.txProducer!.transaction();
  try {
    const txCtx: TransactionContext<T> = {
      send: async (topicOrDesc: any, message: any, sendOpts: SendOptions = {}) => {
        const payload = await preparePayload(ctx, topicOrDesc, [
          {
            value: message,
            key: sendOpts.key,
            headers: sendOpts.headers,
            correlationId: sendOpts.correlationId,
            schemaVersion: sendOpts.schemaVersion,
            eventId: sendOpts.eventId,
          },
        ]);
        await tx.send(payload);
        ctx.metrics.notifyAfterSend(payload.topic, payload.messages.length);
      },
      sendBatch: async (
        topicOrDesc: any,
        messages: BatchMessageItem<any>[],
        batchOpts?: BatchSendOptions,
      ) => {
        const payload = await preparePayload(
          ctx,
          topicOrDesc,
          messages,
          batchOpts?.compression,
        );
        await tx.send(payload);
        ctx.metrics.notifyAfterSend(payload.topic, payload.messages.length);
      },
    };
    await fn(txCtx);
    await tx.commit();
  } catch (error) {
    try {
      await tx.abort();
    } catch (abortError) {
      ctx.logger.error(
        "Failed to abort transaction:",
        (abortError instanceof Error
          ? abortError
          : new Error(String(abortError))
        ).message,
      );
    }
    throw error;
  }
}

// ── Lag throttle wait (used by all send paths) ────────────────────────────────

export async function waitIfThrottled<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
): Promise<void> {
  if (!ctx._lagThrottled) return;
  const maxWait = ctx.lagThrottleOpts?.maxWaitMs ?? 30_000;
  const start = Date.now();
  while (ctx._lagThrottled) {
    if (Date.now() - start >= maxWait) {
      ctx.logger.warn(
        `lagThrottle: maxWaitMs (${maxWait} ms) exceeded — sending anyway`,
      );
      return;
    }
    await new Promise<void>((r) => setTimeout(r, 100));
  }
}
