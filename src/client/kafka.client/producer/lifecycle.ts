import type { IProducer } from "../../transport";
type Producer = IProducer;

import type { KafkaClientContext } from "../context";
import type { TopicMapConstraint } from "../../types";
import { HEADER_LAMPORT_CLOCK } from "../../message/envelope";
import { toError } from "../consumer/pipeline";

/**
 * Process-level registry of active transactional producer IDs.
 * Shared across `transactionImpl` (send.ts) and `createRetryTxProducer`.
 * Cross-process conflicts cannot be detected here — they surface as fencing errors from the broker.
 */
export const _activeTransactionalIds = new Set<string>();

// ── Topic creation ────────────────────────────────────────────────────────────

/**
 * Ensure a topic exists, creating it if `autoCreateTopics` is enabled.
 * Concurrent calls for the same topic are deduplicated via a promise cache.
 */
export async function ensureTopic<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topic: string,
): Promise<void> {
  if (!ctx.autoCreateTopicsEnabled || ctx.ensuredTopics.has(topic)) return;
  let p = ctx.ensureTopicPromises.get(topic);
  if (!p) {
    p = (async () => {
      await ctx.adminOps.ensureConnected();
      await ctx.adminOps.admin.createTopics({
        topics: [{ topic, numPartitions: ctx.numPartitions }],
      });
      ctx.ensuredTopics.add(topic);
    })().finally(() => ctx.ensureTopicPromises.delete(topic));
    ctx.ensureTopicPromises.set(topic, p);
  }
  await p;
}

// ── Transactional producer ────────────────────────────────────────────────────

/**
 * Create and connect a transactional producer for EOS routing.
 * Each retry level consumer uses its own producer with a unique `transactionalId`
 * so Kafka can fence stale producers on restart without affecting other levels.
 */
export async function createRetryTxProducer<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  transactionalId: string,
): Promise<Producer> {
  if (_activeTransactionalIds.has(transactionalId)) {
    ctx.logger.warn(
      `transactionalId "${transactionalId}" is already in use by another KafkaClient in this process. ` +
        `Kafka will fence one of the producers. ` +
        `Set a unique \`transactionalId\` (or distinct \`clientId\`) per instance.`,
    );
  }
  const p = ctx.transport.producer({
    idempotent: true,
    transactionalId,
  });
  await p.connect();
  _activeTransactionalIds.add(transactionalId);
  ctx.retryTxProducers.set(transactionalId, p);
  return p;
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

export async function connectProducerImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
): Promise<void> {
  await ctx.producer.connect();
  await recoverLamportClockImpl(ctx, ctx.clockRecoveryTopics);
  if (ctx.lagThrottleOpts) startLagThrottlePoller(ctx);
  ctx.logger.log("Producer connected");
}

export async function disconnectImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  drainTimeoutMs = 30_000,
): Promise<void> {
  if (ctx._lagThrottleTimer) {
    clearInterval(ctx._lagThrottleTimer);
    ctx._lagThrottleTimer = undefined;
  }
  await ctx.inFlight.waitForDrain(drainTimeoutMs);
  const tasks: Promise<void>[] = [ctx.producer.disconnect()];
  if (ctx.txProducer) {
    tasks.push(ctx.txProducer.disconnect());
    _activeTransactionalIds.delete(ctx.txId);
    ctx.txProducer = undefined;
    ctx.txProducerInitPromise = undefined;
  }
  for (const txId of ctx.retryTxProducers.keys())
    _activeTransactionalIds.delete(txId);
  for (const p of ctx.retryTxProducers.values()) tasks.push(p.disconnect());
  ctx.retryTxProducers.clear();
  for (const consumer of ctx.consumers.values())
    tasks.push(consumer.disconnect());
  tasks.push(ctx.adminOps.disconnect());
  await Promise.allSettled(tasks);
  ctx.consumers.clear();
  ctx.runningConsumers.clear();
  ctx.consumerCreationOptions.clear();
  ctx.companionGroupIds.clear();
  ctx.circuitBreaker.clear();
  ctx.logger.log("All connections closed");
}

// ── Lag throttle ──────────────────────────────────────────────────────────────

export function startLagThrottlePoller<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
): void {
  const opts = ctx.lagThrottleOpts!;
  const { maxLag, pollIntervalMs = 5_000 } = opts;
  const groupId = opts.groupId;

  const poll = async () => {
    try {
      const lags = await ctx.adminOps.getConsumerLag(groupId);
      const total = lags.reduce((sum, e) => sum + e.lag, 0);
      if (total > maxLag && !ctx._lagThrottled) {
        ctx._lagThrottled = true;
        ctx.logger.warn(
          `lagThrottle: lag ${total} > ${maxLag} — producer sends will be delayed`,
        );
      } else if (total <= maxLag && ctx._lagThrottled) {
        ctx._lagThrottled = false;
        ctx.logger.log(
          `lagThrottle: lag ${total} ≤ ${maxLag} — producer sends resumed`,
        );
      }
    } catch {
      // Poll errors do not block sends — silently ignore
    }
  };

  ctx._lagThrottleTimer = setInterval(() => {
    void poll();
  }, pollIntervalMs);

  ctx._lagThrottleTimer.unref?.();
}

// ── Lamport clock recovery ────────────────────────────────────────────────────

/**
 * Recover the Lamport clock from the last message across the given topics.
 * Seeks every non-empty partition to its last offset, reads one message per
 * partition, and extracts the maximum `x-lamport-clock` header value.
 * Topics that are empty or missing are silently skipped.
 */
export async function recoverLamportClockImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topics: string[],
): Promise<void> {
  if (topics.length === 0) return;

  ctx.logger.log(
    `Clock recovery: scanning ${topics.length} topic(s) for Lamport clock...`,
  );
  await ctx.adminOps.ensureConnected();

  const partitionsToRead: Array<{
    topic: string;
    partition: number;
    lastOffset: string;
  }> = [];
  for (const t of topics) {
    let offsets: Array<{ partition: number; low: string; high: string }>;
    try {
      offsets = await ctx.adminOps.admin.fetchTopicOffsets(t);
    } catch {
      ctx.logger.warn(
        `Clock recovery: could not fetch offsets for "${t}", skipping`,
      );
      continue;
    }
    for (const { partition, high, low } of offsets) {
      if (Number.parseInt(high, 10) > Number.parseInt(low, 10)) {
        partitionsToRead.push({
          topic: t,
          partition,
          lastOffset: String(Number.parseInt(high, 10) - 1),
        });
      }
    }
  }

  if (partitionsToRead.length === 0) {
    ctx.logger.log(
      "Clock recovery: all topics empty — keeping Lamport clock at 0",
    );
    return;
  }

  const recoveryGroupId = `${ctx.clientId}-clock-recovery-${Date.now()}`;
  let maxClock = -1;

  await new Promise<void>((resolve, reject) => {
    const consumer = ctx.transport.consumer({ groupId: recoveryGroupId });
    const remaining = new Set(
      partitionsToRead.map((p) => `${p.topic}:${p.partition}`),
    );
    const cleanup = () => {
      consumer
        .disconnect()
        .catch(() => {})
        .finally(() => {
          ctx.adminOps.deleteGroups([recoveryGroupId]).catch(() => {});
        });
    };

    consumer
      .connect()
      .then(async () => {
        const uniqueTopics = [
          ...new Set(partitionsToRead.map((p) => p.topic)),
        ];
        await consumer.subscribe({ topics: uniqueTopics });
        for (const { topic: t, partition, lastOffset } of partitionsToRead) {
          consumer.seek({ topic: t, partition, offset: lastOffset });
        }
      })
      .then(() =>
        consumer.run({
          eachMessage: async ({ topic: t, partition, message }) => {
            const key = `${t}:${partition}`;
            if (!remaining.has(key)) return;
            remaining.delete(key);

            const clockHeader = message.headers?.[HEADER_LAMPORT_CLOCK];
            if (clockHeader !== undefined) {
              const raw = Buffer.isBuffer(clockHeader)
                ? clockHeader.toString()
                : String(clockHeader);
              const clock = Number(raw);
              if (!Number.isNaN(clock) && clock > maxClock) maxClock = clock;
            }

            if (remaining.size === 0) {
              cleanup();
              resolve();
            }
          },
        }),
      )
      .catch((err) => {
        cleanup();
        reject(err);
      });
  });

  if (maxClock >= 0) {
    ctx._lamportClock = maxClock;
    ctx.logger.log(
      `Clock recovery: Lamport clock restored — next clock will be ${maxClock + 1}`,
    );
  } else {
    ctx.logger.log(
      "Clock recovery: no x-lamport-clock headers found — keeping clock at 0",
    );
  }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/**
 * Start a timer that logs a warning if `fn` hasn't resolved within `timeoutMs`.
 * The handler itself is not cancelled — the warning is diagnostic only.
 */
export function wrapWithTimeoutWarning<R>(
  logger: { warn: (msg: string) => void },
  fn: () => Promise<R>,
  timeoutMs: number,
  topic: string,
): Promise<R> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const promise = fn().finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
  timer = setTimeout(() => {
    logger.warn(
      `Handler for topic "${topic}" has not resolved after ${timeoutMs}ms — possible stuck handler`,
    );
  }, timeoutMs);
  return promise;
}

// Re-export toError for callers that only import lifecycle
export { toError };
