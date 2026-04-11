import type { KafkaClientContext } from "../context";
import type { TopicMapConstraint } from "../../types";
import { _activeTransactionalIds } from "../producer/lifecycle";
import { toError } from "./pipeline";

/** Minimal context surface required by this module. */
type StopCtx<T extends TopicMapConstraint<T>> = Pick<
  KafkaClientContext<T>,
  | "circuitBreaker"
  | "companionGroupIds"
  | "consumerCreationOptions"
  | "consumers"
  | "dedupStates"
  | "logger"
  | "retryTxProducers"
  | "runningConsumers"
>;

// ── Stop ──────────────────────────────────────────────────────────────────────

export async function stopConsumerImpl<T extends TopicMapConstraint<T>>(
  ctx: StopCtx<T>,
  groupId?: string,
): Promise<void> {
  if (groupId === undefined) {
    const tasks: Promise<void>[] = [
      ...Array.from(ctx.consumers.values()).map((c) =>
        c.disconnect().catch(() => {}),
      ),
      ...Array.from(ctx.retryTxProducers.values()).map((p) =>
        p.disconnect().catch(() => {}),
      ),
    ];
    await Promise.allSettled(tasks);
    ctx.consumers.clear();
    ctx.runningConsumers.clear();
    ctx.consumerCreationOptions.clear();
    ctx.companionGroupIds.clear();
    ctx.retryTxProducers.clear();
    ctx.dedupStates.clear();
    ctx.circuitBreaker.clear();
    ctx.logger.log("All consumers disconnected");
    return;
  }

  const consumer = ctx.consumers.get(groupId);
  if (!consumer) {
    ctx.logger.warn(
      `stopConsumer: no active consumer for group "${groupId}"`,
    );
    return;
  }
  await consumer
    .disconnect()
    .catch((e) =>
      ctx.logger.warn(
        `Error disconnecting consumer "${groupId}":`,
        toError(e).message,
      ),
    );
  ctx.consumers.delete(groupId);
  ctx.runningConsumers.delete(groupId);
  ctx.consumerCreationOptions.delete(groupId);
  ctx.dedupStates.delete(groupId);
  ctx.circuitBreaker.removeGroup(groupId);
  ctx.logger.log(`Consumer disconnected: group "${groupId}"`);

  // Clean up the main consumer's EOS tx producer (present when retryTopics: true)
  const mainTxId = `${groupId}-main-tx`;
  const mainTxProducer = ctx.retryTxProducers.get(mainTxId);
  if (mainTxProducer) {
    await mainTxProducer
      .disconnect()
      .catch((e) =>
        ctx.logger.warn(
          `Error disconnecting main tx producer "${mainTxId}":`,
          toError(e).message,
        ),
      );
    _activeTransactionalIds.delete(mainTxId);
    ctx.retryTxProducers.delete(mainTxId);
  }

  // Stop all companion retry level consumers and their tx producers
  const companions = ctx.companionGroupIds.get(groupId) ?? [];
  for (const cGroupId of companions) {
    const cConsumer = ctx.consumers.get(cGroupId);
    if (cConsumer) {
      await cConsumer
        .disconnect()
        .catch((e) =>
          ctx.logger.warn(
            `Error disconnecting retry consumer "${cGroupId}":`,
            toError(e).message,
          ),
        );
      ctx.consumers.delete(cGroupId);
      ctx.runningConsumers.delete(cGroupId);
      ctx.consumerCreationOptions.delete(cGroupId);
      ctx.logger.log(`Retry consumer disconnected: group "${cGroupId}"`);
    }
    const txId = `${cGroupId}-tx`;
    const txProducer = ctx.retryTxProducers.get(txId);
    if (txProducer) {
      await txProducer
        .disconnect()
        .catch((e) =>
          ctx.logger.warn(
            `Error disconnecting retry tx producer "${txId}":`,
            toError(e).message,
          ),
        );
      _activeTransactionalIds.delete(txId);
      ctx.retryTxProducers.delete(txId);
    }
  }
  ctx.companionGroupIds.delete(groupId);
}

// ── Pause / Resume ────────────────────────────────────────────────────────────

export function pauseConsumerImpl<T extends TopicMapConstraint<T>>(
  ctx: StopCtx<T>,
  groupId: string | undefined,
  assignments: Array<{ topic: string; partitions: number[] }>,
): void {
  const gid = groupId ?? ctx.defaultGroupId;
  const consumer = ctx.consumers.get(gid);
  if (!consumer) {
    ctx.logger.warn(`pauseConsumer: no active consumer for group "${gid}"`);
    return;
  }
  consumer.pause(
    assignments.flatMap(({ topic, partitions }) =>
      partitions.map((p) => ({ topic, partitions: [p] })),
    ),
  );
}

export function resumeConsumerImpl<T extends TopicMapConstraint<T>>(
  ctx: StopCtx<T>,
  groupId: string | undefined,
  assignments: Array<{ topic: string; partitions: number[] }>,
): void {
  const gid = groupId ?? ctx.defaultGroupId;
  const consumer = ctx.consumers.get(gid);
  if (!consumer) {
    ctx.logger.warn(`resumeConsumer: no active consumer for group "${gid}"`);
    return;
  }
  consumer.resume(
    assignments.flatMap(({ topic, partitions }) =>
      partitions.map((p) => ({ topic, partitions: [p] })),
    ),
  );
}

/** Pause all assigned partitions of a topic for a consumer group (backpressure for consume()). */
export function pauseTopicAllPartitions<T extends TopicMapConstraint<T>>(
  ctx: StopCtx<T>,
  gid: string,
  topic: string,
): void {
  const consumer = ctx.consumers.get(gid);
  if (!consumer) return;
  const assignment = consumer.assignment();
  const partitions = assignment
    .filter((a) => a.topic === topic)
    .map((a) => a.partition);
  if (partitions.length > 0)
    consumer.pause(partitions.map((p) => ({ topic, partitions: [p] })));
}

/** Resume all assigned partitions of a topic for a consumer group (backpressure for consume()). */
export function resumeTopicAllPartitions<T extends TopicMapConstraint<T>>(
  ctx: StopCtx<T>,
  gid: string,
  topic: string,
): void {
  const consumer = ctx.consumers.get(gid);
  if (!consumer) return;
  const assignment = consumer.assignment();
  const partitions = assignment
    .filter((a) => a.topic === topic)
    .map((a) => a.partition);
  if (partitions.length > 0)
    consumer.resume(partitions.map((p) => ({ topic, partitions: [p] })));
}
