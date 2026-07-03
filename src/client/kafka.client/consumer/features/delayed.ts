import type { KafkaClientContext } from "../../context";
import type { TopicMapConstraint, ConsumerHandle, MessageHeaders } from "../../../types";
import {
  decodeHeaders,
  HEADER_DELAYED_TARGET,
  HEADER_DELAYED_UNTIL,
} from "../../../message/envelope";
import { getOrCreateConsumer } from "../ops";
import { subscribeWithRetry } from "../subscribe-retry";
import { createRetryTxProducer, ensureTopic } from "../../producer/lifecycle";
import { sleep, toError } from "../pipeline";
import { stopConsumerImpl } from "../stop";

/** Staging topic name for delayed messages of `topic`. */
export function delayedTopicName(topic: string): string {
  return `${topic}.delayed`;
}

/**
 * Start a relay consumer that delivers messages produced with
 * `SendOptions.deliverAfterMs` from `<topic>.delayed` to their target topic
 * once their `x-delayed-until` deadline passes.
 *
 * Semantics mirror the retry-topic chain:
 * - The partition is paused while waiting for the deadline; the offset is not
 *   committed during the wait, so a crash redelivers the message (at-least-once).
 * - Forwarding uses a Kafka transaction (produce to target + commit source
 *   offset atomically), so no duplicates are relayed even on crash.
 * - Delivery time is a lower bound — head-of-line waiting applies per partition.
 *
 * The relay preserves the original key, value, and headers (minus the
 * `x-delayed-*` control headers), so envelope metadata (`x-event-id`,
 * `x-correlation-id`, `x-lamport-clock`, `traceparent`) survives the hop.
 */
export async function startDelayedRelayImpl<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topics: string[],
  options?: { groupId?: string },
): Promise<ConsumerHandle> {
  if (topics.length === 0) {
    throw new Error("startDelayedRelay: at least one topic is required");
  }
  const gid = options?.groupId ?? `${ctx.defaultGroupId}-delayed-relay`;
  if (ctx.runningConsumers.has(gid)) {
    throw new Error(
      `startDelayedRelay("${gid}") called twice — this group is already consuming. ` +
        `Call stopConsumer("${gid}") first or pass a different groupId.`,
    );
  }

  const delayedTopics = topics.map(delayedTopicName);
  for (const t of delayedTopics) await ensureTopic(ctx, t);

  // Transactional producer: forward to target + commit source offset atomically.
  const txProducer = await createRetryTxProducer(ctx, `${gid}-tx`);

  let resolveReady!: () => void;
  const readyPromise = new Promise<void>((resolve) => {
    resolveReady = resolve;
  });

  // autoCommit: false — offsets are committed only inside the forwarding
  // transaction (or explicitly for skipped messages).
  const consumer = getOrCreateConsumer(
    gid,
    false,
    false,
    ctx.consumerOpsDeps,
    undefined,
    resolveReady,
  );
  await consumer.connect();
  await subscribeWithRetry(consumer, delayedTopics, ctx.logger);

  await consumer.run({
    eachMessage: async ({ topic: stagingTopic, partition, message }) => {
      const nextOffset = {
        topic: stagingTopic,
        partition,
        offset: (parseInt(message.offset, 10) + 1).toString(),
      };

      if (!message.value) {
        await consumer.commitOffsets([nextOffset]);
        return;
      }

      const headers = decodeHeaders(message.headers);
      const target =
        (headers[HEADER_DELAYED_TARGET] as string | undefined) ??
        stagingTopic.replace(/\.delayed$/, "");
      const until = parseInt(
        (headers[HEADER_DELAYED_UNTIL] as string | undefined) ?? "0",
        10,
      );
      const remaining = until - Date.now();

      // Hold the partition until the deadline. Offset stays uncommitted —
      // a crash during the wait redelivers the message on restart.
      if (remaining > 0) {
        consumer.pause([{ topic: stagingTopic, partitions: [partition] }]);
        await sleep(remaining);
        consumer.resume([{ topic: stagingTopic, partitions: [partition] }]);
      }

      const forwardHeaders: MessageHeaders = Object.fromEntries(
        Object.entries(headers).filter(
          ([k]) => k !== HEADER_DELAYED_UNTIL && k !== HEADER_DELAYED_TARGET,
        ),
      ) as MessageHeaders;

      const tx = await txProducer.transaction();
      try {
        await tx.send({
          topic: target,
          messages: [
            {
              // Forward the ORIGINAL wire bytes unchanged — no re-serialization,
              // so binary payloads (Avro/Protobuf) are relayed losslessly.
              value: message.value,
              key: message.key ? message.key.toString() : null,
              headers: forwardHeaders,
            },
          ],
        });
        await tx.sendOffsets({
          consumer,
          topics: [
            {
              topic: nextOffset.topic,
              partitions: [
                { partition: nextOffset.partition, offset: nextOffset.offset },
              ],
            },
          ],
        });
        await tx.commit();
        ctx.logger.debug?.(
          `Delayed message relayed to "${target}" (deadline ${new Date(until).toISOString()})`,
        );
      } catch (txErr) {
        try {
          await tx.abort();
        } catch {}
        ctx.logger.error(
          `Delayed relay to "${target}" failed — message will be redelivered:`,
          toError(txErr).stack,
        );
        // Offset not committed — the staging message is redelivered and retried.
      }
    },
  });

  ctx.runningConsumers.set(gid, "eachMessage");
  ctx.logger.log(
    `Delayed relay started for: ${delayedTopics.join(", ")} (group: ${gid})`,
  );

  return {
    groupId: gid,
    ready: () => readyPromise,
    stop: async () => {
      await stopConsumerImpl(ctx, gid);
    },
  };
}
