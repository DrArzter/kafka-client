import type { KafkaClientContext } from "../context";
import type {
  TopicMapConstraint,
  WindowConsumerOptions,
  WindowMeta,
  ConsumerHandle,
} from "../../types";
import type { EventEnvelope } from "../../message/envelope";
import { startConsumerImpl } from "./start";
import { toError } from "./pipeline";

// ── startWindowConsumer ───────────────────────────────────────────────────────

export async function startWindowConsumerImpl<
  T extends TopicMapConstraint<T>,
  K extends keyof T & string,
>(
  ctx: KafkaClientContext<T>,
  topic: K,
  handler: (envelopes: EventEnvelope<T[K]>[], meta: WindowMeta) => Promise<void>,
  options: WindowConsumerOptions<T>,
): Promise<ConsumerHandle> {
  const { maxMessages, maxMs, ...consumerOptions } = options;

  if (maxMessages <= 0)
    throw new Error("startWindowConsumer: maxMessages must be > 0");
  if (maxMs <= 0) throw new Error("startWindowConsumer: maxMs must be > 0");
  if ((consumerOptions as any).retryTopics) {
    throw new Error(
      "startWindowConsumer() does not support retryTopics. " +
        "Use startConsumer() with retryTopics: true for guaranteed retry delivery.",
    );
  }

  const buffer: EventEnvelope<T[K]>[] = [];
  let flushTimer: ReturnType<typeof setTimeout> | null = null;
  let windowStart = 0;

  const flush = async (trigger: "size" | "time"): Promise<void> => {
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    if (buffer.length === 0) return;
    const envelopes = buffer.splice(0);
    await handler(envelopes, { trigger, windowStart, windowEnd: Date.now() });
  };

  const scheduleFlush = (): void => {
    if (flushTimer !== null) return;
    flushTimer = setTimeout(() => {
      flushTimer = null;
      flush("time").catch((err) => {
        ctx.logger.warn(
          `startWindowConsumer: time-triggered flush error — ${toError(err).message}`,
        );
      });
    }, maxMs);
  };

  const handle = await startConsumerImpl(
    ctx,
    [topic as any],
    async (envelope) => {
      if (buffer.length === 0) windowStart = Date.now();
      buffer.push(envelope as EventEnvelope<T[K]>);
      scheduleFlush();
      if (buffer.length >= maxMessages) await flush("size");
    },
    consumerOptions as any,
  );

  const originalStop = handle.stop.bind(handle);
  handle.stop = async (): Promise<void> => {
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    if (buffer.length > 0) {
      const envelopes = buffer.splice(0);
      await handler(envelopes, {
        trigger: "time",
        windowStart,
        windowEnd: Date.now(),
      }).catch(async (err) => {
        const error = toError(err);
        ctx.logger.warn(
          `startWindowConsumer: shutdown flush error — ${error.message}`,
        );
        for (const envelope of envelopes) {
          await Promise.resolve(
            ctx.onMessageLost?.({
              topic: envelope.topic,
              error,
              attempt: 0,
              headers: envelope.headers,
            }),
          ).catch(() => {});
        }
      });
    }
    return originalStop();
  };

  return handle;
}
