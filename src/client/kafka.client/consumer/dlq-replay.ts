import { KafkaJS } from "@confluentinc/kafka-javascript";
type Consumer = KafkaJS.Consumer;
import type { KafkaLogger, DlqReplayOptions } from "../../types";
import { subscribeWithRetry } from "./subscribe-retry";
import { decodeHeaders } from "../../message/envelope";

/**
 * Dependencies injected into `replayDlqTopic` by `KafkaClient`.
 * Abstracts broker access (offset fetching, producing, consumer creation) so the
 * replay logic can be unit-tested without a real Kafka connection.
 */
export type DlqReplayDeps = {
  logger: KafkaLogger;
  fetchTopicOffsets: (topic: string) => Promise<Array<{ partition: number; low: string; high: string }>>;
  send: (topic: string, messages: Array<{ value: string; headers: Record<string, string> }>) => Promise<void>;
  createConsumer: (groupId: string) => Consumer;
  cleanupConsumer: (consumer: Consumer, groupId: string) => void;
  dlqHeaderKeys: Set<string>;
};

/**
 * Re-publish messages from a dead letter queue back to the original topic.
 *
 * Messages are consumed from `<topic>.dlq` and re-published to `<topic>`.
 * The original topic is determined by the `x-dlq-original-topic` header.
 * The `x-dlq-*` headers are stripped before re-publishing.
 */
export async function replayDlqTopic(
  topic: string,
  options: DlqReplayOptions = {},
  deps: DlqReplayDeps,
): Promise<{ replayed: number; skipped: number }> {
  const dlqTopic = `${topic}.dlq`;

  const partitionOffsets = await deps.fetchTopicOffsets(dlqTopic);
  const activePartitions = partitionOffsets.filter((p) => parseInt(p.high, 10) > 0);
  if (activePartitions.length === 0) {
    deps.logger.log(`replayDlq: "${dlqTopic}" is empty — nothing to replay`);
    return { replayed: 0, skipped: 0 };
  }

  const highWatermarks = new Map(
    activePartitions.map(({ partition, high }) => [partition, parseInt(high, 10)]),
  );
  const processedOffsets = new Map<number, number>();
  let replayed = 0;
  let skipped = 0;

  const tempGroupId = `${dlqTopic}-replay-${Date.now()}`;

  await new Promise<void>((resolve, reject) => {
    const consumer = deps.createConsumer(tempGroupId);
    const cleanup = () => deps.cleanupConsumer(consumer, tempGroupId);

    consumer
      .connect()
      .then(() => subscribeWithRetry(consumer, [dlqTopic], deps.logger))
      .then(() =>
        consumer.run({
          eachMessage: async ({ partition, message }) => {
            if (!message.value) return;

            const offset = parseInt(message.offset, 10);
            processedOffsets.set(partition, offset);

            const headers = decodeHeaders(message.headers);
            const targetTopic = options.targetTopic ?? headers["x-dlq-original-topic"];
            const originalHeaders = Object.fromEntries(
              Object.entries(headers).filter(([k]) => !deps.dlqHeaderKeys.has(k)),
            );
            const value = message.value.toString();
            const shouldProcess = !options.filter || options.filter(headers, value);

            if (!targetTopic || !shouldProcess) {
              skipped++;
            } else if (options.dryRun) {
              deps.logger.log(`[DLQ replay dry-run] Would replay to "${targetTopic}"`);
              replayed++;
            } else {
              await deps.send(targetTopic, [{ value, headers: originalHeaders }]);
              replayed++;
            }

            const allDone = Array.from(highWatermarks.entries()).every(
              ([p, hwm]) => (processedOffsets.get(p) ?? -1) >= hwm - 1,
            );
            if (allDone) {
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

  deps.logger.log(`replayDlq: replayed ${replayed}, skipped ${skipped} from "${dlqTopic}"`);
  return { replayed, skipped };
}
