import type { IConsumer } from "../../transport";
type Consumer = IConsumer;
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
  /**
   * Create a consumer for the given group.
   * @param groupId Consumer group ID.
   * @param fromBeginning When `true`, `auto.offset.reset=earliest` (no committed offsets on a temp group).
   */
  createConsumer: (groupId: string, fromBeginning: boolean) => Consumer;
  /**
   * Disconnect the consumer and, when `deleteGroup` is `true`, delete the group
   * from the broker (used for ephemeral temp groups that should not accumulate).
   */
  cleanupConsumer: (consumer: Consumer, groupId: string, deleteGroup: boolean) => void;
  dlqHeaderKeys: Set<string>;
};

/**
 * Re-publish messages from a dead letter queue back to the original topic.
 *
 * Messages are consumed from `<topic>.dlq` and re-published to `<topic>`.
 * The original topic is determined by the `x-dlq-original-topic` header.
 * The `x-dlq-*` headers are stripped before re-publishing.
 *
 * ### Group ID strategy (driven by `options.fromBeginning`):
 * - `fromBeginning: true` (default) — a new ephemeral group `<topic>.dlq-replay-<ts>` is used
 *   on every call so there are no committed offsets; reads all messages from the beginning
 *   every time. The group is deleted from the broker after the replay finishes.
 * - `fromBeginning: false` — a stable group `<topic>.dlq-replay` is used; committed offsets
 *   persist between calls so only messages added since the previous call are replayed.
 */
export async function replayDlqTopic(
  topic: string,
  deps: DlqReplayDeps,
  options: DlqReplayOptions = {},
): Promise<{ replayed: number; skipped: number }> {
  const dlqTopic = `${topic}.dlq`;

  const partitionOffsets = await deps.fetchTopicOffsets(dlqTopic);
  // Only process partitions that have readable messages (high > low).
  // Checking high > 0 is insufficient: a topic truncated by retention policy
  // can have high > 0 but low == high (zero readable messages), causing an
  // infinite wait when consuming up to high − 1.
  const activePartitions = partitionOffsets.filter(
    (p) => Number.parseInt(p.high, 10) > Number.parseInt(p.low, 10),
  );
  if (activePartitions.length === 0) {
    deps.logger.log(`replayDlq: "${dlqTopic}" is empty — nothing to replay`);
    return { replayed: 0, skipped: 0 };
  }

  const highWatermarks = new Map(
    activePartitions.map(({ partition, high }) => [partition, Number.parseInt(high, 10)]),
  );
  const processedOffsets = new Map<number, number>();
  let replayed = 0;
  let skipped = 0;

  const fromBeginning = options.fromBeginning ?? true;
  // Ephemeral group when replaying from the beginning so no committed offsets
  // interfere with the seek. The group is deleted after use.
  // Stable group when replaying only new messages; committed offsets persist.
  const groupId = fromBeginning
    ? `${dlqTopic}-replay-${Date.now()}`
    : `${dlqTopic}-replay`;

  await new Promise<void>((resolve, reject) => {
    const consumer = deps.createConsumer(groupId, fromBeginning);
    const cleanup = () => deps.cleanupConsumer(consumer, groupId, fromBeginning);

    consumer
      .connect()
      .then(() => subscribeWithRetry(consumer, [dlqTopic], deps.logger))
      .then(() =>
        consumer.run({
          eachMessage: async ({ partition, message }) => {
            if (!message.value) return;

            const offset = Number.parseInt(message.offset, 10);
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
