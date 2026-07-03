import type { KafkaClientContext } from "../../context";
import type {
  TopicMapConstraint,
  ReadSnapshotOptions,
  CheckpointResult,
  CheckpointRestoreResult,
  RestoreCheckpointOptions,
  CheckpointEntry,
} from "../../../types";
import type { EventEnvelope } from "../../../message/envelope";
import { decodeHeaders, extractEnvelope } from "../../../message/envelope";
import { toError } from "../pipeline";

/** Minimal context surface required by this module. */
type SnapshotCtx<T extends TopicMapConstraint<T>> = Pick<
  KafkaClientContext<T>,
  | "adminOps"
  | "clientId"
  | "defaultGroupId"
  | "transport"
  | "logger"
  | "producer"
  | "runningConsumers"
>;

// ── readSnapshot ──────────────────────────────────────────────────────────────

export async function readSnapshotImpl<
  T extends TopicMapConstraint<T>,
  K extends keyof T & string,
>(
  ctx: SnapshotCtx<T>,
  topic: K,
  options: ReadSnapshotOptions = {},
): Promise<Map<string, EventEnvelope<T[K]>>> {
  await ctx.adminOps.ensureConnected();

  let offsets: Array<{ partition: number; low: string; high: string }>;
  try {
    offsets = await ctx.adminOps.admin.fetchTopicOffsets(topic);
  } catch {
    ctx.logger.warn(
      `readSnapshot: could not fetch offsets for "${String(topic)}", returning empty snapshot`,
    );
    return new Map();
  }

  const targets = new Map<number, number>();
  for (const { partition, high, low } of offsets) {
    const highN = Number.parseInt(high, 10);
    const lowN = Number.parseInt(low, 10);
    if (highN > lowN) targets.set(partition, highN - 1);
  }

  if (targets.size === 0) {
    ctx.logger.debug?.(
      `readSnapshot: topic "${String(topic)}" is empty — returning empty snapshot`,
    );
    return new Map();
  }

  const snapshot = new Map<string, EventEnvelope<T[K]>>();
  const remaining = new Set(targets.keys());
  const snapshotGroupId = `${ctx.clientId}-snapshot-${Date.now()}`;

  await new Promise<void>((resolve, reject) => {
    const consumer = ctx.transport.consumer({
      groupId: snapshotGroupId,
      fromBeginning: true,
    });
    const cleanup = () => {
      consumer
        .disconnect()
        .catch(() => {})
        .finally(() => {
          ctx.adminOps.deleteGroups([snapshotGroupId]).catch(() => {});
        });
    };

    consumer
      .connect()
      .then(() => consumer.subscribe({ topics: [topic] }))
      .then(() =>
        consumer.run({
          eachMessage: async ({ topic: t, partition, message }) => {
            if (!remaining.has(partition)) return;

            const msgOffsetN = Number.parseInt(message.offset, 10);
            applySnapshotMessage(snapshot, options, ctx, t, partition, message);

            if (msgOffsetN >= targets.get(partition)!) {
              remaining.delete(partition);
              if (remaining.size === 0) {
                cleanup();
                resolve();
              }
            }
          },
        }),
      )
      .catch((err) => {
        cleanup();
        reject(err);
      });
  });

  ctx.logger.log(
    `readSnapshot: ${snapshot.size} key(s) from "${String(topic)}"`,
  );
  return snapshot;
}

function applySnapshotMessage<
  T extends TopicMapConstraint<T>,
  K extends keyof T & string,
>(
  snapshot: Map<string, EventEnvelope<T[K]>>,
  options: ReadSnapshotOptions,
  ctx: SnapshotCtx<T>,
  t: string,
  partition: number,
  message: { key: Buffer | null; value: Buffer | null; offset: string; headers?: any },
): void {
  let key: string | null = null;
  if (message.key) {
    key = Buffer.isBuffer(message.key)
      ? message.key.toString()
      : String(message.key);
  }

  if (message.value === null || message.value === undefined) {
    if (key !== null) {
      snapshot.delete(key);
      options.onTombstone?.(key);
    }
    return;
  }

  if (key === null) return;

  const rawValue = Buffer.isBuffer(message.value)
    ? message.value.toString()
    : String(message.value);
  try {
    // NOTE: readSnapshot is JSON-only for now — it does NOT yet honour a custom
    // `MessageSerde` (client-wide or per-topic). Compacted-topic snapshots of
    // non-JSON (Avro/Protobuf) payloads are not supported until a later phase.
    const jsonValue = JSON.parse(rawValue);
    const headers = decodeHeaders(message.headers);
    const parsed: T[K] = options.schema ? options.schema.parse(jsonValue) : jsonValue;
    snapshot.set(
      key,
      extractEnvelope<T[K]>(parsed, headers, t, partition, message.offset),
    );
  } catch (err) {
    ctx.logger.warn(
      `readSnapshot: skipping ${t}:${partition}@${message.offset} — ${toError(err).message}`,
    );
  }
}

// ── checkpointOffsets ─────────────────────────────────────────────────────────

export async function checkpointOffsetsImpl<T extends TopicMapConstraint<T>>(
  ctx: SnapshotCtx<T>,
  groupId: string | undefined,
  checkpointTopic: string,
): Promise<CheckpointResult> {
  const gid = groupId ?? ctx.defaultGroupId;
  await ctx.adminOps.ensureConnected();

  const committed = await ctx.adminOps.admin.fetchOffsets({ groupId: gid });

  const offsets: CheckpointEntry[] = [];
  for (const { topic, partitions } of committed) {
    for (const { partition, offset } of partitions) {
      offsets.push({ topic, partition, offset });
    }
  }

  const savedAt = Date.now();
  const payload = JSON.stringify({ groupId: gid, offsets, savedAt });

  await ctx.producer.send({
    topic: checkpointTopic,
    messages: [
      {
        key: gid,
        value: payload,
        headers: {
          "x-checkpoint-group-id": [gid],
          "x-checkpoint-timestamp": [String(savedAt)],
        },
      },
    ],
  });

  const topics = [...new Set(offsets.map((e) => e.topic))];
  ctx.logger.log(
    `checkpointOffsets: saved ${offsets.length} partition(s) for group "${gid}" → "${checkpointTopic}"`,
  );
  return { groupId: gid, topics, partitionCount: offsets.length, savedAt };
}

// ── restoreFromCheckpoint ─────────────────────────────────────────────────────

export async function restoreFromCheckpointImpl<T extends TopicMapConstraint<T>>(
  ctx: SnapshotCtx<T>,
  groupId: string | undefined,
  checkpointTopic: string,
  options: RestoreCheckpointOptions = {},
): Promise<CheckpointRestoreResult> {
  const gid = groupId ?? ctx.defaultGroupId;

  if (ctx.runningConsumers.has(gid)) {
    throw new Error(
      `restoreFromCheckpoint: consumer group "${gid}" is still running. ` +
        `Call stopConsumer("${gid}") before restoring offsets.`,
    );
  }

  await ctx.adminOps.ensureConnected();

  const checkpoints: Array<{ savedAt: number; offsets: CheckpointEntry[] }> = [];

  let hwmOffsets: Array<{ partition: number; low: string; high: string }>;
  try {
    hwmOffsets = await ctx.adminOps.admin.fetchTopicOffsets(checkpointTopic);
  } catch {
    throw new Error(
      `restoreFromCheckpoint: could not fetch offsets for "${checkpointTopic}" — does the topic exist?`,
    );
  }

  const targets = new Map<number, number>();
  for (const { partition, high, low } of hwmOffsets) {
    const highN = Number.parseInt(high, 10);
    if (highN > Number.parseInt(low, 10)) targets.set(partition, highN - 1);
  }

  if (targets.size > 0) {
    const checkpointGroupId = `${ctx.clientId}-checkpoint-restore-${Date.now()}`;
    await new Promise<void>((resolve, reject) => {
      const consumer = ctx.transport.consumer({
        groupId: checkpointGroupId,
        fromBeginning: true,
      });
      const cleanup = () => {
        consumer
          .disconnect()
          .catch(() => {})
          .finally(() => {
            ctx.adminOps.deleteGroups([checkpointGroupId]).catch(() => {});
          });
      };
      const remaining = new Set(targets.keys());

      consumer
        .connect()
        .then(() => consumer.subscribe({ topics: [checkpointTopic] }))
        .then(() =>
          consumer.run({
            eachMessage: async ({ partition, message }) => {
              if (!remaining.has(partition)) return;

              let msgKey: string | null = null;
              if (message.key) {
                msgKey = Buffer.isBuffer(message.key)
                  ? message.key.toString()
                  : String(message.key);
              }

              if (msgKey === gid && message.value) {
                try {
                  const raw = Buffer.isBuffer(message.value)
                    ? message.value.toString()
                    : String(message.value);
                  const parsed = JSON.parse(raw) as {
                    groupId: string;
                    offsets: CheckpointEntry[];
                    savedAt: number;
                  };
                  checkpoints.push({
                    savedAt: parsed.savedAt,
                    offsets: parsed.offsets,
                  });
                } catch {
                  ctx.logger.warn(
                    `restoreFromCheckpoint: skipping malformed checkpoint at partition ${partition}@${message.offset}`,
                  );
                }
              }

              if (
                Number.parseInt(message.offset, 10) >= targets.get(partition)!
              ) {
                remaining.delete(partition);
                if (remaining.size === 0) {
                  cleanup();
                  resolve();
                }
              }
            },
          }),
        )
        .catch((err) => {
          cleanup();
          reject(err);
        });
    });
  }

  if (checkpoints.length === 0) {
    throw new Error(
      `restoreFromCheckpoint: no checkpoints found for group "${gid}" in "${checkpointTopic}"`,
    );
  }

  const target = options.timestamp;
  let best: (typeof checkpoints)[number];
  if (target === undefined) {
    best = checkpoints.reduce(
      (acc, c) => (c.savedAt > acc.savedAt ? c : acc),
      checkpoints[0],
    );
  } else {
    const candidates = checkpoints.filter((c) => c.savedAt <= target);
    if (candidates.length > 0) {
      best = candidates.reduce(
        (acc, c) => (c.savedAt > acc.savedAt ? c : acc),
        candidates[0],
      );
    } else {
      best = checkpoints.reduce(
        (acc, c) => (c.savedAt < acc.savedAt ? c : acc),
        checkpoints[0],
      );
      ctx.logger.warn(
        `restoreFromCheckpoint: no checkpoint at or before ${new Date(target).toISOString()} — ` +
          `using oldest available (${new Date(best.savedAt).toISOString()})`,
      );
    }
  }

  await ctx.adminOps.seekToOffset(gid, best.offsets);

  const checkpointAge = Date.now() - best.savedAt;
  ctx.logger.log(
    `restoreFromCheckpoint: restored ${best.offsets.length} partition(s) for group "${gid}" ` +
      `from checkpoint at ${new Date(best.savedAt).toISOString()} (age: ${checkpointAge}ms)`,
  );

  return {
    groupId: gid,
    offsets: best.offsets,
    restoredAt: best.savedAt,
    checkpointAge,
  };
}
