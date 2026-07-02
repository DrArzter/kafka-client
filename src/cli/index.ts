#!/usr/bin/env node
import { KafkaClient } from "../core";
import type { EventEnvelope } from "../core";
import { ConfluentTransport } from "../client/transport/confluent.transport";
import type { KafkaTransport } from "../client/transport/transport.interface";
import {
  DlqUsageError,
  USAGE,
  parseArgs,
  runDlqCommand,
  type DlqCliClient,
  type PartitionWatermarks,
} from "./dlq";

/** Unique-per-process client id so concurrent invocations don't collide. */
const CLIENT_ID = `dlq-cli-${process.pid}`;
const GROUP_ID = `${CLIENT_ID}-peek`;

/**
 * Production `DlqCliClient` factory. Shares a single `ConfluentTransport`
 * between the `KafkaClient` (for `replayDlq` / `consume`) and a standalone
 * admin (for topic listing and per-partition watermarks used by `ls`).
 */
function createRealClient(brokers: string[]): DlqCliClient {
  const transport: KafkaTransport = new ConfluentTransport(CLIENT_ID, brokers);
  const kafka = new KafkaClient<Record<string, Record<string, unknown>>>(
    CLIENT_ID,
    GROUP_ID,
    brokers,
    { transport, autoCreateTopics: false, strictSchemas: false },
  );
  const admin = transport.admin();
  let adminConnected = false;

  async function ensureAdmin() {
    if (!adminConnected) {
      await admin.connect();
      adminConnected = true;
    }
  }

  return {
    async listTopics(): Promise<string[]> {
      const status = await kafka.checkStatus();
      if (status.status === "down") {
        throw new Error(`Broker unreachable: ${status.error}`);
      }
      return status.topics;
    },

    async fetchTopicOffsets(topic: string): Promise<PartitionWatermarks[]> {
      await ensureAdmin();
      return admin.fetchTopicOffsets(topic);
    },

    async peekMessages(
      dlqTopic: string,
      limit: number,
    ): Promise<Array<EventEnvelope<unknown>>> {
      const collected: Array<EventEnvelope<unknown>> = [];
      // Ephemeral group so peeking never disturbs a real consumer's offsets.
      const iterator = kafka.consume(dlqTopic as never, {
        groupId: `${dlqTopic}.dlq-peek-${Date.now()}`,
        fromBeginning: true,
      });
      // Guard against an empty topic hanging the iterator forever: if the topic
      // has no messages, stop once the high watermark says there's nothing to read.
      await ensureAdmin();
      const watermarks = await admin.fetchTopicOffsets(dlqTopic);
      const available = watermarks.reduce(
        (sum, w) => sum + Math.max(0, Number(w.high) - Number(w.low)),
        0,
      );
      const target = Math.min(limit, available);
      if (target === 0) {
        await iterator.return?.();
        return collected;
      }
      try {
        for await (const env of iterator) {
          collected.push(env as EventEnvelope<unknown>);
          if (collected.length >= target) break;
        }
      } finally {
        await iterator.return?.();
      }
      return collected;
    },

    replayDlq(topic, options) {
      return kafka.replayDlq(topic, options);
    },

    async close(): Promise<void> {
      if (adminConnected) {
        await admin.disconnect().catch(() => {});
      }
      await kafka.disconnect().catch(() => {});
    },
  };
}

async function main(): Promise<number> {
  let cmd;
  try {
    cmd = parseArgs(process.argv.slice(2));
  } catch (err) {
    if (err instanceof DlqUsageError) {
      process.stderr.write(`Error: ${err.message}\n\n`);
      process.stderr.write(USAGE);
      return 2;
    }
    throw err;
  }

  try {
    await runDlqCommand(cmd, {
      createClient: createRealClient,
      out: (line) => process.stdout.write(`${line}\n`),
    });
    return 0;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write(`Error: ${message}\n`);
    return 1;
  }
}

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch((err) => {
    process.stderr.write(`Fatal: ${err?.stack ?? err}\n`);
    process.exitCode = 1;
  });
