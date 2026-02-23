import { KafkaJS } from "@confluentinc/kafka-javascript";
type Consumer = KafkaJS.Consumer;
type Kafka = KafkaJS.Kafka;
import type { SchemaLike } from "../message/topic";
import type { KafkaClientOptions, KafkaLogger } from "../types";
import { resolveTopicName } from "./producer-ops";

export type ConsumerOpsDeps = {
  consumers: Map<string, Consumer>;
  consumerCreationOptions: Map<
    string,
    { fromBeginning: boolean; autoCommit: boolean }
  >;
  kafka: Kafka;
  onRebalance: KafkaClientOptions["onRebalance"];
  logger: KafkaLogger;
};

export function getOrCreateConsumer(
  groupId: string,
  fromBeginning: boolean,
  autoCommit: boolean,
  deps: ConsumerOpsDeps,
): Consumer {
  const { consumers, consumerCreationOptions, kafka, onRebalance, logger } =
    deps;

  if (consumers.has(groupId)) {
    const prev = consumerCreationOptions.get(groupId)!;
    if (
      prev.fromBeginning !== fromBeginning ||
      prev.autoCommit !== autoCommit
    ) {
      logger.warn(
        `Consumer group "${groupId}" already exists with options ` +
          `(fromBeginning: ${prev.fromBeginning}, autoCommit: ${prev.autoCommit}) — ` +
          `new options (fromBeginning: ${fromBeginning}, autoCommit: ${autoCommit}) ignored. ` +
          `Use a different groupId to apply different options.`,
      );
    }
    return consumers.get(groupId)!;
  }

  consumerCreationOptions.set(groupId, { fromBeginning, autoCommit });

  const config: Parameters<typeof kafka.consumer>[0] = {
    kafkaJS: { groupId, fromBeginning, autoCommit },
  };

  if (onRebalance) {
    const cb = onRebalance;
    // rebalance_cb is called by librdkafka on every partition assign/revoke.
    // err.code -175 = ERR__ASSIGN_PARTITIONS, -174 = ERR__REVOKE_PARTITIONS.
    // The library handles the actual assign/unassign in its finally block regardless
    // of what this callback does, so we only need it for the side-effect notification.
    (config as any)["rebalance_cb"] = (err: any, assignment: any[]) => {
      const type = err.code === -175 ? "assign" : "revoke";
      try {
        cb(
          type,
          assignment.map((p) => ({ topic: p.topic, partition: p.partition })),
        );
      } catch (e) {
        logger.warn(`onRebalance callback threw: ${(e as Error).message}`);
      }
    };
  }

  const consumer = kafka.consumer(config);
  consumers.set(groupId, consumer);
  return consumer;
}

export function buildSchemaMap(
  topics: any[],
  schemaRegistry: Map<string, SchemaLike>,
  optionSchemas?: Map<string, SchemaLike>,
  logger?: KafkaLogger,
): Map<string, SchemaLike> {
  const schemaMap = new Map<string, SchemaLike>();

  const registerChecked = (name: string, schema: SchemaLike) => {
    const existing = schemaRegistry.get(name);
    if (existing && existing !== schema) {
      logger?.warn(
        `Schema conflict for topic "${name}": a different schema is already registered. ` +
          `Using the new schema — ensure consistent schemas to avoid silent validation mismatches.`,
      );
    }
    schemaMap.set(name, schema);
    schemaRegistry.set(name, schema);
  };

  for (const t of topics) {
    if (t?.__schema) {
      registerChecked(resolveTopicName(t), t.__schema);
    }
  }
  if (optionSchemas) {
    for (const [k, v] of optionSchemas) {
      registerChecked(k, v);
    }
  }
  return schemaMap;
}
