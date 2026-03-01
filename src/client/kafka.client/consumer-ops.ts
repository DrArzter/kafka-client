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

/**
 * Return an existing consumer for `groupId`, or create and register a new one.
 *
 * If the group already exists with different `fromBeginning` / `autoCommit` options the
 * existing consumer is returned unchanged and a warning is logged — use a distinct
 * `groupId` if different options are required.
 *
 * Partition assignment strategy defaults to `cooperative-sticky`, which minimises
 * partition movement during rebalances and is the safest choice for horizontally
 * scaled deployments.
 *
 * @param groupId Kafka consumer group ID.
 * @param fromBeginning When `true`, the group starts from the earliest available offset.
 * @param autoCommit When `true`, offsets are committed automatically. Set to `false` for manual EOS commits.
 * @param deps Shared client dependencies (consumer map, Kafka instance, logger, …).
 * @param partitionAssigner Assignment strategy — `'cooperative-sticky'` (default), `'roundrobin'`, or `'range'`.
 * @returns The consumer instance for the group (existing or newly created).
 */
export function getOrCreateConsumer(
  groupId: string,
  fromBeginning: boolean,
  autoCommit: boolean,
  deps: ConsumerOpsDeps,
  partitionAssigner?: "roundrobin" | "range" | "cooperative-sticky",
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

  // Default to cooperative-sticky: minimal partition movement during rebalances.
  // This is especially important for horizontal scaling — only the partitions that
  // actually need to be reassigned are moved, avoiding a full stop-the-world rebalance.
  const assigners: KafkaJS.PartitionAssigners[] = [
    partitionAssigner === "roundrobin"
      ? KafkaJS.PartitionAssigners.roundRobin
      : partitionAssigner === "range"
        ? KafkaJS.PartitionAssigners.range
        : KafkaJS.PartitionAssigners.cooperativeSticky,
  ];

  const config: Parameters<typeof kafka.consumer>[0] = {
    kafkaJS: { groupId, fromBeginning, autoCommit, partitionAssigners: assigners },
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

/**
 * Build a local schema map for the topics being subscribed to.
 *
 * Schemas are collected from two sources in order:
 * 1. Inline schemas on `TopicDescriptor` objects in the `topics` array.
 * 2. Explicit overrides passed in `optionSchemas` (e.g. from `ConsumerOptions.schemas`).
 *
 * Both sources are also registered in the shared `schemaRegistry` so that the same
 * schema is used consistently across producers and consumers. A warning is logged when
 * a topic already has a different schema registered.
 *
 * @param topics Array of topic names or `TopicDescriptor` objects.
 * @param schemaRegistry Shared client-wide schema map (mutated in place).
 * @param optionSchemas Additional topic → schema overrides from consumer options.
 * @param logger Optional logger for schema-conflict warnings.
 * @returns A topic → schema map scoped to the current subscription.
 */
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
