import type { IConsumer, KafkaTransport } from "../../transport/transport.interface";
import type { SchemaLike } from "../../message/topic";
import type { KafkaClientOptions, KafkaLogger } from "../../types";
import { resolveTopicName } from "../producer/ops";

export type ConsumerOpsDeps = {
  consumers: Map<string, IConsumer>;
  consumerCreationOptions: Map<
    string,
    { fromBeginning: boolean; autoCommit: boolean }
  >;
  transport: KafkaTransport;
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
 * @param deps Shared client dependencies (consumer map, transport, logger, …).
 * @param partitionAssigner Assignment strategy — `'cooperative-sticky'` (default), `'roundrobin'`, or `'range'`.
 * @returns The consumer instance for the group (existing or newly created).
 */
export function getOrCreateConsumer(
  groupId: string,
  fromBeginning: boolean,
  autoCommit: boolean,
  deps: ConsumerOpsDeps,
  partitionAssigner?: "roundrobin" | "range" | "cooperative-sticky",
  onFirstAssignment?: () => void,
  groupInstanceId?: string,
): IConsumer {
  const { consumers, consumerCreationOptions, transport, onRebalance, logger } =
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
    // Consumer already exists — treat as immediately ready (assignment already happened)
    onFirstAssignment?.();
    return consumers.get(groupId)!;
  }

  consumerCreationOptions.set(groupId, { fromBeginning, autoCommit });

  // Debounced assignment resolver for handle.ready().
  //
  // Rather than resolving on the *first* assign event, we debounce: the promise
  // resolves SETTLE_MS after the *last* assign event with no subsequent assign.
  // This correctly handles multi-consumer groups where the initial solo assignment
  // is immediately followed by a rebalance when peers join — we wait until the
  // group is stable.
  //
  // When fromBeginning is false (auto.offset.reset = latest), librdkafka fires
  // ERR__ASSIGN_PARTITIONS BEFORE completing the async broker round-trip that
  // establishes the initial fetch position. The settle window also covers this
  // offset fetch so messages sent right after ready() resolves are not missed.
  //
  // fromBeginning: true doesn't need a settle window — seeks to offset 0
  // happen synchronously and the consumer catches up from the beginning anyway.
  // Debounced readiness: resolves SETTLE_MS after the last rebalance event
  // (assign OR revoke) with no subsequent event.  We reset on revoke too because
  // in cooperative-sticky rebalancing a REVOKE fires mid-protocol (e.g. ConsumerA
  // loses one partition when a peer joins) and the group isn't stable until the
  // revoke has settled and peers have received their assigns.  We guard against
  // firing before any "assign" has been seen so the promise never resolves on a
  // pure-revoke cycle.
  //
  // fromBeginning: false needs the settle window so librdkafka can complete the
  // async broker round-trip that fetches the initial "latest" offset after each
  // assign.  fromBeginning: true doesn't need it — seeks to offset 0 happen
  // synchronously and the consumer will always catch up from the beginning.
  const SETTLE_MS = fromBeginning ? 0 : 500;
  let hasAssignment = false;
  let settleTimer: ReturnType<typeof setTimeout> | undefined;
  const scheduleSettle = () => {
    if (!hasAssignment) return;
    if (settleTimer) clearTimeout(settleTimer);
    settleTimer = setTimeout(() => {
      settleTimer = undefined;
      onFirstAssignment?.();
    }, SETTLE_MS);
  };
  // Thin wrapper used at the call-site below — keeps the old name readable.
  const fireOnAssignment = () => {
    hasAssignment = true;
    scheduleSettle();
  };

  const consumer = transport.consumer({
    groupId,
    fromBeginning,
    autoCommit,
    partitionAssigner: partitionAssigner ?? "cooperative-sticky",
    groupInstanceId,
    onRebalance: (type, assignments) => {
      if (type === "assign") fireOnAssignment();
      else if (type === "revoke") scheduleSettle(); // reset timer mid-rebalance
      if (onRebalance) {
        try {
          onRebalance(type, assignments);
        } catch (e) {
          logger.warn(`onRebalance callback threw: ${(e as Error).message}`);
        }
      }
    },
  });

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
