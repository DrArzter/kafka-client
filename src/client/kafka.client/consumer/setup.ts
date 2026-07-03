import type { IConsumer, IProducer } from "../../transport/transport.interface";
import type { SchemaLike } from "../../message/topic";
import type { EventEnvelope } from "../../message/envelope";
import type { KafkaClientContext } from "../context";
import type {
  TopicMapConstraint,
  ConsumerOptions,
  ConsumerInterceptor,
  RetryOptions,
} from "../../types";
import type { MessageHandlerDeps, DeduplicationContext } from "./handler";
import { getOrCreateConsumer, buildSchemaMap, buildSerdeMap } from "./ops";
import { subscribeWithRetry } from "./subscribe-retry";
import { startRetryTopicConsumers } from "./retry-topic";
import { createRetryTxProducer, ensureTopic } from "../producer/lifecycle";
import { resolveTopicName } from "../producer/ops";
import { InMemoryDedupStore } from "../infra/dedup.store";

// ── Topic validation ──────────────────────────────────────────────────────────

/** Guard checks shared by startConsumer and startBatchConsumer. */
export function validateTopicConsumerOpts<T extends TopicMapConstraint<T>>(
  topics: any[],
  options: ConsumerOptions<T>,
): void {
  if (options.retryTopics && !options.retry) {
    throw new Error(
      "retryTopics requires retry to be configured — set retry.maxRetries to enable the retry topic chain",
    );
  }
  if (options.retryTopics && topics.some((t) => t instanceof RegExp)) {
    throw new Error(
      "retryTopics is incompatible with regex topic patterns — retry topics require a fixed topic name to build the retry chain.",
    );
  }
}

/** Ensure all required topics exist for a consumer: base, DLQ, and dedup topics. */
export async function ensureConsumerTopics<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topicNames: string[],
  dlq: boolean,
  deduplication: import("../../types").DeduplicationOptions | undefined,
): Promise<void> {
  for (const t of topicNames) await ensureTopic(ctx, t);
  if (dlq) {
    for (const t of topicNames) await ensureTopic(ctx, `${t}.dlq`);
    if (!ctx.autoCreateTopicsEnabled && topicNames.length > 0) {
      await ctx.adminOps.validateDlqTopicsExist(topicNames);
    }
  }
  if (deduplication?.strategy === "topic") {
    const dest = deduplication.duplicatesTopic;
    if (ctx.autoCreateTopicsEnabled) {
      for (const t of topicNames)
        await ensureTopic(ctx, dest ?? `${t}.duplicates`);
    } else if (topicNames.length > 0) {
      await ctx.adminOps.validateDuplicatesTopicsExist(topicNames, dest);
    }
  }
}

// ── Consumer bootstrap ────────────────────────────────────────────────────────

/**
 * Shared consumer bootstrap: groupId conflict check, schema map, connect, subscribe.
 * Returns all values needed by startConsumer / startBatchConsumer.
 */
export async function setupConsumer<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  topics: any[],
  mode: "eachMessage" | "eachBatch",
  options: ConsumerOptions<T>,
) {
  const {
    groupId: optGroupId,
    fromBeginning = false,
    retry,
    dlq = false,
    interceptors = [],
    schemas: optionSchemas,
  } = options;

  const stringTopics: any[] = topics.filter((t) => !(t instanceof RegExp));
  const regexTopics: RegExp[] = topics.filter((t) => t instanceof RegExp);
  const hasRegex = regexTopics.length > 0;

  const gid = optGroupId || ctx.defaultGroupId;
  const existingMode = ctx.runningConsumers.get(gid);
  const oppositeMode = mode === "eachMessage" ? "eachBatch" : "eachMessage";
  if (existingMode === oppositeMode) {
    throw new Error(
      `Cannot use ${mode} on consumer group "${gid}" — it is already running with ${oppositeMode}. ` +
        `Use a different groupId for this consumer.`,
    );
  }
  if (existingMode === mode) {
    const callerName =
      mode === "eachMessage" ? "startConsumer" : "startBatchConsumer";
    throw new Error(
      `${callerName}("${gid}") called twice — this group is already consuming. ` +
        `Call stopConsumer("${gid}") first or pass a different groupId.`,
    );
  }

  let resolveReady!: () => void;
  const readyPromise = new Promise<void>((resolve) => { resolveReady = resolve; });

  const consumer = getOrCreateConsumer(
    gid,
    fromBeginning,
    options.autoCommit ?? true,
    ctx.consumerOpsDeps,
    options.partitionAssigner,
    resolveReady,
    options.groupInstanceId,
  );
  const schemaMap = buildSchemaMap(
    stringTopics,
    ctx.schemaRegistry,
    optionSchemas,
    ctx.logger,
  );
  const serdeMap = buildSerdeMap(stringTopics);
  const topicNames = stringTopics.map((t: any) => resolveTopicName(t));
  const subscribeTopics: (string | RegExp)[] = [...topicNames, ...regexTopics];

  await ensureConsumerTopics(ctx, topicNames, dlq, options.deduplication);

  await consumer.connect();

  // DLQ / retry-topic / duplicates routing produce via the shared producer.
  // Connect it lazily so a consumer-only client (never called connectProducer)
  // can still deliver to those topics instead of dropping to onMessageLost.
  if (dlq || options.retryTopics || options.deduplication) {
    await ctx.producer.connect();
  }

  await subscribeWithRetry(
    consumer,
    subscribeTopics,
    ctx.logger,
    options.subscribeRetry,
  );

  const displayTopics = subscribeTopics
    .map((t) => (t instanceof RegExp ? t.toString() : t))
    .join(", ");
  ctx.logger.log(
    `${mode === "eachBatch" ? "Batch consumer" : "Consumer"} subscribed to topics: ${displayTopics}`,
  );

  return { consumer, schemaMap, serdeMap, topicNames, gid, dlq, interceptors, retry, hasRegex, readyPromise };
}

// ── Deduplication ─────────────────────────────────────────────────────────────

/**
 * Create or retrieve the deduplication context for a consumer group.
 *
 * Uses the user-supplied `options.store` when present, otherwise an
 * `InMemoryDedupStore` backed by `ctx.dedupStates` — so `stopConsumer` /
 * `disconnect` clearing that map continues to reset in-memory dedup state.
 */
export function resolveDeduplicationContext<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  groupId: string,
  options: import("../../types").DeduplicationOptions | undefined,
): DeduplicationContext | undefined {
  if (!options) return undefined;
  const store = options.store ?? new InMemoryDedupStore(ctx.dedupStates);
  return { options, store, groupId };
}

// ── Deps builders ─────────────────────────────────────────────────────────────

/** Build MessageHandlerDeps with circuit-breaker callbacks bound to the given groupId. */
export function messageDepsFor<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  gid: string,
  options?: Pick<import("../../types").ConsumerOptions<T>, "onMessageLost" | "onRetry">,
): MessageHandlerDeps {
  const notifyRetry = ctx.metrics.notifyRetry.bind(ctx.metrics);
  return {
    logger: ctx.logger,
    producer: ctx.producer,
    serde: ctx.serde,
    instrumentation: ctx.instrumentation,
    onMessageLost: options?.onMessageLost ?? ctx.onMessageLost,
    onTtlExpired: ctx.onTtlExpired,
    onRetry: options?.onRetry
      ? (envelope, attempt, max) => {
          notifyRetry(envelope, attempt, max);
          return options.onRetry!(envelope, attempt, max);
        }
      : notifyRetry,
    onDlq: (envelope, reason) => ctx.metrics.notifyDlq(envelope, reason),
    onDuplicate: ctx.metrics.notifyDuplicate.bind(ctx.metrics),
    onMessage: (envelope) => ctx.metrics.notifyMessage(envelope, gid),
    onFailure: (envelope) => ctx.metrics.notifyFailure(envelope, gid),
  };
}

/** Build the RetryTopicDeps object passed to retry topic consumers. */
export function buildRetryTopicDeps<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
) {
  return {
    logger: ctx.logger,
    producer: ctx.producer,
    serde: ctx.serde,
    instrumentation: ctx.instrumentation,
    onMessageLost: ctx.onMessageLost,
    onRetry: ctx.metrics.notifyRetry.bind(ctx.metrics),
    onDlq: ctx.metrics.notifyDlq.bind(ctx.metrics),
    onMessage: ctx.metrics.notifyMessage.bind(ctx.metrics),
    ensureTopic: (t: string) => ensureTopic(ctx, t),
    getOrCreateConsumer: (gid: string, fb: boolean, ac: boolean) =>
      getOrCreateConsumer(gid, fb, ac, ctx.consumerOpsDeps),
    runningConsumers: ctx.runningConsumers,
    createRetryTxProducer: (txId: string) => createRetryTxProducer(ctx, txId),
  };
}

// ── EOS main context ────────────────────────────────��─────────────────────────

/** Create EOS transactional producer context for atomic main → retry.1 routing. */
export async function makeEosMainContext<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  gid: string,
  consumer: IConsumer,
  options: ConsumerOptions<T>,
): Promise<{ txProducer: IProducer; consumer: IConsumer } | undefined> {
  if (!options.retryTopics || !options.retry) return undefined;
  const txProducer = await createRetryTxProducer(ctx, `${gid}-main-tx`);
  return { txProducer, consumer };
}

// ── Retry chain launcher ──────────────────────────────────────────────────────

/** Start companion retry-level consumers and register them under the main groupId. */
export async function launchRetryChain<T extends TopicMapConstraint<T>>(
  ctx: KafkaClientContext<T>,
  gid: string,
  topicNames: string[],
  handleMessage: (env: EventEnvelope<any>) => Promise<void>,
  opts: {
    retry: RetryOptions;
    dlq: boolean;
    interceptors: ConsumerInterceptor<T>[];
    schemaMap: Map<string, SchemaLike>;
    serdeMap?: Map<string, import("../../message/serde").MessageSerde>;
    assignmentTimeoutMs?: number;
  },
): Promise<void> {
  const { retry, dlq, interceptors, schemaMap, serdeMap, assignmentTimeoutMs } =
    opts;
  if (!ctx.autoCreateTopicsEnabled) {
    await ctx.adminOps.validateRetryTopicsExist(topicNames, retry.maxRetries);
  }
  // Pre-register an empty array so stopConsumer can find partially-started
  // companions if startRetryTopicConsumers throws mid-way.
  ctx.companionGroupIds.set(gid, []);
  await startRetryTopicConsumers(
    topicNames,
    gid,
    handleMessage,
    retry,
    dlq,
    interceptors,
    schemaMap,
    {
      ...ctx.retryTopicDeps,
      // Bind circuit breaker events to the MAIN consumer group so failures and
      // successes inside the retry chain drive the same breaker as the main
      // consumer (the retry chain has no breaker config of its own).
      onFailure: (envelope) => ctx.metrics.notifyFailure(envelope, gid),
      onMessage: (envelope) => ctx.metrics.notifyMessage(envelope, gid),
      onLevelStarted: (levelGroupId) => {
        ctx.companionGroupIds.get(gid)!.push(levelGroupId);
      },
    },
    assignmentTimeoutMs,
    serdeMap,
  );
}
