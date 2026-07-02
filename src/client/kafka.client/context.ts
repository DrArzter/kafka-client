import type { KafkaTransport, IProducer, IConsumer } from "../transport/transport.interface";
import type { SchemaLike } from "../message/topic";
import type {
  ClientId,
  KafkaClientOptions,
  KafkaInstrumentation,
  KafkaLogger,
} from "../types";
import type { CircuitBreakerManager } from "./infra/circuit-breaker.manager";
import type { AdminOps } from "./admin/ops";
import type { MetricsManager } from "./infra/metrics.manager";
import type { InFlightTracker } from "./infra/inflight.tracker";
import type { BuildSendPayloadDeps } from "./producer/ops";
import type { ConsumerOpsDeps } from "./consumer/ops";
import type { RetryTopicDeps } from "./consumer/retry-topic";

/**
 * All shared state for a `KafkaClient` instance passed to extracted
 * implementation functions. The class stores this as `this.ctx` and
 * delegates every method to the relevant module.
 */
export type KafkaClientContext<T> = {
  /** @internal Phantom type carrier — keeps the topic-map generic flowing through impl modules. */
  readonly __topicMap?: T;

  // ── Identity ──────────────────────────────────────────────────────
  readonly clientId: ClientId;
  readonly defaultGroupId: string;

  // ── Config ────────────────────────────────────────────────────────
  readonly logger: KafkaLogger;
  readonly autoCreateTopicsEnabled: boolean;
  readonly strictSchemasEnabled: boolean;
  readonly numPartitions: number;
  readonly txId: string;
  readonly clockRecoveryTopics: string[];
  /** Max time to wait for clock recovery before proceeding with a partial result. */
  readonly clockRecoveryTimeoutMs: number;
  readonly lagThrottleOpts: KafkaClientOptions["lagThrottle"];
  readonly instrumentation: KafkaInstrumentation[];
  readonly onMessageLost: KafkaClientOptions["onMessageLost"];
  readonly onTtlExpired: KafkaClientOptions["onTtlExpired"];
  readonly onRebalance: KafkaClientOptions["onRebalance"];

  // ── Transport + connections ───────────────────────────────────────
  readonly transport: KafkaTransport;
  readonly producer: IProducer;
  /** Lazy transactional producer — created on first `transaction()` call. */
  txProducer: IProducer | undefined;
  /** Guards concurrent init so only one producer is created. */
  txProducerInitPromise: Promise<IProducer> | undefined;
  /** Per-transactionalId producers used by retry level consumers. */
  readonly retryTxProducers: Map<string, IProducer>;
  /**
   * Serialises `transaction()` calls — a transactional producer supports only
   * one open transaction at a time, so overlapping calls queue behind this chain.
   */
  _txChain: Promise<void>;

  // ── Consumer tracking ─────────────────────────────────────────────
  readonly consumers: Map<string, IConsumer>;
  readonly runningConsumers: Map<string, "eachMessage" | "eachBatch">;
  readonly consumerCreationOptions: Map<
    string,
    { fromBeginning: boolean; autoCommit: boolean }
  >;
  /** Maps main groupId → companion retry-level groupIds. */
  readonly companionGroupIds: Map<string, string[]>;
  /** Per-groupId Lamport clock dedup state: `"topic:partition"` → last seen clock. */
  readonly dedupStates: Map<string, Map<string, number>>;

  // ── Topic tracking ────────────────────────────────────────────────
  readonly ensuredTopics: Set<string>;
  /** Pending createTopics promises — deduplicates concurrent calls for the same topic. */
  readonly ensureTopicPromises: Map<string, Promise<void>>;
  readonly schemaRegistry: Map<string, SchemaLike>;

  // ── Lag throttle (mutable) ────────────────────────────────────────
  _lagThrottled: boolean;
  _lagThrottleTimer: ReturnType<typeof setInterval> | undefined;

  // ── Lamport clock (mutable) ───────────────────────────────────────
  _lamportClock: number;

  // ── Sub-services ──────────────────────────────────────────────────
  readonly circuitBreaker: CircuitBreakerManager;
  readonly adminOps: AdminOps;
  readonly metrics: MetricsManager;
  readonly inFlight: InFlightTracker;

  // ── Prebuilt deps objects ─────────────────────────────────────────
  readonly producerOpsDeps: BuildSendPayloadDeps;
  readonly consumerOpsDeps: ConsumerOpsDeps;
  retryTopicDeps: RetryTopicDeps;
};
