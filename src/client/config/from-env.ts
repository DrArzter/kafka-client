import type { TTopicMessageMap } from "../types/common";
import type { KafkaClientOptions } from "../types/config.types";
import type {
  ConsumerOptions,
  RetryOptions,
  DeduplicationOptions,
  CircuitBreakerOptions,
  SubscribeRetryOptions,
} from "../types/consumer.types";
import type {
  KafkaSecurityOptions,
  KafkaSaslOptions,
} from "../security/security.types";

/**
 * Structured result of {@link kafkaClientConfigFromEnv}.
 *
 * The three positional `KafkaClient` constructor arguments (`clientId`,
 * `groupId`, `brokers`) are surfaced separately from {@link options} so callers
 * can supply their own fallbacks with `??`:
 *
 * ```ts
 * const { clientId, groupId, brokers, options } = kafkaClientConfigFromEnv();
 * const kafka = new KafkaClient(
 *   clientId ?? 'my-svc',
 *   groupId ?? 'my-grp',
 *   brokers ?? ['localhost:9092'],
 *   options,
 * );
 * ```
 *
 * Any of `clientId`, `groupId`, or `brokers` is `undefined` when its variable is
 * unset, so the caller's fallback (not an empty string / empty array) is used.
 * {@link options} only carries the keys whose variables were present — unset
 * variables never appear, so library defaults and any code-level values you spread
 * afterwards remain untouched.
 */
export interface EnvClientConfig {
  /** `KAFKA_CLIENT_ID` — first `KafkaClient` constructor argument. */
  clientId?: string;
  /** `KAFKA_GROUP_ID` — second `KafkaClient` constructor argument. */
  groupId?: string;
  /** `KAFKA_BROKERS` (comma-separated) — third `KafkaClient` constructor argument. */
  brokers?: string[];
  /** Partial {@link KafkaClientOptions} — only keys whose env vars were present. */
  options: KafkaClientOptions;
}

// ── Primitive parsers ─────────────────────────────────────────────────────────

const TRUE_VALUES = new Set(["true", "1", "yes"]);
const FALSE_VALUES = new Set(["false", "0", "no"]);

/**
 * Parse a boolean env value. Accepts (case-insensitive) `true`/`false`,
 * `1`/`0`, `yes`/`no`. Anything else throws, naming the variable.
 *
 * @throws {Error} when the value is not a recognised boolean literal.
 */
function parseBool(name: string, raw: string): boolean {
  const normalized = raw.trim().toLowerCase();
  if (TRUE_VALUES.has(normalized)) return true;
  if (FALSE_VALUES.has(normalized)) return false;
  throw new Error(
    `Invalid boolean for ${name}: "${raw}". ` +
      `Use one of true/false, 1/0, yes/no (case-insensitive).`,
  );
}

/**
 * Parse a numeric env value via `Number()`. `NaN` throws, naming the variable.
 *
 * @throws {Error} when the value does not parse to a finite number.
 */
function parseNum(name: string, raw: string): number {
  const value = Number(raw.trim());
  if (Number.isNaN(value)) {
    throw new Error(`Invalid number for ${name}: "${raw}".`);
  }
  return value;
}

/**
 * Parse a comma-separated list. Entries are trimmed and empty entries dropped,
 * so trailing commas and stray whitespace are tolerated.
 */
function parseList(raw: string): string[] {
  return raw
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

/**
 * Validate an env value against a fixed set of allowed literals.
 *
 * @throws {Error} when the value is not one of `allowed`.
 */
function parseEnum<V extends string>(
  name: string,
  raw: string,
  allowed: readonly V[],
): V {
  const value = raw.trim() as V;
  if (!allowed.includes(value)) {
    throw new Error(
      `Invalid value for ${name}: "${raw}". Allowed: ${allowed.join(", ")}.`,
    );
  }
  return value;
}

/**
 * Read a variable and, if present and non-empty, invoke `apply` with its raw
 * value. Unset or whitespace-only variables are skipped so no key is emitted —
 * this is what preserves library defaults and code-level values.
 */
function readVar(
  env: NodeJS.ProcessEnv,
  key: string,
  apply: (raw: string) => void,
): void {
  const raw = env[key];
  if (raw === undefined || raw.trim() === "") return;
  apply(raw);
}

// ── Client config ───────────────────────────────────────────────────────────

/**
 * Build partial `KafkaClient` constructor arguments from environment variables.
 *
 * **Precedence — read this carefully.** These helpers only *feed* the constructor
 * arguments; they never override anything. The effective precedence is:
 *
 * > **explicit code options > env vars > built-in library defaults**
 *
 * Env vars fill in what the developer did not hard-code; anything you spread
 * *after* `...options` wins over the env value, and any key absent from the
 * environment leaves the library default in place. The library never reads a
 * `.env` file itself — load one with `node --env-file=.env` (Node 20.6+) or the
 * `dotenv` package **before** constructing the client.
 *
 * ### Supported variables (default prefix `KAFKA_`)
 *
 * | Variable | Type | Maps to |
 * |---|---|---|
 * | `CLIENT_ID` | string | `clientId` (arg) |
 * | `GROUP_ID` | string | `groupId` (arg) |
 * | `BROKERS` | list | `brokers` (arg) |
 * | `AUTO_CREATE_TOPICS` | boolean | `autoCreateTopics` |
 * | `STRICT_SCHEMAS` | boolean | `strictSchemas` |
 * | `NUM_PARTITIONS` | number | `numPartitions` |
 * | `TRANSACTIONAL_ID` | string | `transactionalId` |
 * | `CLOCK_RECOVERY_TOPICS` | list | `clockRecovery.topics` |
 * | `CLOCK_RECOVERY_TIMEOUT_MS` | number | `clockRecovery.timeoutMs` |
 * | `LAG_THROTTLE_MAX_LAG` | number | `lagThrottle.maxLag` (enables the block) |
 * | `LAG_THROTTLE_GROUP_ID` | string | `lagThrottle.groupId` |
 * | `LAG_THROTTLE_POLL_INTERVAL_MS` | number | `lagThrottle.pollIntervalMs` |
 * | `LAG_THROTTLE_MAX_WAIT_MS` | number | `lagThrottle.maxWaitMs` |
 * | `SSL` | boolean | `security.ssl` |
 * | `SASL_MECHANISM` | `plain`\|`scram-sha-256`\|`scram-sha-512` | `security.sasl.mechanism` |
 * | `SASL_USERNAME` | string | `security.sasl.username` |
 * | `SASL_PASSWORD` | string | `security.sasl.password` |
 * | `ALLOW_INSECURE` | boolean | `security.allowInsecure` |
 *
 * `lagThrottle` is only present when `LAG_THROTTLE_MAX_LAG` is set (it is the
 * only required field). `clockRecovery` is only present when
 * `CLOCK_RECOVERY_TOPICS` is set. `security` is only present when at least one of
 * its variables is set.
 *
 * **`oauthbearer` cannot come from env.** Token providers
 * (`awsMskIamProvider`, `gcpAccessTokenProvider`, custom async factories) are
 * functions — configure them in code, not via environment variables. Only the
 * username/password SASL mechanisms are env-configurable.
 *
 * @param env    Environment source. Defaults to `process.env`. Pass an explicit
 *               object in tests to avoid mutating the real environment.
 * @param prefix Variable prefix. Defaults to `"KAFKA_"`.
 *
 * @throws {Error} when a boolean/number/enum value is malformed (the message
 *                 names the offending variable).
 *
 * @example Basic usage with fallbacks — code always wins / extends
 * ```ts
 * import { KafkaClient } from '@drarzter/kafka-client/core';
 * import { kafkaClientConfigFromEnv } from '@drarzter/kafka-client/core';
 *
 * const { clientId, groupId, brokers, options } = kafkaClientConfigFromEnv();
 * const kafka = new KafkaClient(
 *   clientId ?? 'my-svc',
 *   groupId ?? 'my-grp',
 *   brokers ?? ['localhost:9092'],
 *   {
 *     ...options,
 *     onMessageLost: alerting, // code-level value: not env-configurable, always applied
 *   },
 * );
 * ```
 */
export function kafkaClientConfigFromEnv(
  env: NodeJS.ProcessEnv = process.env,
  prefix = "KAFKA_",
): EnvClientConfig {
  const options: KafkaClientOptions = {};
  const result: EnvClientConfig = { options };

  readVar(env, `${prefix}CLIENT_ID`, (raw) => {
    result.clientId = raw.trim();
  });
  readVar(env, `${prefix}GROUP_ID`, (raw) => {
    result.groupId = raw.trim();
  });
  readVar(env, `${prefix}BROKERS`, (raw) => {
    result.brokers = parseList(raw);
  });

  readVar(env, `${prefix}AUTO_CREATE_TOPICS`, (raw) => {
    options.autoCreateTopics = parseBool(`${prefix}AUTO_CREATE_TOPICS`, raw);
  });
  readVar(env, `${prefix}STRICT_SCHEMAS`, (raw) => {
    options.strictSchemas = parseBool(`${prefix}STRICT_SCHEMAS`, raw);
  });
  readVar(env, `${prefix}NUM_PARTITIONS`, (raw) => {
    options.numPartitions = parseNum(`${prefix}NUM_PARTITIONS`, raw);
  });
  readVar(env, `${prefix}TRANSACTIONAL_ID`, (raw) => {
    options.transactionalId = raw.trim();
  });

  // clockRecovery — only present when topics are configured.
  readVar(env, `${prefix}CLOCK_RECOVERY_TOPICS`, (raw) => {
    const topics = parseList(raw);
    if (topics.length === 0) return;
    options.clockRecovery = { topics };
  });
  readVar(env, `${prefix}CLOCK_RECOVERY_TIMEOUT_MS`, (raw) => {
    const timeoutMs = parseNum(`${prefix}CLOCK_RECOVERY_TIMEOUT_MS`, raw);
    if (options.clockRecovery) {
      options.clockRecovery.timeoutMs = timeoutMs;
    }
  });

  // lagThrottle — only present when MAX_LAG (the sole required field) is set.
  readVar(env, `${prefix}LAG_THROTTLE_MAX_LAG`, (raw) => {
    options.lagThrottle = {
      maxLag: parseNum(`${prefix}LAG_THROTTLE_MAX_LAG`, raw),
    };
  });
  readVar(env, `${prefix}LAG_THROTTLE_GROUP_ID`, (raw) => {
    if (options.lagThrottle) options.lagThrottle.groupId = raw.trim();
  });
  readVar(env, `${prefix}LAG_THROTTLE_POLL_INTERVAL_MS`, (raw) => {
    if (options.lagThrottle) {
      options.lagThrottle.pollIntervalMs = parseNum(
        `${prefix}LAG_THROTTLE_POLL_INTERVAL_MS`,
        raw,
      );
    }
  });
  readVar(env, `${prefix}LAG_THROTTLE_MAX_WAIT_MS`, (raw) => {
    if (options.lagThrottle) {
      options.lagThrottle.maxWaitMs = parseNum(
        `${prefix}LAG_THROTTLE_MAX_WAIT_MS`,
        raw,
      );
    }
  });

  const security = securityFromEnv(env, prefix);
  if (security) options.security = security;

  return result;
}

/**
 * Build a {@link KafkaSecurityOptions} from `SSL` / `SASL_*` / `ALLOW_INSECURE`
 * env vars. Returns `undefined` when none of them are set so the `security` key
 * is only emitted on demand.
 *
 * Only username/password SASL mechanisms are env-configurable — `oauthbearer`
 * requires a token-provider function and must be set in code.
 *
 * @throws {Error} when `SASL_MECHANISM` is not one of the allowed literals, or
 *                 when a username/password mechanism is set without both
 *                 `SASL_USERNAME` and `SASL_PASSWORD`.
 */
function securityFromEnv(
  env: NodeJS.ProcessEnv,
  prefix: string,
): KafkaSecurityOptions | undefined {
  let ssl: boolean | undefined;
  let allowInsecure: boolean | undefined;
  let mechanism:
    | "plain"
    | "scram-sha-256"
    | "scram-sha-512"
    | undefined;
  let username: string | undefined;
  let password: string | undefined;

  readVar(env, `${prefix}SSL`, (raw) => {
    ssl = parseBool(`${prefix}SSL`, raw);
  });
  readVar(env, `${prefix}ALLOW_INSECURE`, (raw) => {
    allowInsecure = parseBool(`${prefix}ALLOW_INSECURE`, raw);
  });
  readVar(env, `${prefix}SASL_MECHANISM`, (raw) => {
    mechanism = parseEnum(`${prefix}SASL_MECHANISM`, raw, [
      "plain",
      "scram-sha-256",
      "scram-sha-512",
    ] as const);
  });
  readVar(env, `${prefix}SASL_USERNAME`, (raw) => {
    username = raw.trim();
  });
  readVar(env, `${prefix}SASL_PASSWORD`, (raw) => {
    password = raw;
  });

  if (
    ssl === undefined &&
    allowInsecure === undefined &&
    mechanism === undefined &&
    username === undefined &&
    password === undefined
  ) {
    return undefined;
  }

  const security: KafkaSecurityOptions = {};
  if (ssl !== undefined) security.ssl = ssl;
  if (allowInsecure !== undefined) security.allowInsecure = allowInsecure;

  if (mechanism !== undefined || username !== undefined || password !== undefined) {
    if (mechanism === undefined || username === undefined || password === undefined) {
      throw new Error(
        `Incomplete SASL configuration: ${prefix}SASL_MECHANISM, ` +
          `${prefix}SASL_USERNAME, and ${prefix}SASL_PASSWORD must all be set ` +
          `together (oauthbearer must be configured in code).`,
      );
    }
    const sasl: KafkaSaslOptions = { mechanism, username, password };
    security.sasl = sasl;
  }

  return security;
}

// ── Consumer options ──────────────────────────────────────────────────────────

/**
 * Build a partial {@link ConsumerOptions} from environment variables.
 *
 * Intended as a base layer of defaults that you merge with code-level per-consumer
 * options via {@link mergeConsumerOptions}. As with {@link kafkaClientConfigFromEnv},
 * unset variables emit no key, and env values never override code — merge order
 * decides precedence.
 *
 * ### Supported variables (default prefix `KAFKA_CONSUMER_`)
 *
 * | Variable | Type | Maps to |
 * |---|---|---|
 * | `GROUP_ID` | string | `groupId` |
 * | `FROM_BEGINNING` | boolean | `fromBeginning` |
 * | `AUTO_COMMIT` | boolean | `autoCommit` |
 * | `DLQ` | boolean | `dlq` |
 * | `RETRY_MAX_RETRIES` | number | `retry.maxRetries` (enables `retry`) |
 * | `RETRY_BACKOFF_MS` | number | `retry.backoffMs` |
 * | `RETRY_MAX_BACKOFF_MS` | number | `retry.maxBackoffMs` |
 * | `RETRY_TOPICS` | boolean | `retryTopics` |
 * | `RETRY_TOPIC_ASSIGNMENT_TIMEOUT_MS` | number | `retryTopicAssignmentTimeoutMs` |
 * | `HANDLER_TIMEOUT_MS` | number | `handlerTimeoutMs` |
 * | `MESSAGE_TTL_MS` | number | `messageTtlMs` |
 * | `DEDUPLICATION_STRATEGY` | `drop`\|`dlq`\|`topic` | `deduplication.strategy` (enables `deduplication`) |
 * | `DEDUPLICATION_TOPIC` | string | `deduplication.duplicatesTopic` |
 * | `CIRCUIT_BREAKER_THRESHOLD` | number | `circuitBreaker.threshold` (enables `circuitBreaker`) |
 * | `CIRCUIT_BREAKER_RECOVERY_MS` | number | `circuitBreaker.recoveryMs` |
 * | `CIRCUIT_BREAKER_WINDOW_SIZE` | number | `circuitBreaker.windowSize` |
 * | `CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES` | number | `circuitBreaker.halfOpenSuccesses` |
 * | `QUEUE_HIGH_WATER_MARK` | number | `queueHighWaterMark` |
 * | `PARTITION_ASSIGNER` | `roundrobin`\|`range`\|`cooperative-sticky` | `partitionAssigner` |
 * | `GROUP_INSTANCE_ID` | string | `groupInstanceId` |
 * | `SUBSCRIBE_RETRY_RETRIES` | number | `subscribeRetry.retries` (enables `subscribeRetry`) |
 * | `SUBSCRIBE_RETRY_DELAY_MS` | number | `subscribeRetry.backoffMs` |
 *
 * `retry` is only present when `RETRY_MAX_RETRIES` is set (it is the only required
 * field). `circuitBreaker` is only present when `CIRCUIT_BREAKER_THRESHOLD` is set.
 * `deduplication` is only present when `DEDUPLICATION_STRATEGY` is set.
 * `subscribeRetry` is only present when `SUBSCRIBE_RETRY_RETRIES` is set.
 *
 * @param env    Environment source. Defaults to `process.env`.
 * @param prefix Variable prefix. Defaults to `"KAFKA_CONSUMER_"`.
 *
 * @throws {Error} when a boolean/number/enum value is malformed.
 *
 * @example
 * ```ts
 * const envDefaults = consumerOptionsFromEnv();
 * await kafka.startConsumer(
 *   ['orders'],
 *   handler,
 *   mergeConsumerOptions(envDefaults, { dlq: true }), // code layer wins
 * );
 * ```
 */
export function consumerOptionsFromEnv<
  T extends TTopicMessageMap = TTopicMessageMap,
>(
  env: NodeJS.ProcessEnv = process.env,
  prefix = "KAFKA_CONSUMER_",
): Partial<ConsumerOptions<T>> {
  const options: Partial<ConsumerOptions<T>> = {};

  readVar(env, `${prefix}GROUP_ID`, (raw) => {
    options.groupId = raw.trim();
  });
  readVar(env, `${prefix}FROM_BEGINNING`, (raw) => {
    options.fromBeginning = parseBool(`${prefix}FROM_BEGINNING`, raw);
  });
  readVar(env, `${prefix}AUTO_COMMIT`, (raw) => {
    options.autoCommit = parseBool(`${prefix}AUTO_COMMIT`, raw);
  });
  readVar(env, `${prefix}DLQ`, (raw) => {
    options.dlq = parseBool(`${prefix}DLQ`, raw);
  });

  // retry — only present when RETRY_MAX_RETRIES (the sole required field) is set.
  readVar(env, `${prefix}RETRY_MAX_RETRIES`, (raw) => {
    const retry: RetryOptions = {
      maxRetries: parseNum(`${prefix}RETRY_MAX_RETRIES`, raw),
    };
    options.retry = retry;
  });
  readVar(env, `${prefix}RETRY_BACKOFF_MS`, (raw) => {
    if (options.retry) {
      options.retry.backoffMs = parseNum(`${prefix}RETRY_BACKOFF_MS`, raw);
    }
  });
  readVar(env, `${prefix}RETRY_MAX_BACKOFF_MS`, (raw) => {
    if (options.retry) {
      options.retry.maxBackoffMs = parseNum(`${prefix}RETRY_MAX_BACKOFF_MS`, raw);
    }
  });

  readVar(env, `${prefix}RETRY_TOPICS`, (raw) => {
    options.retryTopics = parseBool(`${prefix}RETRY_TOPICS`, raw);
  });
  readVar(env, `${prefix}RETRY_TOPIC_ASSIGNMENT_TIMEOUT_MS`, (raw) => {
    options.retryTopicAssignmentTimeoutMs = parseNum(
      `${prefix}RETRY_TOPIC_ASSIGNMENT_TIMEOUT_MS`,
      raw,
    );
  });
  readVar(env, `${prefix}HANDLER_TIMEOUT_MS`, (raw) => {
    options.handlerTimeoutMs = parseNum(`${prefix}HANDLER_TIMEOUT_MS`, raw);
  });
  readVar(env, `${prefix}MESSAGE_TTL_MS`, (raw) => {
    options.messageTtlMs = parseNum(`${prefix}MESSAGE_TTL_MS`, raw);
  });

  // deduplication — only present when DEDUPLICATION_STRATEGY is set.
  readVar(env, `${prefix}DEDUPLICATION_STRATEGY`, (raw) => {
    const strategy = parseEnum(`${prefix}DEDUPLICATION_STRATEGY`, raw, [
      "drop",
      "dlq",
      "topic",
    ] as const);
    const dedup: DeduplicationOptions = { strategy };
    options.deduplication = dedup;
  });
  readVar(env, `${prefix}DEDUPLICATION_TOPIC`, (raw) => {
    if (options.deduplication) {
      options.deduplication.duplicatesTopic = raw.trim();
    }
  });

  // circuitBreaker — only present when CIRCUIT_BREAKER_THRESHOLD is set.
  readVar(env, `${prefix}CIRCUIT_BREAKER_THRESHOLD`, (raw) => {
    const cb: CircuitBreakerOptions = {
      threshold: parseNum(`${prefix}CIRCUIT_BREAKER_THRESHOLD`, raw),
    };
    options.circuitBreaker = cb;
  });
  readVar(env, `${prefix}CIRCUIT_BREAKER_RECOVERY_MS`, (raw) => {
    if (options.circuitBreaker) {
      options.circuitBreaker.recoveryMs = parseNum(
        `${prefix}CIRCUIT_BREAKER_RECOVERY_MS`,
        raw,
      );
    }
  });
  readVar(env, `${prefix}CIRCUIT_BREAKER_WINDOW_SIZE`, (raw) => {
    if (options.circuitBreaker) {
      options.circuitBreaker.windowSize = parseNum(
        `${prefix}CIRCUIT_BREAKER_WINDOW_SIZE`,
        raw,
      );
    }
  });
  readVar(env, `${prefix}CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES`, (raw) => {
    if (options.circuitBreaker) {
      options.circuitBreaker.halfOpenSuccesses = parseNum(
        `${prefix}CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES`,
        raw,
      );
    }
  });

  readVar(env, `${prefix}QUEUE_HIGH_WATER_MARK`, (raw) => {
    options.queueHighWaterMark = parseNum(`${prefix}QUEUE_HIGH_WATER_MARK`, raw);
  });
  readVar(env, `${prefix}PARTITION_ASSIGNER`, (raw) => {
    options.partitionAssigner = parseEnum(`${prefix}PARTITION_ASSIGNER`, raw, [
      "roundrobin",
      "range",
      "cooperative-sticky",
    ] as const);
  });
  readVar(env, `${prefix}GROUP_INSTANCE_ID`, (raw) => {
    options.groupInstanceId = raw.trim();
  });

  // subscribeRetry — only present when SUBSCRIBE_RETRY_RETRIES is set.
  readVar(env, `${prefix}SUBSCRIBE_RETRY_RETRIES`, (raw) => {
    const subscribeRetry: SubscribeRetryOptions = {
      retries: parseNum(`${prefix}SUBSCRIBE_RETRY_RETRIES`, raw),
    };
    options.subscribeRetry = subscribeRetry;
  });
  readVar(env, `${prefix}SUBSCRIBE_RETRY_DELAY_MS`, (raw) => {
    if (options.subscribeRetry) {
      options.subscribeRetry.backoffMs = parseNum(
        `${prefix}SUBSCRIBE_RETRY_DELAY_MS`,
        raw,
      );
    }
  });

  return options;
}

// ── Layered merge ──────────────────────────────────────────────────────────────

/** Nested {@link ConsumerOptions} keys that are deep-merged rather than replaced. */
const NESTED_CONSUMER_KEYS = [
  "retry",
  "deduplication",
  "circuitBreaker",
  "subscribeRetry",
] as const;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

/**
 * Merge several partial {@link ConsumerOptions} layers into one, **later layers
 * winning** on conflict. This is the mechanism that implements the
 * *code > env > defaults* precedence rule — pass the env layer first and the
 * code layer last.
 *
 * Top-level keys are replaced by the last defined value. The nested option
 * objects — `retry`, `deduplication`, `circuitBreaker`, and `subscribeRetry` —
 * are **deep-merged** so a later layer can override a single nested field (e.g.
 * bump `retry.maxRetries`) without discarding the other fields set by an earlier
 * layer. `undefined` values never overwrite a previously set value, and
 * `undefined` layers are skipped entirely.
 *
 * @example Env base overridden by code
 * ```ts
 * const env  = { retry: { maxRetries: 3, backoffMs: 500 }, dlq: false };
 * const code = { retry: { maxRetries: 5 }, dlq: true };
 * mergeConsumerOptions(env, code);
 * // → { retry: { maxRetries: 5, backoffMs: 500 }, dlq: true }
 * //   maxRetries overridden, backoffMs preserved, dlq overridden
 * ```
 */
export function mergeConsumerOptions<
  T extends TTopicMessageMap = TTopicMessageMap,
>(
  ...layers: Array<Partial<ConsumerOptions<T>> | undefined>
): ConsumerOptions<T> {
  const result: Record<string, unknown> = {};

  for (const layer of layers) {
    if (!layer) continue;
    for (const [key, value] of Object.entries(layer)) {
      if (value === undefined) continue;

      if (
        (NESTED_CONSUMER_KEYS as readonly string[]).includes(key) &&
        isPlainObject(value) &&
        isPlainObject(result[key])
      ) {
        result[key] = {
          ...(result[key] as Record<string, unknown>),
          ...value,
        };
      } else {
        result[key] = value;
      }
    }
  }

  return result as ConsumerOptions<T>;
}
