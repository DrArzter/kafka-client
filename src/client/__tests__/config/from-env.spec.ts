import {
  kafkaClientConfigFromEnv,
  consumerOptionsFromEnv,
  mergeConsumerOptions,
} from "../../config/from-env";

describe("kafkaClientConfigFromEnv", () => {
  it("returns empty options and no positional args for an empty env", () => {
    const result = kafkaClientConfigFromEnv({});
    expect(result).toEqual({ options: {} });
    expect(result.clientId).toBeUndefined();
    expect(result.groupId).toBeUndefined();
    expect(result.brokers).toBeUndefined();
  });

  it("reads the three positional constructor arguments", () => {
    const result = kafkaClientConfigFromEnv({
      KAFKA_CLIENT_ID: "svc",
      KAFKA_GROUP_ID: "grp",
      KAFKA_BROKERS: "a:9092,b:9092",
    });
    expect(result.clientId).toBe("svc");
    expect(result.groupId).toBe("grp");
    expect(result.brokers).toEqual(["a:9092", "b:9092"]);
    expect(result.options).toEqual({});
  });

  it("parses a list, trimming and dropping empty entries", () => {
    const result = kafkaClientConfigFromEnv({
      KAFKA_BROKERS: " a:9092 , , b:9092 ,",
    });
    expect(result.brokers).toEqual(["a:9092", "b:9092"]);
  });

  describe("booleans", () => {
    it.each([
      ["true", true],
      ["TRUE", true],
      ["1", true],
      ["yes", true],
      ["YES", true],
      ["false", false],
      ["FALSE", false],
      ["0", false],
      ["no", false],
      ["NO", false],
    ])("parses %s → %s", (raw, expected) => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_AUTO_CREATE_TOPICS: raw,
      });
      expect(result.options.autoCreateTopics).toBe(expected);
    });

    it("throws naming the variable on an invalid boolean", () => {
      expect(() =>
        kafkaClientConfigFromEnv({ KAFKA_STRICT_SCHEMAS: "maybe" }),
      ).toThrow(/KAFKA_STRICT_SCHEMAS/);
    });
  });

  describe("numbers", () => {
    it("parses a valid number", () => {
      const result = kafkaClientConfigFromEnv({ KAFKA_NUM_PARTITIONS: "6" });
      expect(result.options.numPartitions).toBe(6);
    });

    it("throws naming the variable on a NaN value", () => {
      expect(() =>
        kafkaClientConfigFromEnv({ KAFKA_NUM_PARTITIONS: "six" }),
      ).toThrow(/KAFKA_NUM_PARTITIONS/);
    });
  });

  it("emits no key for unset or whitespace-only variables", () => {
    const result = kafkaClientConfigFromEnv({
      KAFKA_TRANSACTIONAL_ID: "   ",
    });
    expect("transactionalId" in result.options).toBe(false);
  });

  describe("clockRecovery (conditional nesting)", () => {
    it("is absent when no topics are configured", () => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_CLOCK_RECOVERY_TIMEOUT_MS: "5000",
      });
      expect(result.options.clockRecovery).toBeUndefined();
    });

    it("is present with topics and optional timeout when topics are set", () => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_CLOCK_RECOVERY_TOPICS: "orders.created, orders.completed",
        KAFKA_CLOCK_RECOVERY_TIMEOUT_MS: "5000",
      });
      expect(result.options.clockRecovery).toEqual({
        topics: ["orders.created", "orders.completed"],
        timeoutMs: 5000,
      });
    });
  });

  describe("lagThrottle (conditional nesting)", () => {
    it("is absent when MAX_LAG is not set", () => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_LAG_THROTTLE_POLL_INTERVAL_MS: "1000",
      });
      expect(result.options.lagThrottle).toBeUndefined();
    });

    it("is present with only MAX_LAG when the others are absent", () => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_LAG_THROTTLE_MAX_LAG: "10000",
      });
      expect(result.options.lagThrottle).toEqual({ maxLag: 10000 });
    });

    it("includes all lagThrottle fields when set", () => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_LAG_THROTTLE_MAX_LAG: "10000",
        KAFKA_LAG_THROTTLE_GROUP_ID: "billing",
        KAFKA_LAG_THROTTLE_POLL_INTERVAL_MS: "3000",
        KAFKA_LAG_THROTTLE_MAX_WAIT_MS: "20000",
      });
      expect(result.options.lagThrottle).toEqual({
        maxLag: 10000,
        groupId: "billing",
        pollIntervalMs: 3000,
        maxWaitMs: 20000,
      });
    });
  });

  describe("security (conditional nesting)", () => {
    it("is absent when no security vars are set", () => {
      const result = kafkaClientConfigFromEnv({ KAFKA_CLIENT_ID: "svc" });
      expect(result.options.security).toBeUndefined();
    });

    it("includes ssl / allowInsecure without sasl", () => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_SSL: "true",
        KAFKA_ALLOW_INSECURE: "false",
      });
      expect(result.options.security).toEqual({
        ssl: true,
        allowInsecure: false,
      });
    });

    it("builds a full SASL config", () => {
      const result = kafkaClientConfigFromEnv({
        KAFKA_SASL_MECHANISM: "scram-sha-512",
        KAFKA_SASL_USERNAME: "svc",
        KAFKA_SASL_PASSWORD: "secret",
      });
      expect(result.options.security).toEqual({
        sasl: {
          mechanism: "scram-sha-512",
          username: "svc",
          password: "secret",
        },
      });
    });

    it("throws naming the variable on an invalid SASL mechanism", () => {
      expect(() =>
        kafkaClientConfigFromEnv({ KAFKA_SASL_MECHANISM: "kerberos" }),
      ).toThrow(/KAFKA_SASL_MECHANISM/);
    });

    it("rejects oauthbearer as an env mechanism", () => {
      expect(() =>
        kafkaClientConfigFromEnv({ KAFKA_SASL_MECHANISM: "oauthbearer" }),
      ).toThrow(/KAFKA_SASL_MECHANISM/);
    });

    it("throws when SASL config is incomplete (username without password)", () => {
      expect(() =>
        kafkaClientConfigFromEnv({
          KAFKA_SASL_MECHANISM: "plain",
          KAFKA_SASL_USERNAME: "svc",
        }),
      ).toThrow(/SASL/);
    });
  });

  it("supports a custom prefix", () => {
    const result = kafkaClientConfigFromEnv(
      { MYAPP_CLIENT_ID: "svc", MYAPP_NUM_PARTITIONS: "3" },
      "MYAPP_",
    );
    expect(result.clientId).toBe("svc");
    expect(result.options.numPartitions).toBe(3);
  });

  it("builds a complete option set (equality)", () => {
    const result = kafkaClientConfigFromEnv({
      KAFKA_CLIENT_ID: "svc",
      KAFKA_GROUP_ID: "grp",
      KAFKA_BROKERS: "a:9092,b:9092",
      KAFKA_AUTO_CREATE_TOPICS: "true",
      KAFKA_STRICT_SCHEMAS: "false",
      KAFKA_NUM_PARTITIONS: "3",
      KAFKA_TRANSACTIONAL_ID: "svc-tx-1",
      KAFKA_CLOCK_RECOVERY_TOPICS: "orders.created",
      KAFKA_CLOCK_RECOVERY_TIMEOUT_MS: "10000",
      KAFKA_LAG_THROTTLE_MAX_LAG: "5000",
      KAFKA_SSL: "true",
      KAFKA_SASL_MECHANISM: "plain",
      KAFKA_SASL_USERNAME: "u",
      KAFKA_SASL_PASSWORD: "p",
    });
    expect(result).toEqual({
      clientId: "svc",
      groupId: "grp",
      brokers: ["a:9092", "b:9092"],
      options: {
        autoCreateTopics: true,
        strictSchemas: false,
        numPartitions: 3,
        transactionalId: "svc-tx-1",
        clockRecovery: { topics: ["orders.created"], timeoutMs: 10000 },
        lagThrottle: { maxLag: 5000 },
        security: {
          ssl: true,
          sasl: { mechanism: "plain", username: "u", password: "p" },
        },
      },
    });
  });
});

describe("consumerOptionsFromEnv", () => {
  it("returns an empty object for an empty env", () => {
    expect(consumerOptionsFromEnv({})).toEqual({});
  });

  it("reads scalar options", () => {
    const options = consumerOptionsFromEnv({
      KAFKA_CONSUMER_GROUP_ID: "billing",
      KAFKA_CONSUMER_FROM_BEGINNING: "true",
      KAFKA_CONSUMER_AUTO_COMMIT: "false",
      KAFKA_CONSUMER_DLQ: "yes",
      KAFKA_CONSUMER_HANDLER_TIMEOUT_MS: "5000",
      KAFKA_CONSUMER_MESSAGE_TTL_MS: "60000",
      KAFKA_CONSUMER_QUEUE_HIGH_WATER_MARK: "100",
      KAFKA_CONSUMER_GROUP_INSTANCE_ID: "svc-0",
    });
    expect(options).toEqual({
      groupId: "billing",
      fromBeginning: true,
      autoCommit: false,
      dlq: true,
      handlerTimeoutMs: 5000,
      messageTtlMs: 60000,
      queueHighWaterMark: 100,
      groupInstanceId: "svc-0",
    });
  });

  describe("retry (conditional nesting)", () => {
    it("is absent when RETRY_MAX_RETRIES is not set", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_RETRY_BACKOFF_MS: "500",
      });
      expect(options.retry).toBeUndefined();
    });

    it("is present with maxRetries and optional fields", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_RETRY_MAX_RETRIES: "5",
        KAFKA_CONSUMER_RETRY_BACKOFF_MS: "500",
        KAFKA_CONSUMER_RETRY_MAX_BACKOFF_MS: "10000",
      });
      expect(options.retry).toEqual({
        maxRetries: 5,
        backoffMs: 500,
        maxBackoffMs: 10000,
      });
    });
  });

  describe("deduplication (conditional nesting + enum validation)", () => {
    it("is absent when STRATEGY is not set", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_DEDUPLICATION_TOPIC: "dup",
      });
      expect(options.deduplication).toBeUndefined();
    });

    it.each(["drop", "dlq", "topic"] as const)(
      "accepts strategy %s",
      (strategy) => {
        const options = consumerOptionsFromEnv({
          KAFKA_CONSUMER_DEDUPLICATION_STRATEGY: strategy,
        });
        expect(options.deduplication).toEqual({ strategy });
      },
    );

    it("includes the custom duplicates topic", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_DEDUPLICATION_STRATEGY: "topic",
        KAFKA_CONSUMER_DEDUPLICATION_TOPIC: "orders.dupes",
      });
      expect(options.deduplication).toEqual({
        strategy: "topic",
        duplicatesTopic: "orders.dupes",
      });
    });

    it("throws naming the variable on an invalid strategy", () => {
      expect(() =>
        consumerOptionsFromEnv({
          KAFKA_CONSUMER_DEDUPLICATION_STRATEGY: "ignore",
        }),
      ).toThrow(/KAFKA_CONSUMER_DEDUPLICATION_STRATEGY/);
    });
  });

  describe("circuitBreaker (conditional nesting)", () => {
    it("is absent when THRESHOLD is not set", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_CIRCUIT_BREAKER_RECOVERY_MS: "30000",
      });
      expect(options.circuitBreaker).toBeUndefined();
    });

    it("is present with all fields when THRESHOLD is set", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_CIRCUIT_BREAKER_THRESHOLD: "5",
        KAFKA_CONSUMER_CIRCUIT_BREAKER_RECOVERY_MS: "30000",
        KAFKA_CONSUMER_CIRCUIT_BREAKER_WINDOW_SIZE: "20",
        KAFKA_CONSUMER_CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES: "2",
      });
      expect(options.circuitBreaker).toEqual({
        threshold: 5,
        recoveryMs: 30000,
        windowSize: 20,
        halfOpenSuccesses: 2,
      });
    });
  });

  describe("subscribeRetry (conditional nesting)", () => {
    it("is absent when RETRIES is not set", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_SUBSCRIBE_RETRY_DELAY_MS: "2000",
      });
      expect(options.subscribeRetry).toBeUndefined();
    });

    it("maps RETRIES → retries and DELAY_MS → backoffMs", () => {
      const options = consumerOptionsFromEnv({
        KAFKA_CONSUMER_SUBSCRIBE_RETRY_RETRIES: "10",
        KAFKA_CONSUMER_SUBSCRIBE_RETRY_DELAY_MS: "2000",
      });
      expect(options.subscribeRetry).toEqual({
        retries: 10,
        backoffMs: 2000,
      });
    });
  });

  describe("partitionAssigner (enum validation)", () => {
    it.each(["roundrobin", "range", "cooperative-sticky"] as const)(
      "accepts %s",
      (assigner) => {
        const options = consumerOptionsFromEnv({
          KAFKA_CONSUMER_PARTITION_ASSIGNER: assigner,
        });
        expect(options.partitionAssigner).toBe(assigner);
      },
    );

    it("throws naming the variable on an invalid assigner", () => {
      expect(() =>
        consumerOptionsFromEnv({
          KAFKA_CONSUMER_PARTITION_ASSIGNER: "sticky",
        }),
      ).toThrow(/KAFKA_CONSUMER_PARTITION_ASSIGNER/);
    });
  });

  it("maps retryTopics and retryTopicAssignmentTimeoutMs", () => {
    const options = consumerOptionsFromEnv({
      KAFKA_CONSUMER_RETRY_TOPICS: "true",
      KAFKA_CONSUMER_RETRY_TOPIC_ASSIGNMENT_TIMEOUT_MS: "15000",
    });
    expect(options.retryTopics).toBe(true);
    expect(options.retryTopicAssignmentTimeoutMs).toBe(15000);
  });

  it("supports a custom prefix", () => {
    const options = consumerOptionsFromEnv(
      { CONS_GROUP_ID: "grp", CONS_DLQ: "1" },
      "CONS_",
    );
    expect(options).toEqual({ groupId: "grp", dlq: true });
  });
});

describe("mergeConsumerOptions", () => {
  it("returns an empty object when given nothing", () => {
    expect(mergeConsumerOptions()).toEqual({});
  });

  it("skips undefined layers", () => {
    expect(mergeConsumerOptions(undefined, { dlq: true }, undefined)).toEqual({
      dlq: true,
    });
  });

  it("later layers win on top-level keys", () => {
    const merged = mergeConsumerOptions(
      { dlq: false, fromBeginning: true },
      { dlq: true },
    );
    expect(merged).toEqual({ dlq: true, fromBeginning: true });
  });

  it("undefined values never overwrite a set value", () => {
    const merged = mergeConsumerOptions(
      { groupId: "grp" },
      { groupId: undefined },
    );
    expect(merged).toEqual({ groupId: "grp" });
  });

  it("deep-merges the retry object", () => {
    const merged = mergeConsumerOptions(
      { retry: { maxRetries: 3, backoffMs: 500 } },
      { retry: { maxRetries: 5 } },
    );
    expect(merged.retry).toEqual({ maxRetries: 5, backoffMs: 500 });
  });

  it("deep-merges the deduplication object", () => {
    const merged = mergeConsumerOptions(
      { deduplication: { strategy: "drop" } },
      { deduplication: { duplicatesTopic: "dupes" } },
    );
    expect(merged.deduplication).toEqual({
      strategy: "drop",
      duplicatesTopic: "dupes",
    });
  });

  it("deep-merges circuitBreaker and subscribeRetry", () => {
    const merged = mergeConsumerOptions(
      {
        circuitBreaker: { threshold: 5, recoveryMs: 30000 },
        subscribeRetry: { retries: 5, backoffMs: 1000 },
      },
      {
        circuitBreaker: { threshold: 10 },
        subscribeRetry: { backoffMs: 2000 },
      },
    );
    expect(merged.circuitBreaker).toEqual({ threshold: 10, recoveryMs: 30000 });
    expect(merged.subscribeRetry).toEqual({ retries: 5, backoffMs: 2000 });
  });

  it("composes env defaults with a code layer (code wins)", () => {
    const envDefaults = consumerOptionsFromEnv({
      KAFKA_CONSUMER_GROUP_ID: "env-grp",
      KAFKA_CONSUMER_RETRY_MAX_RETRIES: "3",
      KAFKA_CONSUMER_RETRY_BACKOFF_MS: "500",
      KAFKA_CONSUMER_DLQ: "false",
    });
    const merged = mergeConsumerOptions(envDefaults, {
      dlq: true,
      retry: { maxRetries: 5 },
    });
    expect(merged).toEqual({
      groupId: "env-grp",
      dlq: true,
      retry: { maxRetries: 5, backoffMs: 500 },
    });
  });
});
