import { resolveSecurityOptions } from "../../security/resolve-security";
import {
  awsMskIamProvider,
  gcpAccessTokenProvider,
} from "../../security/providers";
import {
  describeRequiredAcls,
  toKafkaAclCommands,
  toMskIamPolicy,
} from "../../security/acl";
import { makeTestLogger } from "../helpers";
import { ConfluentTransport } from "../../transport/confluent.transport";

describe("resolveSecurityOptions — secure defaults", () => {
  it("auto-enables ssl when sasl is configured without an explicit ssl", () => {
    const logger = makeTestLogger();
    const resolved = resolveSecurityOptions(
      { sasl: { mechanism: "plain", username: "u", password: "p" } },
      ["broker.internal:9092"],
      logger,
    );
    expect(resolved?.ssl).toBe(true);
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("respects an explicit ssl: false but warns about plaintext credentials", () => {
    const logger = makeTestLogger();
    const resolved = resolveSecurityOptions(
      { ssl: false, sasl: { mechanism: "plain", username: "u", password: "p" } },
      ["broker.internal:9092"],
      logger,
    );
    expect(resolved?.ssl).toBe(false);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining("ssl: false"),
    );
  });

  it("warns once for non-local brokers with no security at all", () => {
    const logger = makeTestLogger();
    resolveSecurityOptions(undefined, ["kafka.prod.example.com:9092"], logger);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining("without TLS or SASL"),
    );
  });

  it("does not warn for local brokers or when allowInsecure is set", () => {
    const logger = makeTestLogger();
    resolveSecurityOptions(undefined, ["localhost:9092", "127.0.0.1:29092"], logger);
    resolveSecurityOptions(
      { allowInsecure: true },
      ["kafka.prod.example.com:9092"],
      logger,
    );
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("ssl: true alone passes through without warning", () => {
    const logger = makeTestLogger();
    const resolved = resolveSecurityOptions(
      { ssl: true },
      ["kafka.prod.example.com:9092"],
      logger,
    );
    expect(resolved).toEqual({ ssl: true });
    expect(logger.warn).not.toHaveBeenCalled();
  });
});

describe("ConfluentTransport — security config mapping", () => {
  beforeEach(() => jest.clearAllMocks());

  it("passes ssl and scram sasl into the kafkaJS config", async () => {
    const { KafkaJS } = jest.requireMock("@confluentinc/kafka-javascript");
    new ConfluentTransport("cid", ["b:9092"], {
      ssl: true,
      sasl: { mechanism: "scram-sha-512", username: "u", password: "p" },
    });
    const cfg = (KafkaJS.Kafka as jest.Mock).mock.calls.at(-1)![0];
    expect(cfg.kafkaJS.ssl).toBe(true);
    expect(cfg.kafkaJS.sasl).toEqual({
      mechanism: "scram-sha-512",
      username: "u",
      password: "p",
    });
  });

  it("wraps an oauthbearer provider and fills principal/lifetime defaults", async () => {
    const { KafkaJS } = jest.requireMock("@confluentinc/kafka-javascript");
    new ConfluentTransport("cid", ["b:9092"], {
      ssl: true,
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => ({ value: "tok-123" }),
      },
    });
    const cfg = (KafkaJS.Kafka as jest.Mock).mock.calls.at(-1)![0];
    expect(cfg.kafkaJS.sasl.mechanism).toBe("oauthbearer");
    const token = await cfg.kafkaJS.sasl.oauthBearerProvider();
    expect(token.value).toBe("tok-123");
    expect(token.principal).toBe("kafka-client");
    expect(token.lifetime).toBeGreaterThan(Date.now());
  });
});

describe("cloud auth providers", () => {
  it("awsMskIamProvider maps the signer token and expiry", async () => {
    const importFn = jest.fn().mockResolvedValue({
      generateAuthToken: jest
        .fn()
        .mockResolvedValue({ token: "msk-token", expiryTime: 1_234_567 }),
    });
    const provider = awsMskIamProvider({ region: "eu-west-1", importFn });
    await expect(provider()).resolves.toEqual({
      value: "msk-token",
      principal: "msk-iam",
      lifetimeMs: 1_234_567,
    });
    expect(importFn).toHaveBeenCalledWith("aws-msk-iam-sasl-signer-js");
  });

  it("awsMskIamProvider explains the missing optional dependency", async () => {
    const provider = awsMskIamProvider({
      region: "eu-west-1",
      importFn: jest.fn().mockRejectedValue(new Error("not found")),
    });
    await expect(provider()).rejects.toThrow(
      /npm install aws-msk-iam-sasl-signer-js/,
    );
  });

  it("gcpAccessTokenProvider returns an ADC token with a ttl window", async () => {
    const getAccessToken = jest.fn().mockResolvedValue("gcp-token");
    const importFn = jest.fn().mockResolvedValue({
      GoogleAuth: jest.fn().mockImplementation(() => ({ getAccessToken })),
    });
    const before = Date.now();
    const provider = gcpAccessTokenProvider({ importFn });
    const token = await provider();
    expect(token.value).toBe("gcp-token");
    expect(token.lifetimeMs).toBeGreaterThanOrEqual(before + 49 * 60_000);
  });

  it("gcpAccessTokenProvider fails clearly when no token is returned", async () => {
    const importFn = jest.fn().mockResolvedValue({
      GoogleAuth: jest.fn().mockImplementation(() => ({
        getAccessToken: jest.fn().mockResolvedValue(null),
      })),
    });
    await expect(gcpAccessTokenProvider({ importFn })()).rejects.toThrow(
      /no access token/,
    );
  });
});

describe("describeRequiredAcls — derived resources", () => {
  const input = {
    clientId: "billing-svc",
    groupIds: ["billing-group"],
    produceTopics: ["invoices.created"],
    consumeTopics: ["orders.created"],
    features: {
      retryTopics: { maxRetries: 2 },
      dlq: true,
      delayedDelivery: true,
      dlqReplay: true,
      snapshots: true,
      clockRecovery: true,
      transactions: true,
    },
  };

  it("covers base topics, derived topics, companion groups, and tx ids", () => {
    const acls = describeRequiredAcls(input);
    const names = acls.map((a) => `${a.resourceType}:${a.patternType}:${a.name}`);

    expect(names).toEqual(
      expect.arrayContaining([
        "topic:literal:invoices.created",
        "topic:literal:orders.created",
        "topic:literal:orders.created.dlq",
        "topic:literal:orders.created.retry.1",
        "topic:literal:orders.created.retry.2",
        "topic:literal:orders.created.delayed",
        "topic:literal:invoices.created.delayed",
        "group:literal:billing-group",
        "group:prefixed:billing-group-retry.",
        "group:literal:billing-group-delayed-relay",
        "group:prefixed:orders.created.dlq-replay",
        "group:prefixed:billing-svc-snapshot-",
        "group:prefixed:billing-svc-clock-recovery-",
        "transactional-id:prefixed:billing-group-",
        "transactional-id:literal:billing-svc-tx",
        "transactional-id:literal:billing-group-delayed-relay-tx",
      ]),
    );
    // Ephemeral groups need DELETE (they clean up after themselves)
    const replayGroup = acls.find((a) => a.name === "orders.created.dlq-replay");
    expect(replayGroup!.operations).toContain("DELETE");
  });

  it("merges duplicate resources instead of repeating them", () => {
    const acls = describeRequiredAcls({
      clientId: "c",
      groupIds: ["g"],
      produceTopics: ["t"],
      consumeTopics: ["t"],
    });
    const topicEntries = acls.filter(
      (a) => a.resourceType === "topic" && a.name === "t",
    );
    expect(topicEntries).toHaveLength(1);
    expect(topicEntries[0].operations).toEqual(
      expect.arrayContaining(["READ", "WRITE", "DESCRIBE"]),
    );
  });

  it("renders kafka-acls commands with prefixed pattern flags", () => {
    const cmds = toKafkaAclCommands(
      describeRequiredAcls(input),
      "User:billing-svc",
      "broker:9092",
    );
    expect(cmds.some((c) => c.includes("--topic 'orders.created.dlq'"))).toBe(true);
    expect(
      cmds.some(
        (c) =>
          c.includes("--group 'billing-group-retry.'") &&
          c.includes("--resource-pattern-type prefixed"),
      ),
    ).toBe(true);
  });

  it("renders an MSK IAM policy with Connect + wildcarded prefixes", () => {
    const policy = toMskIamPolicy(describeRequiredAcls(input), {
      region: "eu-west-1",
      accountId: "123456789012",
      clusterName: "prod-cluster",
      clusterUuid: "abc-123",
    });
    const statements = policy.Statement as any[];
    expect(statements[0].Action).toContain("kafka-cluster:Connect");
    const flat = JSON.stringify(policy);
    expect(flat).toContain(
      "arn:aws:kafka:eu-west-1:123456789012:topic/prod-cluster/abc-123/orders.created",
    );
    expect(flat).toContain("group/prod-cluster/abc-123/billing-group-retry.*");
    expect(flat).toContain("kafka-cluster:ReadData");
    expect(flat).toContain("kafka-cluster:WriteData");
  });
});
