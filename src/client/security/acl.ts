/**
 * ACL requirement calculator for the topics, consumer groups, and
 * transactional ids this library derives from your configuration.
 *
 * Features like retry topics, DLQ, delayed delivery, deduplication routing,
 * DLQ replay, snapshots, and clock recovery all create extra topics
 * (`<topic>.retry.N`, `<topic>.dlq`, `<topic>.delayed`, `<topic>.duplicates`)
 * and consumer groups (`<groupId>-retry.N`, `<groupId>-delayed-relay`, plus
 * timestamped ephemeral groups). On a locked-down cluster every one of them
 * needs an ACL — `describeRequiredAcls()` enumerates them so nothing is
 * discovered in production at 3 a.m.
 */

/** One ACL requirement on a Kafka resource. */
export interface AclResource {
  resourceType: "topic" | "group" | "transactional-id" | "cluster";
  /**
   * `literal` — exact name match. `prefixed` — Kafka prefixed ACL pattern;
   * required for the timestamped ephemeral groups this library creates.
   */
  patternType: "literal" | "prefixed";
  name: string;
  /** Kafka operations: READ, WRITE, DESCRIBE, CREATE, DELETE. */
  operations: string[];
  /** Which feature requires this resource. */
  reason: string;
}

/** Input describing how a service uses its `KafkaClient`. */
export interface AclRequirementsInput {
  clientId: string;
  /** Default/typical consumer group ids used by this service. */
  groupIds?: string[];
  /** Topics this service produces to. */
  produceTopics?: string[];
  /** Topics this service consumes from. */
  consumeTopics?: string[];
  features?: {
    /** `retryTopics: true` — adds `.retry.N` topics, `-retry.N` groups, and per-level tx ids. */
    retryTopics?: { maxRetries: number };
    /** `dlq: true` — adds `.dlq` produce rights on consumed topics. */
    dlq?: boolean;
    /** `deliverAfterMs` / `startDelayedRelay` — adds `.delayed` topics, relay group, relay tx id. */
    delayedDelivery?: boolean;
    /** `deduplication.strategy: 'topic'` — adds `.duplicates` produce rights (or the custom topic). */
    duplicatesTopic?: boolean | string;
    /** `replayDlq()` usage — adds prefixed ephemeral replay groups + group deletion. */
    dlqReplay?: boolean;
    /** `readSnapshot` / `checkpointOffsets` / `restoreFromCheckpoint` — prefixed ephemeral groups. */
    snapshots?: boolean;
    /** `clockRecovery.topics` configured — prefixed ephemeral recovery groups. */
    clockRecovery?: boolean;
    /** `transaction()` usage — base transactional id. */
    transactions?: boolean;
    /** `autoCreateTopics: true` — cluster CREATE (avoid in production). */
    autoCreateTopics?: boolean;
  };
}

function addResource(
  out: Map<string, AclResource>,
  r: AclResource,
): void {
  const key = `${r.resourceType}:${r.patternType}:${r.name}`;
  const existing = out.get(key);
  if (existing) {
    for (const op of r.operations)
      if (!existing.operations.includes(op)) existing.operations.push(op);
    if (!existing.reason.includes(r.reason))
      existing.reason += `; ${r.reason}`;
  } else {
    out.set(key, { ...r, operations: [...r.operations] });
  }
}

/**
 * Enumerate every Kafka resource (with required operations) a service
 * principal needs for the given usage profile — including all derived
 * topics, companion groups, ephemeral groups, and transactional ids.
 *
 * @example
 * ```ts
 * const acls = describeRequiredAcls({
 *   clientId: 'billing-svc',
 *   groupIds: ['billing-svc-group'],
 *   produceTopics: ['invoices.created'],
 *   consumeTopics: ['orders.created'],
 *   features: { retryTopics: { maxRetries: 3 }, dlq: true, dlqReplay: true },
 * });
 * console.log(toKafkaAclCommands(acls, 'User:billing-svc'));
 * ```
 */
export function describeRequiredAcls(
  input: AclRequirementsInput,
): AclResource[] {
  const out = new Map<string, AclResource>();
  const f = input.features ?? {};
  const produce = input.produceTopics ?? [];
  const consume = input.consumeTopics ?? [];
  const groups = input.groupIds ?? [];

  for (const t of produce) {
    addResource(out, {
      resourceType: "topic",
      patternType: "literal",
      name: t,
      operations: ["WRITE", "DESCRIBE"],
      reason: "sendMessage/sendBatch",
    });
  }

  for (const t of consume) {
    addResource(out, {
      resourceType: "topic",
      patternType: "literal",
      name: t,
      operations: ["READ", "DESCRIBE"],
      reason: "startConsumer",
    });
  }

  for (const g of groups) {
    addResource(out, {
      resourceType: "group",
      patternType: "literal",
      name: g,
      operations: ["READ", "DESCRIBE"],
      reason: "consumer group membership + offset commits",
    });
  }

  if (f.dlq) {
    for (const t of consume) {
      addResource(out, {
        resourceType: "topic",
        patternType: "literal",
        name: `${t}.dlq`,
        operations: ["WRITE", "DESCRIBE"],
        reason: "dlq: true — failed messages routed to DLQ",
      });
    }
  }

  if (f.retryTopics) {
    for (const t of consume) {
      for (let level = 1; level <= f.retryTopics.maxRetries; level++) {
        addResource(out, {
          resourceType: "topic",
          patternType: "literal",
          name: `${t}.retry.${level}`,
          operations: ["READ", "WRITE", "DESCRIBE"],
          reason: "retryTopics — retry chain produce + companion consume",
        });
      }
    }
    for (const g of groups) {
      // Covers <gid>-retry.1 … <gid>-retry.N companion groups.
      addResource(out, {
        resourceType: "group",
        patternType: "prefixed",
        name: `${g}-retry.`,
        operations: ["READ", "DESCRIBE"],
        reason: "retryTopics — companion retry-level consumer groups",
      });
      // Covers <gid>-main-tx and <gid>-retry.N-tx transactional producers.
      addResource(out, {
        resourceType: "transactional-id",
        patternType: "prefixed",
        name: `${g}-`,
        operations: ["WRITE", "DESCRIBE"],
        reason: "retryTopics — EOS routing transactions per retry level",
      });
    }
  }

  if (f.delayedDelivery) {
    for (const t of [...new Set([...produce, ...consume])]) {
      addResource(out, {
        resourceType: "topic",
        patternType: "literal",
        name: `${t}.delayed`,
        operations: ["READ", "WRITE", "DESCRIBE"],
        reason: "deliverAfterMs staging + startDelayedRelay consume",
      });
    }
    for (const g of groups) {
      addResource(out, {
        resourceType: "group",
        patternType: "literal",
        name: `${g}-delayed-relay`,
        operations: ["READ", "DESCRIBE"],
        reason: "startDelayedRelay consumer group",
      });
      addResource(out, {
        resourceType: "transactional-id",
        patternType: "literal",
        name: `${g}-delayed-relay-tx`,
        operations: ["WRITE", "DESCRIBE"],
        reason: "startDelayedRelay transactional forwarding",
      });
    }
  }

  if (f.duplicatesTopic) {
    if (typeof f.duplicatesTopic === "string") {
      addResource(out, {
        resourceType: "topic",
        patternType: "literal",
        name: f.duplicatesTopic,
        operations: ["WRITE", "DESCRIBE"],
        reason: "deduplication.strategy 'topic' — custom duplicates topic",
      });
    } else {
      for (const t of consume) {
        addResource(out, {
          resourceType: "topic",
          patternType: "literal",
          name: `${t}.duplicates`,
          operations: ["WRITE", "DESCRIBE"],
          reason: "deduplication.strategy 'topic'",
        });
      }
    }
  }

  if (f.dlqReplay) {
    for (const t of consume) {
      // <topic>.dlq-replay (stable, incremental) and <topic>.dlq-replay-<ts> (ephemeral).
      addResource(out, {
        resourceType: "group",
        patternType: "prefixed",
        name: `${t}.dlq-replay`,
        operations: ["READ", "DESCRIBE", "DELETE"],
        reason: "replayDlq — ephemeral/stable replay groups (deleted after use)",
      });
      addResource(out, {
        resourceType: "topic",
        patternType: "literal",
        name: `${t}.dlq`,
        operations: ["READ", "DESCRIBE"],
        reason: "replayDlq — reads the DLQ",
      });
    }
  }

  if (f.snapshots) {
    addResource(out, {
      resourceType: "group",
      patternType: "prefixed",
      name: `${input.clientId}-snapshot-`,
      operations: ["READ", "DESCRIBE", "DELETE"],
      reason: "readSnapshot — timestamped ephemeral groups (deleted after use)",
    });
  }

  if (f.clockRecovery) {
    addResource(out, {
      resourceType: "group",
      patternType: "prefixed",
      name: `${input.clientId}-clock-recovery-`,
      operations: ["READ", "DESCRIBE", "DELETE"],
      reason: "clockRecovery — timestamped ephemeral groups (deleted after use)",
    });
  }

  if (f.transactions) {
    addResource(out, {
      resourceType: "transactional-id",
      patternType: "literal",
      name: `${input.clientId}-tx`,
      operations: ["WRITE", "DESCRIBE"],
      reason: "transaction() — default transactionalId (override-aware: adjust if you set one)",
    });
  }

  if (f.autoCreateTopics) {
    addResource(out, {
      resourceType: "cluster",
      patternType: "literal",
      name: "kafka-cluster",
      operations: ["CREATE"],
      reason: "autoCreateTopics: true — not recommended in production",
    });
  }

  return [...out.values()];
}

/**
 * Render ACL requirements as `kafka-acls.sh` commands for a principal.
 *
 * @example
 * ```ts
 * toKafkaAclCommands(acls, 'User:billing-svc', 'broker:9092')
 * // kafka-acls.sh --bootstrap-server broker:9092 --add --allow-principal User:billing-svc \
 * //   --operation READ --operation DESCRIBE --topic orders.created
 * ```
 */
export function toKafkaAclCommands(
  resources: AclResource[],
  principal: string,
  bootstrapServer = "<bootstrap-server>",
): string[] {
  return resources.map((r) => {
    const ops = r.operations.map((o) => `--operation ${o}`).join(" ");
    const resourceFlag =
      r.resourceType === "topic"
        ? `--topic '${r.name}'`
        : r.resourceType === "group"
          ? `--group '${r.name}'`
          : r.resourceType === "transactional-id"
            ? `--transactional-id '${r.name}'`
            : "--cluster";
    const pattern =
      r.patternType === "prefixed" ? " --resource-pattern-type prefixed" : "";
    return (
      `kafka-acls.sh --bootstrap-server ${bootstrapServer} --add ` +
      `--allow-principal '${principal}' ${ops} ${resourceFlag}${pattern}` +
      `  # ${r.reason}`
    );
  });
}

/** Cluster coordinates for building MSK resource ARNs. */
export interface MskClusterCoordinates {
  region: string;
  accountId: string;
  clusterName: string;
  /** The UUID segment of the cluster ARN. */
  clusterUuid: string;
}

const MSK_TOPIC_ACTIONS: Record<string, string[]> = {
  READ: ["kafka-cluster:ReadData", "kafka-cluster:DescribeTopic"],
  WRITE: ["kafka-cluster:WriteData", "kafka-cluster:DescribeTopic"],
  DESCRIBE: ["kafka-cluster:DescribeTopic"],
  CREATE: ["kafka-cluster:CreateTopic"],
  DELETE: ["kafka-cluster:DeleteTopic"],
};
const MSK_GROUP_ACTIONS: Record<string, string[]> = {
  READ: ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"],
  DESCRIBE: ["kafka-cluster:DescribeGroup"],
  DELETE: ["kafka-cluster:DeleteGroup"],
};
const MSK_TX_ACTIONS: Record<string, string[]> = {
  WRITE: [
    "kafka-cluster:AlterTransactionalId",
    "kafka-cluster:DescribeTransactionalId",
  ],
  DESCRIBE: ["kafka-cluster:DescribeTransactionalId"],
};

/**
 * Render ACL requirements as an AWS IAM policy document for MSK IAM auth.
 *
 * Prefixed patterns become `name*` wildcards in the resource ARN. The
 * generated policy always includes `kafka-cluster:Connect` on the cluster.
 * **Review before use** — validate against current AWS documentation and
 * your organisation's least-privilege standards.
 */
export function toMskIamPolicy(
  resources: AclResource[],
  cluster: MskClusterCoordinates,
): { Version: string; Statement: unknown[] } {
  const { region, accountId, clusterName, clusterUuid } = cluster;
  const arn = (type: string, name: string) =>
    `arn:aws:kafka:${region}:${accountId}:${type}/${clusterName}/${clusterUuid}/${name}`;

  const statements: unknown[] = [
    {
      Sid: "Connect",
      Effect: "Allow",
      Action: ["kafka-cluster:Connect"],
      Resource: [
        `arn:aws:kafka:${region}:${accountId}:cluster/${clusterName}/${clusterUuid}`,
      ],
    },
  ];

  let sid = 0;
  for (const r of resources) {
    const suffix = r.patternType === "prefixed" ? `${r.name}*` : r.name;
    let actions: string[] = [];
    let resource: string | undefined;
    if (r.resourceType === "topic") {
      actions = [...new Set(r.operations.flatMap((o) => MSK_TOPIC_ACTIONS[o] ?? []))];
      resource = arn("topic", suffix);
    } else if (r.resourceType === "group") {
      actions = [...new Set(r.operations.flatMap((o) => MSK_GROUP_ACTIONS[o] ?? []))];
      resource = arn("group", suffix);
    } else if (r.resourceType === "transactional-id") {
      actions = [...new Set(r.operations.flatMap((o) => MSK_TX_ACTIONS[o] ?? []))];
      resource = arn("transactional-id", suffix);
    } else {
      actions = ["kafka-cluster:CreateTopic"];
      resource = `arn:aws:kafka:${region}:${accountId}:topic/${clusterName}/${clusterUuid}/*`;
    }
    if (actions.length === 0 || !resource) continue;
    statements.push({
      Sid: `Acl${sid++}`,
      Effect: "Allow",
      Action: actions,
      Resource: [resource],
    });
  }

  return { Version: "2012-10-17", Statement: statements };
}
