import type { KafkaClientOptions } from "../types";

/**
 * Fail-fast validation of `KafkaClient` constructor arguments.
 * Collects every problem and throws a single error listing all of them, so a
 * misconfigured client fails at construction with a clear message instead of
 * surfacing as a confusing driver error on first use.
 *
 * @throws Error listing every invalid argument/option found.
 */
export function validateClientOptions(
  clientId: string,
  groupId: string,
  brokers: string[],
  options?: KafkaClientOptions,
): void {
  const problems: string[] = [];

  if (typeof clientId !== "string" || clientId.trim() === "") {
    problems.push("clientId must be a non-empty string");
  }
  if (typeof groupId !== "string" || groupId.trim() === "") {
    problems.push("groupId must be a non-empty string");
  }
  // With a custom transport (e.g. FakeTransport in tests) brokers are unused —
  // only validate emptiness when the client will actually dial them.
  if (!Array.isArray(brokers) || (brokers.length === 0 && !options?.transport)) {
    problems.push("brokers must be a non-empty array of broker addresses");
  } else if (brokers.some((b) => typeof b !== "string" || b.trim() === "")) {
    problems.push("brokers must not contain empty entries");
  }

  if (options) {
    const {
      numPartitions,
      transactionalId,
      clockRecovery,
      lagThrottle,
    } = options;

    if (
      numPartitions !== undefined &&
      (!Number.isInteger(numPartitions) || numPartitions < 1)
    ) {
      problems.push(
        `numPartitions must be a positive integer (got ${numPartitions})`,
      );
    }
    if (transactionalId !== undefined && transactionalId.trim() === "") {
      problems.push("transactionalId must be a non-empty string when set");
    }
    if (clockRecovery) {
      if (!Array.isArray(clockRecovery.topics)) {
        problems.push("clockRecovery.topics must be an array of topic names");
      }
      if (
        clockRecovery.timeoutMs !== undefined &&
        !(clockRecovery.timeoutMs > 0)
      ) {
        problems.push(
          `clockRecovery.timeoutMs must be > 0 (got ${clockRecovery.timeoutMs})`,
        );
      }
    }
    if (lagThrottle) {
      if (!(lagThrottle.maxLag >= 0)) {
        problems.push(`lagThrottle.maxLag must be >= 0 (got ${lagThrottle.maxLag})`);
      }
      if (
        lagThrottle.pollIntervalMs !== undefined &&
        !(lagThrottle.pollIntervalMs > 0)
      ) {
        problems.push(
          `lagThrottle.pollIntervalMs must be > 0 (got ${lagThrottle.pollIntervalMs})`,
        );
      }
      if (lagThrottle.maxWaitMs !== undefined && !(lagThrottle.maxWaitMs >= 0)) {
        problems.push(
          `lagThrottle.maxWaitMs must be >= 0 (got ${lagThrottle.maxWaitMs})`,
        );
      }
    }
  }

  if (problems.length > 0) {
    throw new Error(
      `KafkaClient: invalid configuration:\n- ${problems.join("\n- ")}`,
    );
  }
}
