import type { KafkaLogger } from "../types";
import type { KafkaSecurityOptions } from "./security.types";

/** Hosts considered local — plaintext to these never warns. */
const LOCAL_HOST_PATTERNS = [
  /^localhost(:\d+)?$/i,
  /^127\.\d+\.\d+\.\d+(:\d+)?$/,
  /^\[?::1\]?(:\d+)?$/,
  /^0\.0\.0\.0(:\d+)?$/,
  /^host\.docker\.internal(:\d+)?$/i,
];

function isLocalBroker(broker: string): boolean {
  return LOCAL_HOST_PATTERNS.some((re) => re.test(broker.trim()));
}

/**
 * Apply the secure-by-default rules to a user-supplied security config:
 *
 * 1. `sasl` present + `ssl` unset → `ssl: true`. Credentials never travel
 *    over plaintext by accident; opting out requires an explicit `ssl: false`
 *    (which logs a warning).
 * 2. No `ssl`/`sasl` at all while any broker is non-local → one warning per
 *    client, silenceable via `allowInsecure: true`.
 *
 * Never throws and never blocks a connection — the defaults protect, the
 * user stays in control.
 *
 * @returns The effective options passed to the transport (may be `undefined`
 *          when no security was configured).
 */
export function resolveSecurityOptions(
  security: KafkaSecurityOptions | undefined,
  brokers: string[],
  logger: KafkaLogger,
): KafkaSecurityOptions | undefined {
  const hasRemoteBroker = brokers.some((b) => !isLocalBroker(b));

  if (!security?.sasl && security?.ssl !== true) {
    if (hasRemoteBroker && !security?.allowInsecure) {
      logger.warn(
        "Connecting to non-local brokers without TLS or SASL — traffic and payloads travel in " +
          "plaintext. Configure `security: { ssl, sasl }` for production clusters, or set " +
          "`security: { allowInsecure: true }` to acknowledge an intentionally insecure setup.",
      );
    }
    return security;
  }

  if (security.sasl && security.ssl === undefined) {
    return { ...security, ssl: true };
  }

  if (security.sasl && security.ssl === false) {
    logger.warn(
      "SASL credentials are configured with `ssl: false` — credentials will be sent over " +
        "plaintext. This is only safe on fully trusted networks.",
    );
  }

  return security;
}
