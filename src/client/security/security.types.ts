/**
 * OAuth bearer token returned by an {@link OAuthBearerProvider}.
 * Mirrors the shape expected by librdkafka's OAUTHBEARER mechanism.
 */
export interface OAuthBearerToken {
  /** The raw token value presented to the broker. */
  value: string;
  /** Kafka principal name associated with the token. Default: `"kafka-client"`. */
  principal?: string;
  /**
   * Absolute token expiry as epoch milliseconds. The driver refreshes the
   * token before this deadline. Default: now + 15 minutes.
   */
  lifetimeMs?: number;
  /** Optional SASL extensions forwarded to the broker. */
  extensions?: Record<string, string>;
}

/**
 * Async factory that produces a fresh OAuth bearer token.
 * Called by the driver on connect and before each token expiry.
 */
export type OAuthBearerProvider = () => Promise<OAuthBearerToken>;

/** SASL configuration — username/password mechanisms or OAUTHBEARER. */
export type KafkaSaslOptions =
  | {
      mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
      username: string;
      password: string;
    }
  | {
      mechanism: "oauthbearer";
      oauthBearerProvider: OAuthBearerProvider;
    };

/**
 * Transport security configuration for `KafkaClientOptions.security`.
 *
 * Secure-by-default behaviour (see `resolveSecurityOptions`):
 * - When `sasl` is set and `ssl` is not, TLS is enabled automatically —
 *   credentials never travel over plaintext unless you explicitly opt out
 *   with `ssl: false` (which logs a warning).
 * - Connecting to non-local brokers with no security at all logs a one-time
 *   warning. Set `allowInsecure: true` to acknowledge and silence it
 *   (e.g. a trusted private network or an mTLS-terminating service mesh).
 *
 * @example SASL/SCRAM over TLS (ssl auto-enabled)
 * ```ts
 * const kafka = new KafkaClient(id, group, brokers, {
 *   security: {
 *     sasl: { mechanism: 'scram-sha-512', username: 'svc', password: process.env.KAFKA_PASSWORD! },
 *   },
 * });
 * ```
 *
 * @example AWS MSK IAM
 * ```ts
 * import { awsMskIamProvider } from '@drarzter/kafka-client/core';
 *
 * const kafka = new KafkaClient(id, group, brokers, {
 *   security: {
 *     sasl: { mechanism: 'oauthbearer', oauthBearerProvider: awsMskIamProvider({ region: 'eu-west-1' }) },
 *   },
 * });
 * ```
 */
export interface KafkaSecurityOptions {
  /**
   * Enable TLS. Defaults to `true` when `sasl` is configured, otherwise `false`.
   */
  ssl?: boolean;
  /** SASL authentication. */
  sasl?: KafkaSaslOptions;
  /**
   * Acknowledge an intentionally insecure setup (plaintext to non-local
   * brokers) and silence the warning. Has no effect when `ssl`/`sasl` are set.
   */
  allowInsecure?: boolean;
}
