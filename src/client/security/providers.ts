import type { OAuthBearerProvider, OAuthBearerToken } from "./security.types";

/** Injectable dynamic-import function — overridable in tests. */
type ImportFn = (specifier: string) => Promise<any>;

const defaultImport: ImportFn = (specifier) => import(specifier);

/** Options for {@link awsMskIamProvider}. */
export interface AwsMskIamProviderOptions {
  /** AWS region of the MSK cluster, e.g. `"eu-west-1"`. */
  region: string;
  /**
   * Optional AWS credentials profile / role logic is resolved by the signer
   * library from the standard provider chain (env vars, ECS/EKS metadata,
   * shared config). Nothing to configure here for IRSA / task roles.
   */
  /** @internal Injectable import for tests. */
  importFn?: ImportFn;
}

/**
 * OAUTHBEARER token provider for **AWS MSK IAM authentication**.
 *
 * Delegates token signing to the official `aws-msk-iam-sasl-signer-js`
 * package (an optional dependency — install it alongside this library):
 *
 * ```bash
 * npm install aws-msk-iam-sasl-signer-js
 * ```
 *
 * Credentials are resolved from the standard AWS provider chain, so EKS
 * IRSA, ECS task roles, and env credentials all work with no extra config.
 * Authorisation is then governed by IAM policies (`kafka-cluster:*` actions) —
 * see `describeRequiredAcls()` + `toMskIamPolicy()` to generate one covering
 * every topic/group this library derives.
 *
 * @example
 * ```ts
 * const kafka = new KafkaClient(id, group, brokers, {
 *   security: {
 *     sasl: {
 *       mechanism: 'oauthbearer',
 *       oauthBearerProvider: awsMskIamProvider({ region: 'eu-west-1' }),
 *     },
 *   },
 * });
 * ```
 */
export function awsMskIamProvider(
  options: AwsMskIamProviderOptions,
): OAuthBearerProvider {
  const importFn = options.importFn ?? defaultImport;
  return async (): Promise<OAuthBearerToken> => {
    let signer: any;
    try {
      signer = await importFn("aws-msk-iam-sasl-signer-js");
    } catch {
      throw new Error(
        "awsMskIamProvider: package 'aws-msk-iam-sasl-signer-js' is not installed. " +
          "Run `npm install aws-msk-iam-sasl-signer-js` to enable MSK IAM authentication.",
      );
    }
    const { token, expiryTime } = await signer.generateAuthToken({
      region: options.region,
    });
    return {
      value: token,
      principal: "msk-iam",
      // expiryTime is epoch ms per the signer's contract
      lifetimeMs: expiryTime,
    };
  };
}

/** Options for {@link gcpAccessTokenProvider}. */
export interface GcpTokenProviderOptions {
  /** OAuth scopes. Default: `["https://www.googleapis.com/auth/cloud-platform"]`. */
  scopes?: string[];
  /** Principal reported to the broker. Default: `"gcp"`. */
  principal?: string;
  /**
   * Token validity reported to the driver (refresh margin), epoch-relative ms.
   * GCP access tokens live ~1 h; default refresh window is 50 minutes.
   */
  tokenTtlMs?: number;
  /** @internal Injectable import for tests. */
  importFn?: ImportFn;
}

/**
 * OAUTHBEARER token provider for **Google Cloud Managed Service for Apache
 * Kafka** (and any broker accepting Google OAuth access tokens).
 *
 * Delegates to the official `google-auth-library` (optional dependency):
 *
 * ```bash
 * npm install google-auth-library
 * ```
 *
 * Uses Application Default Credentials, so GKE Workload Identity, attached
 * service accounts, and `GOOGLE_APPLICATION_CREDENTIALS` all work unchanged.
 * Verify the exact token format expected by your cluster against current
 * Google documentation — this provider supplies a raw ADC access token.
 *
 * @example
 * ```ts
 * const kafka = new KafkaClient(id, group, brokers, {
 *   security: {
 *     sasl: {
 *       mechanism: 'oauthbearer',
 *       oauthBearerProvider: gcpAccessTokenProvider(),
 *     },
 *   },
 * });
 * ```
 */
export function gcpAccessTokenProvider(
  options: GcpTokenProviderOptions = {},
): OAuthBearerProvider {
  const importFn = options.importFn ?? defaultImport;
  const ttlMs = options.tokenTtlMs ?? 50 * 60_000;
  return async (): Promise<OAuthBearerToken> => {
    let lib: any;
    try {
      lib = await importFn("google-auth-library");
    } catch {
      throw new Error(
        "gcpAccessTokenProvider: package 'google-auth-library' is not installed. " +
          "Run `npm install google-auth-library` to enable GCP authentication.",
      );
    }
    const auth = new lib.GoogleAuth({
      scopes: options.scopes ?? ["https://www.googleapis.com/auth/cloud-platform"],
    });
    const token = await auth.getAccessToken();
    if (!token) {
      throw new Error(
        "gcpAccessTokenProvider: google-auth-library returned no access token — " +
          "check Application Default Credentials.",
      );
    }
    return {
      value: token,
      principal: options.principal ?? "gcp",
      lifetimeMs: Date.now() + ttlMs,
    };
  };
}
