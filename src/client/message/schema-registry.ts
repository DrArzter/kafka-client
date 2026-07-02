import type { SchemaLike, SchemaParseContext } from "./topic";

/** A schema registered in a Confluent-compatible Schema Registry. */
export interface RegisteredSchema {
  /** Globally unique schema id assigned by the registry. */
  id: number;
  /** Version of the schema within its subject. */
  version: number;
  /** The schema definition string (JSON Schema / Avro / Protobuf source). */
  schema: string;
}

/** Options for `SchemaRegistryClient`. */
export interface SchemaRegistryClientOptions {
  /** Registry base URL, e.g. `http://localhost:8081` or a Confluent Cloud SR endpoint. */
  baseUrl: string;
  /** HTTP Basic credentials (Confluent Cloud SR API key/secret). */
  auth?: { username: string; password: string };
  /** Cache TTL for subject lookups in ms. Default: `300_000` (5 min). */
  cacheTtlMs?: number;
  /** Injectable fetch implementation (tests). Default: global `fetch`. */
  fetchFn?: typeof fetch;
}

/** Schema type accepted by Confluent-compatible registries. */
export type RegistrySchemaType = "JSON" | "AVRO" | "PROTOBUF";

/**
 * Minimal, dependency-free client for the Confluent Schema Registry REST API
 * (works with Confluent Platform/Cloud, Redpanda, Karapace, AWS Glue SR proxy).
 *
 * Scope: subject/version management and compatibility checks — the pieces
 * needed to keep locally-defined schemas in lockstep with a central registry.
 * Payload (de)serialisation stays JSON as everywhere in this library; wire-format
 * framing with magic bytes (Avro/Protobuf binary) is intentionally out of scope.
 *
 * @example
 * ```ts
 * const registry = new SchemaRegistryClient({ baseUrl: 'http://localhost:8081' });
 * const { id } = await registry.registerSchema(
 *   'order.created-value',
 *   JSON.stringify(orderJsonSchema),
 *   'JSON',
 * );
 * ```
 */
export class SchemaRegistryClient {
  private readonly fetchFn: typeof fetch;
  private readonly cacheTtlMs: number;
  private readonly latestCache = new Map<
    string,
    { value: RegisteredSchema; expiresAt: number }
  >();

  constructor(private readonly options: SchemaRegistryClientOptions) {
    if (!options.baseUrl) {
      throw new Error("SchemaRegistryClient: baseUrl is required");
    }
    this.fetchFn = options.fetchFn ?? fetch;
    this.cacheTtlMs = options.cacheTtlMs ?? 300_000;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      "Content-Type": "application/vnd.schemaregistry.v1+json",
    };
    if (this.options.auth) {
      const { username, password } = this.options.auth;
      h["Authorization"] =
        "Basic " + Buffer.from(`${username}:${password}`).toString("base64");
    }
    return h;
  }

  private async request<R>(
    method: "GET" | "POST",
    path: string,
    body?: unknown,
  ): Promise<R> {
    const url = `${this.options.baseUrl.replace(/\/$/, "")}${path}`;
    const res = await this.fetchFn(url, {
      method,
      headers: this.headers(),
      ...(body !== undefined && { body: JSON.stringify(body) }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(
        `SchemaRegistry ${method} ${path} failed: ${res.status} ${res.statusText}${text ? ` — ${text}` : ""}`,
      );
    }
    return (await res.json()) as R;
  }

  /** Fetch the latest schema registered under `subject`. Cached for `cacheTtlMs`. */
  async getLatestSchema(subject: string): Promise<RegisteredSchema> {
    const cached = this.latestCache.get(subject);
    if (cached && cached.expiresAt > Date.now()) return cached.value;
    const raw = await this.request<{
      id: number;
      version: number;
      schema: string;
    }>("GET", `/subjects/${encodeURIComponent(subject)}/versions/latest`);
    const value: RegisteredSchema = {
      id: raw.id,
      version: raw.version,
      schema: raw.schema,
    };
    this.latestCache.set(subject, {
      value,
      expiresAt: Date.now() + this.cacheTtlMs,
    });
    return value;
  }

  /** Fetch a specific schema version of a subject. */
  async getSchemaVersion(
    subject: string,
    version: number,
  ): Promise<RegisteredSchema> {
    const raw = await this.request<{
      id: number;
      version: number;
      schema: string;
    }>(
      "GET",
      `/subjects/${encodeURIComponent(subject)}/versions/${version}`,
    );
    return { id: raw.id, version: raw.version, schema: raw.schema };
  }

  /**
   * Register a schema under `subject` (idempotent — re-registering the same
   * schema returns the existing id). Returns the registry-assigned schema id.
   */
  async registerSchema(
    subject: string,
    schema: string,
    schemaType: RegistrySchemaType = "JSON",
  ): Promise<{ id: number }> {
    this.latestCache.delete(subject);
    return this.request<{ id: number }>(
      "POST",
      `/subjects/${encodeURIComponent(subject)}/versions`,
      { schema, schemaType },
    );
  }

  /**
   * Test `schema` against the subject's compatibility policy without registering.
   * Returns `true` when the registry reports the schema as compatible.
   */
  async checkCompatibility(
    subject: string,
    schema: string,
    schemaType: RegistrySchemaType = "JSON",
  ): Promise<boolean> {
    const res = await this.request<{ is_compatible: boolean }>(
      "POST",
      `/compatibility/subjects/${encodeURIComponent(subject)}/versions/latest`,
      { schema, schemaType },
    );
    return res.is_compatible;
  }
}

/** Options for `registrySchema()`. */
export interface RegistrySchemaOptions<T> {
  /**
   * Local structural validator (Zod/Valibot/…) applied to every message.
   * The registry governs schema *evolution*; this governs runtime *shape*.
   */
  validator?: SchemaLike<T>;
  /**
   * When `true` (default), the message's `x-schema-version` must not be newer
   * than the latest version registered for the subject — a producer publishing
   * an unregistered version fails loudly instead of drifting silently.
   */
  enforceVersion?: boolean;
}

/**
 * Bridge a Schema Registry subject to this library's `SchemaLike` seam.
 *
 * On each `parse` the adapter resolves the subject's latest registered version
 * (cached), optionally verifies the message's schema version does not exceed
 * it, and delegates structural validation to the provided local validator.
 * Attach the result to a `TopicDescriptor` like any other schema:
 *
 * @example
 * ```ts
 * const registry = new SchemaRegistryClient({ baseUrl: 'http://localhost:8081' });
 *
 * const OrderCreated = topic('order.created').schema(
 *   registrySchema(registry, 'order.created-value', {
 *     validator: z.object({ orderId: z.string() }),
 *   }),
 * );
 * ```
 */
export function registrySchema<T = any>(
  client: SchemaRegistryClient,
  subject: string,
  options?: RegistrySchemaOptions<T>,
): SchemaLike<T> {
  const enforceVersion = options?.enforceVersion ?? true;
  return {
    async parse(data: unknown, ctx?: SchemaParseContext): Promise<T> {
      const latest = await client.getLatestSchema(subject);
      if (enforceVersion && ctx?.version !== undefined && ctx.version > latest.version) {
        throw new Error(
          `registrySchema: message version ${ctx.version} for subject "${subject}" ` +
            `is newer than the latest registered version ${latest.version} — ` +
            `register the schema before producing with it`,
        );
      }
      if (options?.validator) {
        return options.validator.parse(data, ctx);
      }
      return data as T;
    },
  };
}
