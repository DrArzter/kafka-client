import type { SchemaLike, SchemaParseContext } from "./topic";

/**
 * Options for `versionedSchema()`.
 * @typeParam T - The (latest) output type produced after parsing and migration.
 */
export interface VersionedSchemaOptions<T> {
  /**
   * Called after a message parsed with a non-latest schema version.
   * Receives the parsed data, the version it was parsed with, and the latest
   * registered version. Must return the data in its latest shape.
   *
   * When omitted, older versions are returned as parsed — callers must handle
   * shape differences themselves.
   */
  migrate?: (
    data: any,
    fromVersion: number,
    latestVersion: number,
  ) => T | Promise<T>;
}

/**
 * Compose per-version validators into a single `SchemaLike` that dispatches on
 * the message's `x-schema-version` header (via `SchemaParseContext.version`).
 *
 * - Consume path: the version comes from the `x-schema-version` header
 *   (defaults to `1` when absent).
 * - Send path: the version comes from `SendOptions.schemaVersion`
 *   (defaults to `1`).
 * - No parse context at all (direct `.parse(data)` call): the latest
 *   registered version is assumed.
 *
 * Throws when a message carries a version with no registered schema — a
 * misconfigured producer fails loudly instead of validating against the
 * wrong shape.
 *
 * @typeParam T - The latest message shape (post-migration).
 * @param versions Map of version number → validator for that version.
 * @param options Optional migration hook to upgrade old shapes to the latest.
 *
 * @example
 * ```ts
 * const OrderSchema = versionedSchema<{ orderId: string; amountMinor: number }>(
 *   {
 *     1: z.object({ orderId: z.string(), amount: z.number() }),
 *     2: z.object({ orderId: z.string(), amountMinor: z.number().int() }),
 *   },
 *   {
 *     migrate: (data, from) =>
 *       from === 1 ? { orderId: data.orderId, amountMinor: Math.round(data.amount * 100) } : data,
 *   },
 * );
 *
 * const OrderCreated = topic('order.created').schema(OrderSchema);
 * ```
 */
export function versionedSchema<T = any>(
  versions: Record<number, SchemaLike<any>>,
  options?: VersionedSchemaOptions<T>,
): SchemaLike<T> {
  const registered = Object.keys(versions)
    .map(Number)
    .filter((v) => Number.isInteger(v) && v > 0)
    .sort((a, b) => a - b);
  if (registered.length === 0) {
    throw new Error(
      "versionedSchema: at least one schema version must be registered (keys must be positive integers)",
    );
  }
  const latestVersion = registered[registered.length - 1];

  return {
    async parse(data: unknown, ctx?: SchemaParseContext): Promise<T> {
      const version = ctx?.version ?? latestVersion;
      const schema = versions[version];
      if (!schema) {
        throw new Error(
          `versionedSchema: no schema registered for version ${version}` +
            `${ctx?.topic ? ` (topic "${ctx.topic}")` : ""} — registered versions: ${registered.join(", ")}`,
        );
      }
      const parsed = await schema.parse(data, ctx);
      if (version < latestVersion && options?.migrate) {
        return options.migrate(parsed, version, latestVersion);
      }
      return parsed as T;
    },
  };
}
