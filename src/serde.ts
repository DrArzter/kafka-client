import { createRequire } from "node:module";
import type { MessageSerde, SerdeContext } from "./client/message/serde";
import type { SchemaRegistryClient } from "./client/message/schema-registry";

// Re-export the core serde surface so consumers of `@drarzter/kafka-client/serde`
// don't also have to import from the root/core entry point.
export { JsonSerde } from "./client/message/serde";
export type { MessageSerde, SerdeContext } from "./client/message/serde";
export { SchemaRegistryClient } from "./client/message/schema-registry";

/** Confluent wire-format magic byte. Always `0x00` for framed records. */
const MAGIC_BYTE = 0x00;

/** Injectable dynamic-import function — overridable in tests. */
type ImportFn = (specifier: string) => Promise<any>;

/**
 * Load an optional peer (`avsc` / `protobufjs`).
 *
 * Both are CommonJS packages. We resolve them with a `require` built from
 * `createRequire` so the loader works uniformly across this library's dual
 * CJS/ESM output *and* under the Jest CommonJS VM (a raw `import()` throws
 * "A dynamic import callback was invoked without --experimental-vm-modules"
 * there). The require base is derived from the current module when available
 * (`__filename` in CJS output / Jest), falling back to the process cwd — either
 * resolves a top-level node_modules dependency. We normalise a potential
 * `default` interop wrapper.
 */
const defaultImport: ImportFn = async (specifier) => {
  const base =
    typeof __filename !== "undefined"
      ? __filename
      : `${process.cwd()}/index.js`;
  const req = createRequire(base);
  const mod = req(specifier);
  return mod?.default ?? mod;
};

/**
 * Resolve a subject name for a serde call.
 *
 * Default is Confluent's `TopicNameStrategy`:
 * `<topic>-value` for the value, `<topic>-key` for the key. A `subject` option
 * may override with either a literal string or a function of the context.
 */
function resolveSubject(
  ctx: SerdeContext,
  subject?: string | ((ctx: SerdeContext) => string),
): string {
  if (typeof subject === "function") return subject(ctx);
  if (typeof subject === "string") return subject;
  return `${ctx.topic}-${ctx.isKey ? "key" : "value"}`;
}

/** Read the `[magic][4-byte BE id]` prefix; assert the magic byte. Returns `{ id, offset }`. */
function readWireHeader(
  data: Buffer,
  serdeName: string,
): { id: number; offset: number } {
  if (data.length < 5) {
    throw new Error(
      `${serdeName}: message too short to be Confluent-framed (need >= 5 bytes, got ${data.length}).`,
    );
  }
  const magic = data[0];
  if (magic !== MAGIC_BYTE) {
    throw new Error(
      `${serdeName}: unexpected magic byte 0x${(magic ?? 0).toString(16).padStart(2, "0")} ` +
        `(expected 0x00). The message is not Confluent wire-format — check the producer's serializer.`,
    );
  }
  const id = data.readInt32BE(1);
  return { id, offset: 5 };
}

/** Frame `[magic][4-byte BE id]` + `payloadParts` into a single Buffer. */
function frame(id: number, ...payloadParts: Buffer[]): Buffer {
  const header = Buffer.alloc(5);
  header.writeUInt8(MAGIC_BYTE, 0);
  header.writeInt32BE(id, 1);
  return Buffer.concat([header, ...payloadParts]);
}

/** Options common to the registry-backed serdes. */
interface RegistrySerdeCommonOptions {
  /** Schema Registry client used to resolve/register schema ids. */
  registry: SchemaRegistryClient;
  /**
   * Subject name. Defaults to Confluent `TopicNameStrategy`
   * (`<topic>-value` / `<topic>-key`). Provide a literal string or a
   * function of the {@link SerdeContext} to override.
   */
  subject?: string | ((ctx: SerdeContext) => string);
  /**
   * Register `schema` on first serialize to obtain its id (dev-friendly).
   * Default `false` → the id is resolved via `getLatestSchema(subject)`.
   */
  autoRegister?: boolean;
  /** @internal Injectable dynamic import for tests. */
  importFn?: ImportFn;
}

/** Options for {@link avroSerde}. */
export interface AvroSerdeOptions extends RegistrySerdeCommonOptions {
  /**
   * Avro schema (JSON string or object) used to serialize, and as the
   * write-schema fallback. Required to serialize; deserialize resolves the
   * writer schema from the registry via the wire-format id.
   */
  schema?: string | object;
}

/**
 * Confluent-wire-format **Avro** serde backed by a Schema Registry.
 *
 * Produces/consumes the exact byte layout Java/Go clients use, so this library
 * interoperates with them through a shared registry:
 *
 * ```
 * [magic 0x00][schema id: 4-byte big-endian][avro binary]
 * ```
 *
 * Uses the optional peer dependency [`avsc`](https://www.npmjs.com/package/avsc)
 * via dynamic import — install it to enable Avro:
 *
 * ```bash
 * npm install avsc
 * ```
 *
 * - **serialize**: resolves the subject from the context, obtains the schema id
 *   (`registerSchema` when `autoRegister`, else `getLatestSchema`), Avro-encodes
 *   `value` against `schema`, and frames the bytes.
 * - **deserialize**: reads the magic byte + big-endian id, resolves the writer
 *   schema via `registry.getSchemaById(id)` (cached forever), and Avro-decodes
 *   the remainder. The reader schema equals the writer schema in v1 — full
 *   reader-schema resolution / schema evolution is a future enhancement.
 *
 * The parsed `avsc` type is cached per schema string so repeated messages don't
 * re-parse the schema.
 *
 * @example
 * ```ts
 * import { avroSerde } from '@drarzter/kafka-client/serde';
 * import { SchemaRegistryClient } from '@drarzter/kafka-client';
 *
 * const registry = new SchemaRegistryClient({ baseUrl: 'http://localhost:8081' });
 * const orderSchema = {
 *   type: 'record',
 *   name: 'Order',
 *   fields: [{ name: 'orderId', type: 'string' }, { name: 'amount', type: 'double' }],
 * };
 *
 * // Per-topic:
 * const Orders = topic('orders')
 *   .serde(avroSerde({ registry, schema: orderSchema }))
 *   .type<Order>();
 *
 * // Client-wide:
 * const kafka = new KafkaClient(id, group, brokers, {
 *   serde: avroSerde({ registry, schema: orderSchema }),
 * });
 * ```
 */
export function avroSerde(options: AvroSerdeOptions): MessageSerde {
  const importFn = options.importFn ?? defaultImport;
  const schemaString =
    options.schema === undefined
      ? undefined
      : typeof options.schema === "string"
        ? options.schema
        : JSON.stringify(options.schema);

  /** Parsed-type cache keyed by schema string, so we never re-parse per message. */
  const typeCache = new Map<string, any>();
  let avscMod: any;

  async function loadAvsc(): Promise<any> {
    if (avscMod) return avscMod;
    try {
      avscMod = await importFn("avsc");
    } catch {
      throw new Error(
        "avroSerde: package 'avsc' is not installed. " +
          "Run `npm install avsc` to enable Avro serialization.",
      );
    }
    return avscMod;
  }

  async function typeFor(schemaStr: string): Promise<any> {
    const cached = typeCache.get(schemaStr);
    if (cached) return cached;
    const avsc = await loadAvsc();
    const type = avsc.Type.forSchema(JSON.parse(schemaStr));
    typeCache.set(schemaStr, type);
    return type;
  }

  return {
    async serialize(value: unknown, ctx: SerdeContext): Promise<Buffer> {
      if (schemaString === undefined) {
        throw new Error(
          "avroSerde: `schema` is required to serialize — pass the Avro schema " +
            "(JSON string or object) in the serde options.",
        );
      }
      const subject = resolveSubject(ctx, options.subject);
      const id = options.autoRegister
        ? (await options.registry.registerSchema(subject, schemaString, "AVRO"))
            .id
        : (await options.registry.getLatestSchema(subject)).id;
      const type = await typeFor(schemaString);
      const payload: Buffer = type.toBuffer(value);
      return frame(id, payload);
    },

    async deserialize(data: Buffer, _ctx: SerdeContext): Promise<unknown> {
      const { id, offset } = readWireHeader(data, "avroSerde");
      const registered = await options.registry.getSchemaById(id);
      const type = await typeFor(registered.schema);
      return type.fromBuffer(data.subarray(offset));
    },
  };
}

/** Options for {@link protobufSerde}. */
export interface ProtobufSerdeOptions extends RegistrySerdeCommonOptions {
  /**
   * Fully-qualified Protobuf message name to encode/decode,
   * e.g. `"com.acme.orders.Order"`.
   */
  messageType: string;
  /**
   * `.proto` source string defining {@link ProtobufSerdeOptions.messageType}.
   * Required to serialize; deserialize resolves the writer `.proto` from the
   * registry via the wire-format id.
   */
  schema?: string;
}

/**
 * Confluent-wire-format **Protobuf** serde backed by a Schema Registry.
 *
 * Produces/consumes the exact byte layout Java/Go clients use:
 *
 * ```
 * [magic 0x00][schema id: 4-byte big-endian][message-index][protobuf binary]
 * ```
 *
 * The **message-index** identifies which message type within the `.proto` file
 * was used. For the first/top-level message type (index `[0]`) Confluent writes
 * the single byte `0x00`. This serde implements that top-level case only —
 * multiple/nested message types are a documented v1 limitation and cause a
 * clear error on deserialize.
 *
 * Uses the optional peer dependency
 * [`protobufjs`](https://www.npmjs.com/package/protobufjs) via dynamic import —
 * install it to enable Protobuf:
 *
 * ```bash
 * npm install protobufjs
 * ```
 *
 * - **serialize**: obtains the schema id (`registerSchema` when `autoRegister`,
 *   else `getLatestSchema`), encodes `value` with the `protobufjs` `Type`, and
 *   frames it with the `0x00` message-index byte.
 * - **deserialize**: reads the magic byte + big-endian id + message-index (which
 *   must be the single `0x00` byte), resolves the writer `.proto` via
 *   `registry.getSchemaById(id)` (cached forever), and decodes the remainder.
 *
 * The parsed `protobufjs` `Type` is cached per schema string so repeated
 * messages don't re-parse the `.proto`.
 *
 * @example
 * ```ts
 * import { protobufSerde } from '@drarzter/kafka-client/serde';
 * import { SchemaRegistryClient } from '@drarzter/kafka-client';
 *
 * const registry = new SchemaRegistryClient({ baseUrl: 'http://localhost:8081' });
 * const proto = `
 *   syntax = "proto3";
 *   message Order { string orderId = 1; double amount = 2; }
 * `;
 *
 * // Per-topic:
 * const Orders = topic('orders')
 *   .serde(protobufSerde({ registry, schema: proto, messageType: 'Order' }))
 *   .type<Order>();
 *
 * // Client-wide:
 * const kafka = new KafkaClient(id, group, brokers, {
 *   serde: protobufSerde({ registry, schema: proto, messageType: 'Order' }),
 * });
 * ```
 */
export function protobufSerde(options: ProtobufSerdeOptions): MessageSerde {
  const importFn = options.importFn ?? defaultImport;

  /** Parsed-`Type` cache keyed by `.proto` source string. */
  const typeCache = new Map<string, any>();
  let protobufMod: any;

  async function loadProtobuf(): Promise<any> {
    if (protobufMod) return protobufMod;
    try {
      protobufMod = await importFn("protobufjs");
    } catch {
      throw new Error(
        "protobufSerde: package 'protobufjs' is not installed. " +
          "Run `npm install protobufjs` to enable Protobuf serialization.",
      );
    }
    return protobufMod;
  }

  async function typeFor(protoSource: string): Promise<any> {
    const cacheKey = `${protoSource}::${options.messageType}`;
    const cached = typeCache.get(cacheKey);
    if (cached) return cached;
    const protobuf = await loadProtobuf();
    const parsed = protobuf.parse(protoSource);
    const type = parsed.root.lookupType(options.messageType);
    typeCache.set(cacheKey, type);
    return type;
  }

  return {
    async serialize(value: unknown, ctx: SerdeContext): Promise<Buffer> {
      if (options.schema === undefined) {
        throw new Error(
          "protobufSerde: `schema` is required to serialize — pass the .proto " +
            "source string in the serde options.",
        );
      }
      const subject = resolveSubject(ctx, options.subject);
      const id = options.autoRegister
        ? (
            await options.registry.registerSchema(
              subject,
              options.schema,
              "PROTOBUF",
            )
          ).id
        : (await options.registry.getLatestSchema(subject)).id;
      const type = await typeFor(options.schema);
      const payload: Buffer = Buffer.from(
        type.encode(type.create(value as object)).finish(),
      );
      // Message-index for the top-level type ([0]) is the single byte 0x00.
      const messageIndex = Buffer.from([0x00]);
      return frame(id, messageIndex, payload);
    },

    async deserialize(data: Buffer, _ctx: SerdeContext): Promise<unknown> {
      const { id, offset } = readWireHeader(data, "protobufSerde");
      // Message-index: Confluent writes 0x00 for the top-level type ([0]). Any
      // other leading value is a varint length of a multi-element index array.
      const indexByte = data[offset];
      if (indexByte !== 0x00) {
        throw new Error(
          "protobufSerde: nested/multiple message types are not supported in v1 " +
            `(message-index byte was 0x${(indexByte ?? 0).toString(16).padStart(2, "0")}, ` +
            "expected 0x00 for the top-level message type).",
        );
      }
      const registered = await options.registry.getSchemaById(id);
      const type = await typeFor(registered.schema);
      const decoded = type.decode(data.subarray(offset + 1));
      return type.toObject(decoded, {
        longs: String,
        enums: String,
        bytes: Buffer,
        defaults: true,
      });
    },
  };
}
