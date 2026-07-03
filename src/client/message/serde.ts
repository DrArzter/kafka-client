import type { MessageHeaders } from "../types";

/**
 * Context passed to `MessageSerde.serialize` / `deserialize`.
 *
 * Carries the topic name, decoded message headers, and which side of the
 * record is being (de)serialized. A Confluent Schema Registry serde uses
 * `topic` + `isKey` to derive the subject name (`<topic>-value` / `<topic>-key`)
 * and reads the schema id from the header/magic-byte prefix on `data`.
 */
export interface SerdeContext {
  /** Topic the message is produced to / consumed from. */
  topic: string;
  /** Decoded message headers (envelope headers included). */
  headers: MessageHeaders;
  /**
   * Which side of the Kafka record this call is (de)serializing.
   * `false` / omitted → the value (default); `true` → the key.
   * Used by schema-registry serdes to pick the `value` vs `key` subject.
   */
  isKey?: boolean;
}

/**
 * Pluggable serialization layer for message payloads.
 *
 * A `MessageSerde` converts a validated payload object to the wire form
 * (`Buffer` or `string`) on produce, and back to an object on consume.
 * The default is {@link JsonSerde}, which reproduces the client's historical
 * `JSON.stringify` / `JSON.parse` behaviour exactly.
 *
 * Serde only touches the message VALUE. Envelope metadata
 * (`x-event-id`, `x-correlation-id`, `x-lamport-clock`, `traceparent`, …)
 * always travels in headers and is never serialized through this layer.
 *
 * Set a client-wide serde via `KafkaClientOptions.serde`, or a per-topic
 * override via `topic(...).serde(mySerde)` — the per-topic serde wins for
 * that topic.
 *
 * @example
 * ```ts
 * const kafka = new KafkaClient(id, group, brokers, { serde: new JsonSerde() });
 * ```
 */
export interface MessageSerde {
  /**
   * Serialize a validated payload object to wire bytes (`Buffer`) or a
   * `string`. Validation has already run on `value` before this is called.
   */
  serialize(
    value: unknown,
    ctx: SerdeContext,
  ): Buffer | string | Promise<Buffer | string>;
  /**
   * Deserialize raw wire bytes into a payload object. Schema validation
   * (if any) runs on the returned object afterwards.
   */
  deserialize(data: Buffer, ctx: SerdeContext): unknown | Promise<unknown>;
}

/**
 * Default {@link MessageSerde}: JSON via `JSON.stringify` / `JSON.parse`.
 *
 * Byte-for-byte identical to the client's historical serialization, so it is
 * a zero-behaviour-change default. Produces a UTF-8 `string` on serialize and
 * decodes UTF-8 bytes on deserialize.
 */
export class JsonSerde implements MessageSerde {
  /** JSON-stringify the validated payload. Returns a UTF-8 string. */
  serialize(value: unknown): string {
    return JSON.stringify(value);
  }

  /** JSON-parse UTF-8 wire bytes into an object. */
  deserialize(data: Buffer): unknown {
    return JSON.parse(data.toString("utf8"));
  }
}
