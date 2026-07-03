import {
  TestTopicMap,
  createClient,
  mockSend,
  mockRun,
  KafkaClient,
  topic,
} from "../helpers";
import { JsonSerde } from "../../message/serde";
import type { MessageSerde, SerdeContext } from "../../message/serde";
import type { EventEnvelope } from "../../message/envelope";

describe("MessageSerde", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ── JsonSerde ─────────────────────────────────────────────────────────────

  describe("JsonSerde", () => {
    it("round-trips an object through serialize → deserialize", () => {
      const serde = new JsonSerde();
      const value = { id: "1", value: 42, nested: { ok: true } };
      const wire = serde.serialize(value);
      expect(typeof wire).toBe("string");
      const back = serde.deserialize(Buffer.from(wire));
      expect(back).toEqual(value);
    });

    it("serialize is byte-identical to JSON.stringify", () => {
      const serde = new JsonSerde();
      const value = { id: "1", value: 42 };
      expect(serde.serialize(value)).toBe(JSON.stringify(value));
    });

    it("is the default serde — sends a JSON string on produce", async () => {
      const client = createClient();
      const message = { id: "1", value: 7 };
      await client.sendMessage("test.topic", message);

      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          expect.objectContaining({ value: JSON.stringify(message) }),
        ],
      });
    });
  });

  // ── Custom client-wide serde ────────────────────────────────────────────────

  describe("custom client-wide serde", () => {
    it("is invoked on produce with the correct context", async () => {
      const serialize = jest.fn(
        (v: unknown, _ctx: SerdeContext) => `CUSTOM:${JSON.stringify(v)}`,
      );
      const serde: MessageSerde = {
        serialize,
        deserialize: (data) => JSON.parse(data.toString("utf8")),
      };
      const client = createClient({ serde });

      await client.sendMessage("test.topic", { id: "1", value: 1 });

      expect(serialize).toHaveBeenCalledTimes(1);
      const [value, ctx] = serialize.mock.calls[0]!;
      expect(value).toEqual({ id: "1", value: 1 });
      expect(ctx.topic).toBe("test.topic");
      expect(ctx.isKey).toBe(false);
      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          expect.objectContaining({
            value: `CUSTOM:${JSON.stringify({ id: "1", value: 1 })}`,
          }),
        ],
      });
    });

    it("is invoked on consume to deserialize the value", async () => {
      const deserialize = jest.fn((data: Buffer) => {
        const raw = data.toString("utf8").replace(/^CUSTOM:/, "");
        return JSON.parse(raw);
      });
      const serde: MessageSerde = {
        serialize: (v) => `CUSTOM:${JSON.stringify(v)}`,
        deserialize,
      };
      const client = createClient({ serde });

      const received: EventEnvelope<any>[] = [];
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(`CUSTOM:${JSON.stringify({ id: "9", value: 9 })}`),
            offset: "0",
          },
        });
      });

      await client.startConsumer(["test.topic"], async (env) => {
        received.push(env);
      });

      expect(deserialize).toHaveBeenCalledTimes(1);
      expect(received).toHaveLength(1);
      expect(received[0]!.payload).toEqual({ id: "9", value: 9 });
    });
  });

  // ── Per-topic override ───────────────────────────────────────────────────────

  describe("per-topic serde override", () => {
    it("wins over the client-wide serde on produce", async () => {
      const clientSerialize = jest.fn((v: unknown) => JSON.stringify(v));
      const topicSerialize = jest.fn((v: unknown) => `TOPIC:${JSON.stringify(v)}`);
      const clientSerde: MessageSerde = {
        serialize: clientSerialize,
        deserialize: (d) => JSON.parse(d.toString("utf8")),
      };
      const topicSerde: MessageSerde = {
        serialize: topicSerialize,
        deserialize: (d) => JSON.parse(d.toString("utf8")),
      };

      const client = createClient({ serde: clientSerde });
      const Desc = topic("test.topic")
        .type<{ id: string; value: number }>()
        .serde(topicSerde);

      await client.sendMessage(Desc as any, { id: "1", value: 1 });

      expect(topicSerialize).toHaveBeenCalledTimes(1);
      expect(clientSerialize).not.toHaveBeenCalled();
      expect(mockSend).toHaveBeenCalledWith({
        topic: "test.topic",
        messages: [
          expect.objectContaining({
            value: `TOPIC:${JSON.stringify({ id: "1", value: 1 })}`,
          }),
        ],
      });
    });

    it("wins over the client-wide serde on consume", async () => {
      const clientDeserialize = jest.fn((d: Buffer) =>
        JSON.parse(d.toString("utf8")),
      );
      const topicDeserialize = jest.fn((d: Buffer) =>
        JSON.parse(d.toString("utf8").replace(/^TOPIC:/, "")),
      );
      const clientSerde: MessageSerde = {
        serialize: (v) => JSON.stringify(v),
        deserialize: clientDeserialize,
      };
      const topicSerde: MessageSerde = {
        serialize: (v) => `TOPIC:${JSON.stringify(v)}`,
        deserialize: topicDeserialize,
      };

      const client = createClient({ serde: clientSerde });
      const Desc = topic("test.topic")
        .type<{ id: string; value: number }>()
        .serde(topicSerde);

      const received: EventEnvelope<any>[] = [];
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(`TOPIC:${JSON.stringify({ id: "5", value: 5 })}`),
            offset: "0",
          },
        });
      });

      await client.startConsumer([Desc as any], async (env) => {
        received.push(env);
      });

      expect(topicDeserialize).toHaveBeenCalledTimes(1);
      expect(clientDeserialize).not.toHaveBeenCalled();
      expect(received[0]!.payload).toEqual({ id: "5", value: 5 });
    });

    it("chains with .key() in either order", () => {
      const s: MessageSerde = new JsonSerde();
      const a = topic("test.topic")
        .type<{ id: string; value: number }>()
        .key((m) => m.id)
        .serde(s);
      const b = topic("test.topic")
        .type<{ id: string; value: number }>()
        .serde(s)
        .key((m) => m.id);

      expect(a.__serde).toBe(s);
      expect(a.__key!({ id: "x", value: 1 })).toBe("x");
      expect(b.__serde).toBe(s);
      expect(b.__key!({ id: "y", value: 2 })).toBe("y");
    });
  });

  // ── Binary safety through forwarding ─────────────────────────────────────────

  describe("binary payload survives DLQ forwarding", () => {
    /**
     * A serde that produces/consumes non-UTF-8 binary bytes. The wire form is a
     * length-prefixed buffer, so a naive `.toString()` round-trip on the
     * forwarding path would corrupt it.
     */
    class BinarySerde implements MessageSerde {
      serialize(value: unknown): Buffer {
        const json = Buffer.from(JSON.stringify(value), "utf8");
        // Prefix with a non-UTF-8 magic byte (0xff) to prove bytes are preserved.
        return Buffer.concat([Buffer.from([0xff]), json]);
      }
      deserialize(data: Buffer): unknown {
        if (data[0] !== 0xff) throw new Error("bad magic byte");
        return JSON.parse(data.subarray(1).toString("utf8"));
      }
    }

    it("forwards the ORIGINAL binary bytes to the DLQ, uncorrupted", async () => {
      const serde = new BinarySerde();
      const client = createClient({ serde });

      const payload = { id: "1", value: 99 };
      const wireBytes = serde.serialize(payload); // 0xff + json

      const handler = jest.fn().mockRejectedValue(new Error("boom"));
      mockRun.mockImplementation(async ({ eachMessage }: any) => {
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: { value: wireBytes, offset: "0" },
        });
      });

      await client.startConsumer(["test.topic"], handler, { dlq: true });

      // Handler saw the deserialized object (serde ran on consume).
      expect(handler).toHaveBeenCalledTimes(1);
      expect(handler.mock.calls[0][0].payload).toEqual(payload);

      // DLQ received the EXACT original wire bytes — a Buffer, not a string,
      // byte-for-byte equal to what was produced (0xff magic byte intact).
      const dlqCall = mockSend.mock.calls.find(
        (c: any[]) => c[0].topic === "test.topic.dlq",
      );
      expect(dlqCall).toBeDefined();
      const forwarded = dlqCall![0].messages[0].value;
      expect(Buffer.isBuffer(forwarded)).toBe(true);
      expect(Buffer.compare(forwarded, wireBytes)).toBe(0);
      // And it still deserializes cleanly — proving no corruption.
      expect(serde.deserialize(forwarded)).toEqual(payload);
    });
  });
});
