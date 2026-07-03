import { ConfluentTransport } from "../../../core";
import type { KafkaTransport, IAdmin } from "../../../core";
import { FakeTransport } from "../../../testing/transport.fake";

describe("public transport exports (/core)", () => {
  it("exports ConfluentTransport implementing KafkaTransport", () => {
    const transport: KafkaTransport = new ConfluentTransport("export-check", [
      "localhost:9092",
    ]);
    expect(typeof transport.producer).toBe("function");
    expect(typeof transport.consumer).toBe("function");
    expect(typeof transport.admin).toBe("function");
  });

  it("exposes the admin surface needed for watermark inspection", () => {
    const transport = new ConfluentTransport("export-check", [
      "localhost:9092",
    ]);
    const admin: IAdmin = transport.admin();
    expect(typeof admin.fetchTopicOffsets).toBe("function");
    expect(typeof admin.listTopics).toBe("function");
  });

  it("accepts the security options third argument", () => {
    expect(
      () =>
        new ConfluentTransport("export-check", ["localhost:9092"], {
          ssl: true,
          sasl: { mechanism: "plain", username: "u", password: "p" },
        }),
    ).not.toThrow();
  });

  it("keeps the exported interface types usable for structural typing", () => {
    // Compile-time assertion: FakeTransport satisfies the public KafkaTransport.
    const fake: KafkaTransport = new FakeTransport();
    expect(typeof fake.producer).toBe("function");
  });
});
