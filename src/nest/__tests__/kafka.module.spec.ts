import "reflect-metadata";
import type { Provider } from "@nestjs/common";
import { KafkaModule } from "../kafka.module";
import { getKafkaClientToken } from "../kafka.constants";
import { FakeTransport } from "../../testing/transport.fake";
import type { KafkaClient } from "../../client/kafka.client";

/** Extract the factory provider for the client token from a DynamicModule. */
function getClientProvider(providers: Provider[], token: string) {
  const provider = providers.find(
    (p): p is Extract<Provider, { provide: unknown }> =>
      typeof p === "object" && "provide" in p && p.provide === token,
  );
  if (!provider || !("useFactory" in provider)) {
    throw new Error("client provider not found");
  }
  return provider as { provide: string; useFactory: (...args: any[]) => any };
}

describe("KafkaModule.register — option forwarding", () => {
  it("forwards transactionalId, clockRecovery, lagThrottle, onTtlExpired, and transport to the client", async () => {
    const transport = new FakeTransport();
    const onTtlExpired = jest.fn();

    const dynamicModule = KafkaModule.register({
      clientId: "svc",
      groupId: "svc-group",
      brokers: ["localhost:9092"],
      transactionalId: "svc-custom-tx",
      clockRecovery: { topics: ["snapshot.topic"], timeoutMs: 1234 },
      lagThrottle: { maxLag: 500, pollIntervalMs: 10_000 },
      onTtlExpired,
      transport,
    });

    const token = getKafkaClientToken();
    const provider = getClientProvider(dynamicModule.providers!, token);

    // buildClient() calls connectProducer() — FakeTransport makes this a no-op.
    const client: KafkaClient<any> = await provider.useFactory();
    const ctx = (client as any).ctx;

    expect(ctx.transport).toBe(transport);
    expect(ctx.txId).toBe("svc-custom-tx");
    expect(ctx.clockRecoveryTopics).toEqual(["snapshot.topic"]);
    expect(ctx.clockRecoveryTimeoutMs).toBe(1234);
    expect(ctx.lagThrottleOpts).toEqual({ maxLag: 500, pollIntervalMs: 10_000 });
    expect(ctx.onTtlExpired).toBe(onTtlExpired);

    // A lag-throttle poller is started on connect — tear it down so the test
    // process doesn't keep an interval alive.
    await client.disconnectProducer();
  });

  it("defaults transactionalId to `${clientId}-tx` when not provided", async () => {
    const transport = new FakeTransport();

    const dynamicModule = KafkaModule.register({
      clientId: "svc2",
      groupId: "svc2-group",
      brokers: ["localhost:9092"],
      transport,
    });

    const provider = getClientProvider(
      dynamicModule.providers!,
      getKafkaClientToken(),
    );
    const client: KafkaClient<any> = await provider.useFactory();
    const ctx = (client as any).ctx;

    expect(ctx.txId).toBe("svc2-tx");
    expect(ctx.clockRecoveryTopics).toEqual([]);
    expect(ctx.lagThrottleOpts).toBeUndefined();

    await client.disconnectProducer();
  });
});
