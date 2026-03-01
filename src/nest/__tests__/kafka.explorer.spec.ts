import "reflect-metadata";
import { Logger } from "@nestjs/common";
import { KafkaExplorer } from "../kafka.explorer";
import { KAFKA_SUBSCRIBER_METADATA } from "../kafka.decorator";
import { getKafkaClientToken } from "../kafka.constants";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeWrapper(instance: object | null) {
  return { instance };
}

function attachMeta(constructor: Function, entries: any[]) {
  Reflect.defineMetadata(KAFKA_SUBSCRIBER_METADATA, entries, constructor);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("KafkaExplorer.onModuleInit", () => {
  let explorer: KafkaExplorer;
  let discoveryService: { getProviders: jest.Mock };
  let moduleRef: { get: jest.Mock };
  let mockClient: { startConsumer: jest.Mock; startBatchConsumer: jest.Mock };

  beforeEach(() => {
    mockClient = {
      startConsumer: jest.fn().mockResolvedValue({}),
      startBatchConsumer: jest.fn().mockResolvedValue({}),
    };
    moduleRef = { get: jest.fn().mockReturnValue(mockClient) };
    discoveryService = { getProviders: jest.fn().mockReturnValue([]) };
    explorer = new KafkaExplorer(discoveryService as any, moduleRef as any);
  });

  // 1. No providers with metadata → nothing called
  it("does not call startConsumer or startBatchConsumer when no provider has subscriber metadata", async () => {
    class PlainService {
      async handle() {}
    }
    discoveryService.getProviders.mockReturnValue([makeWrapper(new PlainService())]);

    await explorer.onModuleInit();

    expect(mockClient.startConsumer).not.toHaveBeenCalled();
    expect(mockClient.startBatchConsumer).not.toHaveBeenCalled();
  });

  // 2. Single @SubscribeTo → startConsumer called with correct topics
  it("calls startConsumer with the declared topics for a non-batch subscriber", async () => {
    class OrderService {
      async handleOrder() {}
    }
    const instance = new OrderService();
    attachMeta(OrderService, [
      {
        topics: ["orders.created", "orders.updated"],
        methodName: "handleOrder",
        batch: undefined,
        clientName: undefined,
        options: undefined,
        schemas: undefined,
      },
    ]);
    discoveryService.getProviders.mockReturnValue([makeWrapper(instance)]);

    await explorer.onModuleInit();

    expect(mockClient.startConsumer).toHaveBeenCalledTimes(1);
    expect(mockClient.startConsumer.mock.calls[0][0]).toEqual([
      "orders.created",
      "orders.updated",
    ]);
    expect(mockClient.startBatchConsumer).not.toHaveBeenCalled();
  });

  // 3. Batch subscriber → startBatchConsumer called instead
  it("calls startBatchConsumer when batch: true is set on the entry", async () => {
    class AnalyticsService {
      async handleBatch() {}
    }
    const instance = new AnalyticsService();
    attachMeta(AnalyticsService, [
      {
        topics: ["events.raw"],
        methodName: "handleBatch",
        batch: true,
        clientName: undefined,
        options: undefined,
        schemas: undefined,
      },
    ]);
    discoveryService.getProviders.mockReturnValue([makeWrapper(instance)]);

    await explorer.onModuleInit();

    expect(mockClient.startBatchConsumer).toHaveBeenCalledTimes(1);
    expect(mockClient.startBatchConsumer.mock.calls[0][0]).toEqual(["events.raw"]);
    expect(mockClient.startConsumer).not.toHaveBeenCalled();
  });

  // 4. Named client → moduleRef.get uses correct token
  it("resolves the named client token from moduleRef when clientName is set", async () => {
    class PaymentService {
      async handle() {}
    }
    const instance = new PaymentService();
    attachMeta(PaymentService, [
      {
        topics: ["payments.received"],
        methodName: "handle",
        batch: undefined,
        clientName: "payments",
        options: undefined,
        schemas: undefined,
      },
    ]);
    discoveryService.getProviders.mockReturnValue([makeWrapper(instance)]);

    await explorer.onModuleInit();

    expect(moduleRef.get).toHaveBeenCalledWith(
      getKafkaClientToken("payments"),
      { strict: false },
    );
  });

  // 5. Client not found → error logged, no crash, continues
  it("logs an error and skips the entry when moduleRef.get throws", async () => {
    const errorSpy = jest
      .spyOn(Logger.prototype, "error")
      .mockImplementation(() => {});
    moduleRef.get.mockImplementation(() => {
      throw new Error("Provider not found");
    });

    class MissingClientService {
      async handle() {}
    }
    const instance = new MissingClientService();
    attachMeta(MissingClientService, [
      {
        topics: ["ghost.topic"],
        methodName: "handle",
        batch: undefined,
        clientName: "ghost",
        options: undefined,
        schemas: undefined,
      },
    ]);
    discoveryService.getProviders.mockReturnValue([makeWrapper(instance)]);

    await expect(explorer.onModuleInit()).resolves.toBeUndefined();
    expect(errorSpy).toHaveBeenCalledTimes(1);
    expect(mockClient.startConsumer).not.toHaveBeenCalled();

    errorSpy.mockRestore();
  });

  // 6. Schemas in decorator → merged into consumerOptions.schemas
  it("passes schemas map into consumerOptions when the entry has schemas", async () => {
    const mockSchema = { parse: (d: unknown) => d };
    const schemasMap = new Map([["orders.created", mockSchema]]);

    class SchemedService {
      async handle() {}
    }
    const instance = new SchemedService();
    attachMeta(SchemedService, [
      {
        topics: ["orders.created"],
        methodName: "handle",
        batch: undefined,
        clientName: undefined,
        options: { groupId: "schemed-group" },
        schemas: schemasMap,
      },
    ]);
    discoveryService.getProviders.mockReturnValue([makeWrapper(instance)]);

    await explorer.onModuleInit();

    const passedOptions = mockClient.startConsumer.mock.calls[0][2];
    expect(passedOptions.schemas).toBe(schemasMap);
    expect(passedOptions.groupId).toBe("schemed-group");
  });
});
