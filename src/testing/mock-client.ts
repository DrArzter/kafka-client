import type { IKafkaClient, TopicMapConstraint } from "../client/types";

/**
 * Fully typed mock of `IKafkaClient<T>` where every method is a mock function.
 * Compatible with Jest, Vitest, or any framework whose `fn()` returns
 * an object with `.mock`, `.mockResolvedValue`, etc.
 */
export type MockKafkaClient<T extends TopicMapConstraint<T>> = {
  [K in keyof IKafkaClient<T>]: IKafkaClient<T>[K] & Record<string, any>;
};

/** Factory that creates a no-op mock function (e.g. `() => jest.fn()`). */
export type MockFactory = () => (...args: any[]) => any;

function detectMockFactory(): MockFactory {
  // Jest and Vitest inject their globals (`jest` / `vi`) as module-scope
  // bindings, not as properties of `globalThis`. The only reliable way to
  // detect them without a hard import is via `eval`, which evaluates in the
  // current module scope where those bindings are available.
  try {
    if (eval("typeof jest === 'object' && typeof jest.fn === 'function'")) {
      return () => eval("jest.fn()");
    }
  } catch {
    /* not available */
  }
  try {
    if (eval("typeof vi === 'object' && typeof vi.fn === 'function'")) {
      return () => eval("vi.fn()");
    }
  } catch {
    /* not available */
  }
  throw new Error(
    "createMockKafkaClient: no mock framework detected (jest/vitest). " +
      "Pass a custom mockFactory.",
  );
}

/**
 * Create a fully typed mock implementing every `IKafkaClient<T>` method.
 * Useful for unit-testing services that depend on `KafkaClient` without
 * touching a real broker.
 *
 * Auto-detects Jest (`jest.fn()`) or Vitest (`vi.fn()`). Pass a custom
 * `mockFactory` for other frameworks.
 *
 * All methods resolve to sensible defaults:
 * - `checkStatus()` → `{ status: 'up', clientId: 'mock-client', topics: [] }`
 * - `getClientId()` → `"mock-client"`
 * - void methods → `undefined`
 *
 * @example
 * ```ts
 * const kafka = createMockKafkaClient<MyTopics>();
 *
 * const service = new OrdersService(kafka);
 * await service.createOrder();
 *
 * expect(kafka.sendMessage).toHaveBeenCalledWith(
 *   'order.created',
 *   expect.objectContaining({ orderId: '123' }),
 * );
 * ```
 */
export function createMockKafkaClient<T extends TopicMapConstraint<T>>(
  mockFactory?: MockFactory,
): MockKafkaClient<T> {
  const fn = mockFactory ?? detectMockFactory();

  const mock = () => fn() as any;
  const resolved = (value: unknown) => mock().mockResolvedValue(value);
  const returning = (value: unknown) => mock().mockReturnValue(value);

  return {
    checkStatus: resolved({
      status: "up",
      clientId: "mock-client",
      topics: [],
    }),
    getConsumerLag: resolved([]),
    getClientId: returning("mock-client"),
    sendMessage: resolved(undefined),
    sendBatch: resolved(undefined),
    transaction: mock().mockImplementation(
      async (cb: (ctx: Record<string, unknown>) => Promise<void>) => {
        const ctx = {
          send: resolved(undefined),
          sendBatch: resolved(undefined),
        };
        await cb(ctx);
      },
    ),
    startConsumer: resolved({
      groupId: "mock-group",
      stop: mock().mockResolvedValue(undefined),
    }),
    startBatchConsumer: resolved({
      groupId: "mock-group",
      stop: mock().mockResolvedValue(undefined),
    }),
    stopConsumer: resolved(undefined),
    disconnect: resolved(undefined),
  } as unknown as MockKafkaClient<T>;
}
