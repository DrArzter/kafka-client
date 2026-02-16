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
   
  // Jest and Vitest inject globals into the test environment scope,
  // which may not be on `globalThis`. Use eval to check the actual scope.
  try {
    const jestFn = eval("typeof jest !== 'undefined' && jest.fn");
    if (typeof jestFn === "function") return () => (eval("jest.fn") as () => (...args: any[]) => any)();
  } catch { /* not available */ }
  try {
    const viFn = eval("typeof vi !== 'undefined' && vi.fn");
    if (typeof viFn === "function") return () => (eval("vi.fn") as () => (...args: any[]) => any)();
  } catch { /* not available */ }
   

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
 * - `checkStatus()` → `{ topics: [] }`
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
    checkStatus: resolved({ topics: [] }),
    getClientId: returning("mock-client"),
    sendMessage: resolved(undefined),
    sendBatch: resolved(undefined),
    transaction: mock().mockImplementation(async (cb: (ctx: Record<string, unknown>) => Promise<void>) => {
      const ctx = {
        send: resolved(undefined),
        sendBatch: resolved(undefined),
      };
      await cb(ctx);
    }),
    startConsumer: resolved(undefined),
    startBatchConsumer: resolved(undefined),
    stopConsumer: resolved(undefined),
    disconnect: resolved(undefined),
  } as unknown as MockKafkaClient<T>;
}
