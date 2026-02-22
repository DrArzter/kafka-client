import { KafkaHealthIndicator } from "../kafka.health";

describe("KafkaHealthIndicator", () => {
  let health: KafkaHealthIndicator;

  beforeEach(() => {
    health = new KafkaHealthIndicator();
  });

  it("should return up when checkStatus succeeds", async () => {
    const mockClient = {
      clientId: "test-client",
      checkStatus: jest.fn().mockResolvedValue({
        status: "up",
        clientId: "test-client",
        topics: ["topic1", "topic2"],
      }),
    } as any;

    const result = await health.check(mockClient);

    expect(result).toEqual({
      status: "up",
      clientId: "test-client",
      topics: ["topic1", "topic2"],
    });
  });

  it("should return down when checkStatus returns down", async () => {
    const mockClient = {
      clientId: "test-client",
      checkStatus: jest.fn().mockResolvedValue({
        status: "down",
        clientId: "test-client",
        error: "Connection refused",
      }),
    } as any;

    const result = await health.check(mockClient);

    expect(result).toEqual({
      status: "down",
      clientId: "test-client",
      error: "Connection refused",
    });
  });
});
