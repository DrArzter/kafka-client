import { topic, TopicDescriptor, TopicsFrom } from "./topic";

describe("topic()", () => {
  it("should create a TopicDescriptor with __topic set", () => {
    const OrderCreated = topic("order.created")<{ orderId: string }>();
    expect(OrderCreated.__topic).toBe("order.created");
  });

  it("should have __type as undefined at runtime", () => {
    const OrderCreated = topic("order.created")<{ orderId: string }>();
    expect(OrderCreated.__type).toBeUndefined();
  });

  it("should create distinct descriptors for different topics", () => {
    const A = topic("a")<{ x: number }>();
    const B = topic("b")<{ y: string }>();
    expect(A.__topic).not.toBe(B.__topic);
  });
});

describe("TopicsFrom", () => {
  it("should produce correct type at compile time", () => {
    const OrderCreated = topic("order.created")<{
      orderId: string;
      amount: number;
    }>();
    const OrderCompleted = topic("order.completed")<{
      orderId: string;
      completedAt: string;
    }>();

    // This is a compile-time check — if it compiles, the types work
    type MyTopics = TopicsFrom<typeof OrderCreated | typeof OrderCompleted>;
    const check: MyTopics = {
      "order.created": { orderId: "1", amount: 100 },
      "order.completed": { orderId: "1", completedAt: "2024-01-01" },
    };
    expect(check).toBeDefined();
  });
});
