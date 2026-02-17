import { topic, TopicDescriptor, TopicsFrom, SchemaLike } from "../topic";

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

describe("topic().schema()", () => {
  const mockSchema: SchemaLike<{ orderId: string; amount: number }> = {
    parse: (data: unknown) => data as { orderId: string; amount: number },
  };

  it("should create a descriptor with __schema attached", () => {
    const desc = topic("order.created").schema(mockSchema);
    expect(desc.__topic).toBe("order.created");
    expect(desc.__schema).toBe(mockSchema);
  });

  it("should have __type as undefined at runtime", () => {
    const desc = topic("order.created").schema(mockSchema);
    expect(desc.__type).toBeUndefined();
  });

  it("should work with TopicsFrom", () => {
    const A = topic("a").schema(mockSchema);
    const B = topic("b")<{ x: number }>();
    type Map = TopicsFrom<typeof A | typeof B>;
    const check: Map = {
      a: { orderId: "1", amount: 100 },
      b: { x: 42 },
    };
    expect(check).toBeDefined();
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
