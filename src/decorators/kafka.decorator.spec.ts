import "reflect-metadata";
import { SubscribeTo, KAFKA_SUBSCRIBER_METADATA } from "./kafka.decorator";
import { getKafkaClientToken, KAFKA_CLIENT } from "../module/kafka.constants";
import { topic } from "../client/topic";

describe("SubscribeTo", () => {
  it("should store metadata for a single topic", () => {
    class TestService {
      @SubscribeTo("test.topic")
      async handle() {}
    }

    const metadata = Reflect.getMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      TestService,
    );
    expect(metadata).toHaveLength(1);
    expect(metadata[0]).toMatchObject({
      topics: ["test.topic"],
      methodName: "handle",
    });
  });

  it("should store metadata for multiple topics", () => {
    class TestService {
      @SubscribeTo(["topic.a", "topic.b"])
      async handle() {}
    }

    const metadata = Reflect.getMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      TestService,
    );
    expect(metadata[0].topics).toEqual(["topic.a", "topic.b"]);
  });

  it("should store consumer options", () => {
    class TestService {
      @SubscribeTo("test.topic", {
        fromBeginning: true,
        retry: { maxRetries: 3 },
      })
      async handle() {}
    }

    const metadata = Reflect.getMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      TestService,
    );
    expect(metadata[0].options).toEqual({
      fromBeginning: true,
      retry: { maxRetries: 3 },
    });
  });

  it("should store clientName for named clients", () => {
    class TestService {
      @SubscribeTo("test.topic", { clientName: "orders" })
      async handle() {}
    }

    const metadata = Reflect.getMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      TestService,
    );
    expect(metadata[0].clientName).toBe("orders");
  });

  it("should accumulate multiple decorators on same class", () => {
    class TestService {
      @SubscribeTo("topic.a")
      async handleA() {}

      @SubscribeTo("topic.b")
      async handleB() {}
    }

    const metadata = Reflect.getMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      TestService,
    );
    expect(metadata).toHaveLength(2);
  });

  it("should accept a TopicDescriptor", () => {
    const TestTopic = topic("test.topic")<{ id: string }>();

    class TestService {
      @SubscribeTo(TestTopic)
      async handle() {}
    }

    const metadata = Reflect.getMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      TestService,
    );
    expect(metadata[0].topics).toEqual(["test.topic"]);
  });

  it("should accept an array of TopicDescriptors", () => {
    const TopicA = topic("topic.a")<{ id: string }>();
    const TopicB = topic("topic.b")<{ name: string }>();

    class TestService {
      @SubscribeTo([TopicA, TopicB])
      async handle() {}
    }

    const metadata = Reflect.getMetadata(
      KAFKA_SUBSCRIBER_METADATA,
      TestService,
    );
    expect(metadata[0].topics).toEqual(["topic.a", "topic.b"]);
  });
});

describe("getKafkaClientToken", () => {
  it("should return default token without name", () => {
    expect(getKafkaClientToken()).toBe(KAFKA_CLIENT);
  });

  it("should return named token", () => {
    expect(getKafkaClientToken("orders")).toBe("KAFKA_CLIENT_orders");
  });
});
