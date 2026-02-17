import {
  KafkaProcessingError,
  KafkaRetryExhaustedError,
  KafkaValidationError,
} from "../errors";

describe("KafkaProcessingError", () => {
  it("should store topic and originalMessage", () => {
    const err = new KafkaProcessingError("fail", "my.topic", { id: 1 });
    expect(err.name).toBe("KafkaProcessingError");
    expect(err.topic).toBe("my.topic");
    expect(err.originalMessage).toEqual({ id: 1 });
    expect(err.message).toBe("fail");
  });

  it("should support cause", () => {
    const cause = new Error("root cause");
    const err = new KafkaProcessingError("fail", "t", {}, { cause });
    expect(err.cause).toBe(cause);
  });
});

describe("KafkaValidationError", () => {
  it("should store topic and originalMessage", () => {
    const err = new KafkaValidationError("my.topic", { bad: "data" });
    expect(err.name).toBe("KafkaValidationError");
    expect(err.topic).toBe("my.topic");
    expect(err.originalMessage).toEqual({ bad: "data" });
    expect(err.message).toContain("my.topic");
  });

  it("should support cause", () => {
    const cause = new Error("zod error");
    const err = new KafkaValidationError("t", {}, { cause });
    expect(err.cause).toBe(cause);
  });

  it("should be instanceof Error", () => {
    const err = new KafkaValidationError("t", {});
    expect(err).toBeInstanceOf(Error);
  });
});

describe("KafkaRetryExhaustedError", () => {
  it("should extend KafkaProcessingError", () => {
    const err = new KafkaRetryExhaustedError("my.topic", { id: 1 }, 3);
    expect(err).toBeInstanceOf(KafkaProcessingError);
    expect(err).toBeInstanceOf(Error);
  });

  it("should store attempts and build message", () => {
    const err = new KafkaRetryExhaustedError("my.topic", { id: 1 }, 3);
    expect(err.name).toBe("KafkaRetryExhaustedError");
    expect(err.attempts).toBe(3);
    expect(err.topic).toBe("my.topic");
    expect(err.message).toContain("3 attempts");
    expect(err.message).toContain("my.topic");
  });

  it("should support cause", () => {
    const cause = new Error("handler failed");
    const err = new KafkaRetryExhaustedError("t", {}, 2, { cause });
    expect(err.cause).toBe(cause);
  });
});
