import {
  TestTopicMap,
  createClient,
  envelopeWith,
  setupMessage,
  mockRun,
  KafkaClient,
} from "./helpers";

describe("KafkaClient — Interceptors", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("interceptors", () => {
    it("should call before and after with envelope on success", async () => {
      const before = jest.fn();
      const after = jest.fn();
      const handler = jest.fn().mockResolvedValue(undefined);

      setupMessage();

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [{ before, after }],
      });

      expect(before).toHaveBeenCalledWith(envelopeWith({ id: "1", value: 1 }));
      expect(handler).toHaveBeenCalled();
      expect(after).toHaveBeenCalledWith(envelopeWith({ id: "1", value: 1 }));
    });

    it("should call onError with envelope and error when handler fails", async () => {
      const onError = jest.fn();
      const handler = jest.fn().mockRejectedValue(new Error("boom"));

      setupMessage();

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [{ onError }],
      });

      expect(onError).toHaveBeenCalledWith(
        envelopeWith({ id: "1", value: 1 }),
        expect.any(Error),
      );
    });

    it("should call multiple interceptors in order", async () => {
      const calls: string[] = [];
      const handler = jest.fn().mockResolvedValue(undefined);

      setupMessage();

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          {
            before: () => {
              calls.push("a-before");
            },
            after: () => {
              calls.push("a-after");
            },
          },
          {
            before: () => {
              calls.push("b-before");
            },
            after: () => {
              calls.push("b-after");
            },
          },
        ],
      });

      expect(calls).toEqual(["a-before", "b-before", "a-after", "b-after"]);
    });
  });

  describe("interceptor error handling", () => {
    it("should propagate error when before interceptor throws", async () => {
      setupMessage();
      const handler = jest.fn().mockResolvedValue(undefined);
      const onError = jest.fn();

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          {
            before: () => {
              throw new Error("before failed");
            },
            onError,
          },
        ],
      });

      expect(handler).not.toHaveBeenCalled();
      expect(onError).toHaveBeenCalledWith(
        envelopeWith({ id: "1", value: 1 }),
        expect.objectContaining({ message: "before failed" }),
      );
    });

    it("should propagate error when after interceptor throws", async () => {
      setupMessage();
      const handler = jest.fn().mockResolvedValue(undefined);
      const onError = jest.fn();

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          {
            after: () => {
              throw new Error("after failed");
            },
            onError,
          },
        ],
      });

      expect(handler).toHaveBeenCalled();
      expect(onError).toHaveBeenCalledWith(
        envelopeWith({ id: "1", value: 1 }),
        expect.objectContaining({ message: "after failed" }),
      );
    });

    it("should not throw if onError interceptor throws", async () => {
      setupMessage();
      const handler = jest.fn().mockRejectedValue(new Error("fail"));

      // onError itself throws — should not bubble up (logged but swallowed by consumer loop)
      await expect(
        client.startConsumer(["test.topic"], handler, {
          interceptors: [
            {
              onError: () => {
                throw new Error("onError failed");
              },
            },
          ],
        }),
      ).rejects.toThrow("onError failed");
    });

    it("should not call second before if first before throws", async () => {
      setupMessage();
      const secondBefore = jest.fn();
      const handler = jest.fn().mockResolvedValue(undefined);

      await client.startConsumer(["test.topic"], handler, {
        interceptors: [
          {
            before: () => {
              throw new Error("stop");
            },
          },
          { before: secondBefore },
        ],
      });

      expect(secondBefore).not.toHaveBeenCalled();
      expect(handler).not.toHaveBeenCalled();
    });
  });
});
