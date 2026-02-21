import "reflect-metadata";
import {
  createClient,
  waitForMessages,
  TestTopics,
  KafkaClient,
} from "./helpers";

describe("Integration — ConsumerHandle", () => {
  describe("startConsumer returns a usable handle", () => {
    it("handle.groupId matches the default group", async () => {
      const client = createClient("handle-gid");
      await client.connectProducer();

      const handle = await client.startConsumer(["test.handle"], jest.fn(), {
        fromBeginning: false,
      });

      expect(typeof handle.groupId).toBe("string");
      expect(handle.groupId.length).toBeGreaterThan(0);

      await client.disconnect();
    });

    it("handle.stop() stops the consumer without affecting the producer", async () => {
      const client = createClient("handle-stop");
      await client.connectProducer();

      const { messages, promise } =
        waitForMessages<TestTopics["test.handle"]>(1);

      const handle = await client.startConsumer(
        ["test.handle"],
        async (envelope) => {
          messages.push(envelope.payload);
        },
        { fromBeginning: true },
      );

      await client.sendMessage("test.handle", { seq: 1 });
      await promise;

      // Stop only this consumer — should not throw
      await expect(handle.stop()).resolves.toBeUndefined();

      // Producer still works after consumer is stopped
      await expect(
        client.sendMessage("test.handle", { seq: 2 }),
      ).resolves.toBeUndefined();

      await client.disconnect();
    });

    it("stop() can be called multiple times safely (second call warns, does not throw)", async () => {
      const client = createClient("handle-double-stop");
      await client.connectProducer();

      const handle = await client.startConsumer(["test.handle"], jest.fn());

      await handle.stop(); // first stop — removes the consumer

      // Second stop on a gone consumer should warn but not throw
      await expect(handle.stop()).resolves.toBeUndefined();

      await client.disconnect();
    });
  });

  describe("startBatchConsumer returns a usable handle", () => {
    it("handle.stop() stops the batch consumer", async () => {
      const client = createClient("batch-handle");
      await client.connectProducer();

      const handle = await client.startBatchConsumer(
        ["test.handle"],
        jest.fn().mockResolvedValue(undefined),
        { fromBeginning: false },
      );

      expect(typeof handle.groupId).toBe("string");
      await expect(handle.stop()).resolves.toBeUndefined();

      await client.disconnect();
    });
  });
});
