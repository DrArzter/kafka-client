jest.mock("@confluentinc/kafka-javascript");

import { TestTopicMap, createClient, mockAdmin, KafkaClient } from "./helpers";
import {
  mockFetchOffsets,
  mockFetchTopicOffsets,
} from "../../__mocks__/@confluentinc/kafka-javascript";

describe("KafkaClient — getConsumerLag", () => {
  let client: KafkaClient<TestTopicMap>;

  beforeEach(() => {
    jest.clearAllMocks();
    client = createClient();
  });

  describe("lag calculation", () => {
    it("returns lag = high − committed when consumer is behind", async () => {
      mockFetchOffsets.mockResolvedValueOnce([
        { topic: "test.topic", partitions: [{ partition: 0, offset: "3" }] },
      ]);
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, offset: "3", high: "10", low: "0" },
      ]);

      const lag = await client.getConsumerLag();

      expect(lag).toEqual([{ topic: "test.topic", partition: 0, lag: 7 }]);
    });

    it("returns lag = 0 when committed offset equals high watermark", async () => {
      mockFetchOffsets.mockResolvedValueOnce([
        { topic: "test.topic", partitions: [{ partition: 0, offset: "10" }] },
      ]);
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, offset: "10", high: "10", low: "0" },
      ]);

      const lag = await client.getConsumerLag();

      expect(lag).toEqual([{ topic: "test.topic", partition: 0, lag: 0 }]);
    });

    it("treats committed offset -1 (never committed) as full lag", async () => {
      mockFetchOffsets.mockResolvedValueOnce([
        { topic: "test.topic", partitions: [{ partition: 0, offset: "-1" }] },
      ]);
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, offset: "0", high: "5", low: "0" },
      ]);

      const lag = await client.getConsumerLag();

      expect(lag).toEqual([{ topic: "test.topic", partition: 0, lag: 5 }]);
    });

    it("clamps lag to 0 when committed is ahead of high (stale metadata edge case)", async () => {
      mockFetchOffsets.mockResolvedValueOnce([
        { topic: "test.topic", partitions: [{ partition: 0, offset: "20" }] },
      ]);
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, offset: "20", high: "10", low: "0" },
      ]);

      const lag = await client.getConsumerLag();

      expect(lag[0].lag).toBe(0);
    });
  });

  describe("multi-topic and multi-partition", () => {
    it("aggregates lag across multiple partitions on the same topic", async () => {
      mockFetchOffsets.mockResolvedValueOnce([
        {
          topic: "test.topic",
          partitions: [
            { partition: 0, offset: "2" },
            { partition: 1, offset: "8" },
          ],
        },
      ]);
      mockFetchTopicOffsets.mockResolvedValueOnce([
        { partition: 0, offset: "2", high: "10", low: "0" },
        { partition: 1, offset: "8", high: "10", low: "0" },
      ]);

      const lag = await client.getConsumerLag();

      expect(lag).toEqual([
        { topic: "test.topic", partition: 0, lag: 8 },
        { topic: "test.topic", partition: 1, lag: 2 },
      ]);
    });

    it("queries each topic separately and returns all results", async () => {
      mockFetchOffsets.mockResolvedValueOnce([
        { topic: "test.topic", partitions: [{ partition: 0, offset: "5" }] },
        { topic: "test.other", partitions: [{ partition: 0, offset: "1" }] },
      ]);
      mockFetchTopicOffsets
        .mockResolvedValueOnce([
          { partition: 0, offset: "5", high: "10", low: "0" },
        ])
        .mockResolvedValueOnce([
          { partition: 0, offset: "1", high: "4", low: "0" },
        ]);

      const lag = await client.getConsumerLag();

      expect(lag).toEqual([
        { topic: "test.topic", partition: 0, lag: 5 },
        { topic: "test.other", partition: 0, lag: 3 },
      ]);
      expect(mockFetchTopicOffsets).toHaveBeenCalledTimes(2);
    });

    it("returns empty array when group has no committed offsets", async () => {
      mockFetchOffsets.mockResolvedValueOnce([]);

      const lag = await client.getConsumerLag();

      expect(lag).toEqual([]);
    });
  });

  describe("groupId resolution", () => {
    it("uses the default groupId from constructor when none is provided", async () => {
      mockFetchOffsets.mockResolvedValueOnce([]);

      await client.getConsumerLag();

      expect(mockFetchOffsets).toHaveBeenCalledWith(
        expect.objectContaining({ groupId: "test-group" }),
      );
    });

    it("uses the provided groupId when specified", async () => {
      mockFetchOffsets.mockResolvedValueOnce([]);

      await client.getConsumerLag("another-group");

      expect(mockFetchOffsets).toHaveBeenCalledWith(
        expect.objectContaining({ groupId: "another-group" }),
      );
    });
  });

  describe("admin connection lifecycle", () => {
    it("connects admin on the first call", async () => {
      mockFetchOffsets.mockResolvedValueOnce([]);

      await client.getConsumerLag();

      expect(mockAdmin.connect).toHaveBeenCalledTimes(1);
    });

    it("reuses the existing admin connection on subsequent calls", async () => {
      mockFetchOffsets.mockResolvedValue([]);

      await client.getConsumerLag();
      await client.getConsumerLag();

      expect(mockAdmin.connect).toHaveBeenCalledTimes(1);
    });

    it("shares the admin connection with checkStatus", async () => {
      mockFetchOffsets.mockResolvedValueOnce([]);

      await client.checkStatus(); // establishes connection
      await client.getConsumerLag(); // should reuse it

      expect(mockAdmin.connect).toHaveBeenCalledTimes(1);
    });
  });
});
