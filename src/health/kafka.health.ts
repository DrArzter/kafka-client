import { Injectable } from "@nestjs/common";
import { KafkaClient, TTopicMessageMap } from "../client/kafka.client";

/** Result returned by `KafkaHealthIndicator.check()`. */
export interface KafkaHealthResult {
  status: "up" | "down";
  clientId: string;
  topics?: string[];
  error?: string;
}

/** Health check service. Call `check(client)` to verify broker connectivity. */
@Injectable()
export class KafkaHealthIndicator {
  async check<T extends TTopicMessageMap>(
    client: KafkaClient<T>,
  ): Promise<KafkaHealthResult> {
    try {
      const { topics } = await client.checkStatus();
      return {
        status: "up",
        clientId: client.clientId,
        topics,
      };
    } catch (error) {
      return {
        status: "down",
        clientId: client.clientId,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }
}
