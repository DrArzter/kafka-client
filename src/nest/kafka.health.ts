import { Injectable } from "@nestjs/common";
import { KafkaClient, TopicMapConstraint } from "../client/kafka.client";

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
  async check<T extends TopicMapConstraint<T>>(
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
