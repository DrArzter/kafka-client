import { Injectable } from "@nestjs/common";
import { KafkaClient, TopicMapConstraint } from "../client/kafka.client";
export type { KafkaHealthResult } from "../client/types";

/** Health check service. Call `check(client)` to verify broker connectivity. */
@Injectable()
export class KafkaHealthIndicator {
  async check<T extends TopicMapConstraint<T>>(
    client: KafkaClient<T>,
  ): Promise<import("../client/types").KafkaHealthResult> {
    return client.checkStatus();
  }
}
