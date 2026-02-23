import { Injectable } from "@nestjs/common";
import type { IKafkaClient, KafkaHealthResult, TopicMapConstraint } from "../client/types";
export type { KafkaHealthResult } from "../client/types";

/** Health check service. Call `check(client)` to verify broker connectivity. */
@Injectable()
export class KafkaHealthIndicator {
  async check<T extends TopicMapConstraint<T>>(
    client: IKafkaClient<T>,
  ): Promise<KafkaHealthResult> {
    return client.checkStatus();
  }
}
