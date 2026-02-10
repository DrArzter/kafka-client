/** Default DI token for the Kafka client. */
export const KAFKA_CLIENT = "KAFKA_CLIENT";

/** Returns the DI token for a named (or default) Kafka client instance. */
export const getKafkaClientToken = (name?: string): string =>
  name ? `KAFKA_CLIENT_${name}` : KAFKA_CLIENT;
