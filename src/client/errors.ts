/** Error thrown when a consumer message handler fails. */
export class KafkaProcessingError extends Error {
  declare readonly cause?: Error;

  constructor(
    message: string,
    public readonly topic: string,
    public readonly originalMessage: unknown,
    options?: { cause?: Error },
  ) {
    super(message, options);
    this.name = "KafkaProcessingError";
    if (options?.cause) this.cause = options.cause;
  }
}

/** Error thrown when schema validation fails on send or consume. */
export class KafkaValidationError extends Error {
  declare readonly cause?: Error;

  constructor(
    public readonly topic: string,
    public readonly originalMessage: unknown,
    options?: { cause?: Error },
  ) {
    super(`Schema validation failed for topic "${topic}"`, options);
    this.name = "KafkaValidationError";
    if (options?.cause) this.cause = options.cause;
  }
}

/** Error thrown when all retry attempts are exhausted for a message. */
export class KafkaRetryExhaustedError extends KafkaProcessingError {
  constructor(
    topic: string,
    originalMessage: unknown,
    public readonly attempts: number,
    options?: { cause?: Error },
  ) {
    super(
      `Message processing failed after ${attempts} attempts on topic "${topic}"`,
      topic,
      originalMessage,
      options,
    );
    this.name = "KafkaRetryExhaustedError";
  }
}
