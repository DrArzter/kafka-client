/**
 * Coerce an unknown thrown value to an `Error` instance.
 * Returns the value as-is if it is already an `Error`; otherwise wraps it with `String(error)`.
 * @param error The value caught in a `catch` clause.
 * @returns A guaranteed `Error` instance.
 */
export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Error thrown when a consumer message handler fails.
 * @example
 * ```ts
 * await kafka.startConsumer(['orders'], async (envelope) => {
 *   try { await process(envelope); }
 *   catch (err) {
 *     if (err instanceof KafkaProcessingError) {
 *       console.error(err.topic, err.originalMessage);
 *     }
 *   }
 * });
 * ```
 */
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

/**
 * Error thrown when schema validation fails on send or consume.
 * @example
 * ```ts
 * try { await kafka.sendMessage('orders.created', invalidPayload); }
 * catch (err) {
 *   if (err instanceof KafkaValidationError) {
 *     console.error('Validation failed for topic:', err.topic);
 *   }
 * }
 * ```
 */
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

/**
 * Error thrown when all retry attempts are exhausted for a message.
 * @example
 * ```ts
 * const kafka = new KafkaClient(config, groupId, { onMessageLost: (ctx) => {
 *   if (ctx.error instanceof KafkaRetryExhaustedError) {
 *     console.error(`Exhausted after ${ctx.error.attempts} attempts on ${ctx.error.topic}`);
 *   }
 * }});
 * ```
 */
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
