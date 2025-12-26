export interface RetryOptions {
  maxAttempts?: number; // total attempts including the first
  baseDelayMs?: number; // starting delay
  maxDelayMs?: number; // upper bound on delay
}

/**
 * Executes an asynchronous function with an exponential backoff strategy and randomized jitter.
 * Designed to mitigate "thundering herd" issues by increasing the delay between
 * successive attempts up to a specified maximum.
 * *
 * * @param fn - The asynchronous function or API call to execute
 * @param options - Configuration for max attempts and delay timing (base and cap)
 * @returns The resolved value of the provided function `fn`
 * @throws The final error encountered if the maximum number of attempts is exhausted
 */
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const { maxAttempts = 5, baseDelayMs = 500, maxDelayMs = 10_000 } = options;

  let attempt = 1;
  // small jitter to avoid thundering herd
  const jitter = () => Math.random() * 100;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= maxAttempts) {
        throw err;
      }

      const delay =
        Math.min(baseDelayMs * 2 ** (attempt - 1), maxDelayMs) + jitter();

      console.warn(
        `Retryable error on attempt ${attempt}/${maxAttempts}, retrying in ${Math.round(
          delay
        )}ms`,
        err
      );

      await new Promise((resolve) => setTimeout(resolve, delay));
      attempt += 1;
    }
  }
}
