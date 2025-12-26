import { retryWithBackoff, RetryOptions } from "./retry";

export interface FetchWithRetryOptions extends RetryOptions {
  acceptableStatus?: (status: number) => boolean;
}

/**
 * Wraps the native fetch API with automated retry logic for transient failures.
 * Specifically targets 5xx server errors and network-level exceptions for retries,
 * while allowing for custom success-condition overrides via acceptableStatus.
 * * @param url - The destination endpoint
 * @param init - Standard RequestInit options (headers, method, body, etc.)
 * @param options - Configuration for retry behavior including acceptable status codes
 * @returns A promise resolving to the successful Response object
 * * @example
 * await fetchWithRetry('https://api.gov/data', { method: 'GET' }, { maxAttempts: 3 })
 */
export async function fetchWithRetry(
  url: string,
  init?: RequestInit,
  options: FetchWithRetryOptions = {}
): Promise<Response> {
  const {
    acceptableStatus = (status) => status >= 200 && status < 300,
    maxAttempts,
    baseDelayMs,
    maxDelayMs
  } = options;

  return retryWithBackoff(
    async () => {
      const res = await fetch(url, init);

      if (!acceptableStatus(res.status)) {
        // Retry on 5xx, fail fast on 4xx
        if (res.status >= 500 && res.status < 600) {
          throw new Error(`Server error ${res.status} for ${url}`);
        }

        throw new Error(`Non-retryable HTTP error ${res.status} for ${url}`);
      }

      return res;
    },
    { maxAttempts, baseDelayMs, maxDelayMs }
  );
}
