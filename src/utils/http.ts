import { retryWithBackoff, RetryOptions } from "./retry";

export interface FetchWithRetryOptions extends RetryOptions {
  acceptableStatus?: (status: number) => boolean;
}

/**
 * Fetch with retries on network errors and 5xx responses.
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
