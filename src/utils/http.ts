import { retryWithBackoff, RetryOptions } from "./retry";

export interface FetchWithRetryOptions extends RetryOptions {
  acceptableStatus?: (status: number) => boolean;
}

interface HttpErrorProbe {
  status?: number;
  $metadata?: {
    httpStatusCode?: number;
  };
}

/**
 * Determines if an error/status code is worth retrying.
 * Bridges the gap between fetch responses, SDK errors, and yt-dlp messages.
 * @param error The error object to check
 * @returns True if the error is retryable, false otherwise
 */
export function isRetryable(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);

  let statusCode = 0;

  if (typeof error === "object" && error !== null) {
    const probe = error as HttpErrorProbe;
    statusCode = probe.status ?? probe.$metadata?.httpStatusCode ?? 0;
  }

  // If no code was found on the object, try parsing the message string
  if (statusCode === 0) {
    const match = message.match(/status (\d+)/i);
    if (match) statusCode = parseInt(match[1], 10);
  }

  if (statusCode > 0) {
    // 🟢 RETRYABLE 4xx Codes:
    // 408: Request Timeout
    // 425: Too Early (Standard for replays/fast retries)
    // 429: Too Many Requests (Rate limiting)
    // 499: Client Closed Request (Common in Nginx/Pipes)
    const retryable4xx = [408, 425, 429, 499];
    if (retryable4xx.includes(statusCode)) return true;

    // 🟢 RETRYABLE 5xx Codes:
    // All 5xx are server-side and generally transient
    if (statusCode >= 500 && statusCode < 600) return true;

    // 🔴 FATAL 4xx Codes:
    // 400, 401, 403, 404, 410, etc.
    if (statusCode >= 400 && statusCode < 500) return false;
  }

  // yt-dlp / CLI Specific string matching
  // Explicitly identify terminal errors from the binary's output
  if (/HTTP Error 40(0|1|3|4|10)/i.test(message)) return false;

  // Network-level errors (Pre-HTTP)
  // These indicate the request never reached the server logic
  const networkErrors = [
    "ECONNRESET",
    "ETIMEDOUT",
    "ENOTFOUND",
    "ECONNREFUSED",
    "EAI_AGAIN", // DNS lookup temporary failure
    "fetch failed",
    "socket hang up"
  ];

  if (
    networkErrors.some((netErr) =>
      message.toUpperCase().includes(netErr.toUpperCase())
    )
  ) {
    return true;
  }

  // Default to false: If don't know what it is, don't retry.
  return false;
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
  const { maxAttempts, baseDelayMs, maxDelayMs } = options;

  return retryWithBackoff(
    async () => {
      const res = await fetch(url, init);

      if (res.ok) return res;

      if (isRetryable({ status: res.status })) {
        throw new Error(`Retryable HTTP error ${res.status} for ${url}`);
      }

      // If it's not retryable, we throw a special error or just exit
      throw new Error(`Non-retryable HTTP error ${res.status} for ${url}`);
    },
    { maxAttempts, baseDelayMs, maxDelayMs }
  );
}
