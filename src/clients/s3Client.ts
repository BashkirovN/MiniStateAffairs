import {
  S3Client,
  HeadObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  S3ServiceException
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { loadConfig } from "../config/env";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Upload } from "@aws-sdk/lib-storage";
import { PassThrough } from "stream";
import { State, VideoSource } from "../db/types";
import { spawn } from "child_process";
import { getYtDlpArgs } from "../config/yt-dlp";

const config = loadConfig();
const MIN_VIDEO_SIZE = 5 * 1024 * 1024; // Assume no video can be smaller than 5MB

export const s3Client = new S3Client({
  region: config.awsRegion,
  maxAttempts: 10, // Increase retries for transient network errors
  requestHandler: new NodeHttpHandler({
    connectionTimeout: 5_000, // 5 seconds to establish connection
    socketTimeout: 300_000 // 5 minutes of inactivity allowed before timing out
  })
});

export interface UploadParams {
  bucket?: string;
  key: string;
  body: Buffer | Uint8Array | Blob | string | ReadableStream<Uint8Array>;
  contentType?: string;
}

/**
 * Checks for the existence of an object in S3 using a metadata-only request.
 * Determines if a file is present without downloading the content.
 * @param key - The unique path identifier to check
 * @param bucket - Optional override for the target S3 bucket
 * @returns A promise resolving to true if the object exists, false if it is missing (404)
 * @throws Error if a non-404 network or permission error occurs
 */
export async function objectExists(
  key: string,
  bucket?: string
): Promise<boolean> {
  try {
    const cmd = new HeadObjectCommand({
      Bucket: bucket ?? config.s3Bucket,
      Key: key
    });
    await s3Client.send(cmd);
    return true;
  } catch (err: unknown) {
    if (err instanceof S3ServiceException) {
      if (err.$metadata?.httpStatusCode === 404) {
        return false;
      }
    }

    throw err;
  }
}

/**
 * Builds a structured, hierarchical S3 key for organized video storage.
 * Paths are grouped by state, source, and hearing date to allow for easier
 * lifecycle management and manual browsing.
 * @param state - The geographic state (e.g., 'MI')
 * @param source - The branch of government (e.g., 'house')
 * @param slug - The URL-friendly video identifier
 * @param hearingDate - The date of the hearing used for directory partitioning
 * @returns A string path in the format: videos/{state}/{source}/{year}/{month}/{slug}.mp4
 * @example
 * buildVideoObjectKey('MI', 'house', 'mi-house-agri-111325-2025-12-23', new Date('2025-12-23'))
 * // returns "videos/MI/house/2025/12/mi-house-agri-111325-2025-12-23.mp4"
 */
export function buildVideoObjectKey(
  state: State,
  source: VideoSource,
  slug: string,
  hearingDate: Date
): string {
  const year = hearingDate.getUTCFullYear();
  const month = String(hearingDate.getUTCMonth() + 1).padStart(2, "0");

  return `videos/${state}/${source}/${year}/${month}/${slug}.mp4`;
}

/**
 * Generates a pre-signed URL for a S3 object.
 * @param key - The S3 object key
 * @param expiresInSeconds - The number of seconds the URL should remain valid
 * @returns A promise resolving to the signed URL
 */
export async function getPresignedUrl(
  key: string,
  expiresInSeconds?: number
): Promise<string> {
  // Valid for 1 hour by default, plenty of time for a transcriber to process it
  const expiresIn = expiresInSeconds ?? 3600;
  const command = new GetObjectCommand({
    Bucket: config.s3Bucket,
    Key: key
  });

  return getSignedUrl(s3Client, command, { expiresIn });
}

/**
 * Pipes a live video stream from an external URL directly into an S3 object.
 * Uses a PassThrough stream to bridge a yt-dlp child process with the S3 multipart
 * uploader, ensuring the system can handle large files without high memory overhead.
 * @param input - The video source metadata and destination identifiers
 * @returns A promise resolving to a presigned URL of the finalized S3 object
 * @throws Error if the download process fails or if the resulting file size is below the 5MB safety threshold
 */
export async function uploadVideoFromUrl(input: {
  state: State;
  source: VideoSource;
  slug: string;
  originalVideoUrl: string;
  hearingDate: Date;
}): Promise<string> {
  const { state, source, slug, originalVideoUrl, hearingDate } = input;
  const key = buildVideoObjectKey(state, source, slug, hearingDate);
  const bucket = config.s3Bucket;

  // Idempotency Check
  if (await objectExists(key, bucket)) {
    console.log(`[${slug}] Skip: Video already exists at ${key}`);
    return await getPresignedUrl(key);
  }

  console.log(`[${slug}] Starting yt-dlp stream to S3...`);

  // Bridge between yt-dlp process and S3 Upload
  const passthrough = new PassThrough({ highWaterMark: 1024 * 1024 * 5 });

  // Spawn yt-dlp process with arguments
  const args = getYtDlpArgs(source, originalVideoUrl);
  const ytDlpProcess = spawn("yt-dlp", args);

  // Log yt-dlp stderr
  let stderrData = "";
  ytDlpProcess.stderr.on("data", (data) => {
    stderrData += data.toString();
  });

  // ---------------------------------------------------------
  // 🔴 SAFETY NET: Kill yt-dlp if the main Node process dies
  // ---------------------------------------------------------
  const cleanupListener = () => {
    if (ytDlpProcess && !ytDlpProcess.killed) {
      console.warn(
        `[${slug}] 🧹 Killing orphaned yt-dlp process (PID: ${ytDlpProcess.pid})...`
      );
      ytDlpProcess.kill("SIGKILL");
    }
  };

  // Attach signal listeners
  process.on("SIGTERM", cleanupListener);
  process.on("SIGINT", cleanupListener);
  process.on("exit", cleanupListener);
  // ---------------------------------------------------------

  // Start piping data
  ytDlpProcess.stdout.pipe(passthrough);

  // To avoid 0-byte files in s3 saved from broken links,
  // wait for the first chunk to arrive safely in the buffer
  try {
    await new Promise<void>((resolve, reject) => {
      const onReadable = () => {
        cleanup();
        resolve();
      };
      const onError = (err: Error) => {
        cleanup();
        reject(err);
      };
      const onClose = (code: number) => {
        cleanup();
        if (code !== 0)
          reject(new Error(`Process exited early (code ${code})`));
        else reject(new Error("Process exited without sending data"));
      };

      // Helper to remove listeners so we don't leak memory
      const cleanup = () => {
        passthrough.off("readable", onReadable);
        ytDlpProcess.off("error", onError);
        ytDlpProcess.off("close", onClose);
      };

      // LISTENERS
      // 'readable' means "I have data waiting for you".
      // It does NOT drain the data like 'data' does.
      passthrough.once("readable", onReadable);
      ytDlpProcess.once("error", onError);
      ytDlpProcess.once("close", onClose);

      // Timeout safety
      setTimeout(() => {
        cleanup();
        reject(new Error("Stream timeout"));
      }, 30_000);
    });
  } catch (err) {
    // If startup failed, clean global listeners
    process.off("SIGTERM", cleanupListener);
    process.off("SIGINT", cleanupListener);
    process.off("exit", cleanupListener);
    throw err;
  }

  // Setup S3 Upload
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: bucket,
      Key: key,
      Body: passthrough, // The passthrough has been buffering the first chunk
      ContentType: "video/mp4" // Can also experiment with video/mpeg
    },
    partSize: 1024 * 1024 * 5,
    queueSize: 1, // Can't multi-stream, the connection is too brittle
    leavePartsOnError: false
  });

  // Track progress
  let totalBytes = 0;
  upload.on("httpUploadProgress", (p) => {
    totalBytes = p.loaded || 0;

    const mb = (totalBytes / 1024 / 1024).toFixed(2);
    if (Number(mb) > 0 && Math.floor(Number(mb)) % 50 === 0)
      console.log(`[${slug}] S3 Uploaded: ${mb} MB`);
  });

  try {
    const processExitPromise = new Promise((resolve, reject) => {
      ytDlpProcess.on("close", (code) => {
        // CLEANUP: Remove listeners so we don't leak memory or kill future processes
        process.off("SIGTERM", cleanupListener);
        process.off("SIGINT", cleanupListener);
        process.off("exit", cleanupListener);

        // Post-download Validation
        if (code !== 0) {
          reject(new Error(`yt-dlp failed (code ${code}): ${stderrData}`));
        } else if (totalBytes < MIN_VIDEO_SIZE) {
          reject(
            new Error(
              `Stream finished but file too small (${(
                totalBytes /
                1024 /
                1024
              ).toFixed(2)} MB). Likely a 403 or error page.`
            )
          );
        } else {
          resolve(true);
        }
      });

      ytDlpProcess.on("error", () => {
        reject(new Error(`Failed to start yt-dlp`));
      });
    });

    await Promise.all([upload.done(), processExitPromise]);

    console.log(
      `[${slug}] Successfully uploaded ${(totalBytes / 1024 / 1024).toFixed(
        2
      )} MB`
    );
    return await getPresignedUrl(key);
  } catch (err: unknown) {
    // Tell the S3 uploader to stop any pending part uploads immediately
    ytDlpProcess.kill("SIGKILL");

    try {
      await upload.abort();
    } catch {
      // Ignore if file doesn't exist
    }

    console.error(`[${slug}] Uploading to S3 Failed`);

    // Clean up the S3 object if it was a partial/corrupt upload
    // A small delay or ensure it runs after the abort
    try {
      await s3Client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: key })
      );
      console.log(`[${slug}] Cleaned up failed S3 object.`);
    } catch {
      // Ignore if file doesn't exist
    }

    const errorMessage =
      err instanceof Error ? err.message : JSON.stringify(err);

    const richError = new Error(`[Upload Failed] ${errorMessage}`, {
      cause: err
    });

    // CLEANUP: Remove listeners so we don't leak memory or kill future processes
    process.off("SIGTERM", cleanupListener);
    process.off("SIGINT", cleanupListener);
    process.off("exit", cleanupListener);

    throw richError;
  }
}
