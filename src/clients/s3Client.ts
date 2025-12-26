import {
  S3Client,
  PutObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  S3ServiceException
} from "@aws-sdk/client-s3";
import { loadConfig } from "../config/env";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Upload } from "@aws-sdk/lib-storage";
import { PassThrough } from "stream";
import { State, VideoSource } from "../db/types";
import { spawn } from "child_process";
import { getYtDlpArgs } from "../config/yt-dlp";
import { fetchWithRetry } from "../utils/http";

const config = loadConfig();
const MIN_VIDEO_SIZE = 5 * 1024 * 1024; // Assume no video can be smaller than 5MB

export const s3Client = new S3Client({
  region: config.awsRegion,
  maxAttempts: 30, // Increase retries for transient network errors
  requestHandler: new NodeHttpHandler({
    connectionTimeout: 30000, // 30 seconds to establish connection
    socketTimeout: 600000 // 10 minutes of inactivity allowed before timing out
  })
});

export interface UploadParams {
  bucket?: string;
  key: string;
  body: Buffer | Uint8Array | Blob | string | ReadableStream<Uint8Array>;
  contentType?: string;
}

/**
 * Uploads an arbitrary data object to an S3 bucket.
 * This operation is idempotent; providing an existing key will overwrite the current content.
 * @param params - The payload and destination metadata
 * @param params.bucket - Optional override for the target S3 bucket
 * @param params.key - The unique path identifier within the bucket
 * @param params.body - The data to store (Buffer, Stream, or String)
 * @param params.contentType - The MIME type of the file (e.g., 'application/pdf')
 * @returns A promise that resolves when the upload is successfully acknowledged
 */
export async function uploadObject({
  bucket,
  key,
  body,
  contentType
}: UploadParams): Promise<void> {
  const command = new PutObjectCommand({
    Bucket: bucket ?? config.s3Bucket,
    Key: key,
    Body: body,
    ContentType: contentType
  });

  await s3Client.send(command);
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
 * Generates a publicly accessible HTTPS URL for an S3 object.
 * Note: This assumes the bucket/object permissions are set to public-read.
 * @param key - The S3 object key
 * @param bucket - The name of the S3 bucket
 * @param region - The AWS region where the bucket resides
 * @returns A formatted string URL for direct media access
 */
export function buildPublicS3Url(
  key: string,
  bucket: string,
  region: string
): string {
  return `https://${bucket}.s3.${region}.amazonaws.com/${encodeURIComponent(
    key
  )}`;
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
 * Pipes a live video stream from an external URL directly into an S3 object.
 * Uses a PassThrough stream to bridge a yt-dlp child process with the S3 multipart
 * uploader, ensuring the system can handle large files without high memory overhead.
 * @param input - The video source metadata and destination identifiers
 * @returns A promise resolving to the public URL of the finalized S3 object
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
    return buildPublicS3Url(key, bucket, config.awsRegion);
  }

  // Pre-flight Check: Verify the URL is reachable
  try {
    const headRes = await fetchWithRetry(originalVideoUrl, { method: "HEAD" });
    if (!headRes.ok) {
      throw new Error(
        `Pre-flight check failed: Source returned ${headRes.status}`
      );
    }
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(
      `[${slug}] Pre-flight check failed. URL unreachable: ${message}`
    );
  }

  console.log(`[${slug}] Starting yt-dlp stream to S3...`);

  // Bridge between yt-dlp process and S3 Upload
  const passthrough = new PassThrough({ highWaterMark: 1024 * 1024 * 5 });

  // Track actual bytes received to prevent saving empty/error files
  let totalBytes = 0;
  passthrough.on("data", (chunk) => {
    totalBytes += chunk.length;
  });

  // yt-dlp arguments
  const args = getYtDlpArgs(source, originalVideoUrl);

  const ytDlpProcess = spawn("yt-dlp", args);
  let stderrData = "";

  ytDlpProcess.stderr.on("data", (data) => {
    stderrData += data.toString();
  });

  ytDlpProcess.stdout.pipe(passthrough);

  // Setup S3 Upload
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: bucket,
      Key: key,
      Body: passthrough,
      ContentType: "video/mp4"
    },
    partSize: 1024 * 1024 * 5,
    queueSize: 1, // Can't multi-stream, the connection is too brittle
    leavePartsOnError: false
  });

  // Track progress
  upload.on("httpUploadProgress", (p) => {
    const mb = (p.loaded! / 1024 / 1024).toFixed(2);
    if (Number(mb) > 0 && Math.floor(Number(mb)) % 50 === 0)
      console.log(`[${slug}] S3 Uploaded: ${mb} MB`);
  });

  try {
    const processExitPromise = new Promise((resolve, reject) => {
      ytDlpProcess.on("close", (code) => {
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
    return buildPublicS3Url(key, bucket, config.awsRegion);
  } catch (err: unknown) {
    // Tell the S3 uploader to stop any pending part uploads immediately
    upload.abort();
    ytDlpProcess.kill("SIGKILL");

    console.error(`[${slug}] Uploading to S3 Failed`);

    // Clean up the S3 object if it was a partial/corrupt upload
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

    throw richError;
  }
}
