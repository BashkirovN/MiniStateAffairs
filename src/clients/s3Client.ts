import {
  S3Client,
  PutObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand
} from "@aws-sdk/client-s3";
import { loadConfig } from "../config/env";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Upload } from "@aws-sdk/lib-storage";
import { PassThrough } from "stream";
import { State, VideoSource } from "../db/types";
import { spawn } from "child_process";

const config = loadConfig();

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
  body: Buffer | Uint8Array | Blob | string | ReadableStream<any>;
  contentType?: string;
}

/**
 * Uploads an object to S3. Idempotent if you always use the same key.
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
 * Checks if an object exists in S3.
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
  } catch (err: any) {
    if (err?.$metadata?.httpStatusCode === 404) {
      return false;
    }
    throw err;
  }
}

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
 * Builds a structured S3 key for video storage.
 * Example: videos/MI/house/2025/12/mi-house-agri-111325-2025-12-23.mp4
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

  if (await objectExists(key, bucket)) {
    console.log(`[${slug}] Skip: Video already exists at ${key}`);
    return buildPublicS3Url(key, bucket, config.awsRegion);
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
  const refererUrl =
    source === VideoSource.SENATE
      ? "https://cloud.castus.tv/"
      : "https://www.house.mi.gov/";

  // const args = [
  //   "--no-playlist",
  //   "-f",
  //   "b",
  //   "--merge-output-format",
  //   "mp4",
  //   "--output",
  //   "-",
  //   "--user-agent",
  //   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
  //   "--referer",
  //   refererUrl,
  //   "--add-header",
  //   "Origin:https://cloud.castus.tv",
  //   "--add-header",
  //   "Sec-Fetch-Mode:cors",
  //   "--add-header",
  //   "Sec-Fetch-Site:cross-site",

  //   // 3. Network & Stability
  //   "--socket-timeout",
  //   "30",
  //   "--fragment-retries",
  //   "10",
  //   "--hls-prefer-native",

  //   // 4. Fragment Passing
  //   "--legacy-server-connect",

  //   originalVideoUrl
  // ];

  const args = [
    "--no-playlist",
    "--output",
    "-",
    "--user-agent",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "--add-header",
    `Referer:${refererUrl}`,
    "--add-header",
    "Origin:https://cloud.castus.tv",
    "--add-header",
    "DNT:1",
    "--add-header",
    "Sec-Fetch-Mode:cors",
    "--add-header",
    "Sec-Fetch-Site:cross-site",
    "--socket-timeout",
    "30",
    "--fragment-retries",
    "10",
    "--hls-prefer-native",
    "--legacy-server-connect",
    "--no-check-certificate",
    originalVideoUrl
  ];

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
    if (Math.floor(Number(mb)) % 50 === 0)
      console.log(`[${slug}] S3 Uploaded: ${mb} MB`);
  });

  try {
    const processExitPromise = new Promise((resolve, reject) => {
      ytDlpProcess.on("close", (code) => {
        const MIN_VIDEO_SIZE = 5 * 1024 * 1024; // 5MB threshold

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

      ytDlpProcess.on("error", (err) => {
        reject(new Error(`Failed to start yt-dlp: ${err.message}`));
      });
    });

    await Promise.all([upload.done(), processExitPromise]);

    console.log(
      `[${slug}] Successfully uploaded ${(totalBytes / 1024 / 1024).toFixed(
        2
      )} MB`
    );
    return buildPublicS3Url(key, bucket, config.awsRegion);
  } catch (err: any) {
    // Tell the S3 uploader to stop any pending part uploads immediately
    upload.abort();
    ytDlpProcess.kill("SIGKILL");

    console.error(`[${slug}] Pipeline Failed: ${err.message}`);

    // Clean up the S3 object if it was a partial/corrupt upload
    try {
      await s3Client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: key })
      );
      console.log(`[${slug}] Cleaned up failed S3 object.`);
    } catch (s3Err) {
      // Ignore if file doesn't exist
    }

    throw err;
  }
}
