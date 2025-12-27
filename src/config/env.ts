import "dotenv/config";

export const DEEPGRAM_MODEL = "nova-3";

export interface AppConfig {
  databaseUrl: string;
  awsRegion: string;
  s3Bucket: string;
  deepgramApiKey: string;
}

/**
 * Validates and loads application environment variables into a structured config object.
 * Performs a strict presence check for critical infrastructure keys including
 * database credentials, AWS settings, and transcription API keys.
 * @returns A validated AppConfig object containing all required service credentials
 * @throws Error if any required environment variable is missing from process.env
 */
export function loadConfig(): AppConfig {
  const { DATABASE_URL, AWS_REGION, S3_BUCKET, DEEPGRAM_API_KEY } = process.env;

  if (!DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!AWS_REGION) throw new Error("AWS_REGION is required");
  if (!S3_BUCKET) throw new Error("S3_BUCKET is required");
  if (!DEEPGRAM_API_KEY) throw new Error("DEEPGRAM_API_KEY is required");

  return {
    databaseUrl: DATABASE_URL,
    awsRegion: AWS_REGION,
    s3Bucket: S3_BUCKET,
    deepgramApiKey: DEEPGRAM_API_KEY
  };
}
