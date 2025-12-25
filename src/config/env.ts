import "dotenv/config";

export interface AppConfig {
  databaseUrl: string;
  awsRegion: string;
  s3Bucket: string;
  deepgramApiKey: string;
}

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
