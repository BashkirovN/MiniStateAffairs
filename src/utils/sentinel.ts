import { deepgramClient } from "../clients/deepgramClient";
import { s3Client } from "../clients/s3Client";
import { query } from "../db/client";
import { ListBucketsCommand } from "@aws-sdk/client-s3";

/**
 * Verifies that all external infrastructure (DB, S3) is reachable.
 * Run this at the start of any scheduled job.
 * @throws Error if any of the checks fail
 */
export async function runSentinelCheck(): Promise<void> {
  console.log(
    `[${new Date().toLocaleTimeString()}] 🛡️ Running Sentinel Infrastructure Check...`
  );

  // --- CRITICAL: Database ---
  try {
    await query("SELECT 1");
    console.log(`    ✅ Database: Reachable`);
  } catch (err: unknown) {
    const errorMessage =
      err instanceof Error ? err.message : JSON.stringify(err);

    throw new Error(
      `CRITICAL: Database unreachable. Stopping job. (${errorMessage})`
    );
  }

  // --- CRITICAL: AWS S3 ---
  try {
    await s3Client.send(new ListBucketsCommand({}));
    console.log(`    ✅ AWS S3: Reachable`);
  } catch (err: unknown) {
    const errorMessage =
      err instanceof Error ? err.message : JSON.stringify(err);

    throw new Error(
      `CRITICAL: AWS S3 unreachable. Stopping job. (${errorMessage})`
    );
  }

  // --- NON-CRITICAL: Deepgram ---
  try {
    // Attempt to fetch project info to verify API Key
    const { error } = await deepgramClient.manage.getProjects();
    if (error) throw error;

    console.log(`    ✅ Deepgram: Reachable`);
  } catch (err: unknown) {
    const errorMessage =
      err instanceof Error ? err.message : JSON.stringify(err);

    console.warn("    ⚠️  WARNING: Deepgram health check failed.");
    console.warn(
      "   Scraping and Downloads will proceed, but transcriptions may fail."
    );
    console.warn(`   Reason: ${errorMessage}`);
    // We do NOT throw here. The job continues.
  }
}
