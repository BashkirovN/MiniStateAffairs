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
  } catch {
    throw new Error(`CRITICAL: Database unreachable. Stopping job.`);
  }

  // --- CRITICAL: Database Schema Integrity ---
  try {
    await query("SELECT 1 FROM videos LIMIT 1");
    await query("SELECT 1 FROM transcripts LIMIT 1");
    await query("SELECT 1 FROM job_runs LIMIT 1");
    await query("SELECT 1 FROM job_logs LIMIT 1");
    console.log(`    ✅ Database: Schema OK (Tables exist)`);
  } catch {
    throw new Error(
      `CRITICAL: Database connected, but schema is missing. Did you run 'schema.sql'?`
    );
  }

  // --- CRITICAL: AWS S3 ---
  try {
    await s3Client.send(new ListBucketsCommand({}));
    console.log(`    ✅ AWS S3: Reachable`);
  } catch {
    throw new Error(`CRITICAL: AWS S3 unreachable. Stopping job.`);
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
