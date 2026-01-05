#!/usr/bin/env node

import { execSync } from "child_process";
import { shutdownPool } from "../db/client";
import { State, VideoSource } from "../db/types";
import { runScheduledJob } from "../jobs/orchestrator";
import { resolve } from "path";

// Listen for termination signals (e.g., from Kubernetes, Docker, or Ctrl+C)
process.on("SIGTERM", async () => {
  console.warn("\n🛑 Received SIGTERM signal. Initiating graceful shutdown...");

  // Close Database Connections
  try {
    await shutdownPool();
    console.log("   ✅ Database pool closed.");
  } catch (err) {
    console.error("   ❌ Error closing database pool:", err);
  }

  process.exit(0);
});
// -------------------------------------

async function main() {
  const args = process.argv.slice(2);
  const stateArg = args
    .find((a) => a.startsWith("--state="))
    ?.split("=")[1] as State;
  const sourceArg = args
    .find((a) => a.startsWith("--source="))
    ?.split("=")[1] as VideoSource;
  const daysArg = parseInt(
    args.find((a) => a.startsWith("--days="))?.split("=")[1] || "1"
  );

  if (!stateArg || !sourceArg) {
    console.error(
      "Usage: node ingest.js --state=MI --source=senate [--days=7]"
    );
    process.exit(1);
  }

  // Clean up any stray temp files from previous failed yt-dlp runs
  try {
    const cleanupCwd = resolve(__dirname, "..", "..");
    console.log("Cleaning up temp directory:", cleanupCwd);
    execSync("rm -f -- *--Frag* *.part *.ytdl", { cwd: cleanupCwd });
  } catch {
    // Ignore errors if no files found
  }

  try {
    console.log(
      "========================================\n",
      `🚀 Starting Job: ${stateArg} ${sourceArg} (${daysArg} days lookback)`,
      "\n========================================"
    );
    await runScheduledJob(stateArg, sourceArg, {
      daysBack: daysArg
    });
    process.exit(0);
  } catch (err) {
    console.error(`💥 Fatal Job Error [${stateArg}-${sourceArg}]:`, err);
    await shutdownPool().catch(() => {});
    process.exit(1);
  }
}

void main();
