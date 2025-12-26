#!/usr/bin/env node

import { shutdownPool } from "../db/client";
import { State, VideoSource } from "../db/types";
import { runScheduledJob } from "../jobs/orchestrator";

async function main() {
  const args = process.argv.slice(2);
  const stateArg = args
    .find((a) => a.startsWith("--state="))
    ?.split("=")[1] as State;
  const sourceArg = args
    .find((a) => a.startsWith("--source="))
    ?.split("=")[1] as VideoSource;
  const daysArg = parseInt(
    args.find((a) => a.startsWith("--days="))?.split("=")[1] || "7"
  );

  if (!stateArg || !sourceArg) {
    console.error(
      "Usage: node ingest.js --state=MI --source=senate [--days=7]"
    );
    process.exit(1);
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
