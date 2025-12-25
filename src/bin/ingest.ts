#!/usr/bin/env node

import { State } from "../db/types";
import { VideoRepository } from "../db/videoRepository";
import { runIngestJob } from "../jobs/ingestJob";
import { runScheduledJob } from "../jobs/orchestrator";

async function main() {
  try {
    //await runIngestJob();
    // Reset counts
    // const videoRepo = new VideoRepository();
    // const videos = await videoRepo.getAllVideos(10);
    // for (const vid of videos) {
    //   await videoRepo.resetRetryCount(vid.id);
    // }

    await runScheduledJob(State.MI, 7);
    process.exit(0);
  } catch (err) {
    console.error("Ingest job failed:", err);
    process.exit(1);
  }
}

void main();
