import { IngestService } from "../services/ingestService";
import { JobReporter } from "../services/jobReporter";
import { VideoRepository } from "../db/videoRepository";
import { TranscriptRepository } from "../db/transcriptRepository";
import { TranscriptionService } from "../services/transcriptionService";
import { State } from "../db/types";

/**
 * THE CRON FUNCTION
 * Usage: await runScheduledJob('MI');
 */
export async function runScheduledJob(state: State, daysBack: number = 30) {
  const reporter = new JobReporter(state);

  // 1. Dependency Injection (Manual or via container)
  const videoRepo = new VideoRepository();
  const transcriptRepo = new TranscriptRepository();
  const transcriptionService = new TranscriptionService();
  const ingestService = new IngestService(
    videoRepo,
    transcriptRepo,
    transcriptionService
  );

  try {
    await reporter.startRun(`worker-${process.pid}`);

    // 1. Maintenance: Reset "Zombie" processing jobs
    await ingestService.recoverStuckJobs(10);

    // 2. Discovery: Returns the count of videos found
    await reporter.log("INFO", "Starting Discovery Phase...");
    const countFound = await ingestService.discoverNewVideos(daysBack);
    await reporter.incrementDiscovered(countFound);
    await reporter.log("INFO", `Discovered ${countFound} videos.`);

    // 3. Queue Management: Get list of videos needing action
    const pendingVideos = await videoRepo.findUnfinishedWorkByState(state);
    await reporter.log(
      "INFO",
      `Found ${pendingVideos.length} videos requiring processing.`
    );

    // 4. Processing Loop
    for (const video of pendingVideos) {
      try {
        await ingestService.processFullPipeline(video.id);
        await reporter.incrementProcessed();
      } catch (err: any) {
        await reporter.incrementFailed();
        await reporter.log(
          "ERROR",
          `--- Pipeline Failed ---:\n ${video.id}: ${err.message} `
        );
        // We do NOT throw here; we want to continue processing other videos
      }
    }

    await reporter.finishRun();
  } catch (criticalError: any) {
    // This catches issues like DB connection loss or scraper code crashes
    await reporter.log(
      "ERROR",
      `CRITICAL JOB FAILURE: ${criticalError.message}`
    );
    await reporter.finishRun(criticalError);
    throw criticalError; // Re-throw so the system logs it as a process crash
  }
}
