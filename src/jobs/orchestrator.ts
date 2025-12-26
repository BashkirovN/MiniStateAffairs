import { IngestService } from "../services/ingestService";
import { JobReporter, LogLevel } from "../services/jobReporter";
import { VideoRepository } from "../db/videoRepository";
import { TranscriptRepository } from "../db/transcriptRepository";
import { TranscriptionService } from "../services/transcriptionService";
import { State, VideoSource } from "../db/types";

/**
 * The primary orchestration entry point for a scheduled ingestion job.
 * Manages the full execution lifecycle for a specific state and source:
 * 1. Initializes the JobReporter for monitoring and logging.
 * 2. Runs maintenance to recover "zombie" jobs stuck in processing states.
 * 3. Triggers the discovery phase to sync new videos from government archives.
 * 4. Iterates through the processing queue, handling individual pipeline failures gracefully.
 * 5. Finalizes the run with success/failure metrics.
 * * @param state - The geographic state to process (e.g., 'MI')
 * @param source - The branch or legislative source (e.g., 'senate')
 * @param daysBack - The lookback window for the discovery phase (defaults to 30)
 * @throws A critical error if the infrastructure (DB, Scraper loading) fails
 */
export async function runScheduledJob(
  state: State,
  source: VideoSource,
  daysBack: number = 30
) {
  const reporter = new JobReporter(state, source);

  // Dependency Injection
  const ingestService = new IngestService(
    new VideoRepository(),
    new TranscriptRepository(),
    new TranscriptionService()
  );

  try {
    await reporter.startRun(`worker-${process.pid}`);

    // Maintenance: Reset "Zombie" processing jobs
    await reporter.log(
      LogLevel.INFO,
      "Running maintenance check for stuck videos..."
    );
    const recoveredCount = await ingestService.recoverStuckJobs(
      state,
      source,
      10
    );
    if (recoveredCount > 0) {
      await reporter.log(
        LogLevel.WARN,
        `Reset ${recoveredCount} stuck videos to FAILED state.`
      );
    } else {
      await reporter.log(LogLevel.INFO, `No stuck videos found.`);
    }

    // Discovery: Returns the count of videos found
    await reporter.log(LogLevel.INFO, "Starting Discovery Phase...");
    const countFound = await ingestService.discoverNewVideos(
      state,
      source,
      daysBack
    );
    await reporter.incrementDiscovered(countFound);
    await reporter.log(LogLevel.INFO, `Discovered ${countFound} videos.`);

    // Queue Management: Get list of videos needing action
    const pendingVideos = await ingestService.getQueue(state, source);
    await reporter.log(
      LogLevel.INFO,
      `Found ${pendingVideos.length} videos requiring processing.`
    );

    // Processing Loop
    for (const video of pendingVideos) {
      try {
        await ingestService.processFullPipeline(video.id);
        await reporter.incrementProcessed();
      } catch (err: any) {
        await reporter.incrementFailed();
        await reporter.log(
          LogLevel.ERROR,
          `--- Pipeline Failed ---:\n ${video.id}: ${err.message} `
        );
        // We do NOT throw here; we want to continue processing other videos
      }
    }

    await reporter.finishRun();
  } catch (criticalError: any) {
    // This catches issues like DB connection loss or scraper code crashes
    await reporter.log(
      LogLevel.ERROR,
      `CRITICAL JOB FAILURE: ${criticalError.message}`
    );
    await reporter.finishRun(criticalError);
    throw criticalError; // Re-throw so the system logs it as a process crash
  }
}
