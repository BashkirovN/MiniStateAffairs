import { VideoRepository } from "../db/videoRepository";
import { TranscriptRepository } from "../db/transcriptRepository";
import { State, VideoRow, VideoSource, VideoStatus } from "../db/types";
import { TranscriptionService, TransProvider } from "./transcriptionService";
import { uploadVideoFromUrl } from "../clients/s3Client";

export enum ValidationReason {
  NOT_FOUND = "not_found",
  WRONG_STATUS = "wrong_status",
  ALREADY_COMPLETED = "already_completed",
  MAX_RETRIES_EXCEEDED = "max_retries_exceeded",
  INVALID_METADATA = "invalid_metadata"
}

export type ValidationResult =
  | { success: true; video: VideoRow }
  | {
      success: false;
      reason: ValidationReason;
      message?: string;
      video?: VideoRow;
    };

export class IngestService {
  constructor(
    private videoRepo: VideoRepository,
    private transcriptRepo: TranscriptRepository,
    private transcriptionService: TranscriptionService
  ) {}

  /**
   * Orchestrates the end-to-end processing lifecycle for a single video.
   * Coordinates the transition from raw external URL to S3 storage,
   * followed by automated transcription and database finalization.
   * @param videoId The UUID of the video to process through the pipeline
   */
  async processFullPipeline(videoId: string): Promise<void> {
    console.log(`\n--- Processing Pipeline for video_id: ${videoId} ---`);

    const s3Url = await this.uploadStep(videoId);

    if (s3Url) {
      await this.transcribeStep(videoId);
    }
    console.log(`--- Pipeline Finished: ${videoId} ---\n`);
  }

  /**
   * Executes the discovery phase by dynamically loading a state-and-source-specific scraper.
   * Fetches recent video metadata, cleans hearing dates, and performs an idempotent
   * upsert into the video repository.
   * New videos will have status PENDING.
   * @param state The geographic state to scrape (e.g., 'MI')
   * @param source The branch or source (e.g., 'house')
   * @param daysBack How many days into the past to look for "new" videos
   * @returns The total number of video records synchronized to the database
   * @throws Error if the scraper module or 'fetchRecent' function is missing
   */
  async discoverNewVideos(
    state: State,
    source: VideoSource,
    daysBack: number = 30
  ): Promise<number> {
    try {
      const module = await import(`../scrapers/${state}/${source}`);
      const videos = await module.fetchRecent({ daysBack });

      if (!videos || videos.length === 0) {
        console.log(`[Discovery] No new videos found for ${state} ${source}.`);
        return 0;
      }

      let count = 0;
      for (const v of videos) {
        const cleanDate =
          v.hearingDate instanceof Date
            ? v.hearingDate
            : new Date(v.hearingDate);

        await this.videoRepo.upsertDiscoveredVideo({
          state: v.state,
          source: v.source,
          externalId: v.externalId,
          slug: v.slug,
          title: v.title,
          hearingDate: cleanDate,
          videoPageUrl: v.videoPageUrl,
          originalVideoUrl: v.originalVideoUrl
        });
        count++;
      }

      return count;
    } catch (error: any) {
      if (
        error instanceof TypeError &&
        error.message.includes("fetchRecent is not a function")
      ) {
        throw new Error(
          `Scraper functions must consistently be named fetchRecent(). Please verify the scraper implementation.`
        );
      }
      console.error(
        `[Discovery] Failed for ${state} ${source}:`,
        error.message
      );
      throw error; // Re-throw so the worker reports the discovery failure
    }
  }

  /**
   * Retrieves the current list of prioritized work for a specific state and source.
   * Identifies videos that are either brand new or have previously failed and need retrying.
   * @param state The geographic state
   * @param source The branch of government
   */
  async getQueue(state: State, source: VideoSource): Promise<VideoRow[]> {
    return await this.videoRepo.findUnfinishedWorkByStateAndSource(
      state,
      source
    );
  }

  /**
   * Manages the stream upload of media from government servers to AWS S3.
   * Validates video status before execution and handles "fast-tracking" if the file
   * is already present in S3 storage.
   * Transitions: PENDING/FAILED -> DOWNLOADING -> DOWNLOADED
   * @param videoId The UUID of the video to upload
   * @returns The S3 key/URL upon success, or null if skipped/failed
   */
  async uploadStep(videoId: string): Promise<string | null> {
    const result = await this.validateStep(videoId, [
      VideoStatus.PENDING,
      VideoStatus.FAILED
    ]);

    if (!result.success) {
      if (result.reason === ValidationReason.WRONG_STATUS) {
        // Check if it's already past this step
        const v = (result as any).video;
        if (
          v &&
          (v.status === VideoStatus.DOWNLOADED ||
            v.status === VideoStatus.TRANSCRIBING ||
            v.status === VideoStatus.COMPLETED)
        ) {
          console.log(`[${videoId}] Skip Upload: Video is already in S3.`);
          return v.s3_key;
        }
      }
      console.log(
        `[${videoId}] Skipping Upload: ${result.message || result.reason}`
      );
      return null;
    }

    const { video } = result;

    try {
      //  Lock status
      await this.videoRepo.updateStatus(videoId, VideoStatus.DOWNLOADING);

      console.log(`Uploading to S3...`);
      const s3Url = await uploadVideoFromUrl({
        state: video.state as State,
        source: video.source as VideoSource,
        slug: video.slug,
        originalVideoUrl: video.original_video_url || video.video_page_url, // Fallback if url is missing
        hearingDate: new Date(video.hearing_date)
      });

      await this.videoRepo.updateStatus(videoId, VideoStatus.DOWNLOADED, {
        s3Key: s3Url
      });
      console.log(`[${videoId}] Uploading to S3 completed`);
      return s3Url;
    } catch (error) {
      await this.handleFailure(videoId, "Upload failed", error);
      return null;
    }
  }

  /**
   * Coordinates the transcription of a video file stored in S3.
   * Interfaces with external providers, saves transcript text to the database.
   * Transitions: DOWNLOADED -> TRANSCRIBING -> COMPLETED
   * @param videoId The UUID of the video to transcribe
   */
  async transcribeStep(videoId: string): Promise<void> {
    const result = await this.validateStep(videoId, [VideoStatus.DOWNLOADED]);

    if (!result.success) {
      console.log(
        `[${videoId}] Skipping Transcribe: ${result.message || result.reason}`
      );
      return;
    }

    const { video } = result;

    if (!video.s3_key) {
      await this.handleFailure(
        videoId,
        "Pre-flight Check",
        new Error("Status is DOWNLOADED but s3_key is missing.")
      );
      return;
    }

    try {
      //  Lock status
      await this.videoRepo.updateStatus(videoId, VideoStatus.TRANSCRIBING);
      console.log(`[${videoId}] Sending to Transcription Service...`);

      //  Transcribe
      const transcriptResult =
        await this.transcriptionService.transcribeVideoFromUrl(
          videoId,
          video.s3_key
        );

      //  Save Transcript
      await this.transcriptRepo.createTranscript({
        videoId,
        provider: TransProvider.DEEPGRAM,
        language: "en",
        text: transcriptResult.text,
        rawJson: transcriptResult.raw
      });

      //  Complete
      await this.videoRepo.updateStatus(videoId, VideoStatus.COMPLETED, {
        lastError: null
      });
      console.log(`[${videoId}] Transcribed successfully.`);
    } catch (error) {
      await this.handleFailure(videoId, "Transcription Step", error);
    }
  }

  /**
   * Maintenance method to reset videos that have timed out during processing.
   * Moves 'stuck' jobs back to the FAILED state so they can be picked up by the next worker.
   * @param state The geographic state
   * @param source The branch of government
   * @param hoursThreshold The duration after which a job is considered abandoned
   */
  async recoverStuckJobs(
    state: State,
    source: VideoSource,
    hoursThreshold: number = 6
  ): Promise<number> {
    const count = await this.videoRepo.resetStuckVideos(
      state,
      source,
      hoursThreshold
    );
    return count;
  }

  // --- HELPERS ---

  /**
   * Internal helper to verify if a video is eligible for a specific processing step.
   * Checks for existence, previous completion, max retry limits, and status alignment.
   * @param videoId The UUID of the video
   * @param allowedStatuses An array of statuses valid for the requested action
   */
  private async validateStep(
    videoId: string,
    allowedStatuses: VideoStatus[]
  ): Promise<ValidationResult> {
    const video = await this.videoRepo.findById(videoId);

    if (!video) {
      return {
        success: false,
        reason: ValidationReason.NOT_FOUND,
        message: "Video ID not found in DB"
      };
    }

    // If already done, we stop nicely
    if (video.status === VideoStatus.COMPLETED) {
      return {
        success: false,
        reason: ValidationReason.ALREADY_COMPLETED,
        message: "Video already COMPLETED"
      };
    }

    // Hard Stop on Retries
    if (video.retry_count >= 5) {
      return {
        success: false,
        reason: ValidationReason.MAX_RETRIES_EXCEEDED,
        message: `Max retries (5) hit. Last error: ${video.last_error}`
      };
    }

    // Status Check
    if (!allowedStatuses.includes(video.status as VideoStatus)) {
      return {
        success: false,
        reason: ValidationReason.WRONG_STATUS,
        message: `Current status '${
          video.status
        }' is not in allowed list [${allowedStatuses.join(",")}]`,
        video // Return video so caller can check if it's "ahead" of the step
      };
    }

    return { success: true, video };
  }

  /**
   * Standardized error handler for the ingestion pipeline.
   * Logs contextual error messages, increments retry counts, and re-throws a rich error.
   * @param videoId The UUID of the video that failed
   * @param context A string describing which step of the pipeline failed
   * @param error The original error or exception caught
   */
  private async handleFailure(videoId: string, context: string, error: any) {
    const originalMessage =
      error instanceof Error ? error.message : JSON.stringify(error);

    const fullContext = originalMessage.includes(context)
      ? originalMessage
      : `[${context}] ${originalMessage}`;

    // If it's a "Fatal" error (like 404), maybe we don't want to increment retry?
    // For now, we assume all errors are worth a retry up to the limit.
    await this.videoRepo.updateStatus(videoId, VideoStatus.FAILED, {
      lastError: fullContext,
      incrementRetry: true
    });

    // Re-throw with the context attached
    // This ensures that even without a log here, the caller knows EXACTLY what happened.
    const richError = new Error(fullContext);
    (richError as any).originalError = error;
    throw richError;
  }
}
