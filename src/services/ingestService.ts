import { VideoRepository } from "../db/videoRepository";
import { TranscriptRepository } from "../db/transcriptRepository";
import { State, VideoRow, VideoSource, VideoStatus } from "../db/types";
import { TranscriptionService, TransProvider } from "./transcriptionService";
import { uploadVideoFromUrl } from "../clients/s3Client";
import { fetchWithRetry, isRetryable } from "../utils/http";

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
      await this.transcribeStep(videoId, s3Url);
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
    daysBack: number = 1
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
    } catch (error: unknown) {
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
        error instanceof Error ? error.message : String(error)
      );
      throw error; // Re-throw so the worker reports the discovery failure
    }
  }

  /**
   * Retrieves the current list of prioritized work for a specific state and source.
   * Identifies videos that are either brand new or have previously failed and need retrying.
   * @param state The geographic state
   * @param source The branch of government
   * @returns An array of VideoRow objects
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
    try {
      //  ATOMIC CLAIM (The "Validation" and "Locking" happens here in one go)
      const vidClaimedForDownloading = await this.videoRepo.updateStatus(
        videoId,
        VideoStatus.DOWNLOADING,
        {
          allowedStatuses: [VideoStatus.PENDING, VideoStatus.FAILED]
        }
      );

      // ========== CHECKS BEGIN ==========
      if (!vidClaimedForDownloading) {
        const video = await this.videoRepo.findById(videoId);

        if (!video) {
          console.error(
            `[${videoId}] DATA INTEGRITY ERROR: Video ID in queue but not in DB.`
          );
          return null;
        }
        if (
          video.s3_key &&
          [
            VideoStatus.DOWNLOADED,
            VideoStatus.TRANSCRIBING,
            VideoStatus.COMPLETED
          ].includes(video.status)
        ) {
          console.log(
            `[${videoId}] Couldn't claim for DOWNLOADING - Video already exists in S3.`
          );
          return video.s3_key;
        }

        console.log(
          `[${videoId}] Couldn't claim for DOWNLOADING - Already processing or max retries hit.`
        );
        return null;
      }

      const {
        state,
        source,
        slug,
        original_video_url,
        video_page_url,
        hearing_date
      } = vidClaimedForDownloading;

      // Pre-flight Check: Verify the URL is reachable
      try {
        const headRes = await fetchWithRetry(
          original_video_url || video_page_url,
          { method: "HEAD" }
        );
        if (!headRes.ok) {
          throw new Error(
            `Pre-flight check failed: Source returned ${headRes.status}`
          );
        }
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        throw new Error(
          `[${slug}] Pre-flight check failed. URL unreachable: ${message}`
        );
      }
      // ========== CHECKS END ==========

      console.log(`[${videoId}] Uploading to S3...`);

      const s3Url = await uploadVideoFromUrl({
        state: state as State,
        source: source as VideoSource,
        slug: slug,
        originalVideoUrl: original_video_url || video_page_url, // Fallback if url is missing
        hearingDate: new Date(hearing_date)
      });

      const vidClaimedToFinishDownloading = await this.videoRepo.updateStatus(
        videoId,
        VideoStatus.DOWNLOADED,
        {
          s3Key: s3Url,
          allowedStatuses: [VideoStatus.DOWNLOADING]
        }
      );

      if (!vidClaimedToFinishDownloading) {
        console.log(
          `[${videoId}] Could not transition to DOWNLOADED due to an unexpected status change.`
        );
        return null;
      }

      console.log(`[${videoId}] Uploading to S3 completed`);
      return s3Url;
    } catch (error) {
      await this.handleFailure(videoId, "Upload failed", error);
      // handleFailure will re-throw the error, the next line is unreachable
      return null;
    }
  }

  /**
   * Coordinates the transcription of a video file stored in S3.
   * Interfaces with external providers, saves transcript text to the database.
   * Transitions: DOWNLOADED -> TRANSCRIBING -> COMPLETED
   * @param videoId The UUID of the video to transcribe
   * @param presignedS3Url The S3 key/URL of the video
   */
  async transcribeStep(videoId: string, presignedS3Url: string): Promise<void> {
    try {
      //  Lock status. This atomic call handles the ID check, Status check, and Max Retries check.
      const vidClaimedForTranscribing = await this.videoRepo.updateStatus(
        videoId,
        VideoStatus.TRANSCRIBING,
        {
          allowedStatuses: [VideoStatus.DOWNLOADED, VideoStatus.FAILED],
          lastError: null
        }
      );

      // ========== CHECKS BEGIN ==========
      if (!presignedS3Url) {
        await this.handleFailure(
          videoId,
          "Pre-flight Check",
          new Error("Valid presigned S3 URL is required.")
        );
        return;
      }

      if (!vidClaimedForTranscribing) {
        const currentVideo = await this.videoRepo.findById(videoId);

        if (!currentVideo) {
          console.error(`[${videoId}] ERROR: Video not found.`);
          return;
        }

        if (currentVideo.status === VideoStatus.COMPLETED) {
          console.log(`[${videoId}] Skip Transcribing: Already COMPLETED.`);
          return;
        }

        console.log(
          `[${videoId}] Could not transition to TRANSCRIBING due to an unexpected status change or max retries.`
        );
        return;
      }

      const existingTranscript =
        await this.transcriptRepo.getTranscriptByVideoId(videoId);
      const hasContent = (existingTranscript?.text?.trim().length ?? 0) > 0;
      if (hasContent) {
        console.log(
          `[${videoId}] Skipping Transcribe: Transcript already exists.`
        );
        const vidClaimedToComplete = await this.videoRepo.updateStatus(
          videoId,
          VideoStatus.COMPLETED,
          {
            lastError: null
          }
        );

        if (!vidClaimedToComplete) {
          console.log(
            `[${videoId}] Could not transition to COMPLETED due to an unexpected status change.`
          );
        }
        return;
      }
      // ========== CHECKS END ==========

      console.log(`[${videoId}] Sending to Transcription Service...`);

      //  Transcribe
      const transcriptResult =
        await this.transcriptionService.transcribeVideoFromUrl(presignedS3Url);

      //  Save Transcript
      await this.transcriptRepo.createTranscript({
        videoId,
        provider: TransProvider.DEEPGRAM,
        language: transcriptResult.language,
        text: transcriptResult.text,
        rawJson: transcriptResult.raw
      });

      //  Complete
      const vidClaimedToFinishTranscribing = await this.videoRepo.updateStatus(
        videoId,
        VideoStatus.COMPLETED,
        {
          allowedStatuses: [VideoStatus.TRANSCRIBING],
          lastError: null
        }
      );

      if (!vidClaimedToFinishTranscribing) {
        // This usually means the video was already COMPLETED by a duplicate run,
        // or it was manually ABANDONED while the worker was busy.
        console.warn(
          `[${videoId}] Could not transition to COMPLETED due to an unexpected status change.`
        );
        return;
      }
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
   * Standardized error handler for the ingestion pipeline.
   * Logs contextual error messages, increments retry counts, and re-throws a rich error.
   * @param videoId The UUID of the video that failed
   * @param context A string describing which step of the pipeline failed
   * @param error The original error or exception caught
   */
  private async handleFailure(
    videoId: string,
    context: string,
    error: unknown
  ) {
    const originalMessage =
      error instanceof Error ? error.message : JSON.stringify(error);

    const fullContext = originalMessage.includes(context)
      ? originalMessage
      : `[${context}] ${originalMessage}`;

    const shouldRetry = isRetryable(error);
    const newStatus = shouldRetry
      ? VideoStatus.FAILED
      : VideoStatus.PERMANENT_FAILURE;

    // Fatal errors will get status PERMANENT_FAILURE and retried anymore. All others will get FAILED.
    console.error(`[${videoId}] ${fullContext}`);
    await this.videoRepo.updateStatus(videoId, newStatus, {
      lastError: fullContext,
      incrementRetry: shouldRetry,
      // Allow failing from anywhere EXCEPT when it's already done
      allowedStatuses: [
        VideoStatus.PENDING,
        VideoStatus.DOWNLOADING,
        VideoStatus.DOWNLOADED,
        VideoStatus.TRANSCRIBING
      ]
    });

    // Re-throw with the context attached
    // This ensures that even without a log here, the caller knows EXACTLY what happened.
    const richError = new Error(fullContext, {
      cause: error
    });
    throw richError;
  }
}
