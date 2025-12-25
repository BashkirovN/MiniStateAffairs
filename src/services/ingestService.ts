import { VideoRepository } from "../db/videoRepository";
import { TranscriptRepository } from "../db/transcriptRepository";
import { State, VideoRow, VideoSource, VideoStatus } from "../db/types";
import { TranscriptionService, TransProvider } from "./transcriptionService";
import { fetchHouseRecent } from "../scrapers/MI/house";
import { fetchSenateRecent } from "../scrapers/MI/senate";
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
   * MAIN ENTRY POINT
   * Orchestrates the entire lifecycle for a single video.
   * Can be called individually or via a loop/queue.
   */
  async processFullPipeline(videoId: string) {
    console.log(`\n--- Processing Pipeline for video_id: ${videoId} ---`);

    // Step 1: Upload (or check if already uploaded)
    const s3Url = await this.uploadStep(videoId);

    // Step 2: Transcribe (only if step 1 succeeded or was already done)
    if (s3Url) {
      await this.transcribeStep(videoId);
    }
    console.log(`--- Pipeline Finished: ${videoId} ---\n`);
  }

  /**
   * DISCOVERY PHASE
   * Fetches metadata from external sources.
   * Designed to be idempotent (safe to run multiple times).
   */

  // TODO: Make it work for other states
  async discoverNewVideos(daysBack: number = 30): Promise<number> {
    console.log("Starting discovery...");
    const [houseVideos, senateVideos] = await Promise.allSettled([
      fetchHouseRecent({ daysBack }),
      fetchSenateRecent({ daysBack })
    ]);

    const videos = [
      ...(houseVideos.status === "fulfilled" ? houseVideos.value : []),
      ...(senateVideos.status === "fulfilled" ? senateVideos.value : [])
    ];

    if (houseVideos.status === "rejected")
      console.error("House scraper failed:", houseVideos.reason);
    if (senateVideos.status === "rejected")
      console.error("Senate scraper failed:", senateVideos.reason);

    console.log(`Found ${videos.length} videos total. Syncing to DB...`);

    let count = 0;
    for (const v of videos) {
      // Ensure hearingDate is a Date object before passing to DB
      const cleanDate =
        v.hearingDate instanceof Date ? v.hearingDate : new Date(v.hearingDate);

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
  }

  /**
   * STEP 1: DOWNLOAD & UPLOAD
   * Moves video from external Gov site -> AWS S3
   * Transitions: PENDING/FAILED -> DOWNLOADING -> DOWNLOADED
   */
  async uploadStep(videoId: string): Promise<string | null> {
    // 1. Validate
    const result = await this.validateStep(videoId, [
      VideoStatus.PENDING,
      VideoStatus.FAILED
    ]);

    // 1b. Fast-track if already done
    if (!result.success) {
      if (result.reason === ValidationReason.WRONG_STATUS) {
        // Check if it's already past this step
        const v = (result as any).video; // Hacky cast, but we know it exists if reason is WRONG_STATUS
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
      //Move to DOWNLOADING to prevent other workers from picking it up
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
      console.error(`[${video.slug}] Upload step failed:`, error);
      await this.handleFailure(videoId, "Upload failed", error);
      return null;
    }
  }

  /**
   * STEP 2: TRANSCRIBE
   * Sends S3 file -> Transcription Provider (Deepgram/Whisper)
   * Transitions: DOWNLOADED -> TRANSCRIBING -> COMPLETED
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

    // Safety Check: We cannot transcribe without a file
    if (!video.s3_key) {
      await this.handleFailure(
        videoId,
        "Pre-flight Check",
        new Error("Status is DOWNLOADED but s3_key is missing.")
      );
      return;
    }

    try {
      // 1. Lock status
      await this.videoRepo.updateStatus(videoId, VideoStatus.TRANSCRIBING);
      console.log(`[${videoId}] Sending to Transcription Service...`);

      // 2. Transcribe
      const transcriptResult =
        await this.transcriptionService.transcribeVideoFromUrl(
          videoId,
          video.s3_key
        );

      // 3. Save Transcript
      await this.transcriptRepo.createTranscript({
        videoId,
        provider: TransProvider.DEEPGRAM,
        language: "en",
        text: transcriptResult.text,
        rawJson: transcriptResult.raw
      });

      // 4. Complete
      await this.videoRepo.updateStatus(videoId, VideoStatus.COMPLETED, {
        lastError: null
      });
      console.log(`[${videoId}] Pipeline COMPLETED Successfully.`);
    } catch (error) {
      await this.handleFailure(videoId, "Transcription Step", error);
    }
  }

  /**
   * RECOVERY: Maintenance Mode
   * Resets videos that have been "stuck" in a processing state for too long.
   * Could be run once at the start of the job.
   */
  async recoverStuckJobs(hoursThreshold: number = 6) {
    console.log("Running maintenance check for stuck jobs...");
    const count = await this.videoRepo.resetStuckVideos(hoursThreshold);
    if (count > 0) {
      console.warn(
        `[Maintenance] Reset ${count} stuck videos to FAILED state.`
      );
    }
  }

  // --- HELPERS ---

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

  private async handleFailure(videoId: string, context: string, error: any) {
    const errorMessage =
      error instanceof Error ? error.message : JSON.stringify(error);
    console.error(`[${videoId}] FAILED at ${context}: ${errorMessage}`);

    // If it's a "Fatal" error (like 404), maybe we don't want to increment retry?
    // For now, we assume all errors are worth a retry up to the limit.
    await this.videoRepo.updateStatus(videoId, VideoStatus.FAILED, {
      lastError: `${context}: ${errorMessage}`,
      incrementRetry: true
    });
  }
}
