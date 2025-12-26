import { State } from "../db/types";
import { query } from "./client";
import { VideoRow, VideoSource, VideoStatus } from "./types";

export interface UpsertDiscoveredVideoInput {
  state: State; // 'MI'
  source: VideoSource; // 'house'
  externalId: string; // 'HAGRI-111325.mp4'
  slug: string; // 'mi-house-hagri-111325-2025-12-23'
  title: string;
  hearingDate: Date;
  videoPageUrl: string;
  originalVideoUrl: string;
}

export class VideoRepository {
  constructor(
    private maxRetries: number = 5,
    private stuckThresholdHours: number = 10
  ) {}

  /**
   * Retrieves a single video record by its unique identifier.
   * @param id The UUID of the video
   */
  async findById(id: string): Promise<VideoRow | null> {
    const result = await query<VideoRow>("SELECT * FROM videos WHERE id = $1", [
      id
    ]);
    return result.rows[0] || null;
  }

  /**
   * Performs an upsert operation for newly discovered videos.
   * Updates the slug, title, and URLs if the video already exists based on the
   * unique constraint of state, source, and external identifier.
   * @param input The data object containing video metadata and source identifiers
   */
  async upsertDiscoveredVideo(
    input: UpsertDiscoveredVideoInput
  ): Promise<void> {
    const {
      state,
      source,
      externalId,
      slug,
      title,
      hearingDate,
      videoPageUrl,
      originalVideoUrl
    } = input;

    await query(
      `
      INSERT INTO videos (
        state,
        source, 
        external_id, 
        slug,
        title, 
        hearing_date, 
        video_page_url,
        original_video_url,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
      ON CONFLICT (state, source, external_id) 
      DO UPDATE SET
        slug = EXCLUDED.slug,
        title = EXCLUDED.title,
        video_page_url = EXCLUDED.video_page_url,
        original_video_url = EXCLUDED.original_video_url,
        updated_at = NOW() 
      `,
      [
        state,
        source,
        externalId,
        slug,
        title,
        hearingDate.toISOString(),
        videoPageUrl,
        originalVideoUrl
      ]
    );
  }

  /**
   * Fetches a general list of videos ordered by their discovery date.
   * Primarily used for administrative views or bulk data exports.
   * @param limit The maximum number of records to return
   */
  async getAllVideos(limit: number): Promise<VideoRow[]> {
    const result = await query<VideoRow>(
      `
      SELECT *
      FROM videos
      ORDER BY created_at ASC
      LIMIT $1
      `,
      [limit]
    );

    return result.rows;
  }

  /**
   * Returns videos that are currently "in-flight" or queued, excluding
   * final states like 'completed' or 'permanent_failure'.
   * @param state The geographic state (e.g., 'MI')
   * @param source The branch of government (e.g., 'house')
   * @param [limit=1000] The maximum number of records to return (optional)
   * @param [maximumRetries] The retry ceiling (optional)
   * @returns An array of VideoRow objects
   */
  async findUnfinishedWorkByStateAndSource(
    state: State,
    source: VideoSource,
    limit: number = 1000,
    maximumRetries?: number
  ): Promise<VideoRow[]> {
    const maxRetries = maximumRetries ?? this.maxRetries;
    const { rows } = await query<VideoRow>(
      `
    SELECT * FROM videos 
    WHERE state = $1 
      AND source = $2
      AND status NOT IN ($3, $4)
      AND retry_count < $5
    ORDER BY hearing_date ASC
    LIMIT $6
    `,
      [
        state,
        source,
        VideoStatus.COMPLETED,
        VideoStatus.PERMANENT_FAILURE,
        maxRetries,
        limit
      ]
    );
    return rows;
  }

  /**
   * Transitions a video to a new status and optionally logs errors or increments retries.
   * This is the primary method for moving videos through the processing pipeline.
   * @param id The UUID of the video
   * @param status The new VideoStatus to apply
   * @param options Metadata updates including S3 keys, error messages, and retry increments
   */
  async updateStatus(
    id: string,
    status: VideoStatus,
    options?: {
      s3Key?: string | null;
      lastError?: string | null;
      incrementRetry?: boolean;
    }
  ): Promise<void> {
    const { s3Key, lastError, incrementRetry } = options ?? {};

    await query(
      `
      UPDATE videos
      SET
        status = $2,
        s3_key = COALESCE($3, s3_key),
        last_error = $4,
        retry_count = retry_count + $5,
        updated_at = now()
      WHERE id = $1
      `,
      [id, status, s3Key ?? null, lastError ?? null, incrementRetry ? 1 : 0]
    );
  }

  /**
   * Resets the retry counter for a specific video to zero.
   * Typically used when manual intervention has fixed an underlying issue.
   * @param id The UUID of the video
   */
  async resetRetryCount(id: string): Promise<void> {
    await query(
      `
    UPDATE videos
    SET
      retry_count = 0,
      updated_at = NOW()
    WHERE id = $1
    `,
      [id]
    );
    console.log(`[${id}] Retry count reset.`);
  }

  /**
   * Identifies and resets videos that have been stuck in a processing state
   * (e.g., 'downloading') for longer than the allowed time window.
   * @param state The geographic state (e.g., 'MI')
   * @param source The branch of government (e.g., 'house')
   * @param hoursThreshold Number of hours before a job is considered "stuck"
   * @returns The number of videos moved back to the 'failed' state
   */
  async resetStuckVideos(
    state: State,
    source: VideoSource,
    hoursThreshold?: number
  ): Promise<number> {
    const threshold = hoursThreshold ?? this.stuckThresholdHours;
    const { rows } = await query(
      `
    UPDATE videos
    SET
      status = $3,
      last_error = 'Auto-Reset: Job stuck in processing state',
      updated_at = NOW()
    WHERE state = $1
      AND source = $2
      AND status IN ($4, $5)
      AND updated_at < NOW() - ($6 * INTERVAL '1 hour')
    RETURNING id
    `,
      [
        state,
        source,
        VideoStatus.FAILED,
        VideoStatus.DOWNLOADING,
        VideoStatus.TRANSCRIBING,
        threshold
      ]
    );
    return rows.length;
  }

  /**
   * Retrieves detailed records of videos that have reached the maximum retry limit.
   * These videos remain in the 'failed' state but are excluded from active processing
   * until manual intervention occurs.
   * @param state The geographic state (e.g., 'MI')
   * @param source The branch of government (e.g., 'house')
   * @param [maximumRetries] The retry ceiling (optional)
   * @returns An array of VideoRow objects
   */
  async getAbandonedVideos(
    state: State,
    source: VideoSource,
    maximumRetries?: number
  ): Promise<VideoRow[]> {
    const maxRetries = maximumRetries ?? this.maxRetries;
    const { rows } = await query<VideoRow>(
      `
    SELECT id, title, slug, retry_count, last_error, hearing_date
    FROM videos
    WHERE state = $1 
      AND source = $2
      AND status = $3
      AND retry_count >= $4
    ORDER BY hearing_date DESC
    `,
      [state, source, VideoStatus.FAILED, maxRetries]
    );
    return rows;
  }

  /**
   * Formats and prints a scannable table of abandoned videos to the console.
   * Used for manual oversight to identify specific videos failing consistently.
   * @param state The geographic state (e.g., 'MI')
   * @param source The branch of government (e.g., 'house')
   */
  async printAbandonedReport(state: State, source: VideoSource): Promise<void> {
    const videos = await this.getAbandonedVideos(state, source);

    if (videos.length === 0) {
      return;
    }

    console.log(
      `\n⚠️  ${"=".repeat(
        15
      )} ABANDONED VIDEOS (${state} ${source}) ${"=".repeat(15)}`
    );

    console.table(
      videos.map((v) => ({
        ID: v.id.slice(0, 8),
        Date: v.hearing_date.split("T")[0],
        Retries: v.retry_count,
        Title: v.title.length > 50 ? v.title.substring(0, 47) + "..." : v.title,
        Error: v.last_error?.split("\n")[0] // Only show the first line of the error
      }))
    );

    console.log(`Total Abandoned: ${videos.length}\n`);
  }

  /**
   * Terminates further processing for a video by marking it as a permanent failure.
   * This removes the video from the retry queue and requires a manual reason.
   * @param videoId The UUID of the video
   * @param reason A descriptive reason for why the video cannot be processed
   */
  async markAsPermanentFailure(videoId: string, reason: string): Promise<void> {
    await query(
      `
      UPDATE videos 
      SET status = $2, 
          last_error = $3 
      WHERE id = $1
      `,
      [videoId, VideoStatus.PERMANENT_FAILURE, `Manual Review: ${reason}`]
    );
  }
}
