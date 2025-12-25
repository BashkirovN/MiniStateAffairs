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
  async findById(id: string): Promise<VideoRow | null> {
    const result = await query<VideoRow>("SELECT * FROM videos WHERE id = $1", [
      id
    ]);
    return result.rows[0] || null;
  }

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

  async findProcessable(limit: number): Promise<VideoRow[]> {
    const result = await query<VideoRow>(
      `
      SELECT *
      FROM videos
      WHERE status IN ('pending', 'failed')
        AND retry_count < 5
      ORDER BY created_at ASC
      LIMIT $1
      `,

      [limit]
    );

    return result.rows;
  }

  async findUnfinishedWorkByStateAndSource(
    state: State,
    source: VideoSource,
    limit: number = 1000
  ): Promise<VideoRow[]> {
    const { rows } = await query<VideoRow>(
      `
    SELECT * FROM videos 
    WHERE state = $1 AND source = $2
    AND status != 'completed'
    AND (retry_count < 5)
    ORDER BY hearing_date ASC
    LIMIT $3
    `,
      [state, source, limit]
    );
    return rows;
  }

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
   * Resets a video's retry count and status so it can be re-processed.
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
   * Finds videos stuck in 'downloading' or 'transcribing' for longer than the threshold
   * and resets them to 'failed' so they can be picked up by the retry logic.
   * * @param hoursThreshold Number of hours before a job is considered "stuck"
   * @returns The number of videos reset
   */
  async resetStuckVideos(
    state: State,
    source: VideoSource,
    hoursThreshold: number
  ): Promise<number> {
    const result = await query(
      `
      UPDATE videos
      SET
        status = 'failed',
        last_error = 'Auto-Reset: Job stuck in processing state for too long',
        updated_at = NOW()
      WHERE
        state = $1
        AND source = $2
        AND status IN ('downloading', 'transcribing')
        AND updated_at < NOW() - ($3 * INTERVAL '1 hour')
      RETURNING id
      `,
      [state, source, hoursThreshold]
    );

    return result.rows.length;
  }
}
