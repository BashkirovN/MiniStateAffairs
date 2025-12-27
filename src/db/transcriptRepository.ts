import { query } from "./client";
import { TranscriptRow } from "./types";

export interface CreateTranscriptInput {
  videoId: string;
  provider: string;
  language: string;
  text: string;
  rawJson?: unknown;
}

export class TranscriptRepository {
  /**
   * Persists a video transcript to the database.
   * If a transcript already exists for the given video ID, it updates the existing record
   * with the latest provider data and generated text.
   * @param input The data object containing the transcript text, provider, and raw vendor data
   */
  async createTranscript(input: CreateTranscriptInput): Promise<void> {
    const { videoId, provider, language, text, rawJson } = input;

    await query(
      `
      INSERT INTO transcripts (video_id, provider, language, text, raw_json)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (video_id) DO UPDATE
      SET
        provider = EXCLUDED.provider,
        language = EXCLUDED.language,
        text = EXCLUDED.text,
        raw_json = EXCLUDED.raw_json,
        created_at = now()
      `,
      [videoId, provider, language, text, JSON.stringify(rawJson)]
    );
  }

  /**
   * Retrieves the transcript associated with a specific video.
   * This may be used to fetch text for search indexing.
   * @param videoId The UUID of the video
   */
  async getTranscriptByVideoId(videoId: string): Promise<TranscriptRow | null> {
    const result = await query<TranscriptRow>(
      `SELECT * FROM transcripts WHERE video_id = $1`,
      [videoId]
    );

    return result.rows[0] ?? null;
  }
}
