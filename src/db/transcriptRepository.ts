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

  async getByVideoId(videoId: string): Promise<TranscriptRow | null> {
    const result = await query<TranscriptRow>(
      `SELECT * FROM transcripts WHERE video_id = $1`,
      [videoId]
    );

    return result.rows[0] ?? null;
  }
}
