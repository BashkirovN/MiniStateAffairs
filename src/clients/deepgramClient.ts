import { createClient, SyncPrerecordedResponse } from "@deepgram/sdk";
import { DEEPGRAM_MODEL, loadConfig } from "../config/env";

const config = loadConfig();

export const deepgramClient = createClient(config.deepgramApiKey);

export interface TranscriptionResult {
  text: string;
  language: string;
  raw: SyncPrerecordedResponse;
}

/**
 * Requests an automated transcription from Deepgram for a publicly accessible media URL.
 * Uses the high-performance 'nova-3' model with smart formatting and punctuation enabled.
 * @param url - The remote address of the media file (e.g., a presigned S3 URL)
 * @returns A promise resolving to a structured result containing the transcript text, detected language, and full provider metadata
 * @throws Error if the Deepgram API returns a failure or if the transcription cannot be completed
 */
export async function transcribeFromUrl(
  url: string
): Promise<TranscriptionResult> {
  const { result, error } =
    await deepgramClient.listen.prerecorded.transcribeUrl(
      { url },
      {
        model: DEEPGRAM_MODEL,
        smart_format: true,
        punctuate: true,
        detect_language: true
      }
    );

  if (error) {
    throw new Error(`Deepgram error: ${JSON.stringify(error)}`);
  }

  const channel = result?.results?.channels?.[0];
  const alt = channel?.alternatives?.[0];

  const transcriptText = alt?.transcript ?? "";

  const detectedLang =
    typeof channel?.detected_language === "string"
      ? channel.detected_language
      : "en";

  return {
    text: transcriptText,
    language: detectedLang,
    raw: result as SyncPrerecordedResponse
  };
}
