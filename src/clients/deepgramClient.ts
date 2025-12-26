import * as fs from "node:fs";
import { createClient } from "@deepgram/sdk";
import { loadConfig } from "../config/env";

const config = loadConfig();

export const deepgramClient = createClient(config.deepgramApiKey);

export interface TranscriptionResult {
  text: string;
  language: string;
  raw: unknown;
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
        model: "nova-3",
        smart_format: true,
        punctuate: true
      }
    );

  if (error) {
    throw new Error(`Deepgram error: ${JSON.stringify(error)}`);
  }

  const channel = result?.results?.channels?.[0];
  const alt = channel?.alternatives?.[0];

  const transcriptText = alt?.transcript ?? "";
  const metadata = result?.metadata as any;

  return {
    text: transcriptText,
    language: metadata?.detected_language ?? "en",
    raw: result
  };
}

/**
 * Transcribes a local media file by reading its contents into a buffer and sending it to Deepgram.
 * Ideal for temporary local files or environments where media isn't yet staged in cloud storage.
 * @param path - The local filesystem path to the media file
 * @param mimetype - The standard MIME type of the file (e.g., 'audio/mp3', 'video/mp4')
 * @returns A promise resolving to the structured transcription result and raw metadata
 * @throws Error if the file cannot be read or if the Deepgram API encounters an error
 */
export async function transcribeFromFile(
  path: string,
  mimetype: string
): Promise<TranscriptionResult> {
  const buffer = await fs.promises.readFile(path);

  const { result, error } =
    await deepgramClient.listen.prerecorded.transcribeFile(buffer, {
      mimetype,
      model: "nova-3",
      smart_format: true,
      punctuate: true
    });

  if (error) {
    throw new Error(`Deepgram error: ${JSON.stringify(error)}`);
  }

  const channel = result?.results?.channels?.[0];
  const alt = channel?.alternatives?.[0];
  const metadata = result?.metadata as any;

  return {
    text: alt?.transcript ?? "",
    language: metadata?.detected_language ?? "en",
    raw: result
  };
}
