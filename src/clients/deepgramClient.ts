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
 * Transcribe a remote audio/video URL with Deepgram.
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
